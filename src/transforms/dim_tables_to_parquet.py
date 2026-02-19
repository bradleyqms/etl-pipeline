"""
transforms/dim_tables_to_parquet.py — Bronze CSV → Silver Parquet (orchestrator)

All dimension tables arrive in one email (subject: dim_tables) from SAP dispatcher.
This transform routes each CSV to the correct sub-transform based on filename:

  dim_customer_*_extract.csv  → dim_customer_to_parquet  → silver/dim_customer/
  dim_product_master.csv      → dim_product_to_parquet   → silver/dim_product/
  dim_salesperson.csv         → dim_salesperson_to_parquet → silver/dim_salesperson/

Bronze: bronze/dim_tables/{date}/
Silver: silver/dim_customer/, silver/dim_product/, silver/dim_salesperson/
"""

import io
import logging
from datetime import datetime, timezone
from pathlib import PurePosixPath

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.blob_client import get_container_client
from . import dim_customer_to_parquet as _cust
from . import dim_product_to_parquet as _prod
from . import dim_salesperson_to_parquet as _slp
from . import enrich_dim_customer as _enrich_cust
from . import enrich_dim_product as _enrich_prod

log = logging.getLogger(__name__)

BRONZE_PREFIX = "dim_tables"
RECOMMENDED_CODEC = "zstd"


# ─────────────────────────────────────────────
# File routing — match by filename pattern
# ─────────────────────────────────────────────

def _route(blob_name: str) -> str | None:
    """Return which sub-transform handles this file, or None to skip."""
    name = PurePosixPath(blob_name).name.lower()
    if "dim_customer" in name:
        return "customer"
    if "dim_product" in name or "product_master" in name:
        return "product"
    if "dim_salesperson" in name or "salesperson" in name:
        return "salesperson"
    return None


def _write_parquet(container_client, df: pd.DataFrame, blob_path: str) -> int:
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=RECOMMENDED_CODEC)
    buf.seek(0)
    data = buf.getvalue()
    container_client.upload_blob(name=blob_path, data=data, overwrite=True)
    return len(data)


def _fmt_size(nbytes: int) -> str:
    if nbytes >= 1_048_576:
        return f"{nbytes / 1_048_576:.1f} MB"
    return f"{nbytes / 1024:.1f} KB"


# ─────────────────────────────────────────────
# Main transform
# ─────────────────────────────────────────────

def transform(date: str | None = None, dry_run: bool = False) -> dict:
    """Route bronze/dim_tables CSVs to their respective silver sub-transforms.

    Each CSV is processed independently:
      - dim_customer_*   → dim_customer sub-transform → enrich_dim_customer
      - dim_product_*    → dim_product sub-transform  → enrich_dim_product
      - dim_salesperson  → dim_salesperson sub-transform

    A combined latest.parquet is written per dim type, then the enrichment
    transform runs automatically to produce latest_enriched.parquet.
    """
    container = get_container_client()
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Discover all CSVs under bronze/dim_tables/
    all_blobs = list(container.list_blobs(name_starts_with=f"{BRONZE_PREFIX}/"))
    csv_blobs = [b for b in all_blobs if b.name.endswith(".csv")]

    if not csv_blobs:
        return {"status": "no_data", "message": f"No CSV files in bronze/{BRONZE_PREFIX}/"}

    # Group by date folder
    dates: dict[str, list] = {}
    for b in csv_blobs:
        parts = b.name.split("/")
        if len(parts) >= 3:
            dates.setdefault(parts[1], []).append(b)

    sorted_dates = sorted(dates.keys(), reverse=True)
    target_date = date or sorted_dates[0]

    if target_date not in dates:
        return {"status": "error", "message": f"Date '{target_date}' not found",
                "available": sorted_dates}

    blobs = dates[target_date]
    log.info("dim_tables transform — %d CSVs for %s", len(blobs), target_date)

    # Accumulators per dim type
    customer_frames: list[pd.DataFrame] = []
    product_frames: list[pd.DataFrame] = []
    salesperson_frames: list[pd.DataFrame] = []

    results = []
    skipped = []

    for blob_info in blobs:
        blob_name = blob_info.name
        file_name = PurePosixPath(blob_name).name
        route = _route(blob_name)

        if route is None:
            skipped.append(file_name)
            log.debug("dim_tables: skipping unrouted file %s", file_name)
            continue

        # Download
        blob_data = container.download_blob(blob_name).readall()

        if route == "customer":
            df = _cust.read_bronze_csv(container, blob_name)
            if df is not None:
                df = _cust.clean_dataframe(df, blob_name)
                customer_frames.append(df)
                results.append({"file": file_name, "route": "customer", "rows": len(df)})
                log.info("  dim_customer ← %s (%d rows)", file_name, len(df))

        elif route == "product":
            df = _prod.read_bronze_csv(container, blob_name)
            if df is not None:
                df = _prod.clean_dataframe(df, blob_name)
                product_frames.append(df)
                results.append({"file": file_name, "route": "product", "rows": len(df)})
                log.info("  dim_product  ← %s (%d rows)", file_name, len(df))

        elif route == "salesperson":
            df = _slp.parse_csv(blob_data, blob_name)
            if df is not None:
                salesperson_frames.append(df)
                results.append({"file": file_name, "route": "salesperson", "rows": len(df)})
                log.info("  dim_slp      ← %s (%d rows)", file_name, len(df))

    if dry_run:
        return {"status": "dry_run", "would_process": results, "skipped": skipped}

    silver_written = []

    # ── Write dim_customer silver ──
    if customer_frames:
        combined = pd.concat(customer_frames, ignore_index=True)
        combined.drop_duplicates(subset=["entity", "card_code"], keep="last", inplace=True)

        # Per-file parquets
        for df in customer_frames:
            src = df["_source_file"].iloc[0] if "_source_file" in df.columns else "unknown"
            stem = PurePosixPath(src).stem
            blob_path = f"silver/dim_customer/{target_date}/{stem}.parquet"
            size = _write_parquet(container, df, blob_path)
            log.info("    -> %s (%s, %d rows)", blob_path, _fmt_size(size), len(df))

        # Combined latest
        latest_path = "silver/dim_customer/latest.parquet"
        size = _write_parquet(container, combined, latest_path)
        log.info("    -> %s (%s, %d rows)", latest_path, _fmt_size(size), len(combined))
        silver_written.append({"dim": "customer", "rows": len(combined), "path": latest_path})

        # ── Enrich dim_customer ──
        log.info("  Running enrich_dim_customer...")
        try:
            enrich_result = _enrich_cust.transform(dry_run=dry_run)
            log.info("    -> %s (%d rows, market_group coverage %.1f%%)",
                     enrich_result.get("output_path", "?"),
                     enrich_result.get("total_rows", 0),
                     enrich_result.get("market_group", {}).get("pct", 0))
            silver_written.append({"dim": "customer_enriched",
                                   "rows": enrich_result.get("total_rows", 0),
                                   "path": enrich_result.get("output_path", "?")})
        except Exception as exc:
            log.error("  enrich_dim_customer failed: %s", exc)

    # ── Write dim_product silver ──
    if product_frames:
        combined = pd.concat(product_frames, ignore_index=True)
        combined.drop_duplicates(subset=["item_code"], keep="last", inplace=True)

        blob_path = f"silver/dim_product/{target_date}/dim_product_master.parquet"
        size = _write_parquet(container, combined, blob_path)
        log.info("    -> %s (%s, %d rows)", blob_path, _fmt_size(size), len(combined))

        latest_path = "silver/dim_product/latest.parquet"
        size = _write_parquet(container, combined, latest_path)
        log.info("    -> %s (%s, %d rows)", latest_path, _fmt_size(size), len(combined))
        silver_written.append({"dim": "product", "rows": len(combined), "path": latest_path})

        # ── Enrich dim_product ──
        log.info("  Running enrich_dim_product...")
        try:
            enrich_result = _enrich_prod.transform(dry_run=dry_run)
            log.info("    -> %s (%d rows, %d sellable)",
                     enrich_result.get("output_path", "?"),
                     enrich_result.get("total_rows", 0),
                     enrich_result.get("sellable_rows", 0))
            silver_written.append({"dim": "product_enriched",
                                   "rows": enrich_result.get("total_rows", 0),
                                   "path": enrich_result.get("output_path", "?")})
        except Exception as exc:
            log.error("  enrich_dim_product failed: %s", exc)

    # ── Write dim_salesperson silver ──
    if salesperson_frames:
        combined = pd.concat(salesperson_frames, ignore_index=True)
        combined.drop_duplicates(subset=["entity", "slp_code"], keep="last", inplace=True)

        blob_path = f"silver/dim_salesperson/{target_date}/dim_salesperson.parquet"
        size = _write_parquet(container, combined, blob_path)
        log.info("    -> %s (%s, %d rows)", blob_path, _fmt_size(size), len(combined))

        latest_path = "silver/dim_salesperson/latest.parquet"
        size = _write_parquet(container, combined, latest_path)
        log.info("    -> %s (%s, %d rows)", latest_path, _fmt_size(size), len(combined))
        silver_written.append({"dim": "salesperson", "rows": len(combined), "path": latest_path})

    total_rows = sum(r["rows"] for r in silver_written)
    return {
        "status": "ok",
        "date": target_date,
        "files_processed": len(results),
        "files_skipped": len(skipped),
        "silver_written": silver_written,
        "total_rows": total_rows,
        "files_converted": len(silver_written),
    }
