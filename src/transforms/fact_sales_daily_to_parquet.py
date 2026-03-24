"""
transforms/fact_sales_daily_to_parquet.py — Bronze CSV → Silver Parquet
(Daily incremental sales extract)

Reads raw daily sales CSVs from bronze/fact_sales_daily/, cleans & normalises
(same schema as cold_extract), and writes to silver/fact_sales_daily/.

The daily incremental uses comma separator with CHAR(34) quoting and period
decimals — different from the cold_extract which uses ``=`` separator with
German decimals.  The clean_dataframe() function from cold_extract handles
both formats automatically.

Usage (via CLI):
  python -m src.cli ingest fact_sales_daily
  python -m src.cli transform fact_sales_daily
  python -m src.cli transform fact_sales_daily --date 2026-02-18
"""

import io
import logging
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.blob_client import get_container_client
from ..core.dead_letter import quarantine_blob
from ..core.validation import add_etl_load_timestamp, current_utc_timestamp, validate_dataframe
from .cold_extract_to_parquet import (
    clean_dataframe,
    write_parquet_to_blob,
    fmt_size,
    RECOMMENDED_CODEC,
    REQUIRED_CLEAN_COLUMNS,
    DATETIME_VALIDATION_COLUMNS,
    NUMERIC_VALIDATION_COLUMNS,
    NON_NULL_VALIDATION_COLUMNS,
)

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BRONZE_PREFIX = "fact_sales_daily"
SILVER_PREFIX = "silver/fact_sales_daily"


# ═════════════════════════════════════════════
# Read bronze CSV (comma-separated, CHAR(34) quoted)
# ═════════════════════════════════════════════

def read_bronze_csv(client, blob_name: str) -> pd.DataFrame | None:
    """Download a daily incremental CSV from bronze and parse.

    These files use comma separator with CHAR(34) quoting and period
    decimals (e.g. ``"1.000000"``).  Falls back to ``=`` separator
    if comma doesn't work.
    """
    blob_data = client.download_blob(blob_name).readall()

    for encoding in ["utf-8", "latin-1", "cp1252"]:
        for sep in [",", "=", ";", "\t", "|"]:
            try:
                df = pd.read_csv(
                    io.BytesIO(blob_data),
                    sep=sep,
                    quotechar='"',
                    encoding=encoding,
                    low_memory=False,
                    nrows=5,
                )
                if len(df.columns) <= 1:
                    continue

                # Validate: Entity should be a string column (CH, GmbH, etc.)
                entity_col = df.get("Entity")
                if entity_col is not None and not pd.api.types.is_string_dtype(entity_col):
                    continue

                df = pd.read_csv(
                    io.BytesIO(blob_data),
                    sep=sep,
                    quotechar='"',
                    encoding=encoding,
                    low_memory=False,
                )
                return df
            except Exception:
                continue
    return None


# ═════════════════════════════════════════════
# Main transform
# ═════════════════════════════════════════════

def transform(date: str | None = None, dry_run: bool = False) -> dict:
    """Convert bronze/fact_sales_daily CSVs → silver/fact_sales_daily parquet.

    Each daily email produces one CSV covering all entities for that day.
    The transform writes one parquet per entity per date to silver.

    Returns summary dict with conversion stats.
    """
    client = get_container_client()

    # Discover bronze CSVs
    all_blobs = list(client.list_blobs(name_starts_with=f"{BRONZE_PREFIX}/"))
    csv_blobs = [b for b in all_blobs if b.name.endswith(".csv")]

    if not csv_blobs:
        return {"status": "no_data", "message": f"No CSV files in bronze/{BRONZE_PREFIX}/"}

    # Group by date
    dates = {}
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
    file_results = []
    all_dfs = []
    dead_letter_files = []
    data_quality_warnings = []
    etl_load_timestamp = current_utc_timestamp()

    log.info("fact_sales_daily transform — %d CSVs for %s", len(blobs), target_date)

    # ── Phase 1: Parse all bronze CSVs ──
    for b in sorted(blobs, key=lambda x: x.name):
        short = b.name.split("/")[-1]
        csv_size = b.size or 0
        raw_blob = client.download_blob(b.name).readall()

        df_raw = read_bronze_csv(client, b.name)
        if df_raw is None:
            file_results.append({"file": short, "status": "parse_error"})
            log.error("  ❌ %s — could not parse", short)
            if not dry_run:
                dead_letter_files.append(
                    quarantine_blob(
                        client,
                        pipeline="fact_sales_daily",
                        source_blob_name=b.name,
                        raw_bytes=raw_blob,
                        run_date=target_date,
                        reason="parse_error",
                        details=[{"code": "parse_error", "message": "Could not parse source CSV"}],
                    )
                )
            continue

        df = clean_dataframe(df_raw, b.name)
        validation = validate_dataframe(
            df,
            required_columns=REQUIRED_CLEAN_COLUMNS,
            datetime_columns=DATETIME_VALIDATION_COLUMNS,
            numeric_columns=NUMERIC_VALIDATION_COLUMNS,
            non_null_columns=NON_NULL_VALIDATION_COLUMNS,
        )
        if not validation.is_valid:
            file_results.append({"file": short, "status": "validation_error", "errors": validation.errors})
            data_quality_warnings.extend(validation.warnings)
            if not dry_run:
                dead_letter_files.append(
                    quarantine_blob(
                        client,
                        pipeline="fact_sales_daily",
                        source_blob_name=b.name,
                        raw_bytes=raw_blob,
                        run_date=target_date,
                        reason="schema_validation_failed",
                        details=validation.errors,
                    )
                )
            log.warning("  ⚠️ %s failed validation and was quarantined", short)
            continue

        all_dfs.append(df)
        data_quality_warnings.extend(validation.warnings)
        file_results.append({
            "file": short,
            "status": "parsed",
            "rows": df.shape[0],
            "csv_bytes": csv_size,
        })
        log.info("  ✅ %s — %d rows", short, len(df))

    if not all_dfs:
        return {"status": "error", "message": "No CSVs could be parsed",
                "details": file_results}

    # ── Phase 2: Combine + deduplicate ──
    combined = pd.concat(all_dfs, ignore_index=True)
    rows_before = len(combined)

    # Deduplicate by (entity, doc_entry, line_num)
    DEDUP_KEYS = ["entity", "doc_entry", "line_num"]
    combined = combined.sort_values(
        DEDUP_KEYS + ["doc_date"], na_position="first",
    )
    combined = combined.drop_duplicates(subset=DEDUP_KEYS, keep="last")
    rows_after = len(combined)
    rows_dropped = rows_before - rows_after
    if rows_dropped:
        log.info("  🔄 Deduplicated: %d → %d rows (%d duplicates removed)",
                 rows_before, rows_after, rows_dropped)

    # ── Phase 3: Write per-entity parquets ──
    # Daily incremental covers a few days, so group by entity only
    # (not entity+year like cold_extract).
    results = []
    total_pq = 0

    for entity, group_df in combined.groupby("entity", observed=True):
        group_df = add_etl_load_timestamp(group_df, etl_load_timestamp)
        parquet_name = f"fact_sales_daily_{str(entity).lower()}.parquet"
        silver_path = f"{SILVER_PREFIX}/{target_date}/{parquet_name}"

        if dry_run:
            log.info("  [DRY RUN] Would write %s (%d rows)", silver_path, len(group_df))
            written = 0
        else:
            written = write_parquet_to_blob(client, group_df, silver_path)
            log.info("  📦 %s (%d rows, %s)", silver_path, len(group_df), fmt_size(written))

        total_pq += written
        results.append({
            "entity": str(entity),
            "rows": len(group_df),
            "parquet_bytes": written,
            "silver_path": silver_path,
        })

    total_csv = sum(r.get("csv_bytes", 0) for r in file_results)

    # ── Phase 4: Cleanup stale silver parquets ──
    written_paths = {r["silver_path"] for r in results}
    stale_deleted = []

    if not dry_run:
        existing_silver = [
            b for b in client.list_blobs(
                name_starts_with=f"{SILVER_PREFIX}/{target_date}/"
            )
            if b.name.endswith(".parquet")
        ]
        for b in existing_silver:
            if b.name not in written_paths:
                try:
                    client.delete_blob(b.name)
                    stale_deleted.append(b.name)
                    log.info("  🗑️  Deleted stale: %s", b.name)
                except Exception as e:
                    log.warning("  ⚠️  Failed to delete %s: %s", b.name, e)

        if stale_deleted:
            log.info("  Cleanup: removed %d stale parquet(s)", len(stale_deleted))
        else:
            log.info("  Cleanup: silver layer is clean — no stale files")

    return {
        "status": "ok",
        "pipeline": "fact_sales_daily",
        "date": target_date,
        "bronze_files": len([r for r in file_results if r["status"] == "parsed"]),
        "silver_files": len(results),
        "files_converted": len(results),
        "total_rows_before_dedup": rows_before,
        "total_rows": rows_after,
        "duplicates_removed": rows_dropped,
        "total_csv_bytes": total_csv,
        "total_parquet_bytes": total_pq,
        "compression_ratio": f"{(1 - total_pq / total_csv) * 100:.1f}%" if total_csv else "N/A",
        "stale_deleted": stale_deleted,
        "dead_letter_files": dead_letter_files,
        "data_quality_warnings": data_quality_warnings,
        "etl_load_timestamp": etl_load_timestamp,
        "bronze_details": file_results,
        "silver_details": results,
    }
