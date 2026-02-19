"""
transforms/dim_customer_to_parquet.py — Bronze CSV → Silver Parquet

Customer master dimension table.
Bronze: bronze/dim_tables/{date}/ — 4 CSVs (GmbH, UK, USA, AG)
Silver: silver/dim_customer/{date}/  — per-entity parquets + latest.parquet (combined, deduped)

Columns (from SAP B1 dim_customer extract v2 — all entities now have named headers):
  Entity, CardCode, CardName, GroupName, BillToStreet, BillToCity, BillToZip,
  BillToCountry, ShipToStreet, ShipToCity, ShipToZip, ShipToCountry,
  TerritoryID, SlpCode, CreateDate, UpdateDate, IsActive
"""

import io
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.blob_client import get_container_client

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BRONZE_PREFIX = "dim_customer"
SILVER_PREFIX = "silver/dim_customer"
RECOMMENDED_CODEC = "zstd"

# Final clean column names (snake_case) — v2 schema (all entities have named headers)
FINAL_COLUMNS = {
    "Entity":        "entity",
    "CardCode":      "card_code",
    "CardName":      "card_name",
    "GroupName":     "group_name",       # NEW: customer group label (Vertrieb/Customers/Kunden)
    "BillToStreet":  "bill_to_street",
    "BillToCity":    "bill_to_city",
    "BillToZip":     "bill_to_zip",
    "BillToCountry": "bill_to_country",
    "ShipToStreet":  "ship_to_street",
    "ShipToCity":    "ship_to_city",
    "ShipToZip":     "ship_to_zip",
    "ShipToCountry": "ship_to_country",
    "TerritoryID":   "territory_id",     # RENAMED from Territory
    "SlpCode":       "slp_code",
    "CreateDate":    "create_date",
    "UpdateDate":    "update_date",
    "IsActive":      "is_active",        # RENAMED from validFor
    # Legacy fallbacks (v1 schema — keep for backward compat with old bronze files)
    "GroupCode":     "group_code",
    "Territory":     "territory_id",
    "validFor":      "is_active",
}


# ═════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════

def fmt_size(nbytes: int) -> str:
    if nbytes >= 1_048_576:
        return f"{nbytes / 1_048_576:.1f} MB"
    elif nbytes >= 1024:
        return f"{nbytes / 1024:.1f} KB"
    return f"{nbytes:,} B"


# ═════════════════════════════════════════════
# Read bronze CSV
# ═════════════════════════════════════════════

def read_bronze_csv(client, blob_name: str) -> pd.DataFrame | None:
    """Download a dim_customer CSV from bronze and parse."""
    blob_data = client.download_blob(blob_name).readall()

    # Dim_customer CSVs use comma separator with trailing comma
    for encoding in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        for sep in [",", "=", ";", "\t", "|"]:
            try:
                df = pd.read_csv(
                    io.BytesIO(blob_data),
                    sep=sep,
                    encoding=encoding,
                    low_memory=False,
                    nrows=5,
                )
                if len(df.columns) > 1:
                    df = pd.read_csv(
                        io.BytesIO(blob_data),
                        sep=sep,
                        encoding=encoding,
                        low_memory=False,
                    )
                    # Legacy fallback: SAP v1 exported first 11 cols as Column1..Column11
                    # v2 exports all named headers — keep this for old bronze files only.
                    if "Column1" in df.columns:
                        remap = {
                            "Column1":  "Entity",
                            "Column2":  "CardCode",
                            "Column3":  "CardName",
                            "Column4":  "BillToStreet",
                            "Column5":  "BillToCity",
                            "Column6":  "BillToZip",
                            "Column7":  "BillToCountry",
                            "Column8":  "ShipToStreet",
                            "Column9":  "ShipToCity",
                            "Column10": "ShipToZip",
                            "Column11": "ShipToCountry",
                        }
                        df.rename(columns=remap, inplace=True)
                    return df
            except Exception:
                continue
    return None


# ═════════════════════════════════════════════
# Clean & normalise
# ═════════════════════════════════════════════

def clean_dataframe(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Normalise column names, parse dates, set dtypes for customer master."""
    df = df.copy()

    # Drop trailing empty columns (from trailing separator)
    empty_cols = [c for c in df.columns if c.startswith("Unnamed")]
    df.drop(columns=empty_cols, inplace=True, errors="ignore")

    # Rename to final snake_case
    df.rename(columns=FINAL_COLUMNS, inplace=True)

    # Parse dates — v2 exports ISO (2026-01-19), v1 was German (19.01.2026 00:00:00)
    for col in ["create_date", "update_date"]:
        if col in df.columns and df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Downcast numeric types
    for col in ["group_code", "territory_id", "slp_code"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")

    # card_code as string (leading zeros possible)
    if "card_code" in df.columns:
        df["card_code"] = df["card_code"].astype(str).replace({"nan": None, "None": None})

    # entity as category (low cardinality: GmbH, UK, US, AG)
    if "entity" in df.columns:
        df["entity"] = df["entity"].astype("category")

    # group_name as string (Vertrieb / Customers / Kunden)
    if "group_name" in df.columns:
        df["group_name"] = df["group_name"].astype(str).replace({"nan": None, "None": None})

    # is_active: normalise Y/N (v2) and legacy validFor formats
    if "is_active" in df.columns:
        df["is_active"] = (
            df["is_active"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"Y": "Y", "N": "N", "/": "Y", "NONE": None, "NAN": None})
        )

    # Source metadata
    df["_source_file"] = Path(source_file).name

    return df


# ═════════════════════════════════════════════
# Parquet I/O
# ═════════════════════════════════════════════

def write_parquet_to_blob(client, df: pd.DataFrame, blob_path: str, codec: str = RECOMMENDED_CODEC) -> int:
    """Write DataFrame as Parquet to blob. Returns bytes written."""
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=codec)
    buf.seek(0)
    data = buf.getvalue()
    client.upload_blob(name=blob_path, data=data, overwrite=True)
    return len(data)


# ═════════════════════════════════════════════
# Main transform
# ═════════════════════════════════════════════

def transform(date: str | None = None, dry_run: bool = False) -> dict:
    """Convert bronze/dim_customer CSVs → silver/dim_customer parquet.

    Creates:
      - Per-entity parquets: silver/dim_customer/{date}/dim_customer_{entity}.parquet
      - Combined + deduped:  silver/dim_customer/latest.parquet

    Deduplication: keeps latest row per (card_code, entity) by update_date.
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
    results = []
    all_dfs = []

    log.info("dim_customer transform — %d CSVs for %s", len(blobs), target_date)

    for b in sorted(blobs, key=lambda x: x.name):
        short = b.name.split("/")[-1]
        csv_size = b.size or 0

        df_raw = read_bronze_csv(client, b.name)
        if df_raw is None:
            log.warning("  ⚠️ Could not parse %s — skipping", short)
            results.append({"file": short, "status": "parse_error"})
            continue

        df = clean_dataframe(df_raw, b.name)
        all_dfs.append(df)

        # Per-entity parquet
        entity_label = short.replace(".csv", "").lower()
        parquet_name = f"{entity_label}.parquet"
        silver_path = f"{SILVER_PREFIX}/{target_date}/{parquet_name}"

        if dry_run:
            log.info("  [DRY RUN] Would write %s → %s (%d rows)", short, silver_path, len(df))
            written = 0
        else:
            written = write_parquet_to_blob(client, df, silver_path)
            log.info("  📦 %s → %s (%s, %d rows)", short, silver_path, fmt_size(written), len(df))

        results.append({
            "file": short,
            "status": "converted",
            "rows": df.shape[0],
            "csv_bytes": csv_size,
            "parquet_bytes": written,
            "silver_path": silver_path,
        })

    # Combined + deduped latest.parquet
    latest_stats = {}
    if all_dfs and not dry_run:
        combined = pd.concat(all_dfs, ignore_index=True)

        # Dedup: keep latest row per (card_code, entity)
        if "update_date" in combined.columns:
            combined.sort_values("update_date", ascending=False, inplace=True, na_position="last")
        combined.drop_duplicates(subset=["card_code", "entity"], keep="first", inplace=True)
        combined.sort_values(["entity", "card_code"], inplace=True)
        combined.reset_index(drop=True, inplace=True)

        latest_path = f"{SILVER_PREFIX}/latest.parquet"
        written = write_parquet_to_blob(client, combined, latest_path)
        log.info("  📦 Combined → %s (%s, %d rows)", latest_path, fmt_size(written), len(combined))

        latest_stats = {
            "path": latest_path,
            "rows": len(combined),
            "parquet_bytes": written,
            "entities": combined["entity"].nunique() if "entity" in combined.columns else 0,
        }
    elif dry_run and all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        log.info("  [DRY RUN] Would write combined latest.parquet (%d rows)", len(combined))

    total_rows = sum(r.get("rows", 0) for r in results)
    total_csv = sum(r.get("csv_bytes", 0) for r in results)
    total_pq = sum(r.get("parquet_bytes", 0) for r in results)

    return {
        "status": "ok",
        "pipeline": "dim_customer",
        "date": target_date,
        "files_converted": len([r for r in results if r["status"] == "converted"]),
        "total_rows": total_rows,
        "total_csv_bytes": total_csv,
        "total_parquet_bytes": total_pq,
        "compression_ratio": f"{(1 - total_pq / total_csv) * 100:.1f}%" if total_csv else "N/A",
        "latest": latest_stats,
        "details": results,
    }
