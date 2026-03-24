"""
transforms/dim_product_to_parquet.py — Bronze CSV → Silver Parquet

Product master dimension table.
Bronze: bronze/dim_tables/{date}/  — single CSV (all entities combined)
Silver: silver/dim_product/{date}/  — per-entity parquets + latest.parquet

Columns (from SAP B1 dim_product_master extract v2):
  Entity, ItemCode, Description, ItemGroup, IsActive, Webshop_Active,
  WS_Active_Flag, Is_Prov, Status, Parent_Item, Weight_SU_kg,
  Weight_Primary_g, Weight_Secondary_g, Content_ML, Content_GR,
  ProductLine, Name_EN, Variant_Dim1, CreateDate
"""

import io
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.blob_client import get_container_client
from ..core.dead_letter import quarantine_blob
from ..core.validation import add_etl_load_timestamp, current_utc_timestamp, validate_dataframe

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BRONZE_PREFIX = "dim_product"
SILVER_PREFIX = "silver/dim_product"
RECOMMENDED_CODEC = "zstd"

# Final clean column names (snake_case) — v2 schema
FINAL_COLUMNS = {
    "Entity":              "entity",
    "ItemCode":            "item_code",
    "Description":         "description",
    "ItemGroup":           "item_group",
    "IsActive":            "is_active",
    "Webshop_Active":      "webshop_active",      # NEW: active on webshop
    "WS_Active_Flag":      "ws_active_flag",      # NEW: webshop active flag
    "Is_Prov":             "is_provisional",      # NEW: provisional item
    "Status":              "status",              # NEW: item status
    "Parent_Item":         "parent_item",         # NEW: parent SKU for variants
    "Weight_SU_kg":        "weight_su_kg",        # NEW: shipping unit weight
    "Weight_Primary_g":    "weight_primary_g",    # NEW: primary pack weight
    "Weight_Secondary_g":  "weight_secondary_g",  # NEW: secondary pack weight
    "Content_ML":          "content_ml",          # NEW: volume ml
    "Content_GR":          "content_gr",          # NEW: weight gr
    "ProductLine":         "product_line",        # NEW: product line / brand
    "Name_EN":             "name_en",             # NEW: English product name
    "Variant_Dim1":        "variant_dim1",        # NEW: variant dimension
    "CreateDate":          "create_date",
    # Legacy v1 fallbacks — kept for backward compat with old bronze files
    "IsInventory":         "is_inventory",
    "IsSalesItem":         "is_sales_item",
    "U_Guidanceline":      "guidanceline",
    "U_Kontrollfeld":      "kontrollfeld",
    "PriceListNum":        "price_list_num",
    "PriceListName":       "price_list_name",
    "UpdateDate":          "update_date",
}

# Validation constants (used by transform() and re-exportable)
PRODUCT_VALIDATION = {
    "required_columns": {"entity", "item_code", "description"},
    "datetime_columns": {"create_date"},
    "non_null_columns": {"item_code"},
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
    """Download a dim_product CSV from bronze and parse."""
    blob_data = client.download_blob(blob_name).readall()

    # dim_product CSVs use comma separator with trailing comma
    for encoding in ["utf-8", "latin-1", "cp1252"]:
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
                    return df
            except Exception:
                continue
    return None


# ═════════════════════════════════════════════
# Clean & normalise
# ═════════════════════════════════════════════

def clean_dataframe(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Normalise column names, parse dates, set dtypes for product master."""
    df = df.copy()

    # Drop trailing empty columns (from trailing separator)
    empty_cols = [c for c in df.columns if c.startswith("Unnamed")]
    df.drop(columns=empty_cols, inplace=True, errors="ignore")

    # Rename to final snake_case
    df.rename(columns=FINAL_COLUMNS, inplace=True)

    # Parse dates — v2 exports ISO (2018-11-05), v1 was German format
    for col in ["create_date", "update_date"]:
        if col in df.columns and pd.api.types.is_string_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Downcast numeric types
    for col in ["item_group", "price_list_num"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")

    # Weight / content fields as float
    for col in ["weight_su_kg", "weight_primary_g", "weight_secondary_g",
                "content_ml", "content_gr"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # item_code as string (leading zeros, mixed types)
    if "item_code" in df.columns:
        df["item_code"] = df["item_code"].map(lambda value: None if pd.isna(value) else str(value)).astype(object)

    # entity as category (low cardinality)
    if "entity" in df.columns:
        df["entity"] = df["entity"].astype("category")

    # Boolean-ish Y/N flags
    for col in ["is_active", "is_inventory", "is_sales_item",
                "webshop_active", "ws_active_flag", "is_provisional"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.upper()
                .replace({"Y": "Y", "N": "N", "/": "Y", "NONE": None, "NAN": None})
            )

    # String fields
    for col in ["guidanceline", "kontrollfeld", "product_line", "name_en",
                "variant_dim1", "status", "parent_item"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"nan": None, "None": None})

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

def transform(date: str | None = None, dry_run: bool = False, etl_load_timestamp: str | None = None) -> dict:
    """Convert bronze/dim_product CSVs → silver/dim_product parquet.

    Creates:
      - Per-file parquets:  silver/dim_product/{date}/{filename}.parquet
      - Combined + deduped: silver/dim_product/latest.parquet

    Deduplication: keeps latest row per (item_code, entity) by update_date.
    """
    client = get_container_client()
    etl_load_timestamp = etl_load_timestamp or current_utc_timestamp()
    dead_letter_files: list[dict] = []
    data_quality_warnings: list[dict] = []

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

    log.info("dim_product transform — %d CSVs for %s", len(blobs), target_date)

    for b in sorted(blobs, key=lambda x: x.name):
        short = b.name.split("/")[-1]
        csv_size = b.size or 0

        raw_bytes = client.download_blob(b.name).readall()
        df_raw = read_bronze_csv(client, b.name)
        if df_raw is None:
            log.warning("  ⚠️ Could not parse %s — skipping", short)
            results.append({"file": short, "status": "parse_error"})
            if not dry_run:
                dead_letter_files.append(
                    quarantine_blob(
                        client,
                        pipeline="dim_product",
                        source_blob_name=b.name,
                        raw_bytes=raw_bytes,
                        run_date=target_date,
                        reason="parse_error",
                        details=[{"code": "parse_error", "message": f"Could not parse: {short}"}],
                    )
                )
            continue

        df = clean_dataframe(df_raw, b.name)
        validation = validate_dataframe(df, **PRODUCT_VALIDATION)
        data_quality_warnings.extend(validation.warnings)
        if not validation.is_valid:
            log.warning("  ⚠️ Validation failed for %s — quarantining", short)
            results.append({"file": short, "status": "validation_error", "errors": validation.errors})
            if not dry_run:
                dead_letter_files.append(
                    quarantine_blob(
                        client,
                        pipeline="dim_product",
                        source_blob_name=b.name,
                        raw_bytes=raw_bytes,
                        run_date=target_date,
                        reason="schema_validation_failed",
                        details=validation.errors,
                    )
                )
            continue

        all_dfs.append(df)

        # Per-file parquet
        parquet_name = short.replace(".csv", ".parquet").lower()
        silver_path = f"{SILVER_PREFIX}/{target_date}/{parquet_name}"

        if dry_run:
            log.info("  [DRY RUN] Would write %s → %s (%d rows)", short, silver_path, len(df))
            written = 0
        else:
            written = write_parquet_to_blob(client, add_etl_load_timestamp(df, etl_load_timestamp), silver_path)
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

        # Dedup: keep latest row per (item_code, entity)
        if "update_date" in combined.columns:
            combined.sort_values("update_date", ascending=False, inplace=True, na_position="last")
        combined.drop_duplicates(subset=["item_code", "entity"], keep="first", inplace=True)
        combined.sort_values(["entity", "item_code"], inplace=True)
        combined.reset_index(drop=True, inplace=True)

        latest_path = f"{SILVER_PREFIX}/latest.parquet"
        written = write_parquet_to_blob(client, add_etl_load_timestamp(combined, etl_load_timestamp), latest_path)
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
        "pipeline": "dim_product",
        "date": target_date,
        "files_converted": len([r for r in results if r["status"] == "converted"]),
        "total_rows": total_rows,
        "total_csv_bytes": total_csv,
        "total_parquet_bytes": total_pq,
        "compression_ratio": f"{(1 - total_pq / total_csv) * 100:.1f}%" if total_csv else "N/A",
        "latest": latest_stats,
        "details": results,
        "etl_load_timestamp": etl_load_timestamp,
        "dead_letter_files": dead_letter_files,
        "data_quality_warnings": data_quality_warnings,
    }
