"""
transforms/cold_extract_to_parquet.py — Bronze CSV → Silver Parquet

Reads raw sales CSVs from bronze/cold_extract/, cleans & normalises,
converts to Parquet (zstd), and writes to silver/cold_extract/.

Usage (via CLI):
  python -m src.cli transform cold_extract
  python -m src.cli transform cold_extract --date 2026-02-17
"""

import io
import logging
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.blob_client import get_container_client

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BRONZE_PREFIX = "cold_extract"
SILVER_PREFIX = "silver/cold_extract"
RECOMMENDED_CODEC = "zstd"

# Column name mapping — normalise across GmbH / UK / Inc / AG variants
COLUMN_MAP = {
    "Card Code":   "CardCode",
    "Line_ID":     "LineNum",
    "Description": "Dscription",   # UK files use "Dscription"
    "Item Code":   "ItemCode",
}

# Final clean column names
FINAL_COLUMNS = {
    "Entity":      "entity",
    "DocEntry":    "doc_entry",
    "DocNum":      "doc_num",
    "DocDate":     "doc_date",
    "DocType":     "doc_type",
    "LineNum":     "line_num",
    "CardCode":    "card_code",
    "ItemCode":    "item_code",
    "Dscription":  "description",
    "Quantity":    "quantity",
    "Net Revenue": "net_revenue",
    "SlpCode":     "slp_code",
    "UpdateDate":  "update_date",
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


def compression_ratio(original: int, compressed: int) -> str:
    if original == 0:
        return "N/A"
    return f"{(1 - compressed / original) * 100:.1f}%"


# ═════════════════════════════════════════════
# Read bronze CSV
# ═════════════════════════════════════════════

EXPECTED_COLUMNS = {
    "Entity", "DocEntry", "DocNum", "DocDate", "DocType",
    "Line_ID", "Card Code", "CardCode", "Item Code", "ItemCode",
    "Description", "Dscription", "Quantity", "Net Revenue",
    "SlpCode", "UpdateDate",
}


def read_bronze_csv(client, blob_name: str) -> pd.DataFrame | None:
    """Download a CSV from bronze and parse with correct sep/encoding.

    Tries separators in order (= , ; tab |).  After parsing, validates
    that the resulting column names look like cold-extract columns AND
    that key columns have sensible dtypes.  This prevents false positives
    when comma-separated German decimals (e.g. ``3,000000``) inflate the
    column count with a ``,`` separator.
    """
    blob_data = client.download_blob(blob_name).readall()

    for encoding in ["utf-8", "latin-1", "cp1252"]:
        for sep in ["=", ",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(
                    io.BytesIO(blob_data),
                    sep=sep,
                    encoding=encoding,
                    low_memory=False,
                    nrows=5,
                )
                if len(df.columns) <= 1:
                    continue

                # Validate: real columns should be in the expected set.
                real_cols = [c for c in df.columns if not c.startswith("Unnamed")]
                known = sum(1 for c in real_cols if c in EXPECTED_COLUMNS)

                # Require at least 10 of the 13 expected columns to match.
                if known < 10:
                    continue

                # Validate data sanity: if German-decimal commas inflated
                # the data, the Entity column will be numeric (DocNum
                # values bleed in) instead of a string.  Check that
                # Entity looks like a string column.
                entity_col = df.get("Entity")
                if entity_col is not None and entity_col.dtype != object:
                    continue

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
    """Normalise column names, parse dates, fix German decimals, set dtypes."""
    df = df.copy()

    # Drop trailing empty columns (from trailing = separator)
    empty_cols = [c for c in df.columns if c.startswith("Unnamed")]
    df.drop(columns=empty_cols, inplace=True, errors="ignore")

    # Normalise variant column names
    df.rename(columns=COLUMN_MAP, inplace=True)

    # Rename to final snake_case
    df.rename(columns=FINAL_COLUMNS, inplace=True)

    # Parse German-format decimals: "1,000000" → 1.0
    for col in ["quantity", "net_revenue"]:
        if col in df.columns and df[col].dtype == object:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
            )

    # Parse dates: "02.01.2023 00:00:00" → datetime
    for col in ["doc_date", "update_date"]:
        if col in df.columns and df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], format="%d.%m.%Y %H:%M:%S", errors="coerce")

    # Downcast numeric types
    for col in ["doc_entry", "doc_num", "line_num", "card_code", "slp_code"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")

    # item_code as string (leading zeros, mixed with NaN)
    if "item_code" in df.columns:
        df["item_code"] = df["item_code"].astype(str).replace({"nan": None, "None": None})

    # Low-cardinality categoricals
    for col in ["entity", "doc_type"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # Source file metadata
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


def read_parquet_from_blob(client, blob_path: str) -> pd.DataFrame:
    """Read Parquet from blob into DataFrame."""
    t0 = time.perf_counter()
    blob_data = client.download_blob(blob_path).readall()
    download_ms = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    table = pq.read_table(io.BytesIO(blob_data))
    parse_ms = (time.perf_counter() - t0) * 1000

    df = table.to_pandas()
    log.info("  ⚡ Downloaded in %dms, parsed in %dms", download_ms, parse_ms)
    return df


def benchmark_compression(df: pd.DataFrame) -> dict:
    """Test Parquet codecs and report sizes."""
    table = pa.Table.from_pandas(df)
    results = {}
    for codec in ["none", "snappy", "gzip", "zstd", "brotli"]:
        buf = io.BytesIO()
        t0 = time.perf_counter()
        pq.write_table(table, buf, compression=codec)
        write_ms = (time.perf_counter() - t0) * 1000
        size = buf.tell()
        buf.seek(0)
        t0 = time.perf_counter()
        pq.read_table(buf)
        read_ms = (time.perf_counter() - t0) * 1000
        results[codec] = {"size": size, "write_ms": write_ms, "read_ms": read_ms}
    return results


# ═════════════════════════════════════════════
# Main transform
# ═════════════════════════════════════════════

def transform(date: str | None = None, dry_run: bool = False) -> dict:
    """Convert bronze/cold_extract CSVs → silver/cold_extract parquet.

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

    log.info("cold_extract transform — %d CSVs for %s", len(blobs), target_date)

    # ── Phase 1: Parse all bronze CSVs ──
    for b in sorted(blobs, key=lambda x: x.name):
        short = b.name.split("/")[-1]
        csv_size = b.size or 0

        df_raw = read_bronze_csv(client, b.name)
        if df_raw is None:
            file_results.append({"file": short, "status": "parse_error"})
            continue

        df = clean_dataframe(df_raw, b.name)
        all_dfs.append(df)
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

    # ── Phase 2: Combine + validate + deduplicate ──
    combined = pd.concat(all_dfs, ignore_index=True)
    rows_before = len(combined)

    # Detect data issues: files whose doc_date year doesn't match filename year
    warnings = []
    for r in file_results:
        if r["status"] != "parsed":
            continue
        fname = r["file"]  # e.g. "COLD_EXTRACT_AG_2025.csv"
        # Extract expected year from filename
        parts = fname.replace(".csv", "").split("_")
        file_year = parts[-1] if parts[-1].isdigit() else None
        if not file_year:
            continue
        # Check actual doc_date year range for this source
        mask = combined["_source_file"] == fname
        src_dates = combined.loc[mask, "doc_date"].dropna()
        if src_dates.empty:
            continue
        actual_years = src_dates.dt.year.unique()
        if int(file_year) not in actual_years:
            msg = (f"{fname}: filename says {file_year} but data contains "
                   f"years {sorted(actual_years.tolist())} — possible wrong export")
            warnings.append(msg)
            log.warning("  ⚠️  %s", msg)

    # Deduplicate by (entity, doc_entry, line_num), keeping last occurrence.
    # Files are sorted alphabetically, so within the same entity a 2025 file
    # comes after a 2024 file — if they contain different data the 2025 row
    # wins.  If a file is an exact duplicate (like AG_2025 == AG_2024), the
    # dedup simply collapses it to one copy.
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

    # ── Phase 3: Write per-entity/year parquets ──
    results = []
    total_pq = 0
    combined["_year"] = combined["doc_date"].dt.year

    for (entity, year), group_df in combined.groupby(["entity", "_year"], observed=True):
        group_df = group_df.drop(columns=["_year"])
        parquet_name = f"cold_extract_{str(entity).lower()}_{int(year)}.parquet"
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
            "year": int(year),
            "rows": len(group_df),
            "parquet_bytes": written,
            "silver_path": silver_path,
        })

    total_csv = sum(r.get("csv_bytes", 0) for r in file_results)

    # ── Phase 4: Cleanup stale silver parquets ──
    # Remove any parquets in this date folder that weren't just written.
    # This handles renamed files, old naming conventions, and orphaned
    # parquets from previous runs (e.g. cold_extract_ag_2025.parquet
    # left over from the old 1-per-CSV naming).
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
        "pipeline": "cold_extract",
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
        "warnings": warnings,
        "bronze_details": file_results,
        "silver_details": results,
    }
