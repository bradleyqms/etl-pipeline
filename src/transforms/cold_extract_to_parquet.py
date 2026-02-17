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

def read_bronze_csv(client, blob_name: str) -> pd.DataFrame | None:
    """Download a CSV from bronze and parse with correct sep/encoding."""
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
    results = []

    log.info("cold_extract transform — %d CSVs for %s", len(blobs), target_date)

    for b in sorted(blobs, key=lambda x: x.name):
        short = b.name.split("/")[-1]
        csv_size = b.size or 0

        df_raw = read_bronze_csv(client, b.name)
        if df_raw is None:
            results.append({"file": short, "status": "parse_error"})
            continue

        df = clean_dataframe(df_raw, b.name)

        parquet_name = short.replace(".csv", ".parquet").lower()
        silver_path = f"{SILVER_PREFIX}/{target_date}/{parquet_name}"

        if dry_run:
            log.info("  [DRY RUN] Would write %s → %s (%d rows)", short, silver_path, len(df))
            written = 0
        else:
            written = write_parquet_to_blob(client, df, silver_path)
            log.info("  📦 %s → %s (%s)", short, silver_path, fmt_size(written))

        results.append({
            "file": short,
            "status": "converted",
            "rows": df.shape[0],
            "csv_bytes": csv_size,
            "parquet_bytes": written,
            "silver_path": silver_path,
        })

    total_rows = sum(r.get("rows", 0) for r in results)
    total_csv = sum(r.get("csv_bytes", 0) for r in results)
    total_pq = sum(r.get("parquet_bytes", 0) for r in results)

    return {
        "status": "ok",
        "pipeline": "cold_extract",
        "date": target_date,
        "files_converted": len([r for r in results if r["status"] == "converted"]),
        "total_rows": total_rows,
        "total_csv_bytes": total_csv,
        "total_parquet_bytes": total_pq,
        "compression_ratio": f"{(1 - total_pq / total_csv) * 100:.1f}%" if total_csv else "N/A",
        "details": results,
    }
