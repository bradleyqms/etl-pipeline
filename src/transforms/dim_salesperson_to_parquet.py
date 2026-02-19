"""
transforms/dim_salesperson_to_parquet.py — Bronze CSV → Silver Parquet

Salesperson dimension table (from SAP B1 OSLP table).
Bronze: bronze/dim_tables/{date}/dim_salesperson.csv
Silver: silver/dim_salesperson/latest.parquet

Columns (from SAP B1 dim_salesperson extract):
  Entity, SlpCode, SlpName, Active, Commission, Locked
"""

import io
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

SILVER_PREFIX = "silver/dim_salesperson"
RECOMMENDED_CODEC = "zstd"

# Known SAP column names → snake_case
FINAL_COLUMNS = {
    "Entity":      "entity",
    "SlpCode":     "slp_code",
    "SlpName":     "slp_name",
    "Active":      "is_active",
    "Commission":  "commission",
    "Locked":      "is_locked",
    # Fallback Column-style headers (if SAP omits aliases)
    "Column1":     "entity",
    "Column2":     "slp_code",
    "Column3":     "slp_name",
    "Column4":     "is_active",
    "Column5":     "commission",
    "Column6":     "is_locked",
}

EXPECTED_FINAL_COLS = ["entity", "slp_code", "slp_name", "is_active", "commission", "is_locked"]


def parse_csv(blob_data: bytes, source_name: str) -> pd.DataFrame | None:
    """Parse a dim_salesperson CSV from bronze bytes."""
    for encoding in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(
                io.BytesIO(blob_data),
                sep=",",
                encoding=encoding,
                low_memory=False,
            )
            if len(df.columns) > 1:
                break
        except Exception:
            continue
    else:
        log.warning("dim_salesperson: could not parse %s", source_name)
        return None

    # Drop trailing unnamed columns
    df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")], inplace=True, errors="ignore")

    # Remap column names
    df.rename(columns=FINAL_COLUMNS, inplace=True)

    # Validate we got the key columns
    if "slp_code" not in df.columns or "slp_name" not in df.columns:
        log.warning("dim_salesperson: missing expected columns in %s — got %s", source_name, list(df.columns))
        return None

    # Normalise types
    df["slp_code"] = pd.to_numeric(df["slp_code"], errors="coerce").astype("Int32")

    if "is_active" in df.columns:
        df["is_active"] = df["is_active"].astype(str).str.strip().str.upper()

    if "is_locked" in df.columns:
        df["is_locked"] = df["is_locked"].astype(str).str.strip().str.upper()

    if "commission" in df.columns:
        df["commission"] = pd.to_numeric(df["commission"], errors="coerce").astype("float64")

    if "entity" not in df.columns:
        df["entity"] = "GmbH"  # default — salesperson table is usually GmbH master

    df["entity"] = df["entity"].astype("category")
    df["_source_file"] = Path(source_name).name

    return df


def write_parquet(container_client, df: pd.DataFrame, blob_path: str) -> int:
    """Write DataFrame as Parquet to blob. Returns bytes written."""
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=RECOMMENDED_CODEC)
    buf.seek(0)
    data = buf.getvalue()
    container_client.upload_blob(name=blob_path, data=data, overwrite=True)
    return len(data)
