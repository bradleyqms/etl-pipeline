"""
transforms/us_budget_v0_to_parquet.py

Cold pipeline for US budget v0 workbook ingestion.

Purpose
-------
Convert the manually maintained workbook into canonical files that are stable
for downstream ETL ingestion.

Input
-----
An Excel workbook with regional tabs (Northeast/Central/West/Southeast/Other/Lost)
where row 2 is the header row.

Output
------
Under data/reference/v0_outputs/<version_label>/:
- us_budget_customer_v2.parquet / .csv
- us_budget_monthly_v2.parquet / .csv
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

REGION_SHEETS = ["Northeast", "Central", "West", "Southeast", "Other", "Lost"]

ID_COL_RENAME = {
    "Customer Code": "customer_code",
    "Customer Name": "customer_name",
    "Region": "customer_region",
    "Sales person": "sales_person",
    "New door year": "new_door_year",
    "Active?": "is_active",
    "2025 Cluster": "cluster_2025",
    "2026 Cluster": "cluster_2026",
    "2025A": "actual_2025",
    "2026B": "budget_2026_total",
    "Size indicator": "size_indicator",
    "Customer Growth Class": "growth_class",
}


@dataclass(frozen=True)
class OutputPaths:
    customer_parquet: Path
    customer_csv: Path
    monthly_parquet: Path
    monthly_csv: Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_input_path() -> Path:
    return (
        _project_root()
        / "data"
        / "reference"
        / "v0_inputs"
        / "2026.01.22 US Spa Sales Budget 2026 FINAL.xlsx"
    )


def _default_output_root() -> Path:
    return _project_root() / "data" / "reference" / "v0_outputs"


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c if not isinstance(c, str) else c.strip() for c in out.columns]
    return out


def _coerce_month_columns(df: pd.DataFrame) -> list[str]:
    """Return columns that represent month buckets.

    This workbook stores monthly columns as true datetime headers on regional tabs.
    We convert those headers to YYYY-MM-01 strings for deterministic downstream keys.
    """
    month_cols: list[str] = []
    rename: dict[object, str] = {}
    for col in df.columns:
        if isinstance(col, (datetime, pd.Timestamp)):
            month_key = pd.Timestamp(col).normalize().strftime("%Y-%m-01")
            rename[col] = month_key
            month_cols.append(month_key)
    if rename:
        df.rename(columns=rename, inplace=True)
    return month_cols


def _read_region_sheet(xlsx_path: Path, sheet: str) -> tuple[pd.DataFrame, list[str]]:
    # Header is row 2 in all regional tabs.
    df = pd.read_excel(xlsx_path, sheet_name=sheet, header=1)
    df = _normalise_columns(df)

    # Drop fully empty columns and housekeeping unnamed columns used as separators.
    df = df.dropna(axis=1, how="all")
    keep_cols = [
        c for c in df.columns
        if not (isinstance(c, str) and c.startswith("Unnamed:"))
    ]
    df = df[keep_cols].copy()

    month_cols = _coerce_month_columns(df)
    df["source_sheet"] = sheet
    return df, month_cols


def _clean_customer_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Rename known business columns where present.
    present_map = {k: v for k, v in ID_COL_RENAME.items() if k in out.columns}
    out.rename(columns=present_map, inplace=True)

    # Keep only real customer rows.
    if "customer_code" in out.columns:
        out["customer_code"] = pd.to_numeric(out["customer_code"], errors="coerce")
    out = out[out["customer_code"].notna()].copy()
    out["customer_code"] = out["customer_code"].astype("Int64")

    if "customer_name" in out.columns:
        name = out["customer_name"].astype(str)
        out = out[~name.str.strip().str.lower().str.startswith("total")].copy()

    # Normalize common text fields.
    for col in ["customer_name", "customer_region", "sales_person", "cluster_2025", "cluster_2026", "growth_class", "is_active"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip().replace({"nan": None, "None": None, "": None})

    if "new_door_year" in out.columns:
        out["new_door_year"] = pd.to_numeric(out["new_door_year"], errors="coerce").astype("Int64")

    if "actual_2025" in out.columns:
        out["actual_2025"] = pd.to_numeric(out["actual_2025"], errors="coerce")
    if "budget_2026_total" in out.columns:
        out["budget_2026_total"] = pd.to_numeric(out["budget_2026_total"], errors="coerce")

    return out


def _build_customer_snapshot(regions_df: pd.DataFrame, source_file_name: str) -> pd.DataFrame:
    cols = [
        "customer_code",
        "customer_name",
        "customer_region",
        "sales_person",
        "new_door_year",
        "is_active",
        "cluster_2025",
        "cluster_2026",
        "growth_class",
        "actual_2025",
        "budget_2026_total",
        "source_sheet",
    ]
    cols = [c for c in cols if c in regions_df.columns]

    customers = regions_df[cols].drop_duplicates(subset=["customer_code"], keep="first").copy()
    customers["source_file_name"] = source_file_name
    customers["extract_type"] = "us_budget_v2_customer_snapshot"
    return customers


def _build_monthly_budget(regions_df: pd.DataFrame, month_cols: list[str], version_label: str, source_file_name: str) -> pd.DataFrame:
    if not month_cols:
        raise ValueError("No monthly columns detected in workbook regional tabs")

    id_vars = [
        c for c in [
            "customer_code",
            "customer_name",
            "customer_region",
            "sales_person",
            "cluster_2026",
            "growth_class",
            "source_sheet",
        ]
        if c in regions_df.columns
    ]

    monthly = regions_df.melt(
        id_vars=id_vars,
        value_vars=month_cols,
        var_name="budget_month",
        value_name="budget_amount_usd",
    ).copy()

    monthly["budget_month"] = pd.to_datetime(monthly["budget_month"], errors="coerce")
    monthly["budget_amount_usd"] = pd.to_numeric(monthly["budget_amount_usd"], errors="coerce")

    monthly = monthly.dropna(subset=["budget_month", "budget_amount_usd"]).copy()
    monthly["version_label"] = version_label
    monthly["currency_code"] = "USD"
    monthly["source_file_name"] = source_file_name
    monthly["extract_type"] = "us_budget_v2_monthly"
    monthly["load_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Guard: one row per customer_code + month + version.
    monthly = monthly.drop_duplicates(
        subset=["customer_code", "budget_month", "version_label"],
        keep="last",
    )

    return monthly.sort_values(["customer_code", "budget_month"]).reset_index(drop=True)


def _build_output_paths(output_root: Path, version_label: str) -> OutputPaths:
    out_dir = output_root / version_label
    out_dir.mkdir(parents=True, exist_ok=True)
    return OutputPaths(
        customer_parquet=out_dir / "us_budget_customer_v2.parquet",
        customer_csv=out_dir / "us_budget_customer_v2.csv",
        monthly_parquet=out_dir / "us_budget_monthly_v2.parquet",
        monthly_csv=out_dir / "us_budget_monthly_v2.csv",
    )


def transform(
    xlsx_path: str | Path | None = None,
    output_root: str | Path | None = None,
    version_label: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Run cold conversion of the US budget v0 workbook into canonical v2 files."""
    source_path = Path(xlsx_path) if xlsx_path else _default_input_path()
    out_root = Path(output_root) if output_root else _default_output_root()

    if not source_path.exists():
        return {
            "status": "error",
            "message": f"Input workbook not found: {source_path}",
        }

    if not version_label:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        version_label = f"us_budget_v2_{stamp}"

    frames: list[pd.DataFrame] = []
    month_cols_union: set[str] = set()

    for sheet in REGION_SHEETS:
        df_raw, month_cols = _read_region_sheet(source_path, sheet)
        cleaned = _clean_customer_rows(df_raw)
        frames.append(cleaned)
        month_cols_union.update(month_cols)
        log.info("Budget cold: %s -> %d customer rows, %d month cols", sheet, len(cleaned), len(month_cols))

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["customer_code", "source_sheet"], keep="first")

    monthly = _build_monthly_budget(
        combined,
        month_cols=sorted(month_cols_union),
        version_label=version_label,
        source_file_name=source_path.name,
    )
    customer = _build_customer_snapshot(combined, source_file_name=source_path.name)

    paths = _build_output_paths(out_root, version_label)

    if not dry_run:
        customer.to_parquet(paths.customer_parquet, index=False)
        customer.to_csv(paths.customer_csv, index=False)
        monthly.to_parquet(paths.monthly_parquet, index=False)
        monthly.to_csv(paths.monthly_csv, index=False)

    return {
        "status": "dry_run" if dry_run else "ok",
        "pipeline": "us_budget_v0_cold",
        "source_file": str(source_path),
        "version_label": version_label,
        "customer_rows": int(len(customer)),
        "monthly_rows": int(len(monthly)),
        "months_detected": sorted(month_cols_union),
        "outputs": {
            "customer_parquet": str(paths.customer_parquet),
            "customer_csv": str(paths.customer_csv),
            "monthly_parquet": str(paths.monthly_parquet),
            "monthly_csv": str(paths.monthly_csv),
        },
    }
