"""Tests for US budget v0 cold conversion pipeline."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.transforms.us_budget_v0_to_parquet import REGION_SHEETS, transform


def _build_region_df(sheet_name: str, customer_code: int) -> pd.DataFrame:
    """Create a minimal region sheet matching the observed workbook shape."""
    return pd.DataFrame(
        {
            "Unnamed: 0": [None, None],
            "Customer Code": [customer_code, "Total Active Existing Doors"],
            "Customer Name": [f"{sheet_name} Spa", "Total"],
            "Region": ["CA", None],
            "Sales person": ["Amy", None],
            "New door year": [2025, None],
            "Active?": ["Yes", None],
            "2025 Cluster": ["A", None],
            "2026 Cluster": ["A+", None],
            "2025A": [20000, None],
            "2026B": [30000, None],
            "Size indicator": [30000, None],
            "Customer Growth Class": ["Grow", None],
            datetime(2026, 1, 1): [2500, None],
            datetime(2026, 2, 1): [2600, None],
            datetime(2026, 3, 1): [2700, None],
        }
    )


def _write_test_workbook(path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for i, sheet in enumerate(REGION_SHEETS, start=1):
            # Start at row 2 so header lands on row index 2, matching real workbook.
            _build_region_df(sheet, customer_code=25000 + i).to_excel(
                xw,
                sheet_name=sheet,
                index=False,
                startrow=1,
            )


class TestUsBudgetV0ColdTransform:

    def test_transform_creates_canonical_files(self, tmp_path: Path):
        xlsx = tmp_path / "budget_v0.xlsx"
        _write_test_workbook(xlsx)

        out_root = tmp_path / "v0_outputs"
        result = transform(
            xlsx_path=xlsx,
            output_root=out_root,
            version_label="unit_test_v2",
            dry_run=False,
        )

        assert result["status"] == "ok"
        assert result["customer_rows"] == len(REGION_SHEETS)
        assert result["monthly_rows"] == len(REGION_SHEETS) * 3

        monthly_path = Path(result["outputs"]["monthly_parquet"])
        customer_path = Path(result["outputs"]["customer_parquet"])
        assert monthly_path.exists()
        assert customer_path.exists()

        monthly = pd.read_parquet(monthly_path)
        customer = pd.read_parquet(customer_path)

        # Only real customer rows should remain (totals removed).
        assert customer["customer_code"].nunique() == len(REGION_SHEETS)
        assert not customer["customer_name"].astype(str).str.lower().str.startswith("total").any()

        # Month normalization + metadata should exist.
        assert set(monthly["currency_code"].unique()) == {"USD"}
        assert monthly["budget_month"].notna().all()
        assert monthly["budget_amount_usd"].notna().all()
        assert set(monthly["source_sheet"].unique()) == set(REGION_SHEETS)

    def test_dry_run_does_not_write_files(self, tmp_path: Path):
        xlsx = tmp_path / "budget_v0.xlsx"
        _write_test_workbook(xlsx)

        out_root = tmp_path / "v0_outputs"
        result = transform(
            xlsx_path=xlsx,
            output_root=out_root,
            version_label="dry_run_test",
            dry_run=True,
        )

        assert result["status"] == "dry_run"
        assert result["customer_rows"] == len(REGION_SHEETS)
        assert result["monthly_rows"] == len(REGION_SHEETS) * 3

        assert not Path(result["outputs"]["customer_parquet"]).exists()
        assert not Path(result["outputs"]["monthly_parquet"]).exists()
