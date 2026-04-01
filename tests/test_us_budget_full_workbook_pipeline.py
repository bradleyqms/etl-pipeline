"""Tests for full-workbook US budget canonical transform."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.transforms.us_budget_workbook_to_canonical import transform


def _write_workbook(path: Path) -> None:
    definition = pd.DataFrame(
        {
            "Cluster": ["A+", "B", "Lost"],
            "Definition": ["Key", "Mid", "Lost door"],
        }
    )
    list_df = pd.DataFrame(
        {
            "YesNo": ["Yes", "No"],
            "Growth": ["Grow", "Lose"],
        }
    )
    summary = pd.DataFrame(
        {
            "Metric": ["Total", "Budget"],
            "Value": [100, 200],
        }
    )

    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        # startrow=1 mimics the source workbook where row 2 often acts as header
        definition.to_excel(xw, sheet_name="Definition", index=False, startrow=1)
        list_df.to_excel(xw, sheet_name="List", index=False, startrow=1)
        summary.to_excel(xw, sheet_name="Summary", index=False, startrow=1)


class TestUsBudgetFullWorkbookTransform:

    def test_full_workbook_outputs_written(self, tmp_path: Path):
        xlsx = tmp_path / "budget_v0.xlsx"
        _write_workbook(xlsx)

        out_root = tmp_path / "v0_outputs"
        result = transform(
            xlsx_path=xlsx,
            output_root=out_root,
            version_label="full_unit",
            dry_run=False,
        )

        assert result["status"] == "ok"
        assert result["sheet_count"] == 3
        assert result["sheet_row_records"] > 0

        sheet_rows = pd.read_parquet(result["outputs"]["sheet_rows_parquet"])
        definition_map = pd.read_parquet(result["outputs"]["definition_parquet"])

        assert set(sheet_rows["sheet_name"].unique()) == {"Definition", "List", "Summary"}
        assert "extract_type" in sheet_rows.columns
        assert len(definition_map) >= 1

    def test_dry_run_does_not_write_files(self, tmp_path: Path):
        xlsx = tmp_path / "budget_v0.xlsx"
        _write_workbook(xlsx)

        out_root = tmp_path / "v0_outputs"
        result = transform(
            xlsx_path=xlsx,
            output_root=out_root,
            version_label="full_dry",
            dry_run=True,
        )

        assert result["status"] == "dry_run"
        assert not Path(result["outputs"]["sheet_rows_parquet"]).exists()
        assert not Path(result["outputs"]["profiles_json"]).exists()
