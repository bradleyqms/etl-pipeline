"""Tests for multi-workbook budget canonical builder."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.transforms.budget_multi_workbook_canonical import build_canonical


def _write_matrix_sheet(xw: pd.ExcelWriter, sheet_name: str, rows: list[list[object]]) -> None:
    width = max(len(row) for row in rows)
    normalized = [row + [None] * (width - len(row)) for row in rows]
    pd.DataFrame(normalized).to_excel(xw, sheet_name=sheet_name, index=False, header=False)


def _write_us_like_workbook(path: Path) -> None:
    regional = pd.DataFrame(
        {
            "Customer Code": [25001, 25002],
            "Customer Name": ["A Spa", "B Spa"],
            "Region": ["CA", "NY"],
            pd.Timestamp("2026-01-01"): [1000, 1500],
            pd.Timestamp("2026-02-01"): [1100, 1400],
        }
    )
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        regional.to_excel(xw, sheet_name="Northeast", index=False, startrow=1)
        regional.to_excel(xw, sheet_name="Central", index=False, startrow=1)
        pd.DataFrame({"x": [1]}).to_excel(xw, sheet_name="Summary", index=False)


def _write_group_like_workbook(path: Path) -> None:
    init = pd.DataFrame(
        {
            "Initiative": ["Brand Visibility"],
            "Segment": ["Core"],
            "Monthly": ["5000"],
        }
    )
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        init.to_excel(xw, sheet_name="Initiative Log - Detailed", index=False, startrow=2)
        pd.DataFrame({"x": [1]}).to_excel(xw, sheet_name="Definition", index=False)


def _write_uk_report_like_workbook(path: Path) -> None:
    summary_rows = [
        [None] * 24,
        [None, "EUR", "Full Year"],
        [None, None, "25A", "25B", "25F vs 25B", "24A", "25F vs 24A", None, "26B", "25A", "26B vs 25F", None, pd.Timestamp("2026-01-01"), pd.Timestamp("2026-02-01"), pd.Timestamp("2026-03-01")],
        [None, "Spa", 0, 0, 0, 0, 0, None, 0, 0, 0, None, 19000.0, 23000.0, 25600.0],
        [None, "Retail", 0, 0, 0, 0, 0, None, 0, 0, 0, None, 14000.0, 14000.0, 16200.0],
        [None, "Global eTailer", 0, 0, 0, 0, 0, None, 0, 0, 0, None, 3000.0, 3000.0, 2900.0],
    ]
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        _write_matrix_sheet(xw, "Summary", summary_rows)
        pd.DataFrame({"Customer Code": [1], pd.Timestamp("2026-01-01"): [1]}).to_excel(xw, sheet_name="UK Spa Budget", index=False)
        pd.DataFrame({"Customer Code": [2], pd.Timestamp("2026-01-01"): [1]}).to_excel(xw, sheet_name="UK Retail Budget", index=False)


def _write_core_report_like_workbook(path: Path) -> None:
    summary_rows = [
        [None] * 30,
        [None, "EUR", "Full Year", None, None, None, None, None, "YTD", None, None, None, None, None, "Full Year", None, None, None, "Total sales"],
        [None, None, "25A", "25B", "25F vs 25B", "24A", "25F vs 24A", None, "25A", "25B", "25A vs 25B", "24A", "25A vs 24A", None, "26B", "25A", "26B vs 25F", None, pd.Timestamp("2026-01-01"), pd.Timestamp("2026-02-01"), pd.Timestamp("2026-03-01")],
        [None, "North", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 70400.0],
        [None, "North East", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 44500.0],
        [None, "Bayern", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 64900.0],
        [None, "South West", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 189400.0],
        [None, "Retail", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 20000.0],
        [None, "DE Other", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 11700.0],
        [None, "NL Central", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 99400.0],
        [None, "NL Other + BL", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 106500.0],
        [None, "NL Other", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 5300.0],
        [None, "German Switzerland", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 73200.0],
        [None, "French Switzerland", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 10000.0],
        [None, "Other Switzerland", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 0.0],
        [None, "Spain", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 21500.0],
        [None, "France North", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 7500.0],
        [None, "France South", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 9000.0],
        [None, "Italy", 0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, None, 0, 0, 0, None, 0.0, 0.0, 5500.0],
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
        [None] * 30,
    ]

    nrw_marina_rows = [
        [None, None, None, None, None, None, None, None, None, None, pd.Timestamp("2026-01-01"), pd.Timestamp("2026-02-01"), pd.Timestamp("2026-03-01")],
        [None, "Total Active Existing Doors", None, None, None, None, None, None, None, None, 42938.61, 42938.61, 53673.26],
        [None, "Total New Doors 2025", None, None, None, None, None, None, None, None, 4592.37, 4592.37, 5740.46],
        [None, "Total New Doors 2026", None, None, None, None, None, None, None, None, 0.0, 4000.0, 4000.0],
    ]

    nrw_ulrike_rows = [
        [None, None, None, None, None, None, None, None, None, None, pd.Timestamp("2026-01-01"), pd.Timestamp("2026-02-01"), pd.Timestamp("2026-03-01")],
        [None, "Total Active Existing Doors", None, None, None, None, None, None, None, None, 2774.0, 2774.0, 2774.0],
        [None, "Total New Doors 2025", None, None, None, None, None, None, None, None, 3000.0, 3000.0, 3000.0],
        [None, "Total New Doors 2026", None, None, None, None, None, None, None, None, 0.0, 0.0, 3000.0],
    ]

    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        _write_matrix_sheet(xw, "Summary", summary_rows)
        _write_matrix_sheet(xw, "NRW Marina", nrw_marina_rows)
        _write_matrix_sheet(xw, "NRW Ulrike", nrw_ulrike_rows)
        pd.DataFrame({"x": [1]}).to_excel(xw, sheet_name="Region Review", index=False)


def _write_group_report_like_workbook(path: Path) -> None:
    rows = [
        [None] * 24,
        [None, None, "Full Year", "Full Year"],
        [None, "kEUR", "2026B", "2025A", "26B vs 25A", None, None, None, None, None, None, None, pd.Timestamp("2026-01-01"), pd.Timestamp("2026-02-01"), pd.Timestamp("2026-03-01")],
        [None, "Distributor - Austria", 430.0, 448.0, 0.0, None, None, None, None, None, None, None, 38.7, 38.7, 38.7],
        [None, "Distributor - South Africa", 625.0, 568.0, 0.0, None, None, None, None, None, None, None, 0.0, 100.0, 75.0],
        [None, "Distributor - Russia", 350.0, 277.0, 0.0, None, None, None, None, None, None, None, 0.0, 70.0, 0.0],
        [None, "Distributor - Other EU", 457.5, 444.0, 0.0, None, None, None, None, None, None, None, 36.6, 32.0, 45.75],
        [None, "Distributor - Other ROW", 30.0, 35.0, 0.0, None, None, None, None, None, None, None, 2.4, 2.1, 3.0],
        [None, "Distributor - New", 20.0, 15.0, 0.0, None, None, None, None, None, None, None, 0.0, 0.0, 0.0],
        [None, "Export - Direct business", 32.0, 32.0, 0.0, None, None, None, None, None, None, None, 2.56, 2.56, 2.88],
        [None, "Distributor - China", 800.0, 501.0, 0.0, None, None, None, None, None, None, None, 24.0, 24.0, 26.4],
        [None, "Distributor - Middle East", 200.0, 106.0, 0.0, None, None, None, None, None, None, None, 18.0, 18.0, 18.0],
        [None, "Distributor - APAC", 145.0, 0.0, 0.0, None, None, None, None, None, None, None, 0.0, 0.0, 0.0],
    ]
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        _write_matrix_sheet(xw, "2026B", rows)
        pd.DataFrame({"x": [1]}).to_excel(xw, sheet_name="Initiative Log - Detailed", index=False)


def _write_core_salesperson_like_workbook(path: Path) -> None:
    north = pd.DataFrame(
        {
            "Customer Code": [20097, 20263],
            "Customer Name": ["Heidi Soenksen", "Brigitte Preikschat"],
            "Country": ["Germany", "Germany"],
            "Region": ["North", "North"],
            pd.Timestamp("2026-01-01"): [20000.0, 26000.0],
            pd.Timestamp("2026-02-01"): [24000.0, 32000.0],
        }
    )
    summary = pd.DataFrame({"x": [1]})
    region_review = pd.DataFrame({"x": [1]})
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        north.to_excel(xw, sheet_name="North", index=False, startrow=1)
        summary.to_excel(xw, sheet_name="Summary", index=False)
        region_review.to_excel(xw, sheet_name="Region Review", index=False)


def _write_reference_budget_csvs(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "Year": 2026,
                "Month": "01/01/2026",
                "Market_Group": "UK",
                "Region": "Spa",
                "Channel_Level": "Unspecified",
                "Subchannel / Partner": None,
                "Sales Employee / Account": None,
                "Company_Group": "Company 1",
                "Currency": "EUR",
                "Metric": "Budget",
                "Value_kEUR": 19,
                "Value_EUR": 19000,
                "Date": "01/01/2026",
            },
            {
                "Year": 2026,
                "Month": "01/01/2026",
                "Market_Group": "UK",
                "Region": "Retail",
                "Channel_Level": "Unspecified",
                "Subchannel / Partner": None,
                "Sales Employee / Account": None,
                "Company_Group": "Company 1",
                "Currency": "EUR",
                "Metric": "Budget",
                "Value_kEUR": 14,
                "Value_EUR": 14000,
                "Date": "01/01/2026",
            },
            {
                "Year": 2026,
                "Month": "01/01/2026",
                "Market_Group": "UK",
                "Region": "Global eTailers",
                "Channel_Level": "Unspecified",
                "Subchannel / Partner": None,
                "Sales Employee / Account": None,
                "Company_Group": "Company 3",
                "Currency": "EUR",
                "Metric": "Budget",
                "Value_kEUR": 3,
                "Value_EUR": 3000,
                "Date": "01/01/2026",
            },
        ]
    ).to_csv(path / "budget_2026_processed.csv", index=False)

    pd.DataFrame(
        [
            {
                "Year": 2026,
                "Month": 1,
                "Date": "01/01/2026",
                "Market_Group": "Core Markets",
                "Region": "Germany",
                "Channel_Level": None,
                "Subchannel / Partner": None,
                "Sub Region": "North",
                "Sales Employee / Account": "Kerstin",
                "Company_Group": "Company 1",
                "Currency": "EUR",
                "Metric": "Budget",
                "Value_kEUR": 46,
                " Value_EUR ": 46000,
                "Existing_Budget_EUR": 46000,
                "New_Budget_EUR": 0,
            },
            {
                "Year": 2026,
                "Month": 2,
                "Date": "01/02/2026",
                "Market_Group": "Core Markets",
                "Region": "Germany",
                "Channel_Level": None,
                "Subchannel / Partner": None,
                "Sub Region": "North",
                "Sales Employee / Account": "Kerstin",
                "Company_Group": "Company 1",
                "Currency": "EUR",
                "Metric": "Budget",
                "Value_kEUR": 56,
                " Value_EUR ": 56000,
                "Existing_Budget_EUR": 56000,
                "New_Budget_EUR": 0,
            },
        ]
    ).to_csv(path / "budget_GVL_2026.csv", index=False)

    pd.DataFrame(
        [
            {
                "Year": 2026,
                "Month": 1,
                "Date": "01/01/2026",
                "Market_Group": "USA",
                "Region": "Northeast",
                "Channel_Level": None,
                "Subchannel / Partner": None,
                "Sales Employee / Account": None,
                "Company_Group": "Company 1",
                "Currency": "USD",
                "Metric": "Budget",
                "Value_kUSD": 2500,
                "Value_kEUR": None,
                "Value_EUR": None,
            },
            {
                "Year": 2026,
                "Month": 2,
                "Date": "01/02/2026",
                "Market_Group": "USA",
                "Region": "Northeast",
                "Channel_Level": None,
                "Subchannel / Partner": None,
                "Sales Employee / Account": None,
                "Company_Group": "Company 1",
                "Currency": "USD",
                "Metric": "Budget",
                "Value_kUSD": 2500,
                "Value_kEUR": None,
                "Value_EUR": None,
            },
            {
                "Year": 2026,
                "Month": 1,
                "Date": "01/01/2026",
                "Market_Group": "USA",
                "Region": "Central",
                "Channel_Level": None,
                "Subchannel / Partner": None,
                "Sales Employee / Account": None,
                "Company_Group": "Company 1",
                "Currency": "USD",
                "Metric": "Budget",
                "Value_kUSD": 2500,
                "Value_kEUR": None,
                "Value_EUR": None,
            },
            {
                "Year": 2026,
                "Month": 2,
                "Date": "01/02/2026",
                "Market_Group": "USA",
                "Region": "Central",
                "Channel_Level": None,
                "Subchannel / Partner": None,
                "Sales Employee / Account": None,
                "Company_Group": "Company 1",
                "Currency": "USD",
                "Metric": "Budget",
                "Value_kUSD": 2500,
                "Value_kEUR": None,
                "Value_EUR": None,
            },
        ]
    ).to_csv(path / "budget_USA_spa_2026.csv", index=False)


class TestBuildCanonical:

    def test_builds_split_outputs_by_default_for_mixed_workbooks(self, tmp_path: Path):
        in_dir = tmp_path / "in"
        in_dir.mkdir()

        _write_us_like_workbook(in_dir / "2026.01.22 US Spa Sales Budget 2026 FINAL.xlsx")
        _write_group_like_workbook(in_dir / "22025.11.06 QMS Detailed Budget v1.xlsx")

        out_root = tmp_path / "out"
        result = build_canonical(
            input_dir=in_dir,
            output_root=out_root,
            version_label="unit_multi",
            dry_run=False,
        )

        assert result["status"] == "ok"
        assert result["include_combined"] is False
        assert result["workbook_count"] == 2
        assert result["sales_monthly_rows"] > 0
        assert result["group_lines_rows"] > 0
        assert set(result["regional_views"]) == {"us_budget", "group_budget"}
        assert "sales_monthly_csv" not in result["outputs"]
        assert not (out_root / "unit_multi" / "sales_budget_monthly_canonical.csv").exists()
        assert not (out_root / "unit_multi" / "sales_budget_monthly_canonical.parquet").exists()
        assert Path(result["regional_views"]["us_budget"]["outputs"]["sales_monthly"]["csv"]).name == "us_sales_budget_monthly_canonical.csv"
        assert Path(result["regional_views"]["group_budget"]["outputs"]["group_lines"]["csv"]).name == "group_budget_lines_canonical.csv"

        catalog = pd.read_csv(result["outputs"]["catalog_csv"])
        us_sales = pd.read_csv(result["regional_views"]["us_budget"]["outputs"]["sales_monthly"]["csv"])
        group_lines = pd.read_csv(result["regional_views"]["group_budget"]["outputs"]["group_lines"]["csv"])
        validation = pd.read_csv(result["outputs"]["validation_summary_csv"])
        readme = Path(result["outputs"]["readme_md"]).read_text(encoding="utf-8")

        assert "workbook_type" in catalog.columns
        assert set(catalog["workbook_type"]) == {"us_budget", "group_budget"}
        assert us_sales["budget_amount"].notna().all()
        assert set(us_sales["workbook_type"]) == {"us_budget"}
        assert set(group_lines["workbook_type"]) == {"group_budget"}
        assert set(validation["regional_view"]) == {"us_budget", "group_budget"}
        assert "split-only for review runs" in readme

    def test_can_optionally_write_combined_outputs(self, tmp_path: Path):
        in_dir = tmp_path / "in"
        in_dir.mkdir()

        _write_us_like_workbook(in_dir / "2026.01.22 US Spa Sales Budget 2026 FINAL.xlsx")
        _write_group_like_workbook(in_dir / "22025.11.06 QMS Detailed Budget v1.xlsx")

        out_root = tmp_path / "out"
        result = build_canonical(
            input_dir=in_dir,
            output_root=out_root,
            version_label="unit_multi_combined",
            dry_run=False,
            include_combined=True,
        )

        assert result["include_combined"] is True
        assert "sales_monthly_parquet" in result["outputs"]
        assert Path(result["regional_views"]["us_budget"]["outputs"]["sales_customer"]["csv"]).name == "us_sales_budget_customer_canonical.csv"
        combined_sales = pd.read_parquet(result["outputs"]["sales_monthly_parquet"])
        assert combined_sales["budget_amount"].notna().all()

    def test_builds_report_budget_views_from_authoritative_workbook_sections(self, tmp_path: Path):
        in_dir = tmp_path / "in"
        in_dir.mkdir()

        _write_us_like_workbook(in_dir / "2026.01.22 US Spa Sales Budget 2026 FINAL.xlsx")
        _write_uk_report_like_workbook(in_dir / "2026.01.19 UK Sales Budget 2026 FINAL.xlsx")
        _write_core_report_like_workbook(in_dir / "2026.03.18 Core Markets Sales Budget 2026 New Regions FINAL.xlsx")
        _write_group_report_like_workbook(in_dir / "22025.11.06 QMS Detailed Budget v1.xlsx")

        result = build_canonical(
            input_dir=in_dir,
            output_root=tmp_path / "out",
            version_label="unit_report_views",
            dry_run=False,
        )

        assert result["status"] == "ok"
        assert set(result["regional_views"]) == {"us_budget", "uk_budget", "core_markets_budget", "export_budget", "group_budget"}

        uk_report = pd.read_csv(result["regional_views"]["uk_budget"]["outputs"]["report_monthly"]["csv"])
        core_report = pd.read_csv(result["regional_views"]["core_markets_budget"]["outputs"]["report_monthly"]["csv"])
        export_report = pd.read_csv(result["regional_views"]["export_budget"]["outputs"]["report_monthly"]["csv"])

        uk_march = uk_report.loc[uk_report["budget_month"] == "2026-03-01"]
        assert int(uk_march.loc[uk_march["region"] == "Spa", "budget_amount_report_k"].iloc[0]) == 16
        assert int(uk_march.loc[uk_march["region"] == "Retail", "budget_amount_report_k"].iloc[0]) == 26
        assert int(uk_march.loc[uk_march["region"] == "Global eTailers", "budget_amount_report_k"].iloc[0]) == 3

        core_march = core_report.loc[core_report["budget_month"] == "2026-03-01"]
        assert int(core_march.loc[core_march["region"] == "Germany", "budget_amount_report_k"].iloc[0]) == 470
        assert int(core_march.loc[core_march["region"] == "Benelux", "budget_amount_report_k"].iloc[0]) == 211
        assert int(core_march.loc[core_march["region"] == "Switzerland", "budget_amount_report_k"].iloc[0]) == 83

        export_march = export_report.loc[export_report["budget_month"] == "2026-03-01"]
        assert int(export_march.loc[export_march["region"] == "Distributor - Austria", "budget_amount_report_k"].iloc[0]) == 39
        assert int(export_march.loc[export_march["region"] == "Distributor - South Africa", "budget_amount_report_k"].iloc[0]) == 75
        assert int(export_march.loc[export_march["region"] == "Distributor - China", "budget_amount_report_k"].iloc[0]) == 26

    def test_writes_explicit_reference_comparisons_with_salesperson_and_normalization_metadata(self, tmp_path: Path):
        in_dir = tmp_path / "in"
        ref_dir = tmp_path / "refs"
        in_dir.mkdir()

        _write_us_like_workbook(in_dir / "2026.01.22 US Spa Sales Budget 2026 FINAL.xlsx")
        _write_uk_report_like_workbook(in_dir / "2026.01.19 UK Sales Budget 2026 FINAL.xlsx")
        _write_core_salesperson_like_workbook(in_dir / "2026.03.18 Core Markets Sales Budget 2026 New Regions FINAL.xlsx")
        _write_reference_budget_csvs(ref_dir)

        result = build_canonical(
            input_dir=in_dir,
            output_root=tmp_path / "out",
            version_label="unit_reference_compare",
            dry_run=False,
            reference_budget_dir=ref_dir,
        )

        comparison = pd.read_csv(result["outputs"]["reference_comparison_csv"])
        core_sales = pd.read_csv(result["regional_views"]["core_markets_budget"]["outputs"]["sales_monthly"]["csv"])
        validation = pd.read_csv(result["outputs"]["validation_summary_csv"])

        assert "sales_person" in core_sales.columns
        assert set(core_sales["sales_person"].dropna()) == {"Kerstin"}

        gvl_match = comparison[
            (comparison["comparison_family"] == "core_gvl_salesperson")
            & (comparison["sales_person"] == "Kerstin")
            & (comparison["sub_region"] == "North")
            & (comparison["match_status"] == "matched")
        ]
        assert not gvl_match.empty

        uk_match = comparison[
            (comparison["comparison_family"] == "processed_management")
            & (comparison["workbook_type"] == "uk_budget")
            & (comparison["match_status"] == "matched")
        ]
        assert not uk_match.empty
        assert set(uk_match["canonical_compare_basis"].dropna()) == {"uk_summary_eur_report_k"}

        usa_match = comparison[
            (comparison["comparison_family"] == "usa_region")
            & (comparison["match_status"] == "matched")
        ]
        assert not usa_match.empty
        assert set(usa_match["normalization_method"].dropna()) == {"reference_amount_compare = Value_kUSD / 1000"}

        core_validation = validation[validation["regional_view"] == "core_markets_budget"].iloc[0]
        assert int(core_validation["sales_monthly_null_sales_person_rows"]) == 0
