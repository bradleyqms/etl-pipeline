# Budget Canonical Review Outputs

This folder contains verification-friendly canonical outputs generated from the SharePoint budget workbooks.

## Structure

- workbook_catalog.csv: workbook-level inventory with classification and sheet names.
- report_budget_monthly_canonical.parquet/csv: combined report-grain monthly budget facts when combined outputs are enabled.
- regional_validation_summary.csv: machine-readable validation summary by regional view.
- regional_validation_summary.md: human-readable validation summary by regional view.
- reference_budget_comparison.csv: machine-readable comparison against budget reference CSVs.
- reference_budget_comparison.md: human-readable comparison summary against budget reference CSVs.
- budget_pbix_fact.csv: unified customer-month budget fact table for PBIX (2026 rows).
- budget_sources_inventory.csv: source inventory for grain/currency/monthly/salesperson availability.
- budget_qa_market_month_summary.csv: tiny QA table with row counts by market/month for refresh regression checks.
- regional_views/: split outputs for each workbook family used for review.

## Regional Views

- us_budget: US report-grain monthly budget facts plus customer-detail review outputs.
- uk_budget: UK report-grain monthly budget facts plus customer-detail review outputs.
- core_markets_budget: Core Markets report-grain monthly budget facts plus customer-detail review outputs.
- export_budget: Export report-grain monthly budget facts sourced from the group budget workbook plus customer-detail review outputs.
- group_budget: Group planning lines captured as row-level JSON payloads.

## Run Mode

- combined_outputs_included: False
- default behavior is split-only for review runs; combined outputs are optional.

## Generated Views

- us_budget: {"report_monthly_rows": 75, "sales_monthly_rows": 3783, "sales_customer_rows": 74}
- uk_budget: {"report_monthly_rows": 45, "sales_monthly_rows": 387, "sales_customer_rows": 25}
- core_markets_budget: {"report_monthly_rows": 105, "sales_monthly_rows": 14904, "sales_customer_rows": 733}
- export_budget: {"report_monthly_rows": 123, "sales_monthly_rows": 215, "sales_customer_rows": 49}
- group_budget: {"group_lines_rows": 2571}
- ecommerce_budget: {"report_monthly_rows": 60}