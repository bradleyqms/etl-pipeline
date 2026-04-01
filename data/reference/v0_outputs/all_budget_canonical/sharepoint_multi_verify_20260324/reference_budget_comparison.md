# Reference Budget Comparison

These comparisons keep management/processed, USA regional, and Core GVL salesperson validations separate.
No values are overwritten; any source normalization used for comparison is recorded explicitly per comparison family.

## core_gvl_salesperson / core_markets_budget

- reference_file: budget_GVL_2026.csv
- comparison_grain: region_sub_region_salesperson_month
- matched_rows: 204
- canonical_only_rows: 0
- reference_only_rows: 0
- canonical_total_compare: 8314.72028078
- reference_total_compare: 8467.0
- delta_total_compare: -152.27971921999935
- normalization_method: canonical_amount_compare = canonical_amount_raw * CHF_TO_EUR_RATE(1.05)|region remap: France/Italy -> Italy/Italy

## processed_management / core_markets_budget

- reference_file: budget_2026_processed.csv
- comparison_grain: report_region_month
- matched_rows: 72
- canonical_only_rows: 0
- reference_only_rows: 0
- canonical_total_compare: 7560.0
- reference_total_compare: 8226.0
- delta_total_compare: -666.0
- normalization_method: 

## processed_management / export_budget

- reference_file: budget_2026_processed.csv
- comparison_grain: report_region_month
- matched_rows: 120
- canonical_only_rows: 0
- reference_only_rows: 0
- canonical_total_compare: 3091.0
- reference_total_compare: 3091.0
- delta_total_compare: 0.0
- normalization_method: 

## processed_management / uk_budget

- reference_file: budget_2026_processed.csv
- comparison_grain: report_region_month
- matched_rows: 36
- canonical_only_rows: 0
- reference_only_rows: 0
- canonical_total_compare: 567.0
- reference_total_compare: 534.0
- delta_total_compare: 33.0
- normalization_method: 

## processed_management / nan

- reference_file: budget_2026_processed.csv
- comparison_grain: report_region_month
- matched_rows: 0
- canonical_only_rows: 0
- reference_only_rows: 60
- canonical_total_compare: 0.0
- reference_total_compare: 2270.0
- delta_total_compare: 0.0
- normalization_method: 

## usa_region / us_budget

- reference_file: budget_USA_spa_2026.csv
- comparison_grain: region_month
- matched_rows: 49
- canonical_only_rows: 11
- reference_only_rows: 0
- canonical_total_compare: 1137.8637166496233
- reference_total_compare: 1224.0
- delta_total_compare: -163.94608162127003
- normalization_method: reference_amount_compare = Value_kUSD / 1000|region swap fix: USA West <-> Southeast|reference_amount_compare = Value_kUSD / 1000
