"""Validate gold star-schema outputs and produce sales-prep spot-check workbook."""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from dotenv import load_dotenv

from src.core.blob_client import get_container_client

load_dotenv(Path(__file__).resolve().parent / ".env")

CUSTOMER_CODE = "10049"


def _read_parquet_blob(client, blob_path: str) -> pd.DataFrame:
    data = client.get_blob_client(blob_path).download_blob().readall()
    return pq.read_table(io.BytesIO(data)).to_pandas()


def _pct(n: int, d: int) -> float:
    if d == 0:
        return 0.0
    return (n / d) * 100.0


def main() -> None:
    client = get_container_client(container="bronze")

    tables = {
        "fact_sales": _read_parquet_blob(client, "gold/fact_sales.parquet"),
        "fact_budget": _read_parquet_blob(client, "gold/fact_budget.parquet"),
        "dim_customer": _read_parquet_blob(client, "gold/dim_customer.parquet"),
        "dim_product": _read_parquet_blob(client, "gold/dim_product.parquet"),
        "dim_salesperson": _read_parquet_blob(client, "gold/dim_salesperson.parquet"),
        "dim_date": _read_parquet_blob(client, "gold/dim_date.parquet"),
    }

    fact_sales = tables["fact_sales"].copy()
    fact_budget = tables["fact_budget"].copy()
    dim_customer = tables["dim_customer"].copy()
    dim_product = tables["dim_product"].copy()

    fact_sales["doc_date"] = pd.to_datetime(fact_sales["doc_date"], errors="coerce")
    fact_sales["year"] = fact_sales["doc_date"].dt.year
    fact_sales["month"] = fact_sales["doc_date"].dt.month

    print("=== Gold Row Counts ===")
    for name, df in tables.items():
        print(f"{name:16} {len(df):>10,}")

    print("\n=== Fact Sales Key Fill % ===")
    fs_rows = len(fact_sales)
    print(f"customer_key    {_pct(fact_sales['customer_key'].notna().sum(), fs_rows):6.2f}%")
    print(f"product_key     {_pct(fact_sales['product_key'].notna().sum(), fs_rows):6.2f}%")
    print(f"salesperson_key {_pct(fact_sales['salesperson_key'].notna().sum(), fs_rows):6.2f}%")

    print("\n=== Referential Integrity (orphan rows) ===")
    cust_keys = set(tables["dim_customer"]["customer_key"].dropna().tolist())
    prod_keys = set(tables["dim_product"]["product_key"].dropna().tolist())
    slp_keys = set(tables["dim_salesperson"]["salesperson_key"].dropna().tolist())
    # Exclude null FKs from orphan count — null is reported separately via fill % above
    fs_cust = fact_sales["customer_key"].dropna()
    fs_prod = fact_sales["product_key"].dropna()
    fs_slp  = fact_sales["salesperson_key"].dropna()
    print(f"customer_key orphans: {(~fs_cust.isin(cust_keys)).sum():,}  (null: {fact_sales['customer_key'].isna().sum():,})")
    print(f"product_key orphans : {(~fs_prod.isin(prod_keys)).sum():,}  (null: {fact_sales['product_key'].isna().sum():,})")
    print(f"salesperson orphans : {(~fs_slp.isin(slp_keys)).sum():,}  (null: {fact_sales['salesperson_key'].isna().sum():,})")

    customer_sales = fact_sales[fact_sales["card_code"].astype(str) == CUSTOMER_CODE].copy()
    customer_budget = fact_budget[fact_budget["customer_code"].astype(str) == CUSTOMER_CODE].copy()

    sales_by_year = (
        customer_sales.groupby("year", dropna=True)["revenue_eur"]
        .sum()
        .reset_index(name="sales_eur")
        .sort_values("year")
    )
    budget_by_year = (
        customer_budget.assign(year=pd.to_datetime(customer_budget["budget_month"], errors="coerce").dt.year)
        .groupby("year", dropna=True)["budget_amount_eur"]
        .sum()
        .reset_index(name="budget_eur")
    )
    year_compare = sales_by_year.merge(budget_by_year, on="year", how="outer").fillna(0)
    year_compare["vs_budget_eur"] = year_compare["sales_eur"] - year_compare["budget_eur"]

    sales_by_month = (
        customer_sales[customer_sales["year"].isin([2025, 2026])]
        .groupby(["year", "month"], dropna=True)["revenue_eur"]
        .sum()
        .reset_index()
        .pivot(index="month", columns="year", values="revenue_eur")
        .reset_index()
        .rename(columns={2025: "sales_2025_eur", 2026: "sales_2026_eur"})
    )

    budget_by_month = (
        customer_budget.assign(
            year=pd.to_datetime(customer_budget["budget_month"], errors="coerce").dt.year,
            month=pd.to_datetime(customer_budget["budget_month"], errors="coerce").dt.month,
        )
        .query("year == 2026")
        .groupby("month", dropna=True)["budget_amount_eur"]
        .sum()
        .reset_index(name="budget_2026_eur")
    )
    month_compare = sales_by_month.merge(budget_by_month, on="month", how="left").fillna(0)
    if "sales_2026_eur" not in month_compare.columns:
        month_compare["sales_2026_eur"] = 0.0
    month_compare["vs_budget_2026_eur"] = month_compare["sales_2026_eur"] - month_compare["budget_2026_eur"]

    product_mix = (
        customer_sales.merge(dim_product[["product_key", "sku_channel"]], on="product_key", how="left")
        .assign(group=lambda d: d["sku_channel"].fillna("Other"))
        .groupby(["year", "group"], dropna=True)["revenue_eur"]
        .sum()
        .reset_index()
    )

    total_per_year = product_mix.groupby("year", dropna=True)["revenue_eur"].sum().rename("year_total")
    product_mix = product_mix.merge(total_per_year, on="year", how="left")
    product_mix["pct_split"] = product_mix["revenue_eur"] / product_mix["year_total"]

    top10_revenue = (
        customer_sales[customer_sales["year"].isin([2025, 2026])]
        .groupby(["year", "item_code"], dropna=True)["revenue_eur"]
        .sum()
        .reset_index()
        .sort_values(["year", "revenue_eur"], ascending=[True, False])
        .groupby("year", group_keys=False)
        .head(10)
    )

    top10_units = (
        customer_sales[customer_sales["year"].isin([2025, 2026])]
        .groupby(["year", "item_code"], dropna=True)["quantity"]
        .sum()
        .reset_index()
        .sort_values(["year", "quantity"], ascending=[True, False])
        .groupby("year", group_keys=False)
        .head(10)
    )

    purchased_2026 = set(customer_sales.loc[customer_sales["year"] == 2026, "item_code"].astype(str).tolist())
    sellable = dim_product[dim_product["is_sellable"].fillna(False)]
    not_purchased = sellable[~sellable["item_code"].astype(str).isin(purchased_2026)][
        ["entity", "item_code", "name_en", "product_line_clean", "product_category"]
    ].copy()

    customer_info = dim_customer[dim_customer["card_code"].astype(str) == CUSTOMER_CODE][
        ["entity", "card_code", "card_name", "market_group", "region", "channel"]
    ].drop_duplicates()

    out_path = Path(f"gold_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        customer_info.to_excel(writer, sheet_name="customer_info", index=False)
        year_compare.to_excel(writer, sheet_name="sales_by_year", index=False)
        month_compare.to_excel(writer, sheet_name="sales_by_month", index=False)
        product_mix.to_excel(writer, sheet_name="product_mix", index=False)
        top10_revenue.to_excel(writer, sheet_name="top10_revenue", index=False)
        top10_units.to_excel(writer, sheet_name="top10_units", index=False)
        not_purchased.to_excel(writer, sheet_name="not_purchased", index=False)

    print(f"\nValidation workbook written: {out_path}")


if __name__ == "__main__":
    main()
