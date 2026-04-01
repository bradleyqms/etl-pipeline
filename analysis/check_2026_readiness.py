import io, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pandas as pd
from src.transforms.build_gold import get_container_client, GOLD_PATHS

c    = get_container_client()
fact = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["fact_sales"]).download_blob().readall()))
dc   = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["dim_customer"]).download_blob().readall()))
dp   = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["dim_product"]).download_blob().readall()))
dslp = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["dim_salesperson"]).download_blob().readall()))
bud  = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["fact_budget"]).download_blob().readall()))

fact["keur"] = fact["revenue_eur"] / 1000
y26 = fact[fact["doc_date"].dt.year == 2026].copy()
y25 = fact[fact["doc_date"].dt.year == 2025].copy()

print("=== 2026 YTD ACTUALS (Jan-Mar) ===")
print(f"rows: {len(y26):,}   total: {y26['keur'].sum():,.1f} kEUR")
print()
print("by entity:")
print(y26.groupby("entity")["keur"].sum().round(1).to_string())
print()
print("by month:")
print(y26.groupby(y26["doc_date"].dt.to_period("M"))["keur"].sum().round(1).to_string())
print()

# FK coverage 2026
total = len(y26)
print("=== 2026 FK COVERAGE ===")
for col in ["customer_key", "product_key", "salesperson_key"]:
    nulls = y26[col].isna().sum()
    keur  = y26[y26[col].isna()]["keur"].sum()
    print(f"  {col}: {nulls:,} nulls ({nulls/total*100:.1f}%)  =>  {keur:,.1f} kEUR unlinked")
print()

# Budget 2026
bud["year"] = pd.to_datetime(bud["budget_month"]).dt.year
b26 = bud[bud["year"] == 2026]
print("=== 2026 BUDGET ===")
print(f"rows: {len(b26):,}   total: {b26['budget_amount_eur'].sum()/1000:,.1f} kEUR")
print()
print("by entity:")
print(b26.groupby("entity")["budget_amount_eur"].sum().div(1000).round(1).to_string())
print()
print("by month (kEUR):")
print(b26.groupby("budget_month")["budget_amount_eur"].sum().div(1000).round(1).to_string())
print()

# Actuals vs budget for Jan-Mar 2026 where we have both
act_ytd = y26["keur"].sum()
bud_ytd_keur = b26[pd.to_datetime(b26["budget_month"]).dt.month <= 3]["budget_amount_eur"].sum() / 1000
print(f"=== 2026 YTD ACTUALS vs BUDGET (Jan-Mar) ===")
print(f"  Actuals YTD:  {act_ytd:,.1f} kEUR")
print(f"  Budget YTD:   {bud_ytd_keur:,.1f} kEUR")
print(f"  Gap:          {act_ytd - bud_ytd_keur:+,.1f} kEUR")
print()

# dim_customer unmapped in 2026
y26_dm = y26.merge(dc[["customer_key", "market_group", "region", "channel"]], on="customer_key", how="left")
unmap_cust = y26_dm[y26_dm["market_group"].isna()]
print(f"=== 2026 revenue with no market_group: {unmap_cust['keur'].sum():,.1f} kEUR ===")
if len(unmap_cust):
    print(unmap_cust.groupby(["entity", "card_code"])["keur"].sum().sort_values(ascending=False).head(10).to_string())
print()

# dim_salesperson unmapped in 2026
y26_slp = y26.merge(dslp[["salesperson_key", "display_name"]], on="salesperson_key", how="left")
unmap_slp = y26_slp[y26_slp["display_name"].isna()]
print(f"=== 2026 revenue with no slp display_name: {unmap_slp['keur'].sum():,.1f} kEUR ===")
if len(unmap_slp):
    print(unmap_slp.groupby(["entity", "slp_code"])["keur"].sum().sort_values(ascending=False).head(12).to_string())
print()

# Product coverage 2026
prod_null = y26[y26["product_key"].isna()]
print(f"=== 2026 product_key null: {prod_null['keur'].sum():,.1f} kEUR ===")
# Would entity-agnostic fix work?
gmbh_codes = set(dp["item_code"].astype(str))
prod_null2 = prod_null.copy()
prod_null2["ic_clean"] = prod_null2["item_code"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
recoverable = prod_null2[prod_null2["ic_clean"].isin(gmbh_codes)]
print(f"  recoverable via entity-agnostic join: {recoverable['keur'].sum():,.1f} kEUR")
print(f"  still unresolvable:                   {(prod_null2['keur'].sum() - recoverable['keur'].sum()):,.1f} kEUR")
print()

# slp display_name gap — which reps have the most 2026 revenue unmapped?
print("=== 2026 top unmapped salesperson revenue (for prioritising display_name fixes) ===")
print(unmap_slp.groupby(["entity", "slp_code"])["keur"].sum().sort_values(ascending=False).head(15).to_string())
