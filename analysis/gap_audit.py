import io, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pandas as pd
from src.transforms.build_gold import get_container_client, GOLD_PATHS

c = get_container_client()
fact  = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["fact_sales"]).download_blob().readall()))
dc    = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["dim_customer"]).download_blob().readall()))
dp    = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["dim_product"]).download_blob().readall()))
dslp  = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["dim_salesperson"]).download_blob().readall()))
bud   = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["fact_budget"]).download_blob().readall()))

# ── Dataset size ─────────────────────────────────────────────────────
print("=== DATASET SIZE ===")
for name, df in [("fact_sales",fact),("fact_budget",bud),("dim_customer",dc),("dim_product",dp),("dim_salesperson",dslp)]:
    print(f"  {name}: {len(df):,} rows x {len(df.columns)} cols")
print()

print("=== FACT_SALES DATE RANGE ===")
print(f"  {fact['doc_date'].min().date()}  ->  {fact['doc_date'].max().date()}")
print()

print("=== REVENUE BY YEAR (all entities, kEUR) ===")
fact["year"] = fact["doc_date"].dt.year
print(fact.groupby("year")["revenue_eur"].sum().div(1000).round(1).to_string())
print()

print("=== REVENUE BY ENTITY 2025 (kEUR) ===")
y25 = fact[fact["year"]==2025]
print(y25.groupby("entity")["revenue_eur"].sum().div(1000).round(1).to_string())
print()

# ── FK coverage in fact_sales ────────────────────────────────────────
total = len(fact)
total_keur = fact["revenue_eur"].sum()/1000
print("=== FACT_SALES FK NULL COVERAGE ===")
print(f"  total rows: {total:,}   total kEUR: {total_keur:,.1f}")
for col in ["customer_key", "product_key", "salesperson_key", "date_key"]:
    nulls = fact[col].isna().sum()
    keur  = fact[fact[col].isna()]["revenue_eur"].sum()/1000
    print(f"  {col}: {nulls:,} nulls ({nulls/total*100:.1f}%)  =>  {keur:+,.1f} kEUR unlinked")
print()

# ── 2025 breakdown of null salesperson ──────────────────────────────
slp_null = y25[y25["salesperson_key"].isna()]
print(f"=== 2025 NULL salesperson_key: {slp_null['revenue_eur'].sum()/1000:,.1f} kEUR ===")
print(slp_null.groupby(["entity","slp_code"])["revenue_eur"].sum().div(1000).sort_values(ascending=False).head(12).to_string())
print()

# ── 2025 null customer ───────────────────────────────────────────────
cust_null = y25[y25["customer_key"].isna()]
print(f"=== 2025 NULL customer_key: {cust_null['revenue_eur'].sum()/1000:,.1f} kEUR ===")
if len(cust_null):
    print(cust_null.groupby(["entity","card_code"])["revenue_eur"].sum().div(1000).sort_values(ascending=False).head(10).to_string())
print()

# ── dim_customer nulls ───────────────────────────────────────────────
print("=== DIM_CUSTOMER NULLS ===")
any_null = False
for col in dc.columns:
    n = dc[col].isna().sum()
    if n:
        any_null = True
        print(f"  {col}: {n} / {len(dc)}")
if not any_null:
    print("  none")
print()

# ── dim_salesperson nulls ────────────────────────────────────────────
print("=== DIM_SALESPERSON NULLS ===")
any_null = False
for col in dslp.columns:
    n = dslp[col].isna().sum()
    if n:
        any_null = True
        print(f"  {col}: {n} / {len(dslp)}")
if not any_null:
    print("  none")
print()

# ── unmapped salesperson display_name ──────────────────────────────
unmapped = dslp[dslp["display_name"].isna()]
print(f"=== DIM_SALESPERSON: unmapped display_name: {len(unmapped)} rows ===")
if len(unmapped):
    print(unmapped[["entity","slp_code","slp_name"]].to_string())
print()

# ── dim_product gaps ─────────────────────────────────────────────────
print("=== DIM_PRODUCT ===")
print(f"  entity coverage: only GmbH ({len(dp):,} rows) — AG/UK/US have no product master loaded")
sellable = dp[dp["is_sellable"]==True]
name_ok = sellable["name_en"].notna().sum()
name_missing = sellable["name_en"].isna().sum()
print(f"  sellable SKUs: {len(sellable)}  |  name_en present: {name_ok}  |  missing: {name_missing}")
other_line = (dp["product_line_clean"]=="Other").sum()
print(f"  product_line_clean='Other' (uncategorised): {other_line} SKUs")
print()

# ── budget coverage ──────────────────────────────────────────────────
print("=== FACT_BUDGET ===")
bud["year"] = pd.to_datetime(bud["budget_month"]).dt.year
print(f"  budget_month range: {bud['budget_month'].min()}  ->  {bud['budget_month'].max()}")
print()
print("  budget by entity / year (kEUR):")
print(bud.groupby(["entity","year"])["budget_amount_eur"].sum().div(1000).round(1).to_string())
print()
print("  budget nulls:")
any_null = False
for col in bud.columns:
    n = bud[col].isna().sum()
    if n:
        any_null = True
        print(f"    {col}: {n} / {len(bud)}")
if not any_null:
    print("    none")
print()

# ── budget vs sales entity overlap ──────────────────────────────────
bud_entities = set(bud["entity"].unique())
fact_entities = set(fact["entity"].unique())
print(f"=== BUDGET / SALES ENTITY OVERLAP ===")
print(f"  fact_sales entities: {sorted(fact_entities)}")
print(f"  fact_budget entities: {sorted(bud_entities)}")
print(f"  in sales but NOT in budget: {sorted(fact_entities - bud_entities)}")
print(f"  in budget but NOT in sales: {sorted(bud_entities - fact_entities)}")
