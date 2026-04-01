import io, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from src.core.blob_client import get_container_client
from src.transforms.build_gold import GOLD_PATHS
import pandas as pd

c = get_container_client()

for prefix in ["cold/", "silver/", "gold/"]:
    blobs = sorted(c.list_blobs(name_starts_with=prefix),
                   key=lambda b: b["last_modified"], reverse=True)
    print(f"=== {prefix} (10 most recent) ===")
    for b in blobs[:10]:
        ts = b["last_modified"].strftime("%Y-%m-%d %H:%M")
        kb = b["size"] / 1024
        print(f"  {ts}  {b['name']}  ({kb:.0f} KB)")
    print()

# Also check max doc_date in current gold fact_sales
fact = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["fact_sales"]).download_blob().readall()))
print("=== GOLD fact_sales max doc_date by entity ===")
print(fact.groupby("entity")["doc_date"].max().to_string())
print()
print("=== 2026-04-01 rows in gold fact_sales ===")
today = fact[fact["doc_date"].dt.date == pd.Timestamp("2026-04-01").date()]
print(f"  rows: {len(today):,}   kEUR: {today['revenue_eur'].sum()/1000:,.1f}")
