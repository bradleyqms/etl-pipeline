"""
Inject stub rows for new accounts not yet in the Feb-2026 dim_customer extract.
Run once; subsequent full extracts will replace these with real records.

New accounts found in 2026 YTD fact_sales with no customer_key:
  AG  / 21118  – new CH spa account (slp 9 = G. Monopoli)
  GmbH/ 21118  – new CH spa account (slp 77 = Ch. Rose, GmbH-billed CH accounts)
  GmbH/ 23123  – new NL spa account (slp 38 = Mark Stanlein NL)
  GmbH/ 23124  – new NL spa account (slp 90)
"""
import io, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.transforms.build_gold import get_container_client

SILVER_PATH = "silver/dim_customer/latest.parquet"

c = get_container_client()
raw = pq.read_table(io.BytesIO(c.get_blob_client(SILVER_PATH).download_blob().readall())).to_pandas()
print(f"Silver rows before: {len(raw)}")

# Check none of these are already present
existing = set(zip(raw["entity"].astype(str), raw["card_code"].astype(str)))
stubs = [
    # entity, card_code, card_name, group_name, bill_to_country, territory_id, slp_code
    ("AG",   "21118", "New Account 21118 (AG)",   "Kunden",  "CH",  1,   9),
    ("GmbH", "21118", "New Account 21118 (GmbH)", "Kunden",  "CH", -2,  77),
    ("GmbH", "23123", "New Account 23123",         "Kunden",  "NL", 22,  38),
    ("GmbH", "23124", "New Account 23124",         "Kunden",  "NL", 22,  90),
]

cols = raw.columns.tolist()
rows_to_add = []
for entity, card_code, card_name, group_name, bill_to_country, territory_id, slp_code in stubs:
    if (entity, card_code) in existing:
        print(f"  SKIP {entity}/{card_code} — already in silver")
        continue
    row = {c: pd.NA for c in cols}
    row.update({
        "entity":           entity,
        "card_code":        card_code,
        "card_name":        card_name,
        "group_name":       group_name,
        "bill_to_country":  bill_to_country,
        "ship_to_country":  bill_to_country,
        "territory_id":     territory_id,
        "slp_code":         slp_code,
        "is_active":        "Y",
        "_source_file":     "stub_injection_2026",
    })
    rows_to_add.append(row)
    print(f"  ADD  {entity}/{card_code} ({card_name})")

if not rows_to_add:
    print("Nothing to add.")
else:
    stubs_df = pd.DataFrame(rows_to_add, columns=cols)
    updated = pd.concat([raw, stubs_df], ignore_index=True)
    print(f"Silver rows after:  {len(updated)}")

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(updated), buf)
    buf.seek(0)
    c.get_blob_client(SILVER_PATH).upload_blob(buf, overwrite=True)
    print("Uploaded updated silver/dim_customer/latest.parquet ✓")
    print()
    print("Next steps:")
    print("  python -m src.transforms.enrich_dim_customer")
    print("  python -m src.transforms.build_gold")
