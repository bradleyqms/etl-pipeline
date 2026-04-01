import io, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pandas as pd
from src.transforms.build_gold import get_container_client, GOLD_PATHS
c = get_container_client()
fact = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["fact_sales"]).download_blob().readall()))
dc   = pd.read_parquet(io.BytesIO(c.get_blob_client(GOLD_PATHS["dim_customer"]).download_blob().readall()))
y = fact[fact["doc_date"].dt.year==2025].merge(
    dc[["customer_key","market_group","region","channel","bill_to_country","card_code","card_name"]].rename(columns={"card_code":"cc","card_name":"cn"}),
    on="customer_key", how="left")
y["keur"] = y["revenue_eur"]/1000

# Is reference Switzerland = AG entity?
ag = y[y["entity"]=="AG"]
print("AG total:            ", round(ag["keur"].sum(),1))
print("AG excl interco:     ", round(ag[ag["channel"]!="Interco"]["keur"].sum(),1))
print("Reference Switzerland: 643")

# Is reference Germany = GmbH entity Germany region?
gmbh_de_excl = y[(y["entity"]=="GmbH") & (y["region"]=="Germany") & (y["channel"]!="Interco")]
gmbh_ch_noni = y[(y["entity"]=="GmbH") & (y["region"]=="Switzerland") & (y["channel"]!="Interco")]
print("\nGmbH/Germany excl interco:     ", round(gmbh_de_excl["keur"].sum(),1))
print("GmbH/Switzerland excl interco: ", round(gmbh_ch_noni["keur"].sum(),1))
print("GmbH/Germany + GmbH/CH:        ", round(gmbh_de_excl["keur"].sum() + gmbh_ch_noni["keur"].sum(),1), " vs ref Germany: 4531")

# Interco fees in reference total?
interco_fees = y[y["cc"].isin(["51200","21990","51600"])]
print("\nInterco fees sitting in revenue:")
print("  DESCOMED ILG (51200):     ", round(y[y["cc"]=="51200"]["keur"].sum(),1))
print("  QMS AG service fee (21990):", round(y[y["cc"]=="21990"]["keur"].sum(),1))
print("  DESCOMED Ltd (51600):     ", round(y[y["cc"]=="51600"]["keur"].sum(),1))
print("  Total:                    ", round(interco_fees["keur"].sum(),1))
print("\nReference total incl these fees:", 12040)
print("ETL excl interco:               ", round(y[y["channel"]!="Interco"]["keur"].sum(),1))
print("ETL incl interco:               ", round(y["keur"].sum(),1))
print("Gap if ref INCLUDES interco:    ", round(y["keur"].sum()-12040,1))
print("Gap if ref EXCLUDES interco:    ", round(y[y["channel"]!="Interco"]["keur"].sum()-12040,1))

# Who are the GmbH Switzerland spa accounts?
print("\nGmbH entity, Switzerland region, non-interco-channel — top accounts:")
print(gmbh_ch_noni.groupby(["cc","cn","bill_to_country"])["keur"].sum()
      .sort_values(ascending=False).head(15).to_string())
