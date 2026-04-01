"""
Quick verification of card-code fix impact.
Compares gold 2025 actual vs management report reference values.
"""
import io
import pandas as pd
from src.transforms.build_gold import GOLD_PATHS, get_container_client


def main() -> None:
    client = get_container_client()
    def rp(p): return pd.read_parquet(io.BytesIO(client.get_blob_client(p).download_blob().readall()))

    fact = rp(GOLD_PATHS["fact_sales"])
    dc   = rp(GOLD_PATHS["dim_customer"])
    merged = fact.merge(dc, on="customer_key", how="left")

    yr2025 = merged[merged["doc_date"].dt.year == 2025].copy()
    rev    = yr2025.groupby(
        ["company_group", "market_group", "region"], dropna=False
    )["revenue_eur"].sum().reset_index()
    rev["rev_keur"] = (rev["revenue_eur"] / 1000).round(1)

    print("=" * 65)
    print("2025 FULL YEAR REVENUE BY COMPANY / MARKET GROUP / REGION (kEUR)")
    print("=" * 65)
    print(rev.sort_values(["company_group", "market_group", "rev_keur"], ascending=[True, True, False])
          .to_string(index=False))

    # ── Totals by company_group ──────────────────────────────────────────────
    print("\n── Company group totals ────────────────────────────────────────")
    cg = yr2025.groupby("company_group", dropna=False)["revenue_eur"].sum() / 1000
    print(cg.round(1).sort_index())
    grand = yr2025["revenue_eur"].sum() / 1000
    print(f"\nGrand total: {grand:.1f} kEUR  (mgmt report: 12,041 kEUR)")

    # ── Key regions from the report ──────────────────────────────────────────
    checks = {
        "UK": (yr2025["region"] == "UK"),
        "Interco": (yr2025["market_group"] == "UK") & (yr2025["region"] == "Interco"),
        "Distributor - APAC": (yr2025["region"] == "Distributor - APAC"),
        "Distributor - China": (yr2025["region"] == "Distributor - China"),
        "Distributor - Middle East": (yr2025["region"] == "Distributor - Middle East"),
        "Distributor - Other ROW": (yr2025["region"] == "Distributor - Other ROW"),
        "Switzerland": (yr2025["region"] == "Switzerland"),
        "France": (yr2025["region"] == "France"),
        "Italy": (yr2025["region"] == "Italy"),
        "eCommerce USA": (yr2025["region"] == "eCommerce USA"),
    }
    print("\n── Region spot checks ─────────────────────────────────────────────")
    print(f"{'Region':<30} {'Gold kEUR':>10}  {'Report kEUR':>12}")
    report_refs = {
        "UK": 201,
        "Interco": None,
        "Distributor - APAC": 0,
        "Distributor - China": 501,
        "Distributor - Middle East": 106,
        "Distributor - Other ROW": 35,
        "Switzerland": 642,
        "France": None,
        "Italy": None,
        "eCommerce USA": 396,
    }
    for name, mask in checks.items():
        val = yr2025.loc[mask, "revenue_eur"].sum() / 1000
        ref = report_refs.get(name)
        ref_str = f"{ref:.0f}" if ref is not None else "n/a"
        print(f"{name:<30} {val:>10.1f}  {ref_str:>12}")

    # ── dim_customer spot check: were fixes applied? ─────────────────────────
    print("\n── dim_customer: APAC accounts now reclassified ───────────────────")
    apac_left = dc[dc["region"] == "Distributor - APAC"]
    print(f"  Rows still in Distributor-APAC: {len(apac_left)}")
    print(apac_left[["entity","card_code","card_name","region","market_group","company_group","channel"]].to_string(index=False))

    print("\n── GmbH 21xxx/27xxx/28xxx (sample) ────────────────────────────────")
    cc = dc["card_code"].astype(str)
    moved = dc[(dc["entity"]=="GmbH") & (cc.str.match(r"^(21|27|28)\d{3,}$"))]
    print(moved[["card_code","card_name","region","market_group","company_group","channel"]].to_string(index=False))

    print("\n── DESCOMED ILG (51200) ────────────────────────────────────────────")
    ilg = dc[(dc["entity"]=="GmbH") & (dc["card_code"]=="51200")]
    print(ilg[["card_code","card_name","region","channel","company_group"]].to_string(index=False))


if __name__ == "__main__":
    main()
