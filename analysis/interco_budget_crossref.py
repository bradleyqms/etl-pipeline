"""
interco_budget_crossref.py
--------------------------
Read-only analysis against gold parquets. No production data is modified.

Produces three views to help identify interco / unclassified accounts:

  A) budgeted_2026         – customers with at least 1 budget row for FY2026
  B) new_customers_2026    – customers whose first revenue post is in 2026
  C) unbudgeted_revenue    – customers with revenue >= 2025-01-01 and zero 2026 budget
                             (sorted by 2025+2026 revenue desc)

Cross-references dim_customer.channel so any account already flagged 'Interco'
is highlighted in the unbudgeted list.

Usage:
    cd etl_pipeline
    python -m analysis.interco_budget_crossref
"""

import io
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# ── env / path setup ────────────────────────────────────────────────────────
load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.transforms.build_gold import get_container_client, GOLD_PATHS  # noqa: E402

# ── helpers ──────────────────────────────────────────────────────────────────

def _read(client, key: str) -> pd.DataFrame:
    data = client.get_blob_client(GOLD_PATHS[key]).download_blob().readall()
    return pd.read_parquet(io.BytesIO(data))


def _sep(label: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {label}")
    print('='*70)


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Loading gold parquets …")
    c = get_container_client()
    fact   = _read(c, "fact_sales")
    budget = _read(c, "fact_budget")
    dc     = _read(c, "dim_customer")

    print(f"  fact_sales  : {len(fact):,} rows")
    print(f"  fact_budget : {len(budget):,} rows")
    print(f"  dim_customer: {len(dc):,} rows")

    # ── dim_customer lookup (lean) ───────────────────────────────────────────
    dim = dc[["customer_key", "card_code", "card_name",
              "market_group", "region", "channel", "entity"]].copy()
    dim["channel_lower"] = dim["channel"].str.lower().fillna("")

    # ── A) 2026 budgeted customers ───────────────────────────────────────────
    bud26 = budget[budget["date_key"] >= 20260101].copy()
    budgeted_keys = set(bud26["customer_key"].unique())

    bud26_summary = (
        bud26.groupby("customer_key", as_index=False)["budget_amount_eur"]
        .sum()
        .rename(columns={"budget_amount_eur": "budget_2026_eur"})
        .merge(dim[["customer_key", "card_code", "card_name",
                     "market_group", "region", "channel"]].drop_duplicates("customer_key"),
               on="customer_key", how="left")
        .sort_values("budget_2026_eur", ascending=False)
    )

    _sep("A) CUSTOMERS WITH A 2026 BUDGET")
    print(f"  {len(bud26_summary):,} budgeted customer keys")
    print(bud26_summary[["card_code", "card_name", "market_group",
                          "region", "channel", "budget_2026_eur"]]
          .head(30).to_string(index=False))

    # ── B) New customers in 2026 (first ever revenue) ────────────────────────
    first_post = (
        fact.groupby("customer_key", as_index=False)["doc_date"]
        .min()
        .rename(columns={"doc_date": "first_doc_date"})
    )
    new_2026 = first_post[first_post["first_doc_date"].dt.year == 2026].copy()
    new_2026 = new_2026.merge(
        dim[["customer_key", "card_code", "card_name",
             "market_group", "region", "channel"]].drop_duplicates("customer_key"),
        on="customer_key", how="left"
    )
    new_2026["has_2026_budget"] = new_2026["customer_key"].isin(budgeted_keys)

    _sep("B) NEW CUSTOMERS — FIRST REVENUE IN 2026")
    print(f"  {len(new_2026):,} new customer keys")
    print(new_2026.sort_values("first_doc_date")
          [["card_code", "card_name", "market_group", "region",
            "channel", "first_doc_date", "has_2026_budget"]]
          .to_string(index=False))

    # ── C) Unbudgeted revenue (2025-01-01 onwards, no 2026 budget) ───────────
    recent = fact[fact["doc_date"] >= "2025-01-01"].copy()
    recent_rev = (
        recent.groupby("customer_key", as_index=False)["revenue_eur"]
        .sum()
        .rename(columns={"revenue_eur": "revenue_2025plus_eur"})
    )

    # revenue by year for context
    rev_2025 = (
        recent[recent["doc_date"].dt.year == 2025]
        .groupby("customer_key", as_index=False)["revenue_eur"]
        .sum().rename(columns={"revenue_eur": "rev_2025_eur"})
    )
    rev_2026 = (
        recent[recent["doc_date"].dt.year == 2026]
        .groupby("customer_key", as_index=False)["revenue_eur"]
        .sum().rename(columns={"revenue_eur": "rev_2026_eur"})
    )

    unbudgeted = (
        recent_rev[~recent_rev["customer_key"].isin(budgeted_keys)]
        .merge(rev_2025, on="customer_key", how="left")
        .merge(rev_2026, on="customer_key", how="left")
        .merge(dim[["customer_key", "card_code", "card_name",
                     "market_group", "region", "channel"]].drop_duplicates("customer_key"),
               on="customer_key", how="left")
        .fillna({"rev_2025_eur": 0, "rev_2026_eur": 0})
        .sort_values("revenue_2025plus_eur", ascending=False)
    )

    unbudgeted["already_flagged_interco"] = (
        unbudgeted["channel"].str.lower().fillna("") == "interco"
    )
    unbudgeted["keur_2025plus"] = (unbudgeted["revenue_2025plus_eur"] / 1000).round(1)
    unbudgeted["keur_2025"]     = (unbudgeted["rev_2025_eur"] / 1000).round(1)
    unbudgeted["keur_2026"]     = (unbudgeted["rev_2026_eur"] / 1000).round(1)

    _sep("C) UNBUDGETED REVENUE (2025+ and no 2026 budget)")
    print(f"  {len(unbudgeted):,} customer keys with revenue but no 2026 budget")

    interco_unflagged = unbudgeted[
        ~unbudgeted["already_flagged_interco"] &
        (unbudgeted["keur_2025plus"] > 1)
    ]
    interco_already = unbudgeted[unbudgeted["already_flagged_interco"]]

    print(f"\n  Already channel=Interco ({len(interco_already)} accounts):")
    print(interco_already[["card_code", "card_name", "market_group",
                            "region", "keur_2025", "keur_2026"]]
          .to_string(index=False))

    print(f"\n  NOT yet flagged Interco — >1 kEUR unbudgeted "
          f"({len(interco_unflagged)} accounts, sorted descending):")
    print(interco_unflagged[["card_code", "card_name", "market_group", "region",
                              "channel", "keur_2025", "keur_2026", "keur_2025plus"]]
          .head(40).to_string(index=False))

    # ── summary numbers ──────────────────────────────────────────────────────
    _sep("SUMMARY")
    print(f"  Budgeted 2026 accounts          : {len(budgeted_keys):,}")
    print(f"  New accounts (first rev 2026)   : {len(new_2026):,}")
    print(f"    → of which have 2026 budget   : {new_2026['has_2026_budget'].sum():,}")
    print(f"  Unbudgeted with 2025+ revenue   : {len(unbudgeted):,}")
    print(f"    → already flagged Interco     : {unbudgeted['already_flagged_interco'].sum():,}")
    print(f"    → NOT flagged, >1 kEUR        : {len(interco_unflagged):,}")
    total_unflagged_keur = interco_unflagged["keur_2025plus"].sum()
    print(f"    → combined revenue (kEUR)     : {total_unflagged_keur:,.1f}")


if __name__ == "__main__":
    main()
