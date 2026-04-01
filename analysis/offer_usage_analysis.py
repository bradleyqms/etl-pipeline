"""
Analyze offer usage from gold fact sales.

Focus:
- A-prefixed item codes (e.g. A26012)
- Product text containing offer keywords (offer, angebot, rabatt, discount, promo)

Outputs are written to data/outputs/:
- offer_usage_monthly.csv
- offer_usage_by_code.csv
- offer_usage_customer_monthly.csv
- offer_usage_focus_<CODE>.csv (when --offer-code is provided)
- offer_window_expected_vs_actual.csv (when --offer-calendar is provided)
"""

from __future__ import annotations

import io
import os
import re
import argparse
import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.transforms.build_gold import GOLD_PATHS, get_container_client


KEYWORDS_REGEX = re.compile(r"offer|angebot|rabatt|discount|promo|aktion", re.IGNORECASE)


def rp(client, path: str) -> pd.DataFrame:
    data = client.get_blob_client(path).download_blob().readall()
    return pd.read_parquet(io.BytesIO(data))


def _safe_text(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _split_codes(value: str | None) -> set[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return set()
    text = str(value).strip()
    if not text:
        return set()
    tokens = re.split(r"[;,|\\s]+", text)
    return {t.strip().upper() for t in tokens if t and t.strip()}


def _make_order_id(df: pd.DataFrame) -> pd.Series:
    # Prefer entity+doc_entry when present; fall back to entity+doc_num.
    has_doc_entry = "doc_entry" in df.columns
    entry = _safe_text(df["doc_entry"]) if has_doc_entry else pd.Series("", index=df.index, dtype="string")
    num = _safe_text(df["doc_num"]) if "doc_num" in df.columns else pd.Series("", index=df.index, dtype="string")
    base = entry.where(entry.ne(""), num)
    return _safe_text(df["entity"]) + "|" + base


def build_offer_frame(offer_code: str | None = None, year: int | None = None) -> pd.DataFrame:
    client = get_container_client()

    fact_sales = rp(client, GOLD_PATHS["fact_sales"])
    dim_product = rp(client, GOLD_PATHS["dim_product"])
    dim_customer = rp(client, GOLD_PATHS["dim_customer"])

    # Keep only required columns and normalize keys to avoid join surprises.
    fact = fact_sales[[
        "doc_date", "entity", "card_code", "item_code", "customer_key", "product_key",
        "quantity", "net_revenue", "revenue_eur", "currency", "doc_num", "doc_entry", "line_num"
    ]].copy()
    fact["item_code"] = _safe_text(fact["item_code"])
    fact["card_code"] = _safe_text(fact["card_code"])
    fact["doc_date"] = pd.to_datetime(fact["doc_date"], errors="coerce")

    prod_cols = [c for c in [
        "product_key", "item_code", "description", "product_line", "product_line_clean",
        "product_category", "sku_type", "sku_channel", "is_sellable"
    ] if c in dim_product.columns]
    prod = dim_product[prod_cols].copy()

    if "product_key" in prod.columns and "product_key" in fact.columns:
        fact = fact.merge(prod, on="product_key", how="left", suffixes=("", "_dim"))

    if "item_code_dim" in fact.columns:
        fact["item_code"] = fact["item_code"].mask(fact["item_code"].eq(""), fact["item_code_dim"])
        fact = fact.drop(columns=["item_code_dim"])

    cust_cols = [c for c in [
        "customer_key", "card_name", "market_group", "region", "sub_region", "channel", "company_group"
    ] if c in dim_customer.columns]
    if "customer_key" in fact.columns and "customer_key" in dim_customer.columns:
        fact = fact.merge(dim_customer[cust_cols], on="customer_key", how="left")

    text_fields = [c for c in ["description", "product_line", "product_line_clean"] if c in fact.columns]
    if text_fields:
        text_blob = pd.Series("", index=fact.index, dtype="string")
        for c in text_fields:
            text_blob = text_blob + " " + _safe_text(fact[c])
    else:
        text_blob = pd.Series("", index=fact.index, dtype="string")

    fact["is_a_prefix_offer"] = fact["item_code"].str.match(r"^A\\d+", na=False)
    fact["is_offer_keyword"] = text_blob.str.contains(KEYWORDS_REGEX, na=False)

    if "sku_type" in fact.columns:
        fact["is_offer_sku_type"] = _safe_text(fact["sku_type"]).str.lower().eq("internal")
    else:
        fact["is_offer_sku_type"] = False

    fact["is_offer_line"] = fact[["is_a_prefix_offer", "is_offer_keyword", "is_offer_sku_type"]].any(axis=1)

    if year is not None:
        fact = fact[fact["doc_date"].dt.year.eq(year)].copy()

    if offer_code:
        code = offer_code.strip().upper()
        fact = fact[fact["item_code"].str.upper().eq(code)].copy()

    fact["month"] = fact["doc_date"].dt.to_period("M").astype("string")
    fact["quantity"] = pd.to_numeric(fact["quantity"], errors="coerce").fillna(0.0)
    fact["revenue_eur"] = pd.to_numeric(fact["revenue_eur"], errors="coerce").fillna(0.0)
    fact["order_id"] = _make_order_id(fact)

    return fact


def build_expected_vs_actual(df: pd.DataFrame, offer_calendar_path: str) -> pd.DataFrame:
    cal = pd.read_csv(offer_calendar_path)
    required = {"offer_code", "offer_name", "valid_from", "valid_to"}
    missing = sorted(required.difference(cal.columns))
    if missing:
        raise ValueError(f"offer calendar missing required columns: {', '.join(missing)}")

    cal = cal.copy()
    cal["offer_code"] = _safe_text(cal["offer_code"]).str.upper()
    cal["offer_name"] = _safe_text(cal["offer_name"])
    cal["valid_from"] = pd.to_datetime(cal["valid_from"], errors="coerce")
    cal["valid_to"] = pd.to_datetime(cal["valid_to"], errors="coerce")
    cal = cal[cal["offer_code"].ne("") & cal["valid_from"].notna() & cal["valid_to"].notna()].copy()

    for c in ["expected_orders", "expected_customers", "expected_revenue_eur", "qualifying_item_codes"]:
        if c not in cal.columns:
            cal[c] = pd.NA

    work = df.copy()
    work["item_code_upper"] = _safe_text(work["item_code"]).str.upper()
    work["card_code"] = _safe_text(work.get("card_code", pd.Series("", index=work.index)))
    work["card_name"] = _safe_text(work.get("card_name", pd.Series("", index=work.index)))

    rows: list[dict] = []
    for _, offer in cal.iterrows():
        code = str(offer["offer_code"])
        start = pd.Timestamp(offer["valid_from"])
        end = pd.Timestamp(offer["valid_to"])
        mask_window = work["doc_date"].between(start, end, inclusive="both")
        w = work[mask_window].copy()

        offer_lines = w[w["item_code_upper"].eq(code)].copy()
        offer_order_ids = set(offer_lines["order_id"].dropna().astype(str).tolist())

        qualifying_codes = _split_codes(offer.get("qualifying_item_codes"))
        if qualifying_codes:
            qualifying_orders = set(
                w[w["item_code_upper"].isin(qualifying_codes)]["order_id"].dropna().astype(str).tolist()
            )
        else:
            qualifying_orders = set(w["order_id"].dropna().astype(str).tolist())

        overlap_orders = offer_order_ids.intersection(qualifying_orders)

        exp_orders = pd.to_numeric(pd.Series([offer.get("expected_orders")]), errors="coerce").iloc[0]
        exp_customers = pd.to_numeric(pd.Series([offer.get("expected_customers")]), errors="coerce").iloc[0]
        exp_revenue = pd.to_numeric(pd.Series([offer.get("expected_revenue_eur")]), errors="coerce").iloc[0]

        act_orders = len(offer_order_ids)
        act_customers = int(offer_lines["customer_key"].nunique()) if "customer_key" in offer_lines.columns else 0
        act_revenue = float(offer_lines["revenue_eur"].sum())
        act_qty = float(offer_lines["quantity"].sum())
        act_lines = int(len(offer_lines))

        uptake_vs_qualifying = (len(overlap_orders) / len(qualifying_orders)) if qualifying_orders else 0.0
        orders_attainment = (act_orders / exp_orders) if pd.notna(exp_orders) and exp_orders != 0 else pd.NA
        customers_attainment = (act_customers / exp_customers) if pd.notna(exp_customers) and exp_customers != 0 else pd.NA
        revenue_attainment = (act_revenue / exp_revenue) if pd.notna(exp_revenue) and exp_revenue != 0 else pd.NA

        rows.append({
            "offer_code": code,
            "offer_name": offer.get("offer_name"),
            "valid_from": start.date().isoformat(),
            "valid_to": end.date().isoformat(),
            "window_days": int((end - start).days + 1),
            "expected_orders": exp_orders,
            "actual_orders": act_orders,
            "delta_orders": (act_orders - exp_orders) if pd.notna(exp_orders) else pd.NA,
            "orders_attainment_pct": (orders_attainment * 100) if pd.notna(orders_attainment) else pd.NA,
            "expected_customers": exp_customers,
            "actual_customers": act_customers,
            "delta_customers": (act_customers - exp_customers) if pd.notna(exp_customers) else pd.NA,
            "customers_attainment_pct": (customers_attainment * 100) if pd.notna(customers_attainment) else pd.NA,
            "expected_revenue_eur": exp_revenue,
            "actual_revenue_eur": act_revenue,
            "delta_revenue_eur": (act_revenue - exp_revenue) if pd.notna(exp_revenue) else pd.NA,
            "revenue_attainment_pct": (revenue_attainment * 100) if pd.notna(revenue_attainment) else pd.NA,
            "actual_offer_qty": act_qty,
            "actual_offer_lines": act_lines,
            "qualifying_order_count": len(qualifying_orders),
            "orders_with_offer_and_qualifying_item": len(overlap_orders),
            "uptake_rate_vs_qualifying_orders": uptake_vs_qualifying,
            "qualifying_item_codes": offer.get("qualifying_item_codes"),
        })

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["valid_from", "offer_code"], ascending=[True, True]).reset_index(drop=True)
    return out


def run(offer_code: str | None, year: int | None, offer_calendar: str | None) -> dict[str, str]:
    df = build_offer_frame(offer_code=offer_code, year=year)
    out_dir = os.path.join("data", "outputs")
    os.makedirs(out_dir, exist_ok=True)

    monthly = (
        df[df["is_offer_line"]]
        .groupby(["month", "entity"], dropna=False)
        .agg(
            offer_lines=("item_code", "size"),
            offer_qty=("quantity", "sum"),
            offer_revenue_eur=("revenue_eur", "sum"),
            unique_offer_codes=("item_code", "nunique"),
        )
        .reset_index()
        .sort_values(["month", "offer_revenue_eur"], ascending=[True, False])
    )

    by_code = (
        df[df["is_offer_line"]]
        .groupby(["item_code", "description", "sku_type", "product_category"], dropna=False)
        .agg(
            lines=("item_code", "size"),
            qty=("quantity", "sum"),
            revenue_eur=("revenue_eur", "sum"),
            first_seen=("doc_date", "min"),
            last_seen=("doc_date", "max"),
        )
        .reset_index()
        .sort_values("revenue_eur", ascending=False)
    )

    customer_monthly = (
        df[df["is_offer_line"]]
        .groupby(["month", "market_group", "region", "channel", "card_code", "card_name"], dropna=False)
        .agg(
            offer_lines=("item_code", "size"),
            offer_qty=("quantity", "sum"),
            offer_revenue_eur=("revenue_eur", "sum"),
            unique_offer_codes=("item_code", "nunique"),
        )
        .reset_index()
        .sort_values(["month", "offer_revenue_eur"], ascending=[True, False])
    )

    monthly_path = os.path.join(out_dir, "offer_usage_monthly.csv")
    by_code_path = os.path.join(out_dir, "offer_usage_by_code.csv")
    customer_monthly_path = os.path.join(out_dir, "offer_usage_customer_monthly.csv")

    monthly.to_csv(monthly_path, index=False)
    by_code.to_csv(by_code_path, index=False)
    customer_monthly.to_csv(customer_monthly_path, index=False)

    outputs = {
        "monthly": monthly_path,
        "by_code": by_code_path,
        "customer_monthly": customer_monthly_path,
    }

    if offer_code:
        focus = df.copy()
        focus_path = os.path.join(out_dir, f"offer_usage_focus_{offer_code.upper()}.csv")
        focus.to_csv(focus_path, index=False)
        outputs["focus"] = focus_path

    if offer_calendar:
        exp_vs_act = build_expected_vs_actual(df=df, offer_calendar_path=offer_calendar)
        exp_vs_act_path = os.path.join(out_dir, "offer_window_expected_vs_actual.csv")
        exp_vs_act.to_csv(exp_vs_act_path, index=False)
        outputs["expected_vs_actual"] = exp_vs_act_path

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze offer usage in fact_sales gold data")
    parser.add_argument("--offer-code", type=str, default=None, help="Optional exact offer code, e.g. A26012")
    parser.add_argument("--year", type=int, default=None, help="Optional year filter, e.g. 2026")
    parser.add_argument(
        "--offer-calendar",
        type=str,
        default=None,
        help="Optional CSV with offer windows and expectations",
    )
    args = parser.parse_args()

    outputs = run(offer_code=args.offer_code, year=args.year, offer_calendar=args.offer_calendar)

    print("Offer usage analysis complete.")
    for name, path in outputs.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
