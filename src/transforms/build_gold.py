"""Build gold star-schema parquet tables from silver layer data."""

from __future__ import annotations

import hashlib
import io
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.blob_client import get_container_client, upload_to_blob
from ..reference_data.constants import (
    BUDGET_FX_RATES,
    ENTITY_CURRENCY,
    EOY_2025_FX_RATES,
    FX_RATES,
)

log = logging.getLogger(__name__)

GOLD_PATHS: dict[str, str] = {
    "dim_date": "gold/dim_date.parquet",
    "dim_customer": "gold/dim_customer.parquet",
    "dim_product": "gold/dim_product.parquet",
    "dim_salesperson": "gold/dim_salesperson.parquet",
    "fact_sales": "gold/fact_sales.parquet",
    "fact_budget": "gold/fact_budget.parquet",
}

BUDGET_VERSION_LABEL = "sharepoint_multi_verify_20260324"

# Regional budget parquet paths (concatenated to form fact_budget)
_BUDGET_REGIONAL_PREFIX = (
    f"silver/budget/canonical/{BUDGET_VERSION_LABEL}/regional_views"
)
BUDGET_REGIONAL_PATHS = [
    f"{_BUDGET_REGIONAL_PREFIX}/core_markets_budget/core_markets_sales_budget_monthly_canonical.parquet",
    f"{_BUDGET_REGIONAL_PREFIX}/export_budget/export_sales_budget_monthly_canonical.parquet",
    f"{_BUDGET_REGIONAL_PREFIX}/uk_budget/uk_sales_budget_monthly_canonical.parquet",
    f"{_BUDGET_REGIONAL_PREFIX}/us_budget/us_sales_budget_monthly_canonical.parquet",
]

ECOMMERCE_REPORT_PATH = (
    f"{_BUDGET_REGIONAL_PREFIX}/ecommerce_budget/ecommerce_report_budget_monthly_canonical.parquet"
)

# market_group value → entity (for budget → dim_customer key join)
_MARKET_GROUP_ENTITY: dict[str, str] = {
    "Core Markets": "GmbH",
    "Germany":      "GmbH",
    "Export":       "GmbH",
    "UK":           "UK",
    "USA":          "US",
    "US":           "US",
}

_COMPANY2_REGIONS: frozenset[str] = frozenset({
    "Distributor - China",
    "Distributor - APAC",
    "Distributor - Middle East",
})

_DISTRIBUTOR_NAME_MAP: dict[str, str] | None = None


def _resolve_actuals_fx_rates() -> dict[str, float]:
    profile = os.getenv("ACTUALS_FX_PROFILE", "default").strip().lower()
    if profile in {"eoy_2025", "eoy2025", "report_2025"}:
        log.info("build_gold: using ACTUALS_FX_PROFILE=%s", profile)
        return EOY_2025_FX_RATES
    return FX_RATES


def _read_parquet_blob(container_client, blob_path: str) -> pd.DataFrame:
    data = container_client.get_blob_client(blob_path).download_blob().readall()
    return pq.read_table(io.BytesIO(data)).to_pandas()


def _write_parquet_blob(container_client, blob_path: str, df: pd.DataFrame) -> int:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    payload = buf.getvalue()
    upload_to_blob(container_client, blob_path=blob_path, content=payload, overwrite=True)
    return len(payload)


def _delete_blob_if_exists(container_client, blob_path: str) -> None:
    blob_client = container_client.get_blob_client(blob_path)
    try:
        if blob_client.exists():
            blob_client.delete_blob(delete_snapshots="include")
            log.info("build_gold: deleted obsolete blob %s", blob_path)
    except Exception as exc:
        log.warning("build_gold: failed deleting obsolete blob %s (%s)", blob_path, exc)


def _normalise_entity(series: pd.Series) -> pd.Series:
    return series.astype(str).replace({"CH": "AG", "USA": "US"})


def _make_key(*parts: object) -> int:
    raw = "|".join(str(p) for p in parts)
    return int(hashlib.md5(raw.encode("utf-8")).hexdigest(), 16) % (2**31)


def _normalise_customer_code(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .str.lstrip("-")
        .replace({"": pd.NA, "<NA>": pd.NA, "nan": pd.NA, "None": pd.NA})
    )


def _get_distributor_name_map() -> dict[str, str]:
    global _DISTRIBUTOR_NAME_MAP
    if _DISTRIBUTOR_NAME_MAP is not None:
        return _DISTRIBUTOR_NAME_MAP

    mapping_path = (
        Path(__file__).resolve().parents[3]
        / "sales_report_v2_independent"
        / "data"
        / "inputs"
        / "mappings"
        / "entity_mappings.csv"
    )
    if not mapping_path.exists():
        log.warning("build_gold: mapping file not found at %s", mapping_path)
        _DISTRIBUTOR_NAME_MAP = {}
        return _DISTRIBUTOR_NAME_MAP

    try:
        mappings = pd.read_csv(mapping_path, dtype=str)
    except Exception as exc:
        log.warning("build_gold: failed reading mapping file %s (%s)", mapping_path, exc)
        _DISTRIBUTOR_NAME_MAP = {}
        return _DISTRIBUTOR_NAME_MAP

    cols = ["Entity", "Market_Group", "Region", "Customer_Name"]
    for col in cols:
        if col not in mappings.columns:
            _DISTRIBUTOR_NAME_MAP = {}
            return _DISTRIBUTOR_NAME_MAP

    work = mappings[cols].copy()
    work = work[(work["Entity"] == "Export") & (work["Market_Group"] == "Export")]
    work["Region"] = work["Region"].astype("string").str.strip()
    work["Customer_Name"] = work["Customer_Name"].astype("string").str.strip()
    work = work[
        work["Region"].str.startswith("Distributor -", na=False)
        & work["Customer_Name"].notna()
        & (work["Customer_Name"] != "")
    ]

    # Only map regions that have a single unambiguous customer in mappings.
    # Regions with multiple mapped customers (e.g. Distributor - Other EU/ROW)
    # must remain aggregate distributor rows to avoid assigning the full budget
    # to an arbitrary single account.
    region_counts = work.groupby("Region", dropna=False)["Customer_Name"].nunique(dropna=True)
    unique_regions = region_counts[region_counts == 1].index
    unique_work = work[work["Region"].isin(unique_regions)].copy()
    dedup = unique_work.drop_duplicates(subset=["Region"], keep="first")
    _DISTRIBUTOR_NAME_MAP = dict(zip(dedup["Region"].tolist(), dedup["Customer_Name"].tolist()))
    return _DISTRIBUTOR_NAME_MAP


def _apply_distributor_customer_name_mapping(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    dist_map = _get_distributor_name_map()
    if not dist_map:
        return df

    work = df.copy()
    region_series = work.get("region", pd.Series([pd.NA] * len(work))).astype("string").str.strip()
    customer_series = work.get("customer_name", pd.Series([pd.NA] * len(work))).astype("string").str.strip()

    by_region = region_series.map(dist_map)
    by_customer = customer_series.map(dist_map)
    mapped_name = by_region.fillna(by_customer)

    mask = mapped_name.notna() & (mapped_name != "")
    work.loc[mask, "customer_name"] = mapped_name.loc[mask]
    return work


def _build_dim_date(fact_sales: pd.DataFrame, fact_budget: pd.DataFrame) -> pd.DataFrame:
    sales_dates = pd.to_datetime(fact_sales.get("doc_date"), errors="coerce") if not fact_sales.empty else pd.Series(dtype="datetime64[ns]")
    budget_dates = pd.to_datetime(fact_budget.get("budget_month"), errors="coerce") if not fact_budget.empty else pd.Series(dtype="datetime64[ns]")

    candidates = pd.concat([sales_dates.dropna(), budget_dates.dropna()], ignore_index=True)
    if candidates.empty:
        start_date = pd.Timestamp("2024-01-01")
        end_date = pd.Timestamp("2028-12-31")
    else:
        start_date = candidates.min().normalize() - pd.DateOffset(years=1)
        end_date = candidates.max().normalize() + pd.DateOffset(years=1)

    dates = pd.date_range(start_date, end_date, freq="D")
    df = pd.DataFrame({"date": dates})
    iso = df["date"].dt.isocalendar()
    df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["date"].dt.year
    df["quarter"] = "Q" + df["date"].dt.quarter.astype(str)
    df["month_num"] = df["date"].dt.month
    df["month_name"] = df["date"].dt.month_name()
    df["week_num"] = iso.week.astype(int)
    df["day_of_week"] = df["date"].dt.day_name()
    return df[["date_key", "date", "year", "quarter", "month_num", "month_name", "week_num", "day_of_week"]]


def _latest_fsd_snapshot(container_client) -> str:
    blobs = [
        b.name for b in container_client.list_blobs(name_starts_with="silver/fact_sales_daily/")
        if b.name.endswith(".parquet")
    ]
    if not blobs:
        raise RuntimeError("No fact_sales_daily parquet files found under silver/fact_sales_daily/")
    dates = sorted({b.split("/")[2] for b in blobs if len(b.split("/")) >= 4})
    if not dates:
        raise RuntimeError("Could not infer fact_sales_daily snapshot dates")
    return dates[-1]


def _load_cold_extract_history(container_client) -> pd.DataFrame:
    blobs = [
        b.name for b in container_client.list_blobs(name_starts_with="silver/cold_extract/")
        if b.name.endswith(".parquet")
    ]
    if not blobs:
        return pd.DataFrame()
    frames = []
    for path in blobs:
        frames.append(_read_parquet_blob(container_client, path))
    return pd.concat(frames, ignore_index=True)


def _load_latest_daily_incremental(container_client) -> pd.DataFrame:
    snapshot = _latest_fsd_snapshot(container_client)
    tags = ["ch", "gmbh", "uk", "us"]
    frames = []
    for tag in tags:
        path = f"silver/fact_sales_daily/{snapshot}/fact_sales_daily_{tag}.parquet"
        try:
            frames.append(_read_parquet_blob(container_client, path))
        except Exception:
            log.warning("build_gold: missing expected daily file %s", path)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _prepare_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["entity"] = _normalise_entity(work["entity"])
    work["card_code"] = work["card_code"].astype(str)
    work["slp_code"] = pd.to_numeric(work.get("slp_code"), errors="coerce").astype("Int64")
    work["customer_key"] = work.apply(lambda r: _make_key(r["entity"], r["card_code"]), axis=1)
    work["salesperson_key"] = work.apply(
        lambda r: _make_key(r["entity"], int(r["slp_code"])) if pd.notna(r["slp_code"]) else pd.NA,
        axis=1,
    )
    work["salesperson_key"] = pd.array(work["salesperson_key"], dtype="Int64")
    # Reclassify Shopify customers: eCommerce US for US entity, eCommerce elsewhere
    shopify_mask = work["card_name"].str.contains("Shopify", case=False, na=False)
    work.loc[shopify_mask & (work["entity"] != "US"), "channel"] = "eCommerce"
    # Reclassify GmbH Shopify accounts (48001-48028) from Core Markets into eCommerce segment
    eu_shopify_mask = shopify_mask & (work["entity"] == "GmbH")
    work.loc[eu_shopify_mask, "market_group"] = "eCommerce"
    work.loc[eu_shopify_mask, "region"] = "eCommerce EU (incl. UK)"
    # Normalise company_group to Company 1/2/3 labels
    work = _apply_company_group_overrides(work)
    # Enforce permitted hierarchy values (channel, region, market_group)
    work = _normalise_hierarchy_values(work)
    cols = [
        "customer_key", "entity", "card_code", "card_name", "slp_code", "salesperson_key",
        "market_group", "region", "sub_region", "channel", "company_group", "is_active", "bill_to_country",
    ]
    for col in cols:
        if col not in work.columns:
            work[col] = pd.NA
    return work[cols].drop_duplicates(subset=["entity", "card_code"], keep="last")


def _append_budget_only_customers(dim_customer: pd.DataFrame, budget_df: pd.DataFrame) -> pd.DataFrame:
    if budget_df.empty:
        return dim_customer

    work = _apply_distributor_customer_name_mapping(budget_df)
    work["entity"] = work["market_group"].map(_MARKET_GROUP_ENTITY).fillna("GmbH")
    # Switzerland budget-only customers are AG entity (CHF, 21xxx card codes)
    _bonly_ch = work.get("region", pd.Series([""] * len(work))).astype(str).str.contains("Switzerland", case=False, na=False)
    work.loc[_bonly_ch, "entity"] = "AG"
    work["card_code"] = _normalise_customer_code(work.get("customer_code", pd.Series([pd.NA] * len(work))))
    work["card_name"] = work.get("customer_name", pd.Series([pd.NA] * len(work)))
    work["region"] = work.get("region", pd.Series([pd.NA] * len(work)))
    work["sub_region"] = work.get("sub_region", pd.Series([pd.NA] * len(work)))
    work["channel"] = work.get("channel", pd.Series([pd.NA] * len(work)))
    # Derive company_group from region/channel signals for budget-only rows
    _reg = work["region"].astype("string")
    _ch = work["channel"].astype("string").str.lower().fillna("")
    work["company_group"] = pd.NA
    work.loc[_reg.isin(_COMPANY2_REGIONS), "company_group"] = "Company 2"
    work.loc[_ch.str.contains("shopify|amazon|ecommerce", na=False) & work["company_group"].isna(), "company_group"] = "Company 3"
    work.loc[_ch.ne("interco") & work["company_group"].isna(), "company_group"] = "Company 1"
    work["is_active"] = "Y"
    work["bill_to_country"] = pd.NA
    work["slp_code"] = pd.Series([pd.NA] * len(work), dtype="Int64")
    work["salesperson_key"] = pd.Series([pd.NA] * len(work), dtype="Int64")

    work = work[work["card_code"].notna()].copy()
    work["customer_key"] = work.apply(lambda r: _make_key(r["entity"], r["card_code"]), axis=1)

    cols = [
        "customer_key", "entity", "card_code", "card_name", "slp_code", "salesperson_key",
        "market_group", "region", "sub_region", "channel", "company_group", "is_active", "bill_to_country",
    ]
    budget_customers = work[cols].drop_duplicates(subset=["entity", "card_code"], keep="last")
    # Apply the same hierarchy normalisation that _prepare_dim_customer uses
    budget_customers = _normalise_hierarchy_values(budget_customers)

    combined = pd.concat([dim_customer, budget_customers], ignore_index=True)
    return combined.drop_duplicates(subset=["entity", "card_code"], keep="first")


def _prepare_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["entity"] = _normalise_entity(work["entity"])
    work["item_code"] = work["item_code"].astype(str)
    work["product_key"] = work.apply(lambda r: _make_key(r["entity"], r["item_code"]), axis=1)
    cols = [
        "product_key", "entity", "item_code", "name_en", "description", "product_line_clean",
        "product_category", "sku_type", "sku_channel", "is_sellable", "is_active",
    ]
    for col in cols:
        if col not in work.columns:
            work[col] = pd.NA
    return work[cols].drop_duplicates(subset=["entity", "item_code"], keep="last")


def _prepare_dim_salesperson(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["entity"] = _normalise_entity(work["entity"])
    work["slp_code"] = pd.to_numeric(work["slp_code"], errors="coerce").fillna(-1).astype(int)
    work["salesperson_key"] = work.apply(lambda r: _make_key(r["entity"], r["slp_code"]), axis=1)
    work["display_name"] = work.get("display_name", pd.Series([pd.NA] * len(work))).astype("string").str.strip()
    sub_region_lookup = _load_salesperson_subregion_lookup()
    if not sub_region_lookup.empty:
        work = work.merge(
            sub_region_lookup,
            left_on=["entity", "display_name"],
            right_on=["entity", "salesperson_name"],
            how="left",
        )
        work = work.drop(columns=["salesperson_name"])
    # US: promote SAP territory region → sub_region ONLY when lookup hasn't already supplied one.
    # The lookup (revised_mapping_simple Salesperson sheet) takes priority over SAP territory values.
    _us_terr = {"Northeast", "Central", "Southeast", "West"}
    us_slp = work["entity"] == "US"
    us_terr_slp = us_slp & work["region"].isin(_us_terr)
    no_lookup_sub = work["sub_region"].isna()
    work.loc[us_terr_slp & no_lookup_sub, "sub_region"]       = work.loc[us_terr_slp & no_lookup_sub, "region"]
    work.loc[us_terr_slp & no_lookup_sub, "sub_region_count"] = 1
    work.loc[us_terr_slp, "region"] = "USA"
    us_retail_slp = us_slp & (work["region"] == "Retail")
    work.loc[us_retail_slp, "region"] = "USA"
    us_other_slp = us_slp & work["region"].isin({"Other", "USA"})
    work.loc[us_other_slp, "region"] = "USA"
    cols = [
        "salesperson_key", "entity", "slp_code", "slp_name", "display_name", "market_group", "region", "sub_region", "sub_region_count", "is_active",
    ]
    for col in cols:
        if col not in work.columns:
            work[col] = pd.NA
    return work[cols].drop_duplicates(subset=["entity", "slp_code"], keep="last")


def _normalise_hierarchy_values(work: pd.DataFrame) -> pd.DataFrame:
    """Enforce permitted values per revised_mapping_simple.

    Permitted channel values: Spa, Retail, Distributor, eCommerce, Interco
    region/market_group follow the mapping table — UK and USA use channel
    to distinguish spa vs retail; they do NOT encode that in the region name.
    """
    w = work.copy()

    # ── Card-code-based region corrections (SAP territory misclassifications) ──
    # Must run first, before channel/region normalisation.  company_group is also
    # patched here because _apply_company_group_overrides has already executed.
    if "card_code" in w.columns and "entity" in w.columns:
        _cc  = w["card_code"].astype(str)
        _nm  = w.get("card_name", pd.Series("", index=w.index)).astype(str).fillna("")
        is_gmbh = w["entity"] == "GmbH"
        in_apac = is_gmbh & (w["region"].astype(str) == "Distributor - APAC")

        # 1. DESCOMED ILG Goods In (GmbH 51200) – interco stock transfer, not revenue
        _ilg = is_gmbh & (_cc == "51200") & _nm.str.contains("ILG", case=False, na=False)
        w.loc[_ilg, "channel"]       = "Interco"
        w.loc[_ilg, "market_group"]  = "UK"
        w.loc[_ilg, "region"]        = "Interco"
        w.loc[_ilg, "company_group"] = "Company 1"

        # 1b. QMS AG CH service-fee account (GmbH 21990) – interco service charge, not spa revenue
        _svc_ag = is_gmbh & (_cc == "21990")
        w.loc[_svc_ag, "channel"]       = "Interco"
        w.loc[_svc_ag, "market_group"]  = "Core Markets"
        w.loc[_svc_ag, "region"]        = "Switzerland"
        w.loc[_svc_ag, "company_group"] = "Company 1"

        # 1c. DESCOMED Ltd service account (GmbH 51600) – interco service charge
        _svc_uk = is_gmbh & (_cc == "51600")
        w.loc[_svc_uk, "channel"]       = "Interco"
        w.loc[_svc_uk, "market_group"]  = "UK"
        w.loc[_svc_uk, "region"]        = "Interco"
        w.loc[_svc_uk, "company_group"] = "Company 1"

        # 1d. Consignment settlement accounts (KaDeWe 30066, Oberpollinger 30676,
        #     Alsterhaus 30678) – SAP marks these BP as "Internal" which maps to
        #     Interco, but they are real retail revenue settled by Steve Byrom-Chadd.
        _konsi = is_gmbh & _cc.isin({"30066", "30676", "30678"})
        w.loc[_konsi, "channel"]       = "Retail"
        w.loc[_konsi, "market_group"]  = "Core Markets"
        w.loc[_konsi, "region"]        = "Germany"
        w.loc[_konsi, "company_group"] = "Company 1"

        # 2. GmbH 21xxx – Swiss spa/institute cards incorrectly assigned to APAC
        _ch_mask = in_apac & _cc.str.match(r"^21\d{3,}$")
        w.loc[_ch_mask, "market_group"]  = "Core Markets"
        w.loc[_ch_mask, "region"]        = "Switzerland"
        w.loc[_ch_mask, "channel"]       = "Spa"
        w.loc[_ch_mask, "company_group"] = "Company 1"

        # 3. GmbH 27xxx – French spa/institute cards incorrectly assigned to APAC
        _fr_mask = in_apac & _cc.str.match(r"^27\d{3,}$")
        w.loc[_fr_mask, "market_group"]  = "Core Markets"
        w.loc[_fr_mask, "region"]        = "France"
        w.loc[_fr_mask, "channel"]       = "Spa"
        w.loc[_fr_mask, "company_group"] = "Company 1"

        # 4. GmbH 28xxx – Italian spa/institute cards incorrectly assigned to APAC
        _it_mask = in_apac & _cc.str.match(r"^28\d{3,}$")
        w.loc[_it_mask, "market_group"]  = "Core Markets"
        w.loc[_it_mask, "region"]        = "Italy"
        w.loc[_it_mask, "channel"]       = "Spa"
        w.loc[_it_mask, "company_group"] = "Company 1"

        # 5. Elena Grigoreva specific cards (unconditional – apply regardless of current region)
        w.loc[is_gmbh & (_cc == "30765"), ["market_group", "region", "company_group"]] = [
            "Core Markets", "Italy", "Company 1",
        ]
        w.loc[is_gmbh & (_cc == "31026"), ["market_group", "region", "company_group"]] = [
            "Core Markets", "Switzerland", "Company 1",
        ]

        # 6. GmbH China distributor cards (currently lumped in Distributor-APAC) → Distributor-China
        #    company_group stays Company 2 – just splits the C2 sub-bucket
        _china_cards = {"10038", "10041", "10043"}
        w.loc[is_gmbh & _cc.isin(_china_cards), "region"] = "Distributor - China"

        # 7. GmbH Middle East distributors sitting in Other ROW → Distributor-Middle East (C2)
        in_other_row = is_gmbh & (w["region"].astype(str) == "Distributor - Other ROW")
        _me_mask = in_other_row & _nm.str.contains(
            r"dubai|united arab|uae|saudi|kuwait|qatar|jordan|oman|riyadh|abu dhabi|bahrain",
            case=False, na=False, regex=True,
        )
        w.loc[_me_mask, "region"]        = "Distributor - Middle East"
        w.loc[_me_mask, "market_group"]  = "Export"
        w.loc[_me_mask, "company_group"] = "Company 2"

    # ── Channel normalization ─────────────────────────────────────────────────
    ch_map = {
        "B2B Trade":        "Spa",
        "B2C Online":       "Spa",
        "Internal":         "Interco",
        "Global eTailers":  "eCommerce",   # Global etailers is a region, not a channel
        "Global etailers":  "eCommerce",   # lowercase variant from raw data
        "eCommerce US":     "eCommerce",
        "B2B Distributor":  "Distributor",
        "Own eCommerce":    "eCommerce",
        "Amazon":           "eCommerce",   # Amazon is a region, not a channel
    }
    w["channel"] = w["channel"].replace(ch_map)

    # ── US eCommerce reclassifications ───────────────────────────────────────
    # Shopify USA → eCommerce market_group, eCommerce USA region
    us_shopify = (w["entity"] == "US") & w["card_name"].str.contains("Shopify", case=False, na=False)
    w.loc[us_shopify, "market_group"] = "eCommerce"
    w.loc[us_shopify, "region"]       = "eCommerce USA"
    w.loc[us_shopify, "channel"]      = "eCommerce"
    # Amazon (card 41000, US entity) → eCommerce market_group, Amazon region
    us_amazon = (w["entity"] == "US") & (w["card_code"] == "41000")
    w.loc[us_amazon, "market_group"] = "eCommerce"
    w.loc[us_amazon, "region"]       = "Amazon"
    w.loc[us_amazon, "channel"]      = "eCommerce"

    # ── UK eCommerce reclassifications ───────────────────────────────────────
    # UK Shopify (59100, 59200) → eCommerce market_group, eCommerce EU (incl. UK) region
    uk_shopify = (w["entity"] == "UK") & w["card_name"].str.contains("Shopify", case=False, na=False)
    w.loc[uk_shopify, "market_group"] = "eCommerce"
    w.loc[uk_shopify, "region"]       = "eCommerce EU (incl. UK)"
    w.loc[uk_shopify, "channel"]      = "eCommerce"
    # Global etailers rows (UK/GmbH entity, channel=eCommerce after mapping) → eCommerce market_group
    # Exclude GmbH EU Shopify accounts (already placed in eCommerce EU (incl. UK) before this function)
    uk_ge_by_channel = (
        w["entity"].isin(["UK", "GmbH"])
        & (w["channel"] == "eCommerce")
        & (w["company_group"] == "Company 3")
        & ~uk_shopify
        & w["region"].ne("eCommerce EU (incl. UK)")
    )
    w.loc[uk_ge_by_channel, "market_group"] = "eCommerce"
    w.loc[uk_ge_by_channel, "region"]       = "Global etailers"
    # Also catch rows where region itself is "Global eTailers" / "Global etailers"
    uk_ge_by_region = (
        w["entity"].isin(["UK", "GmbH"])
        & w["region"].str.lower().eq("global etailers")
        & ~uk_shopify
    )
    w.loc[uk_ge_by_region, "market_group"] = "eCommerce"
    w.loc[uk_ge_by_region, "region"]       = "Global etailers"
    w.loc[uk_ge_by_region, "channel"]      = "eCommerce"

    # ── UK: region stays 'UK'; channel (Spa/Retail) differentiates ───────────
    # Nothing to change for the region column — it is already 'UK'.

    # ── US territory: promote region → sub_region, region = 'USA' ───────────
    # Applies to all entities with market_group=USA (incl. legacy GmbH records)
    _us_territory = {"Northeast", "Central", "Southeast", "West"}
    us_b2b = w["market_group"] == "USA"
    # Named territory → sub_region; region becomes 'USA'
    us_terr = us_b2b & w["region"].isin(_us_territory)
    w.loc[us_terr, "sub_region"] = w.loc[us_terr, "region"]
    w.loc[us_terr, "region"]     = "USA"
    # 'Retail' region rows: region = 'USA', channel = 'Retail'
    us_ret = us_b2b & (w["region"] == "Retail")
    w.loc[us_ret, "channel"] = "Retail"
    w.loc[us_ret, "region"]  = "USA"
    # Americas / Other / remaining catch-all → region = 'USA'
    us_catchall = us_b2b & w["region"].isin({"Other", "USA", "Americas"})
    w.loc[us_catchall, "region"] = "USA"
    # If an eCommerce region leaked into USA market_group, promote to eCommerce
    us_ecomm_leak = us_b2b & w["region"].isin({"eCommerce USA", "Amazon", "eCommerce EU (incl. UK)", "Global etailers"})
    w.loc[us_ecomm_leak, "market_group"] = "eCommerce"

    # ── Export: region normalization + force channel = 'Distributor' ─────────
    exp_region_map = {
        "Eastern Europe": "Distributor - Other EU",
        "International":  "Distributor - Other ROW",
        "Americas":       "Distributor - Other ROW",
    }
    exp_mask = w["market_group"] == "Export"
    w.loc[exp_mask, "region"]  = w.loc[exp_mask, "region"].replace(exp_region_map)
    w.loc[exp_mask, "channel"] = "Distributor"

    # ── Core Markets: Nordics → Other Core Markets ───────────────────────────
    nordics_mask = (w["market_group"] == "Core Markets") & (w["region"] == "Nordics")
    w.loc[nordics_mask, "region"] = "Other Core Markets"

    return w


def _apply_company_group_overrides(work: pd.DataFrame) -> pd.DataFrame:
    """Replace SAP legal-entity company_group values with Company 1/2/3 labels."""
    mapping_path = (
        Path(__file__).resolve().parents[3]
        / "sales_report_v2_independent"
        / "data"
        / "inputs"
        / "mappings"
        / "entity_mappings.csv"
    )
    _entity_remap = {
        "AG": "AG", "CH": "AG", "Descomed": "UK", "Export": "GmbH",
        "GmBH": "GmbH", "GmbH": "GmbH", "Inc.": "US", "US": "US", "USA": "US", "UK": "UK",
    }

    # Build (entity, card_code) → company_group lookup from entity_mappings rows with explicit codes
    code_lookup: dict[tuple[str, str], str] = {}
    if mapping_path.exists():
        try:
            m = pd.read_csv(mapping_path, dtype=str)
            for col in ["Entity", "Customer_Code", "Company_Group"]:
                if col not in m.columns:
                    m[col] = pd.NA
                m[col] = m[col].astype("string").str.strip()
            has_code = m["Customer_Code"].notna() & m["Customer_Code"].ne("")
            has_cg = m["Company_Group"].notna() & m["Company_Group"].ne("")
            for _, row in m[has_code & has_cg].iterrows():
                ent = _entity_remap.get(str(row["Entity"]), str(row["Entity"]))
                code_lookup[(ent, str(row["Customer_Code"]))] = str(row["Company_Group"])
        except Exception as exc:
            log.warning("build_gold: company_group code lookup failed (%s)", exc)

    result = work.copy()
    result["company_group"] = pd.NA

    # 1. Explicit per-code mapping (covers US/UK rows listed in entity_mappings)
    if code_lookup:
        lookup_df = pd.DataFrame(
            [(ent, code, cg) for (ent, code), cg in code_lookup.items()],
            columns=["entity", "card_code", "_cg_explicit"],
        )
        result = result.merge(lookup_df, on=["entity", "card_code"], how="left")
        has_expl = result["_cg_explicit"].notna()
        result.loc[has_expl, "company_group"] = result.loc[has_expl, "_cg_explicit"]
        result = result.drop(columns=["_cg_explicit"])

    # 2. eCommerce / Shopify / Amazon channel → Company 3
    ch = result["channel"].astype("string").str.lower().fillna("")
    ecomm_mask = ch.str.contains("shopify|amazon|ecommerce", na=False)
    result.loc[ecomm_mask & result["company_group"].isna(), "company_group"] = "Company 3"

    # 3. Export distributor regions served by Company 2 → Company 2
    reg = result["region"].astype("string")
    c2_mask = reg.isin(_COMPANY2_REGIONS)
    result.loc[c2_mask & result["company_group"].isna(), "company_group"] = "Company 2"

    # 4. Interco rows carry no commercial company group
    result.loc[ch.eq("interco"), "company_group"] = pd.NA

    # 5. Everything else with a known entity → Company 1
    has_entity = result["entity"].notna() & result["entity"].astype("string").ne("")
    result.loc[has_entity & result["company_group"].isna(), "company_group"] = "Company 1"

    return result


def _load_salesperson_subregion_lookup() -> pd.DataFrame:
    mapping_path = (
        Path(__file__).resolve().parents[3]
        / "sales_report_v2_independent"
        / "data"
        / "inputs"
        / "mappings"
        / "entity_mappings.csv"
    )
    revised_mapping_path = (
        Path(__file__).resolve().parents[3]
        / "etl_pipeline"
        / "data"
        / "reference"
        / "revised_mapping_simple.xlsx"
    )

    entity_map = {
        "AG": "AG", "CH": "AG", "Descomed": "UK", "Export": "GmbH",
        "GmBH": "GmbH", "GmbH": "GmbH", "Inc.": "US", "US": "US",
        "USA": "US", "USA/Inc.": "US", "UK": "UK",
    }
    empty = pd.DataFrame(columns=["entity", "salesperson_name", "sub_region", "sub_region_count"])
    frames: list[pd.DataFrame] = []

    # ── Source 1: entity_mappings.csv (GmbH / AG / UK / US) ──────────────────
    if mapping_path.exists():
        try:
            mappings = pd.read_csv(mapping_path, dtype=str)
            needed = ["Entity", "Sub Region", "Sales_Employee", "Sales_Employee_Cleaned"]
            for col in needed:
                if col not in mappings.columns:
                    mappings[col] = pd.NA
                mappings[col] = mappings[col].astype("string").str.strip()

            mappings["salesperson_name"] = mappings["Sales_Employee_Cleaned"]
            missing_clean = mappings["salesperson_name"].isna() | mappings["salesperson_name"].eq("")
            mappings.loc[missing_clean, "salesperson_name"] = mappings.loc[missing_clean, "Sales_Employee"]
            mappings["entity"] = mappings["Entity"].map(entity_map).fillna(mappings["Entity"])

            work = mappings[
                mappings["entity"].notna()
                & mappings["salesperson_name"].notna()
                & ~mappings["salesperson_name"].eq("")
                & mappings["Sub Region"].notna()
                & ~mappings["Sub Region"].eq("")
            ][["entity", "salesperson_name", "Sub Region"]].drop_duplicates()
            if not work.empty:
                frames.append(work)
        except Exception as exc:
            log.warning("build_gold: failed reading salesperson sub-region mapping %s (%s)", mapping_path, exc)

    # ── Source 2: revised_mapping_simple.xlsx Salesperson sheet ──────────────
    # Provides authoritative sub_regions for US and any other reps in the sheet.
    if revised_mapping_path.exists():
        try:
            slp_sheet = pd.read_excel(revised_mapping_path, sheet_name="Salesperson", dtype=str)
            for col in ["Entity", "Salesperson", "Sub Region"]:
                if col not in slp_sheet.columns:
                    slp_sheet[col] = pd.NA
                slp_sheet[col] = slp_sheet[col].astype("string").str.strip()
            slp_sheet["entity"] = slp_sheet["Entity"].map(entity_map).fillna(slp_sheet["Entity"])
            slp_work = slp_sheet[
                slp_sheet["entity"].notna()
                & slp_sheet["Salesperson"].notna()
                & ~slp_sheet["Salesperson"].eq("")
                & slp_sheet["Sub Region"].notna()
                & ~slp_sheet["Sub Region"].eq("")
            ][["entity", "Salesperson", "Sub Region"]].rename(columns={"Salesperson": "salesperson_name"}).drop_duplicates()
            if not slp_work.empty:
                frames.append(slp_work)
        except Exception as exc:
            log.warning("build_gold: failed reading Salesperson sheet from revised_mapping_simple (%s)", exc)

    if not frames:
        return empty

    combined = pd.concat(frames, ignore_index=True)
    # revised_mapping_simple takes priority: keep last (Source 2 appended last)
    combined = combined.drop_duplicates(subset=["entity", "salesperson_name"], keep="last")

    grouped = (
        combined.groupby(["entity", "salesperson_name"], as_index=False)["Sub Region"]
        .agg(lambda values: sorted({str(v).strip() for v in values if pd.notna(v) and str(v).strip()}))
    )
    grouped["sub_region_count"] = grouped["Sub Region"].apply(len).astype("Int64")
    grouped["sub_region"] = grouped["Sub Region"].apply(
        lambda values: " | ".join(values) if len(values) > 0 else pd.NA
    )
    return grouped[["entity", "salesperson_name", "sub_region", "sub_region_count"]]


def _prepare_fact_sales(
    cold_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_salesperson: pd.DataFrame,
) -> pd.DataFrame:
    frames = []
    if not cold_df.empty:
        cold = cold_df.copy()
        cold["_source_rank"] = 1
        frames.append(cold)
    if not daily_df.empty:
        daily = daily_df.copy()
        daily["_source_rank"] = 2
        frames.append(daily)
    if not frames:
        return pd.DataFrame()

    fact = pd.concat(frames, ignore_index=True)
    fact["entity"] = _normalise_entity(fact["entity"])
    fact["card_code"] = fact["card_code"].astype(str)
    fact["item_code"] = fact["item_code"].astype(str)
    fact["slp_code"] = pd.to_numeric(fact["slp_code"], errors="coerce").fillna(-1).astype(int)
    fact["doc_entry"] = pd.to_numeric(fact["doc_entry"], errors="coerce")
    fact["line_num"] = pd.to_numeric(fact["line_num"], errors="coerce")
    fact["doc_date"] = pd.to_datetime(fact["doc_date"], errors="coerce")
    fact["net_revenue"] = pd.to_numeric(fact["net_revenue"], errors="coerce").fillna(0.0)
    fact["quantity"] = pd.to_numeric(fact["quantity"], errors="coerce").fillna(0.0)

    fact = fact.sort_values(["entity", "doc_entry", "line_num", "_source_rank"]).drop_duplicates(
        subset=["entity", "doc_entry", "line_num"], keep="last"
    )

    # Exclude known intercompany customer from external sales reporting.
    interco_lookup = dim_customer[["entity", "card_code", "card_name"]].copy()
    interco_lookup["card_code"] = interco_lookup["card_code"].astype(str)
    interco_mask = interco_lookup["card_name"].astype(str).str.contains(
        r"^\s*qms\s+inc\.?\s*;\s*usa\s*$",
        case=False,
        na=False,
        regex=True,
    )
    interco_pairs = interco_lookup.loc[interco_mask, ["entity", "card_code"]].drop_duplicates()
    if not interco_pairs.empty:
        fact = fact.merge(
            interco_pairs.assign(_exclude_interco=True),
            on=["entity", "card_code"],
            how="left",
        )
        excluded_rows = int(fact["_exclude_interco"].fillna(False).sum())
        if excluded_rows:
            log.info("build_gold: excluded %s intercompany rows from fact_sales", excluded_rows)
        fact = fact[fact["_exclude_interco"].isna()].copy()
        fact = fact.drop(columns=["_exclude_interco"])

    fact["currency"] = fact["entity"].map(ENTITY_CURRENCY)
    actuals_fx_rates = _resolve_actuals_fx_rates()
    fact["fx_rate"] = fact["currency"].map(actuals_fx_rates).fillna(1.0)
    fact["revenue_eur"] = fact["net_revenue"] * fact["fx_rate"]
    fact["date_key"] = fact["doc_date"].dt.strftime("%Y%m%d")
    fact["date_key"] = pd.to_numeric(fact["date_key"], errors="coerce")

    fact = fact.merge(
        dim_customer[["entity", "card_code", "customer_key"]],
        on=["entity", "card_code"],
        how="left",
    )
    # Normalise item_code: strip accidental float suffix (e.g. "1003100.0" → "1003100")
    # that appears when non-GmbH entities are parsed from CSV/Excel via float columns.
    fact["item_code"] = fact["item_code"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    # Product join is entity-agnostic: all entities share the QMS catalog and only
    # GmbH has a product master loaded.  Dedup to unique item_code → product_key.
    _prod_lookup = (
        dim_product[["item_code", "product_key"]]
        .drop_duplicates(subset="item_code", keep="first")
    )
    fact = fact.merge(_prod_lookup, on="item_code", how="left")
    fact = fact.merge(
        dim_salesperson[["entity", "slp_code", "salesperson_key"]],
        on=["entity", "slp_code"],
        how="left",
    )

    fact["is_bonus_credit"] = fact["item_code"].str.match(r"^9999", na=False)

    out_cols = [
        "date_key", "customer_key", "product_key", "salesperson_key", "entity",
        "doc_num", "doc_entry", "line_num", "doc_type", "quantity", "net_revenue", "revenue_eur", "currency",
        "card_code", "item_code", "slp_code", "doc_date", "is_bonus_credit",
    ]
    for col in out_cols:
        if col not in fact.columns:
            fact[col] = pd.NA

    return fact[out_cols]


def _prepare_fact_budget(
    budget_df: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_salesperson: pd.DataFrame,
    fact_sales: pd.DataFrame,
) -> pd.DataFrame:
    if budget_df.empty:
        return pd.DataFrame()

    work = _apply_distributor_customer_name_mapping(budget_df)

    # Some historical rows are missing market_group; infer it from workbook_type
    # so entity derivation and customer joins don't default incorrectly to GmbH.
    workbook_market_group_map = {
        "us_budget": "USA",
        "uk_budget": "UK",
        "core_markets_budget": "Core Markets",
        "export_budget": "Export",
    }
    if "workbook_type" in work.columns:
        blank_mg = work.get("market_group", pd.Series([pd.NA] * len(work))).isna() | work.get("market_group", pd.Series([pd.NA] * len(work))).astype(str).str.strip().eq("")
        inferred_mg = work["workbook_type"].astype("string").str.strip().map(workbook_market_group_map)
        work.loc[blank_mg, "market_group"] = inferred_mg.loc[blank_mg]

    # Derive entity from market_group (budget has no entity column)
    work["entity"] = work["market_group"].map(_MARKET_GROUP_ENTITY).fillna("GmbH")

    work["currency_code"] = work.get("currency_code", pd.Series(["EUR"] * len(work))).astype(str)

    # Switzerland budget rows are recorded by the AG entity in CHF.
    # _MARKET_GROUP_ENTITY maps "Core Markets" → "GmbH" which would produce the
    # wrong customer_key for 21xxx card codes.  Override with "AG" using the
    # CHF currency signal (Switzerland DE and FR sheets are always in CHF).
    _ch_currency = work["currency_code"].str.upper() == "CHF"
    _ch_region   = work.get("region", pd.Series([""] * len(work))).astype(str).str.contains("Switzerland", case=False, na=False)
    work.loc[_ch_currency | _ch_region, "entity"] = "AG"

    # Use pre-computed EUR values from canonical; fall back to native*fx
    if "budget_amount_eur_compare" in work.columns:
        work["budget_amount_eur"] = pd.to_numeric(work["budget_amount_eur_compare"], errors="coerce").fillna(0.0)
    else:
        native = pd.to_numeric(
            work.get("budget_amount_native", work.get("budget_amount", 0)),
            errors="coerce",
        ).fillna(0.0)
        work["budget_amount_eur"] = native * work["currency_code"].map(BUDGET_FX_RATES).fillna(1.0)

    work["budget_amount_native"] = pd.to_numeric(
        work.get("budget_amount_native", work.get("budget_amount", 0)), errors="coerce"
    ).fillna(0.0)

    work["budget_month"] = pd.to_datetime(work.get("budget_month"), errors="coerce")
    work["date_key"] = pd.to_numeric(work["budget_month"].dt.strftime("%Y%m%d"), errors="coerce")

    work["customer_code"] = _normalise_customer_code(work.get("customer_code", pd.Series([pd.NA] * len(work))))

    if "sales_person" not in work.columns:
        work["sales_person"] = pd.NA

    work["sales_person"] = work["sales_person"].replace({"": pd.NA})

    # Build name-based preferred code lookup before key join
    name_base = (
        dim_customer[["entity", "card_name", "card_code"]]
        .dropna(subset=["entity", "card_name", "card_code"])
        .drop_duplicates(subset=["entity", "card_name", "card_code"])
        .copy()
    )
    name_base["_is_distributor_code"] = name_base["card_code"].astype(str).str.startswith("2")

    def _pick_code(grp: pd.DataFrame) -> str | None:
        non_dist = grp.loc[~grp["_is_distributor_code"], "card_code"].astype(str).unique().tolist()
        if len(non_dist) == 1:
            return non_dist[0]
        all_codes = grp["card_code"].astype(str).unique().tolist()
        if len(all_codes) == 1:
            return all_codes[0]
        return None

    name_lookup = (
        name_base.groupby(["entity", "card_name"], as_index=False)
        .apply(lambda g: pd.Series({"card_code": _pick_code(g)}))
        .reset_index(drop=True)
    )
    name_lookup = name_lookup[name_lookup["card_code"].notna()]

    # For distributor-style codes, prefer non-distributor code by mapped name if available
    work = work.merge(
        name_lookup,
        left_on=["entity", "customer_name"],
        right_on=["entity", "card_name"],
        how="left",
        suffixes=("", "_name"),
    )
    if "card_code_name" not in work.columns and "card_code" in work.columns:
        work = work.rename(columns={"card_code": "card_code_name"})
    dist_code_mask = work["customer_code"].astype("string").str.startswith("2", na=False)
    preferred_mask = dist_code_mask & work["card_code_name"].notna()
    work.loc[preferred_mask, "customer_code"] = work.loc[preferred_mask, "card_code_name"]
    work = work.drop(columns=[c for c in ["card_name", "card_code_name"] if c in work.columns])

    # Join customer_key via entity + card_code
    cust_lookup = dim_customer[["entity", "card_code", "customer_key"]].drop_duplicates(
        subset=["entity", "card_code"], keep="first"
    )
    work = work.merge(
        cust_lookup,
        left_on=["entity", "customer_code"],
        right_on=["entity", "card_code"],
        how="left",
    )

    # If code-based join still fails, fall back to preferred name-based mapping.
    work = work.merge(
        name_lookup,
        left_on=["entity", "customer_name"],
        right_on=["entity", "card_name"],
        how="left",
        suffixes=("", "_name"),
    )
    fallback_mask = work["customer_key"].isna() & work["card_code_name"].notna()
    work.loc[fallback_mask, "customer_code"] = work.loc[fallback_mask, "card_code_name"]

    work = work.drop(columns=[c for c in ["customer_key", "card_code", "card_name", "card_code_name"] if c in work.columns])
    work = work.merge(
        cust_lookup,
        left_on=["entity", "customer_code"],
        right_on=["entity", "card_code"],
        how="left",
    )

    # Join salesperson_key via entity + display_name
    slp_lookup = dim_salesperson[["entity", "display_name", "salesperson_key"]].copy()
    slp_lookup = slp_lookup[slp_lookup["display_name"].notna()].copy()
    slp_lookup["display_name"] = slp_lookup["display_name"].astype(str)
    work["sales_person"] = work["sales_person"].where(work["sales_person"].notna(), pd.NA)
    work = work.merge(
        slp_lookup,
        left_on=["entity", "sales_person"],
        right_on=["entity", "display_name"],
        how="left",
        suffixes=("", "_slp"),
    )

    out_cols = [
        "date_key", "customer_key", "salesperson_key", "entity", "budget_amount_native",
        "budget_amount_eur", "currency_code", "workbook_type", "market_group", "region", "sub_region", "channel",
        "customer_code", "customer_name", "sales_person", "budget_month",
    ]
    for col in out_cols:
        if col not in work.columns:
            work[col] = pd.NA

    out = work[out_cols].copy()
    # Some canonical rows (especially historical budget snapshots) can carry blank
    # market_group/region/channel even when customer_key is present. Backfill from
    # dim_customer to keep report slicers/grouping complete.
    customer_attr = dim_customer[["customer_key", "market_group", "region", "channel"]].drop_duplicates(
        subset=["customer_key"], keep="first"
    )
    out = out.merge(
        customer_attr.rename(
            columns={
                "market_group": "market_group_dim",
                "region": "region_dim",
                "channel": "channel_dim",
            }
        ),
        on="customer_key",
        how="left",
    )

    blank_mg = out["market_group"].isna() | out["market_group"].astype(str).str.strip().eq("")
    blank_rg = out["region"].isna() | out["region"].astype(str).str.strip().eq("")
    blank_ch = out["channel"].isna() | out["channel"].astype(str).str.strip().eq("")

    out.loc[blank_mg, "market_group"] = out.loc[blank_mg, "market_group_dim"]
    out.loc[blank_rg, "region"] = out.loc[blank_rg, "region_dim"]
    out.loc[blank_ch, "channel"] = out.loc[blank_ch, "channel_dim"]

    # Keep reporting dimensions non-blank even for malformed historical rows.
    blank_rg_after = out["region"].isna() | out["region"].astype(str).str.strip().eq("")
    out.loc[blank_rg_after, "region"] = "Unknown"

    out = out.drop(columns=["market_group_dim", "region_dim", "channel_dim"])

    out = _allocate_export_distributor_budgets_final(out, dim_customer, fact_sales)
    out = out.drop_duplicates(
        subset=["entity", "customer_code", "budget_month", "workbook_type", "market_group"]
    )
    return out


def _allocate_export_distributor_budgets_final(
    budget_out: pd.DataFrame,
    dim_customer: pd.DataFrame,
    fact_sales: pd.DataFrame,
) -> pd.DataFrame:
    if budget_out.empty or dim_customer.empty:
        return budget_out

    out = budget_out.copy()
    month_year = pd.to_datetime(out.get("budget_month"), errors="coerce").dt.year
    aggregate_mask = (
        out.get("market_group", pd.Series([pd.NA] * len(out))).astype(str).eq("Export")
        & month_year.eq(2026)
        & out.get("customer_name", pd.Series([pd.NA] * len(out))).astype("string").str.startswith("Distributor -", na=False)
    )
    if not aggregate_mask.any():
        return out

    # Keep non-aggregate rows as-is; aggregate rows will be replaced by allocated rows.
    passthrough = out[~aggregate_mask].copy()
    aggregates = out[aggregate_mask].copy()

    cust = dim_customer.copy()
    for col in ["entity", "card_code", "card_name", "market_group", "region", "customer_key", "channel"]:
        if col not in cust.columns:
            cust[col] = pd.NA
    cust = cust[
        cust["market_group"].astype(str).eq("Export")
        & cust["region"].notna()
        & cust["card_code"].notna()
        & ~cust["card_name"].astype(str).str.startswith("Distributor -", na=False)
        & ~cust["card_code"].astype(str).str.startswith("2", na=False)
    ][["entity", "region", "card_code", "card_name", "customer_key", "channel"]].drop_duplicates()
    if cust.empty:
        return out

    sales_2025 = pd.DataFrame(columns=["customer_key", "sales_2025_eur"])
    if not fact_sales.empty:
        s = fact_sales[[c for c in ["customer_key", "doc_date", "revenue_eur"] if c in fact_sales.columns]].copy()
        if not s.empty:
            if "doc_date" not in s.columns:
                s["doc_date"] = pd.NaT
            if "revenue_eur" not in s.columns:
                s["revenue_eur"] = 0.0
            s["doc_date"] = pd.to_datetime(s["doc_date"], errors="coerce")
            s["revenue_eur"] = pd.to_numeric(s["revenue_eur"], errors="coerce").fillna(0.0)
            s = s[s["doc_date"].dt.year.eq(2025)]
            if not s.empty:
                sales_2025 = (
                    s.groupby("customer_key", as_index=False)["revenue_eur"]
                    .sum()
                    .rename(columns={"revenue_eur": "sales_2025_eur"})
                )

    targets = cust.merge(sales_2025, on="customer_key", how="left")
    targets["sales_2025_eur"] = pd.to_numeric(targets["sales_2025_eur"], errors="coerce").fillna(0.0).clip(lower=0.0)

    allocated_rows: list[pd.DataFrame] = []
    for _, src in aggregates.iterrows():
        region_targets = targets[
            targets["entity"].astype(str).eq(str(src.get("entity")))
            & targets["region"].astype(str).eq(str(src.get("region")))
        ].copy()
        if region_targets.empty:
            allocated_rows.append(pd.DataFrame([src]))
            continue

        total_sales = float(region_targets["sales_2025_eur"].sum())
        if total_sales > 0:
            region_targets["weight"] = region_targets["sales_2025_eur"] / total_sales
        else:
            region_targets["weight"] = 1.0 / len(region_targets)

        src_native = float(pd.to_numeric(src.get("budget_amount_native", 0.0), errors="coerce") or 0.0)
        src_eur = float(pd.to_numeric(src.get("budget_amount_eur", 0.0), errors="coerce") or 0.0)

        expanded = region_targets.copy().reset_index(drop=True)
        for col, val in src.items():
            if col not in ["customer_key", "customer_code", "customer_name", "channel", "budget_amount_native", "budget_amount_eur"]:
                expanded[col] = val

        expanded["customer_key"] = expanded["customer_key"].values
        expanded["customer_code"] = expanded["card_code"].astype(str).values
        expanded["customer_name"] = expanded["card_name"].astype(str).values
        expanded["channel"] = expanded["channel"].where(expanded["channel"].notna(), src.get("channel"))
        expanded["budget_amount_native"] = src_native * expanded["weight"].astype(float)
        expanded["budget_amount_eur"] = src_eur * expanded["weight"].astype(float)

        expanded = expanded[[c for c in out.columns if c in expanded.columns]].copy()
        allocated_rows.append(expanded)

    if not allocated_rows:
        return out

    allocated = pd.concat(allocated_rows, ignore_index=True)
    log.info(
        "build_gold: final-stage distributed %s aggregate export rows into %s customer rows",
        len(aggregates),
        len(allocated),
    )
    return pd.concat([passthrough, allocated], ignore_index=True)


def _allocate_export_distributor_budgets(
    budget_work: pd.DataFrame,
    dim_customer: pd.DataFrame,
    fact_sales: pd.DataFrame,
) -> pd.DataFrame:
    if budget_work.empty or dim_customer.empty:
        return budget_work

    work = budget_work.copy()

    year = pd.to_datetime(work.get("budget_month"), errors="coerce").dt.year
    aggregate_mask = (
        work.get("market_group", pd.Series([pd.NA] * len(work))).astype(str).eq("Export")
        & year.eq(2026)
        & work.get("customer_name", pd.Series([pd.NA] * len(work))).astype("string").str.startswith("Distributor -", na=False)
    )
    if not aggregate_mask.any():
        return work

    cust = dim_customer.copy()
    for col in ["entity", "card_code", "card_name", "market_group", "region", "customer_key", "channel"]:
        if col not in cust.columns:
            cust[col] = pd.NA

    cust = cust[
        cust["market_group"].astype(str).eq("Export")
        & cust["region"].notna()
        & cust["card_code"].notna()
    ].copy()

    # Exclude aggregate pseudo customers and placeholder distributor names.
    cust = cust[
        ~cust["card_name"].astype(str).str.startswith("Distributor -", na=False)
        & ~cust["card_code"].astype(str).str.startswith("2", na=False)
    ].copy()
    if cust.empty:
        return work

    sales_2025 = pd.DataFrame(columns=["customer_key", "sales_2025_eur"])
    if not fact_sales.empty:
        s = fact_sales.copy()
        for col in ["customer_key", "doc_date", "revenue_eur"]:
            if col not in s.columns:
                s[col] = pd.NA
        s["doc_date"] = pd.to_datetime(s["doc_date"], errors="coerce")
        s["revenue_eur"] = pd.to_numeric(s["revenue_eur"], errors="coerce").fillna(0.0)
        s = s[s["doc_date"].dt.year.eq(2025)].copy()
        if not s.empty:
            sales_2025 = (
                s.groupby("customer_key", as_index=False)["revenue_eur"]
                .sum()
                .rename(columns={"revenue_eur": "sales_2025_eur"})
            )

    target = cust[["entity", "region", "card_code", "card_name", "customer_key", "channel"]].drop_duplicates().copy()
    target = target.merge(sales_2025, on="customer_key", how="left")
    target["sales_2025_eur"] = pd.to_numeric(target["sales_2025_eur"], errors="coerce").fillna(0.0).clip(lower=0.0)

    alloc_rows = work[aggregate_mask].copy()
    passthrough = work[~aggregate_mask].copy()

    distributed: list[pd.DataFrame] = []
    for _, src in alloc_rows.iterrows():
        entity = src.get("entity")
        region = src.get("region")
        region_targets = target[
            target["entity"].astype(str).eq(str(entity))
            & target["region"].astype(str).eq(str(region))
        ].copy()

        if region_targets.empty:
            distributed.append(pd.DataFrame([src]))
            continue

        total_sales = float(region_targets["sales_2025_eur"].sum())
        if total_sales > 0:
            region_targets["weight"] = region_targets["sales_2025_eur"] / total_sales
        else:
            region_targets["weight"] = 1.0 / len(region_targets)

        src_eur = float(pd.to_numeric(src.get("budget_amount_eur_compare", 0.0), errors="coerce") or 0.0)
        src_native = float(pd.to_numeric(src.get("budget_amount_native", 0.0), errors="coerce") or 0.0)

        expanded = region_targets.copy().reset_index(drop=True)
        for col, val in src.items():
            if col not in ["customer_code", "customer_name", "channel", "budget_amount_eur_compare", "budget_amount_native"]:
                expanded[col] = val
        expanded["customer_code"] = expanded["card_code"].astype(str)
        expanded["customer_name"] = expanded["card_name"].astype(str)
        expanded["channel"] = expanded["channel"].where(expanded["channel"].notna(), src.get("channel"))
        expanded["budget_amount_eur_compare"] = src_eur * expanded["weight"].astype(float)
        expanded["budget_amount_native"] = src_native * expanded["weight"].astype(float)
        expanded = expanded.drop(columns=[c for c in ["card_code", "card_name", "customer_key", "sales_2025_eur", "weight"] if c in expanded.columns])
        distributed.append(expanded)

    if not distributed:
        return work

    allocated = pd.concat(distributed, ignore_index=True)
    out = pd.concat([passthrough, allocated], ignore_index=True)
    log.info("build_gold: distributed %s aggregate export budget rows into %s customer rows", len(alloc_rows), len(allocated))
    return out


def _prepare_ecommerce_budget_rows(ecomm_df: pd.DataFrame, dim_customer: pd.DataFrame) -> pd.DataFrame:
    """Map ecommerce report-level budget rows into fact_budget-compatible schema."""
    if ecomm_df.empty:
        return pd.DataFrame()

    work = ecomm_df.copy()
    if "source_label" not in work.columns:
        return pd.DataFrame()

    source_label = work["source_label"].astype("string").str.strip()
    source_label_norm = source_label.str.lower()

    # Keep aliases aligned with mapping naming (e.g. "eCommerce (excl. USA)").
    label_alias_map = {
        "ecommerce eu (incl. uk)": "ecommerce eu (incl. uk)",
        "ecommerce (excl. usa)": "ecommerce eu (incl. uk)",
        "ecommerce usa": "ecommerce usa",
        "amazon": "amazon",
        "global etailers": "global etailers",
        "retail usa": "retail usa",
    }
    canonical_label = source_label_norm.map(label_alias_map)

    # Map report-level ecommerce labels to stable pseudo/known customer codes.
    # Known US eCom customers are mapped to real SAP card codes for joinability.
    label_map: dict[str, tuple[str, str, str]] = {
        "ecommerce eu (incl. uk)": ("UK", "ECOMM_EU", "eCommerce EU (incl. UK)"),
        "ecommerce usa": ("USA", "40000", "Shopify, USA"),
        "amazon": ("USA", "41000", "Amazon"),
        "global etailers": ("UK", "GLOBAL_ETAILERS", "Global eTailers"),
        "retail usa": ("USA", "RETAIL_USA", "Retail USA"),
    }

    mapped = canonical_label.map(label_map)
    work = work[mapped.notna()].copy()
    if work.empty:
        return pd.DataFrame()

    mapped = mapped[mapped.notna()]
    work["_canonical_label"] = canonical_label.loc[work.index]
    work["market_group"] = mapped.map(lambda x: x[0])
    work["customer_code"] = mapped.map(lambda x: x[1])
    work["customer_name"] = mapped.map(lambda x: x[2])
    work["channel"] = "eCommerce"
    work["sub_region"] = pd.NA
    work["sales_person"] = pd.NA
    work["currency_code"] = work.get("currency_code", pd.Series(["EUR"] * len(work))).astype(str)
    work["budget_month"] = pd.to_datetime(work.get("budget_month"), errors="coerce")
    work["budget_amount_eur_compare"] = pd.to_numeric(
        work.get("budget_amount_eur_compare", work.get("budget_amount_raw", 0)),
        errors="coerce",
    ).fillna(0.0)
    work["budget_amount_native"] = pd.to_numeric(
        work.get("budget_amount_native", work.get("budget_amount_raw", 0)),
        errors="coerce",
    ).fillna(0.0)
    # Detailed workbook ecommerce lines are in kEUR; convert to EUR for fact_budget consistency.
    work["budget_amount_eur_compare"] = work["budget_amount_eur_compare"] * 1000.0
    work["budget_amount_native"] = work["budget_amount_native"] * 1000.0
    work["workbook_type"] = work.get("workbook_type", pd.Series(["group_budget"] * len(work))).fillna("group_budget")

    # Split combined eCommerce EU monthly budget across all non-US Shopify accounts.
    # This aligns detailed monthly kEUR totals to split Shopify account structure.
    shopify_targets = dim_customer[
        dim_customer["card_name"].astype(str).str.contains("Shopify", case=False, na=False)
        & dim_customer["entity"].astype(str).ne("US")
    ][["entity", "card_code", "card_name", "region", "channel"]].drop_duplicates()

    ecomm_eu_mask = work["_canonical_label"].eq("ecommerce eu (incl. uk)")
    ecomm_eu_rows = work[ecomm_eu_mask].copy()
    non_eu_rows = work[~ecomm_eu_mask].copy()

    ecomm_split = pd.DataFrame()
    if not ecomm_eu_rows.empty and not shopify_targets.empty:
        targets = shopify_targets.copy()
        targets["_k"] = 1
        ecomm_eu_rows["_k"] = 1
        ecomm_split = ecomm_eu_rows.merge(targets, on="_k", suffixes=("", "_target")).drop(columns=["_k"])

        n_targets = len(shopify_targets)
        ecomm_split["budget_amount_eur_compare"] = ecomm_split["budget_amount_eur_compare"] / n_targets
        ecomm_split["budget_amount_native"] = ecomm_split["budget_amount_native"] / n_targets
        ecomm_split["customer_code"] = ecomm_split["card_code"].astype(str)
        ecomm_split["customer_name"] = ecomm_split["card_name"].astype(str)
        ecomm_split["channel"] = ecomm_split["channel_target"].where(
            ecomm_split["channel_target"].notna(), ecomm_split["channel"]
        )
        ecomm_split["region"] = ecomm_split["region_target"].where(
            ecomm_split["region_target"].notna(), ecomm_split["region"]
        )
        ecomm_split["market_group"] = ecomm_split["entity"].map({"GmbH": "Core Markets", "UK": "UK", "AG": "Core Markets"}).fillna("Core Markets")

    if not ecomm_split.empty:
        work = pd.concat([non_eu_rows, ecomm_split], ignore_index=True)
    else:
        work = pd.concat([non_eu_rows, ecomm_eu_rows], ignore_index=True)

    keep_cols = [
        "budget_month",
        "workbook_type",
        "market_group",
        "region",
        "sub_region",
        "channel",
        "currency_code",
        "budget_amount_native",
        "budget_amount_eur_compare",
        "customer_code",
        "customer_name",
        "sales_person",
    ]
    for col in keep_cols:
        if col not in work.columns:
            work[col] = pd.NA

    return work[keep_cols]


def transform(date: str | None = None, dry_run: bool = False) -> dict:
    """Build and publish gold parquet tables from silver inputs."""
    _ = date
    container = get_container_client(container="bronze")

    dim_customer_raw = _read_parquet_blob(container, "silver/dim_customer/latest_enriched.parquet")
    dim_product_raw = _read_parquet_blob(container, "silver/dim_product/latest_enriched.parquet")
    dim_salesperson_raw = _read_parquet_blob(container, "silver/dim_salesperson/latest_enriched.parquet")
    budget_frames = []
    for bp in BUDGET_REGIONAL_PATHS:
        try:
            budget_frames.append(_read_parquet_blob(container, bp))
        except Exception:
            log.warning("build_gold: budget file missing — %s", bp)

    try:
        ecommerce_budget_raw = _read_parquet_blob(container, ECOMMERCE_REPORT_PATH)
    except Exception:
        ecommerce_budget_raw = pd.DataFrame()
        log.warning("build_gold: ecommerce budget file missing — %s", ECOMMERCE_REPORT_PATH)

    dim_customer = _prepare_dim_customer(dim_customer_raw)

    budget_raw = pd.concat(budget_frames, ignore_index=True) if budget_frames else pd.DataFrame()
    ecommerce_budget = _prepare_ecommerce_budget_rows(ecommerce_budget_raw, dim_customer)
    if not ecommerce_budget.empty:
        budget_raw = pd.concat([budget_raw, ecommerce_budget], ignore_index=True)

    dim_customer = _append_budget_only_customers(dim_customer, budget_raw)
    dim_product = _prepare_dim_product(dim_product_raw)
    dim_salesperson = _prepare_dim_salesperson(dim_salesperson_raw)

    # Back-fill customer sub_region from salesperson where not already set
    slp_sub = (
        dim_salesperson[["salesperson_key", "sub_region"]]
        .dropna(subset=["sub_region"])
        .rename(columns={"sub_region": "_slp_sub"})
    )
    dim_customer = dim_customer.merge(slp_sub, on="salesperson_key", how="left")
    needs_fill = dim_customer["sub_region"].isna() & dim_customer["_slp_sub"].notna()
    dim_customer.loc[needs_fill, "sub_region"] = dim_customer.loc[needs_fill, "_slp_sub"]
    dim_customer = dim_customer.drop(columns=["_slp_sub"])

    cold_history = _load_cold_extract_history(container)
    daily_latest = _load_latest_daily_incremental(container)
    fact_sales = _prepare_fact_sales(cold_history, daily_latest, dim_customer, dim_product, dim_salesperson)
    fact_budget = _prepare_fact_budget(budget_raw, dim_customer, dim_salesperson, fact_sales)
    dim_date = _build_dim_date(fact_sales, fact_budget)

    tables = {
        "dim_date": dim_date,
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_salesperson": dim_salesperson,
        "fact_sales": fact_sales,
        "fact_budget": fact_budget,
    }

    upload_sizes: dict[str, int] = {}
    if not dry_run:
        _delete_blob_if_exists(container, "gold/bridge_customer_salesperson_subregion.parquet")
        for name, df in tables.items():
            upload_sizes[name] = _write_parquet_blob(container, GOLD_PATHS[name], df)

    customer_key_fill = float(fact_sales["customer_key"].notna().mean()) if not fact_sales.empty else 0.0
    product_key_fill = float(fact_sales["product_key"].notna().mean()) if not fact_sales.empty else 0.0
    salesperson_key_fill = float(fact_sales["salesperson_key"].notna().mean()) if not fact_sales.empty else 0.0

    return {
        "status": "dry_run" if dry_run else "success",
        "run_ts": datetime.utcnow().isoformat() + "Z",
        "rows": {name: int(len(df)) for name, df in tables.items()},
        "fact_sales_key_fill": {
            "customer_key": round(customer_key_fill, 4),
            "product_key": round(product_key_fill, 4),
            "salesperson_key": round(salesperson_key_fill, 4),
        },
        "output_paths": GOLD_PATHS,
        "uploaded_bytes": upload_sizes,
    }


if __name__ == "__main__":
    import json as _json
    _result = transform()
    print(_json.dumps(_result, indent=2))
