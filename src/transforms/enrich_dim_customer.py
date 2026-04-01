"""
transforms/enrich_dim_customer.py — Silver dim_customer enrichment

Adds four derived columns to silver/dim_customer/latest.parquet:
  - market_group   : high-level market segment (Germany, UK, USA, Export, …)
  - channel        : sales channel (B2C Online, B2C Retail, B2B Spa/Trade, …)
  - region         : geographic sub-region (DACH, Benelux, Nordics, …)
  - company_group  : legal entity operating the account

Resolution order (first match wins):
  1. entity_mappings.csv — explicit card_code lookup (highest trust)
  2. Entity-level rules  — entity=UK/US/AG implies market_group directly
  3. group_name rules    — Endverbraucher → B2C, Kunden → B2B, Mitarbeiter → Internal
  4. Card-code prefix    — number ranges encode entity sub-types (GmbH only)
  5. Territory ID        — SAP territory hierarchy for GmbH
  6. Bill-to-country     — fallback geography inference

Pattern discoveries from data analysis
───────────────────────────────────────
Entity breakdown (4,410 customers):
  GmbH  4070  — Germany-based entity, mixed B2C/B2B/Export
  UK     137  — Descomed Ltd (UK entity), all card_codes 51xx/53xx/59xx
  AG     104  — Swiss entity, card_codes mostly 21xx, group_name=Kunden
  US      99  — QMS Inc. (US entity), card_codes mostly 25xx

GmbH card_code prefix ranges:
  10xx  — Vertrieb / Export distributors (international trade accounts)
  18889 — inactive legacy record
  20xx  — AG-overlap zone (DACH wholesale/spa Kunden)
  21xx  — AG entity (Swiss Kunden)
  22-29 — GmbH B2B trade/spa accounts (various regions)
  30-32 — Mitarbeiter (employees / internal)
  36xx  — internal/intercompany
  40-41 — Endverbraucher DACH (B2C, mostly DE, some AT/CH) — territory 8
  46xx  — Endverbraucher EU (non-DACH B2C, BE/NL/IT/ES/FR)
  51xx  — UK entity (Descomed GB accounts) / GmbH UK-routed
  53xx  — UK entity (Descomed Ltd direct)
  59xx  — UK entity (various)

GmbH territory_id meanings (derived from data):
  8   → B2C Endverbraucher (DACH + EU direct consumers)
  1   → B2B Germany
  2   → B2B Germany South/Bavaria
  3   → B2B Export / International
  4   → B2B Germany North
  5   → B2B Germany Central
  6   → B2B Export / Americas
  11  → B2B Benelux
  12  → B2B DACH (CH/AT focused)
  15  → B2B Nordics
  21  → B2B France
  22  → B2B Iberia
  23  → B2B Italy
  24  → B2B Eastern Europe
  26  → B2B Export ROW
  27  → B2B APAC
  28  → B2B Middle East / Africa
  -2  → unassigned / legacy

Bill-to-country → region (fallback):
  ISO country codes mapped to geographic sub-regions.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.blob_client import get_container_client
from ..core.validation import add_etl_load_timestamp, current_utc_timestamp

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
SILVER_PATH = "silver/dim_customer/latest.parquet"
ENRICHED_PATH = "silver/dim_customer/latest_enriched.parquet"
ENTITY_MAPPINGS_PATH = (
    r"c:\Users\bradley\OneDrive - QMS Medicosmetics\Desktop\python_projects"
    r"\sales_report_v2_independent\data\inputs\mappings\entity_mappings.csv"
)


# ═════════════════════════════════════════════
# Reference tables
# ═════════════════════════════════════════════

# GmbH territory_id → market_group + region
TERRITORY_MAP: dict[int, tuple[str, str]] = {
    8:  ("Core Markets", "Germany"),      # B2C Endverbraucher (DACH consumers)
    1:  ("Core Markets", "Germany"),
    2:  ("Core Markets", "Germany"),
    4:  ("Core Markets", "Germany"),
    5:  ("Core Markets", "Germany"),
    3:  ("Export",       "International"),
    6:  ("Export",       "Americas"),
    11: ("Core Markets", "Benelux"),
    12: ("Core Markets", "Switzerland"),
    15: ("Core Markets", "Nordics"),
    21: ("Core Markets", "France"),
    22: ("Core Markets", "Spain"),
    23: ("Core Markets", "Italy"),
    24: ("Export",       "Eastern Europe"),
    26: ("Export",       "Export - Other ROW"),
    27: ("Export",       "Distributor - APAC"),
    28: ("Export",       "Distributor - Other ROW"),
}

# bill_to_country (ISO-2) → (market_group, region)
COUNTRY_MAP: dict[str, tuple[str, str]] = {
    "DE": ("Core Markets", "Germany"),
    "AT": ("Core Markets", "Germany"),      # Austria billed under Germany region
    "CH": ("Core Markets", "Switzerland"),
    "LI": ("Core Markets", "Switzerland"),
    "GB": ("UK",           "UK"),
    "IE": ("Core Markets", "UK"),
    "US": ("USA",          "USA"),
    "CA": ("USA",          "Americas"),
    "NL": ("Core Markets", "Benelux"),
    "BE": ("Core Markets", "Benelux"),
    "LU": ("Core Markets", "Benelux"),
    "FR": ("Core Markets", "France"),
    "ES": ("Core Markets", "Spain"),
    "ESX":("Core Markets", "Spain"),        # Canaries
    "IT": ("Core Markets", "Italy"),
    "SE": ("Core Markets", "Nordics"),
    "NO": ("Core Markets", "Nordics"),
    "DK": ("Core Markets", "Nordics"),
    "FI": ("Core Markets", "Nordics"),
    "PL": ("Export",       "Eastern Europe"),
    "CZ": ("Export",       "Eastern Europe"),
    "SK": ("Export",       "Eastern Europe"),
    "HU": ("Export",       "Eastern Europe"),
    "RO": ("Export",       "Eastern Europe"),
    "HR": ("Export",       "Eastern Europe"),
    "SI": ("Export",       "Eastern Europe"),
    "LT": ("Export",       "Eastern Europe"),
    "LV": ("Export",       "Eastern Europe"),
    "EE": ("Export",       "Eastern Europe"),
    "RU": ("Export",       "Distributor - Russia"),
    "UA": ("Export",       "Distributor - Other EU"),
    "ZA": ("Export",       "Distributor - South Africa"),
    "AE": ("Export",       "Distributor - Other ROW"),
    "SA": ("Export",       "Distributor - Other ROW"),
    "QA": ("Export",       "Distributor - Other ROW"),
    "IL": ("Export",       "Distributor - Other ROW"),
    "TR": ("Export",       "Distributor - Other ROW"),
    "IN": ("Export",       "Distributor - Other ROW"),
    "TH": ("Export",       "Distributor - APAC"),
    "SG": ("Export",       "Distributor - APAC"),
    "HK": ("Export",       "Distributor - APAC"),
    "CN": ("Export",       "Distributor - APAC"),
    "KR": ("Export",       "Distributor - APAC"),
    "MY": ("Export",       "Distributor - APAC"),
    "JP": ("Export",       "Distributor - APAC"),
    "AU": ("Export",       "Distributor - APAC"),
    "GR": ("Export",       "Distributor - Other EU"),
    "CY": ("Export",       "Distributor - Other EU"),
    "PT": ("Export",       "Distributor - Other EU"),
    "AL": ("Export",       "Export - Direct business"),
    "MV": ("Export",       "Distributor - Other ROW"),
    "VG": ("USA",          "Americas"),
}

# Per-country overrides for territory_id=26 ("Export - Other ROW") customers.
# These countries exist in COUNTRY_MAP but their default mapping is wrong in a
# distributor context (e.g. AT→Core Markets/Germany is correct for domestic
# B2B trade, but an AT distributor belongs to Distributor - Austria).
DISTRIBUTOR_COUNTRY_OVERRIDE: dict[str, tuple[str, str]] = {
    "AT": ("Export", "Distributor - Austria"),
    "IT": ("Export", "Distributor - Other EU"),
    "DK": ("Export", "Distributor - Other ROW"),
    "NO": ("Export", "Distributor - Other ROW"),
}

# group_name → channel
GROUP_NAME_CHANNEL: dict[str, str] = {
    "Endverbraucher": "B2C Online",      # consumers — direct / webshop
    "Kunden":         "B2B Trade",       # trade customers
    "Mitarbeiter":    "Internal",        # employees
    "Customers":      "B2B Trade",       # UK/US trade accounts
    "Vertrieb":       "B2B Distributor", # distributor / export trade
    "Kunde/Lieferant":"B2B Trade",       # customer/supplier hybrid
    "Konsolidierungsempfä": "Interco",   # intercompany consolidation
}

# ─────────────────────────────────────────────────────────────────────────────
# Per-card-code overrides (highest priority — beats entity_mappings and rules).
# Format: card_code → (market_group, region)
# Add entries here when a specific customer is systematically mis-classified by
# the territory / country fallback logic.
# ─────────────────────────────────────────────────────────────────────────────
CARD_CODE_OVERRIDE: dict[str, tuple[str, str]] = {
    # Eastern Europe distributors: territory-24 mis-classifies HR/HU as
    # "Eastern Europe"; App (entity_mappings) calls them Distributor - Other EU.
    "10049": ("Export", "Distributor - Other EU"),   # Biokozmetika D.O.O. (HR)
    "10031": ("Export", "Distributor - Other EU"),   # SpaTrend Wellness Kft (HU)
    # US webshop / eCommerce catch-all account
    "40000": ("USA",    "eCommerce USA"),
    # Shopify UK → eCommerce (shared EU+UK webshop pool), not UK/UK
    "59100": ("Core Markets", "eCommerce EU (incl. UK)"),
}


# Card-code prefix → channel override (GmbH-specific patterns)
# These take precedence over group_name for channel when present
CC_PREFIX_CHANNEL: dict[str, str] = {
    "10": "B2B Distributor",   # 10xx export/international distributors
    "40": "B2C Online",        # 40/41xx DACH consumers via webshop
    "41": "B2C Online",
    "46": "B2C Online",        # 46xx EU consumers via webshop
    "51": "B2B Trade",         # 51xx Descomed GB trade/hotel/spa
    "53": "B2B Trade",         # 53xx Descomed Ltd direct
    "59": "B2B Trade",         # 59xx Descomed misc
    "30": "Internal",          # 30/31/32 employees
    "31": "Internal",
    "32": "Internal",
}

# Entity → company_group
ENTITY_COMPANY_MAP: dict[str, str] = {
    "GmbH": "QMS Medicosmetics GmbH",
    "UK":   "Descomed Ltd",
    "AG":   "QMS Medicosmetics AG",
    "US":   "QMS Medicosmetics Inc.",
}


# ═════════════════════════════════════════════
# Core enrichment logic
# ═════════════════════════════════════════════

def _load_entity_mappings() -> pd.DataFrame | None:
    """Load entity_mappings.csv and normalise to card_code lookup."""
    try:
        em = pd.read_csv(ENTITY_MAPPINGS_PATH)
    except FileNotFoundError:
        log.warning("entity_mappings.csv not found at %s — skipping CSV join", ENTITY_MAPPINGS_PATH)
        return None

    # Normalise column names
    em.columns = [c.strip().lower().replace(" ", "_") for c in em.columns]

    valid = em["customer_code"].notna()
    em = em[valid].copy()
    em["card_code"] = em["customer_code"].astype(int).astype(str)

    # Keep only the columns we care about, de-duplicate on card_code
    # (entity_mappings can have one card_code mapped to multiple channels)
    keep = ["card_code", "market_group", "channel_level", "region", "sub_region", "company_group"]
    keep = [c for c in keep if c in em.columns]
    em = em[keep].copy()

    # Where there are duplicates, prefer non-Interco rows; otherwise take first
    non_interco = em[em.get("channel_level", pd.Series(dtype=str)) != "Interco"]
    if not non_interco.empty:
        em = pd.concat([non_interco, em[em.get("channel_level", pd.Series(dtype=str)) == "Interco"]])
    em = em.drop_duplicates("card_code", keep="first")

    return em.rename(columns={
        "market_group":  "em_market_group",
        "channel_level": "em_channel",
        "region":        "em_region",
        "company_group": "em_company_group",
    })


def _derive_row(entity: str, card_code: str, group_name, territory_id, bill_to_country) -> tuple[str, str, str, str]:
    """Rule-based derivation of (market_group, channel, region, company_group)."""

    # Normalise inputs
    entity       = str(entity or "").strip()
    card_code    = str(card_code or "").strip()
    group_name   = str(group_name or "").strip()
    bill_country = str(bill_to_country or "").strip().upper()
    prefix2      = card_code[:2]

    try:
        terr = int(territory_id)
    except (TypeError, ValueError):
        terr = None

    company_group = ENTITY_COMPANY_MAP.get(entity, entity)

    # ── Entity-level shortcuts ────────────────────────────────────────────
    if entity == "US":
        # Use bill_to_country to derive the correct market/region rather than
        # always defaulting to USA.  This fixes US-entity accounts (e.g. German
        # B2C webshop consumers) that are billed outside the US and should not
        # count as USA sales.  entity_mappings wins at Step 3 of enrich(), so
        # card_codes with an explicit mapping (25xxx spa, 40xxx eCommerce, etc.)
        # are unaffected by this rule.
        channel = GROUP_NAME_CHANNEL.get(group_name, "B2B Trade")
        if bill_country in COUNTRY_MAP:
            market_group, region = COUNTRY_MAP[bill_country]
            return market_group, channel, region, company_group
        return "USA", channel, "USA", company_group

    if entity == "AG":
        # Swiss entity — wholesale/spa Kunden (mostly DACH)
        channel = GROUP_NAME_CHANNEL.get(group_name, "B2B Trade")
        region  = "Switzerland" if bill_country in ("CH", "LI") else "Germany" if bill_country in ("DE", "AT") else "International"
        return "Core Markets", channel, region, company_group

    if entity == "UK":
        # All UK entity accounts → UK market
        channel = GROUP_NAME_CHANNEL.get(group_name, "B2B Trade")
        # Distinguish spa/hotel accounts (51xx/53xx) from retail/etail
        if prefix2 in ("51", "53", "59"):
            # channel set by entity_mappings (more granular); fallback B2B Trade
            channel = "B2B Trade"
        return "UK", channel, "UK", company_group

    # ── GmbH — layered rules ─────────────────────────────────────────────
    # 1. Channel from card_code prefix (high confidence)
    channel = CC_PREFIX_CHANNEL.get(prefix2)

    # 2. Channel fallback from group_name
    if channel is None:
        channel = GROUP_NAME_CHANNEL.get(group_name, "B2B Trade")

    # 3. market_group + region from territory
    #    Exception: if territory would classify as Export/International but
    #    bill_to_country resolves to a known Core Markets country, trust the
    #    country (more reliable geography than a catch-all territory assignment).
    if terr is not None and terr in TERRITORY_MAP:
        market_group, region = TERRITORY_MAP[terr]
        # Exception 1: territory 3 ("Export/International") but country resolves
        # to a known market — trust the country.
        if market_group == "Export" and region == "International" and bill_country in COUNTRY_MAP:
            market_group, region = COUNTRY_MAP[bill_country]
        # Exception 2: territory 26/28 ("Export - Other ROW" / "Distributor - Other
        # ROW") — use country to find the specific distributor region.  Check the
        # override map first (for countries whose COUNTRY_MAP entry is wrong in a
        # distributor context), then fall back to the standard COUNTRY_MAP.
        elif region in ("Export - Other ROW", "Distributor - Other ROW"):
            if bill_country in DISTRIBUTOR_COUNTRY_OVERRIDE:
                market_group, region = DISTRIBUTOR_COUNTRY_OVERRIDE[bill_country]
            elif bill_country in COUNTRY_MAP:
                market_group, region = COUNTRY_MAP[bill_country]
        # Exception 3: territory gives Core Markets/<region_A> but bill_to_country
        # indicates Core Markets/<region_B>.  Territory reflects the sales rep
        # assignment (organisational), country is the actual geography — trust it.
        elif market_group == "Core Markets" and bill_country in COUNTRY_MAP:
            country_mg, country_region = COUNTRY_MAP[bill_country]
            if country_mg == "Core Markets" and country_region != region:
                region = country_region
        # Exception 4: territory gives Export/Eastern Europe but bill_to_country
        # indicates a Core Markets country (e.g. French customers assigned to
        # territory 24 for sales-org reasons).  Trust the country.
        elif region == "Eastern Europe" and bill_country in COUNTRY_MAP:
            country_mg, country_region = COUNTRY_MAP[bill_country]
            if country_mg == "Core Markets":
                market_group, region = country_mg, country_region
        return market_group, channel, region, company_group

    # 4. market_group + region from bill_to_country
    if bill_country in COUNTRY_MAP:
        market_group, region = COUNTRY_MAP[bill_country]
        return market_group, channel, region, company_group

    # 5. Final fallback
    return "Export", channel, "International", company_group


def enrich(df: pd.DataFrame, entity_mappings: pd.DataFrame | None) -> pd.DataFrame:
    """Apply enrichment to a dim_customer DataFrame.

    Adds columns: market_group, channel, region, company_group.
    """
    df = df.copy()

    # ── Step 1: entity_mappings join ─────────────────────────────────────
    if entity_mappings is not None:
        df = df.merge(entity_mappings, on="card_code", how="left")
    else:
        df["em_market_group"] = None
        df["em_channel"]      = None
        df["em_region"]       = None
        df["em_company_group"]= None

    # ── Step 2: rule-based derivation for all rows ────────────────────────
    derived = df.apply(
        lambda r: _derive_row(
            r.get("entity"),
            r.get("card_code"),
            r.get("group_name"),
            r.get("territory_id"),
            r.get("bill_to_country"),
        ),
        axis=1,
        result_type="expand",
    )
    derived.columns = ["r_market_group", "r_channel", "r_region", "r_company_group"]
    df = pd.concat([df, derived], axis=1)

    # ── Step 3: merge — entity_mappings wins where present ────────────────
    df["market_group"]  = df["em_market_group"].where(df["em_market_group"].notna(), df["r_market_group"])
    df["channel"]       = df["em_channel"].where(df["em_channel"].notna(), df["r_channel"])
    df["region"]        = df["em_region"].where(df["em_region"].notna(), df["r_region"])
    df["company_group"] = df["em_company_group"].where(df["em_company_group"].notna(), df["r_company_group"])

    # Strip whitespace from entity_mappings values (source has trailing spaces)
    for col in ["market_group", "channel", "region", "company_group"]:
        if col in df.columns:
            df[col] = df[col].map(lambda value: value.strip() if isinstance(value, str) else value)

    # Drop working columns
    drop_cols = ["em_market_group", "em_channel", "em_region", "em_company_group",
                 "r_market_group",  "r_channel",  "r_region",  "r_company_group"]
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    # ── Step 4: per-card overrides (highest priority) ─────────────────────
    if CARD_CODE_OVERRIDE:
        ov_df = pd.DataFrame(
            [{"card_code": k, "ov_mg": v[0], "ov_rg": v[1]} for k, v in CARD_CODE_OVERRIDE.items()]
        )
        df = df.merge(ov_df, on="card_code", how="left")
        df["market_group"] = df["ov_mg"].combine_first(df["market_group"])
        df["region"]       = df["ov_rg"].combine_first(df["region"])
        df.drop(columns=["ov_mg", "ov_rg"], inplace=True)

    return df


# ═════════════════════════════════════════════
# Main transform
# ═════════════════════════════════════════════

def transform(dry_run: bool = False, etl_load_timestamp: str | None = None) -> dict:
    """Read silver dim_customer, enrich, write back as latest_enriched.parquet."""

    client = get_container_client()

    # Load silver
    raw = client.download_blob(SILVER_PATH).readall()
    df = pq.read_table(io.BytesIO(raw)).to_pandas()
    log.info("Loaded %d dim_customer rows from %s", len(df), SILVER_PATH)

    # Load entity_mappings
    em = _load_entity_mappings()
    em_rows = len(em) if em is not None else 0
    log.info("entity_mappings loaded: %d rows", em_rows)

    # Enrich
    etl_load_timestamp = etl_load_timestamp or current_utc_timestamp()
    enriched = add_etl_load_timestamp(enrich(df, em), etl_load_timestamp)

    # Coverage stats
    total = len(enriched)
    mg_filled  = enriched["market_group"].notna().sum()
    ch_filled  = enriched["channel"].notna().sum()
    reg_filled = enriched["region"].notna().sum()
    log.info(
        "Enrichment coverage — market_group: %d/%d (%.1f%%), channel: %d/%d (%.1f%%), region: %d/%d (%.1f%%)",
        mg_filled, total, mg_filled / total * 100,
        ch_filled, total, ch_filled / total * 100,
        reg_filled, total, reg_filled / total * 100,
    )

    if dry_run:
        log.info("[DRY RUN] Would write %s (%d rows)", ENRICHED_PATH, len(enriched))
        return _stats(enriched, total, mg_filled, ch_filled, reg_filled, dry_run=True)

    # Write enriched parquet
    table = pa.Table.from_pandas(enriched)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    data = buf.getvalue()
    client.upload_blob(name=ENRICHED_PATH, data=data, overwrite=True)
    log.info("Written %s (%.1f KB)", ENRICHED_PATH, len(data) / 1024)

    return _stats(enriched, total, mg_filled, ch_filled, reg_filled)


def _stats(df: pd.DataFrame, total: int, mg: int, ch: int, reg: int, dry_run: bool = False) -> dict:
    return {
        "status": "ok" if not dry_run else "dry_run",
        "pipeline": "enrich_dim_customer",
        "total_rows": total,
        "market_group": {
            "filled": int(mg),
            "pct": round(mg / total * 100, 1),
            "breakdown": df["market_group"].value_counts().to_dict(),
        },
        "channel": {
            "filled": int(ch),
            "pct": round(ch / total * 100, 1),
            "breakdown": df["channel"].value_counts().to_dict(),
        },
        "region": {
            "filled": int(reg),
            "pct": round(reg / total * 100, 1),
        },
        "output_path": ENRICHED_PATH,
    }
