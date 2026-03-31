"""
transforms/enrich_dim_product.py — Silver dim_product enrichment

Adds derived columns to silver/dim_product/latest.parquet:
  - product_line_clean  : normalised product line (consistent casing/naming)
  - product_category    : high-level category grouping
  - sku_type            : functional SKU classification
  - is_sellable         : True if item is active, not internal, not packaging
  - item_code_prefix    : 2-char prefix for downstream analysis

Pattern discoveries from data analysis
───────────────────────────────────────
product_line has casing inconsistencies (e.g. "Offers" vs "OFFERS",
"Hydromax" vs "HYDROMAX").  Normalised to UPPER for canonical grouping.

product_line → product_category mapping:
  Treatment lines    → Skincare Treatment
  Packaging/wrapping → Packaging
  Offers/Sets        → Promotional
  Accessories        → Accessories
  Other/Sonstiges    → Uncategorised

item_code prefix patterns (from 5,478 rows):
  10xx  → Active catalogue items (mixed active/inactive), ~1,269 rows
  11xx  → Treatment items (active), ~315 rows
  13xx  → Service/misc items
  20xx  → Active items (276/300 active), likely replenishment/refill range
  21xx  → Active items (~158/188), skincare treatment
  40xx  → Packaging items (mostly inactive)
  41xx  → Packaging items (mostly inactive)
  43-45 → Obsolete/discontinued items (0 active)
  50xx  → Active sellable items (164/282 active)
  54-69 → Mostly inactive/legacy items
  89-99 → Inactive legacy items
  94xx  → Completely inactive (0/288 active)
  A1xx  → Non-sellable (0 active), likely internal/test codes
  A2xx  → Non-sellable (16 active very low), likely service codes

sku_type rules:
  item_code starts with A         → internal/system
  product_line contains PACKAGING/VERPACKUNG/AUFSTELLER/POSTER/INFOKARTE → packaging
  product_line contains OFFERS/SETS/SET                                  → promotional
  product_line contains ACCESSORIES/ACCESSOIRES/EQUIPMENT                → accessories
  otherwise                                                               → sellable_product
"""

from __future__ import annotations

import io
import logging
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.blob_client import get_container_client
from ..core.validation import add_etl_load_timestamp, current_utc_timestamp

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
SILVER_PATH   = "silver/dim_product/latest.parquet"
ENRICHED_PATH = "silver/dim_product/latest_enriched.parquet"


# ═════════════════════════════════════════════
# Reference tables
# ═════════════════════════════════════════════

# Canonical product_line names (normalised UPPER → display name)
# Handles casing variants from SAP data entry inconsistencies
PRODUCT_LINE_CANONICAL: dict[str, str] = {
    "OFFERS":                       "Offers",
    "SETS":                         "Sets",
    "AGE PREVENT":                  "Age Prevent",
    "HYDROMAX":                     "Hydromax",
    "DERMA EXPERT":                 "Derma Expert",
    "DERMA EXPERT FACE":            "Derma Expert Face",
    "DERMA EXPERT BODY":            "Derma Expert Body",
    "DERMA RESTORE":                "Derma Restore",
    "EPIGEN PROTECT":               "Epigen Protect",
    "PRECISION CARE":               "Precision Care",
    "COLLAGEN SYSTEM":              "Collagen System",
    "BODY BALANCE":                 "Body Balance",
    "ACTIVE GLOW":                  "Active Glow",
    "HYDROMAX":                     "Hydromax",
    "EXFOLIANT SYSTEM":             "Exfoliant System",
    "CLEANSE SYSTEM":               "Cleanse System",
    "OXYGEN":                       "Oxygen",
    "CORE SYSTEM":                  "Core System",
    "SONSTIGES":                    "Other",
    "OTHER":                        "Other",
    "-":                            "Other",
    "VERPACKUNG":                   "Packaging",
    "PACKAGING":                    "Packaging",
    "ACCESSORIES":                  "Accessories",
    "ACCESSOIRES":                  "Accessories",
    "TREATMENT ACCESSORIES":        "Treatment Accessories",
    "PROFESSIONAL EQUIPMENT":       "Professional Equipment",
    "INFOKARTE":                    "Info Cards",
    "AUFSTELLER":                   "Displays",
    "POSTER MIT POSTERSCHIENE":     "Posters",
    "OLD PACKAGING":                "Packaging (Legacy)",
}

# product_line (normalised upper) → product_category
PRODUCT_LINE_CATEGORY: dict[str, str] = {
    # Skincare treatment lines
    "AGE PREVENT":         "Skincare Treatment",
    "HYDROMAX":            "Skincare Treatment",
    "DERMA EXPERT":        "Skincare Treatment",
    "DERMA EXPERT FACE":   "Skincare Treatment",
    "DERMA EXPERT BODY":   "Skincare Treatment",
    "DERMA RESTORE":       "Skincare Treatment",
    "EPIGEN PROTECT":      "Skincare Treatment",
    "PRECISION CARE":      "Skincare Treatment",
    "COLLAGEN SYSTEM":     "Skincare Treatment",
    "BODY BALANCE":        "Skincare Treatment",
    "ACTIVE GLOW":         "Skincare Treatment",
    "EXFOLIANT SYSTEM":    "Skincare Treatment",
    "CLEANSE SYSTEM":      "Skincare Treatment",
    "OXYGEN":              "Skincare Treatment",
    "CORE SYSTEM":         "Skincare Treatment",
    # Promotional / bundles
    "OFFERS":              "Promotional",
    "SETS":                "Promotional",
    # Packaging & merchandising
    "VERPACKUNG":          "Packaging",
    "PACKAGING":           "Packaging",
    "OLD PACKAGING":       "Packaging",
    "INFOKARTE":           "Merchandising",
    "AUFSTELLER":          "Merchandising",
    "POSTER MIT POSTERSCHIENE": "Merchandising",
    # Accessories
    "ACCESSORIES":         "Accessories",
    "ACCESSOIRES":         "Accessories",
    "TREATMENT ACCESSORIES": "Accessories",
    "PROFESSIONAL EQUIPMENT": "Accessories",
    # Catch-all
    "SONSTIGES":           "Uncategorised",
    "OTHER":               "Uncategorised",
    "-":                   "Uncategorised",
    "ISOVERKAUF":          "Uncategorised",
}

# product_line_clean value → sales channel classification for reporting
# Retail  : packaged skincare products sold direct to consumers / retailers
# Professional : treatment / spa-consumable products sold to spa professionals
# Other   : packaging, promotional bundles, accessories, non-sellable items
# Update this dict whenever new product lines are added to the catalogue.
SKU_CHANNEL_MAP: dict[str, str] = {
    # ── Retail skincare lines ──────────────────────────────────────────────
    "Age Prevent":           "Retail",
    "Hydromax":              "Retail",
    "Derma Expert":          "Retail",
    "Derma Expert Face":     "Retail",
    "Derma Expert Body":     "Retail",
    "Derma Restore":         "Retail",
    "Epigen Protect":        "Retail",
    "Precision Care":        "Retail",
    "Collagen System":       "Retail",
    "Body Balance":          "Retail",
    "Active Glow":           "Retail",
    # ── Professional / spa treatment lines ───────────────────────────────
    "Core System":           "Professional",
    "Exfoliant System":      "Professional",
    "Cleanse System":        "Professional",
    "Oxygen":                "Professional",
    "Treatment Accessories": "Professional",
    "Professional Equipment": "Professional",
    # ── Other / non-product ──────────────────────────────────────────────
    "Offers":                "Other",
    "Sets":                  "Other",
    "Other":                 "Other",
    "Packaging":             "Other",
    "Packaging (Legacy)":    "Other",
    "Accessories":           "Other",
    "Info Cards":            "Other",
    "Displays":              "Other",
    "Posters":               "Other",
    "Merchandising":         "Other",
}

# Keywords in product_line (UPPER) that imply non-sellable sku_type
_PACKAGING_KW   = {"VERPACKUNG", "PACKAGING", "OLD PACKAGING", "INFOKARTE",
                   "AUFSTELLER", "POSTER", "POSTERSCHIENE"}
_PROMOTIONAL_KW = {"OFFERS", "SETS", "SET", "XMAS", "X-MAS", "GIFT",
                   "FESTIVE", "DISCOVERY", "TRAVEL", "STARTER", "KIT",
                   "ACQUISITION", "ENERGISING", "GLOWING"}
_ACCESSORY_KW   = {"ACCESSORIES", "ACCESSOIRES", "EQUIPMENT", "TREATMENT ACCESSORIES"}

# Service/admin keywords in description (lower) that flag non-sellable items
_SERVICE_KW = {
    "versandkosten", "frachtkosten", "rückerstattung", "gutschein", "warengutschein",
    "weiterberechnung", "erstattung", "shipping cost", "freight", "refund", "voucher",
    "credit note", "servicegebühr", "managementgebühr", "logistik",
    "versandtasche", "umschlag", "packseidenpapier", "dokumententasche",
    "shipment cost", "stock transfer", "marketing/promotion",
}

# ═════════════════════════════════════════════
# Normalisation helpers
# ═════════════════════════════════════════════

def _normalise_product_line(raw: str | None) -> str:
    """Map raw product_line to canonical display name."""
    if raw is None or str(raw).strip() in ("", "None", "nan", "NaN"):
        return "Other"
    upper = str(raw).strip().upper()
    return PRODUCT_LINE_CANONICAL.get(upper, str(raw).strip().title())


def _derive_product_category(pl_upper: str) -> str:
    """Map normalised-upper product_line to product_category."""
    cat = PRODUCT_LINE_CATEGORY.get(pl_upper)
    if cat:
        return cat
    # Keyword scan for set/xmas names from SAP free-text
    for kw in _PROMOTIONAL_KW:
        if kw in pl_upper:
            return "Promotional"
    for kw in _PACKAGING_KW:
        if kw in pl_upper:
            return "Packaging"
    for kw in _ACCESSORY_KW:
        if kw in pl_upper:
            return "Accessories"
    return "Skincare Treatment"   # default — most items are treatment products


def _derive_sku_type(item_code: str, pl_upper: str, is_provisional: str | None,
                     description: str | None = None) -> str:
    """Classify SKU into a functional type."""
    code = str(item_code or "").strip().upper()
    desc_lower = str(description or "").lower()

    # Internal/system codes (A-prefix)
    if code.startswith("A"):
        return "internal"

    # Very short numeric codes (≤3 chars) = service/admin records
    if len(code) <= 3 and code.isdigit():
        return "service"

    # Service/admin by description keyword
    for kw in _SERVICE_KW:
        if kw in desc_lower:
            return "service"

    # Packaging / marketing materials
    for kw in _PACKAGING_KW | {"DISPLAYS", "POSTERSCHIENE"}:
        if kw in pl_upper:
            return "packaging"

    # Promotional / bundles
    for kw in _PROMOTIONAL_KW:
        if kw in pl_upper:
            return "promotional"

    # Accessories / equipment
    for kw in _ACCESSORY_KW:
        if kw in pl_upper:
            return "accessories"

    # Provisional items (prototypes / samples)
    if str(is_provisional or "").strip().upper() == "Y":
        return "provisional"

    return "product"


def _is_sellable(is_active: str | None, sku_type: str, webshop_active: str | None) -> bool:
    """True if item can generate revenue (active product, not internal/packaging/service)."""
    active = str(is_active or "").strip().upper() == "Y"
    non_sellable = sku_type in ("internal", "packaging", "service")
    return active and not non_sellable


# ═════════════════════════════════════════════
# Core enrichment
# ═════════════════════════════════════════════

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Apply product enrichment to a dim_product DataFrame."""
    df = df.copy()

    # 1. product_line_clean — canonical display name
    df["product_line_clean"] = df["product_line"].apply(_normalise_product_line)

    # 2. Upper version for lookups (temporary, dropped at end)
    pl_upper = df["product_line"].fillna("").str.strip().str.upper()

    # 3. product_category — high-level grouping
    df["product_category"] = pl_upper.apply(_derive_product_category)

    # 4. sku_type — functional classification
    df["sku_type"] = [
        _derive_sku_type(ic, plu, ip, desc)
        for ic, plu, ip, desc in zip(
            df["item_code"].fillna(""),
            pl_upper,
            df.get("is_provisional", pd.Series(["N"] * len(df))).fillna("N"),
            df.get("description", pd.Series([""] * len(df))).fillna(""),
        )
    ]

    # 5. is_sellable — boolean flag
    df["is_sellable"] = [
        _is_sellable(ia, st, wa)
        for ia, st, wa in zip(
            df.get("is_active", pd.Series([None] * len(df))),
            df["sku_type"],
            df.get("webshop_active", pd.Series([None] * len(df))),
        )
    ]

    # 6. sku_channel — Retail / Professional / Other (from SKU_CHANNEL_MAP)
    df["sku_channel"] = df["product_line_clean"].map(SKU_CHANNEL_MAP).fillna("Other")

    # 7. item_code_prefix — first 2 chars for analysis
    df["item_code_prefix"] = df["item_code"].str[:2].str.upper()

    return df


# ═════════════════════════════════════════════
# Main transform
# ═════════════════════════════════════════════

def transform(dry_run: bool = False, etl_load_timestamp: str | None = None) -> dict:
    """Read silver dim_product, enrich, write back as latest_enriched.parquet."""
    client = get_container_client()

    raw = client.download_blob(SILVER_PATH).readall()
    df = pq.read_table(io.BytesIO(raw)).to_pandas()
    log.info("Loaded %d dim_product rows from %s", len(df), SILVER_PATH)

    etl_load_timestamp = etl_load_timestamp or current_utc_timestamp()
    enriched = add_etl_load_timestamp(enrich(df), etl_load_timestamp)

    total     = len(enriched)
    sellable  = int(enriched["is_sellable"].sum())
    by_cat    = enriched["product_category"].value_counts().to_dict()
    by_type   = enriched["sku_type"].value_counts().to_dict()

    log.info(
        "Enrichment done — %d total, %d sellable (%.1f%%)",
        total, sellable, sellable / total * 100,
    )

    if dry_run:
        log.info("[DRY RUN] Would write %s (%d rows)", ENRICHED_PATH, len(enriched))
        return _stats(total, sellable, by_cat, by_type, dry_run=True)

    table = pa.Table.from_pandas(enriched)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    data = buf.getvalue()
    client.upload_blob(name=ENRICHED_PATH, data=data, overwrite=True)
    log.info("Written %s (%.1f KB)", ENRICHED_PATH, len(data) / 1024)

    return _stats(total, sellable, by_cat, by_type)


def _stats(total: int, sellable: int, by_cat: dict, by_type: dict, dry_run: bool = False) -> dict:
    return {
        "status": "ok" if not dry_run else "dry_run",
        "pipeline": "enrich_dim_product",
        "total_rows": total,
        "sellable_rows": sellable,
        "sellable_pct": round(sellable / total * 100, 1),
        "product_category": by_cat,
        "sku_type": by_type,
        "output_path": ENRICHED_PATH,
    }
