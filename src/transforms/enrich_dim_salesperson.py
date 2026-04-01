"""
transforms/enrich_dim_salesperson.py — Silver dim_salesperson enrichment

Adds organisational view columns to silver/dim_salesperson/latest.parquet
that mirror the App's Sales_Employee → Market_Group/Region mapping from
entity_mappings.csv.

Output: silver/dim_salesperson/latest_enriched.parquet
Columns added:
  - market_group  : organisational market segment
  - region        : organisational region (matches App's regional P&L view)
  - channel       : sales channel

Usage:
    cd etl_pipeline
    python -c "
    import sys; sys.path.insert(0, '.')
    from dotenv import load_dotenv; load_dotenv('.env')
    import src.transforms.enrich_dim_salesperson as m
    import logging, json; logging.basicConfig(level=logging.INFO)
    print(json.dumps(m.transform(), indent=2))
    "
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.blob_client import get_container_client
from src.core.validation import current_utc_timestamp

log = logging.getLogger(__name__)

SILVER_PATH   = "silver/dim_salesperson/latest.parquet"
ENRICHED_PATH = "silver/dim_salesperson/latest_enriched.parquet"

# ─────────────────────────────────────────────────────────────────────────────
# Crosswalk: (entity_norm, slp_code) → (market_group, region, channel)
#
# Derived by matching dim_salesperson.slp_name ↔ entity_mappings.sales_employee.
# entity_norm: 'AG' for Swiss entity (facts use 'CH', dim uses 'AG').
# Covers all slp_codes that carry material revenue in 2026 MTD.
# Unmapped codes fall through to a None / unknown marker so callers can
# decide how to handle them (e.g. fall back to dim_customer geography).
# ─────────────────────────────────────────────────────────────────────────────

# fmt: off
SLP_REGION_MAP: dict[tuple[str, int], tuple[str, str, str]] = {
    # ── Swiss AG entity ───────────────────────────────────────────────────
    ("AG",   -1): ("Core Markets", "Switzerland",           "B2B Trade"),
    ("AG",    1): ("Core Markets", "Switzerland",           "B2B Trade"),       # S. Braune (inactive)
    ("AG",    2): ("Core Markets", "Switzerland",           "B2B Trade"),       # G. Monopoli → Christiane
    ("AG",    4): ("Core Markets", "Switzerland",           "B2B Trade"),       # S. Loresco → Other Switzerland
    ("AG",    6): ("Core Markets", "Switzerland",           "B2B Trade"),       # S. Loresco Neukd
    ("AG",    9): ("Core Markets", "Switzerland",           "B2B Trade"),       # Ch. Rose Neukd
    ("AG",   10): ("Core Markets", "Switzerland",           "B2B Trade"),       # Ch. Rose → Christiane
    ("AG",   11): ("Core Markets", "Switzerland",           "B2B Trade"),       # Y. Januario → French Switzerland (kept as Switzerland at AG level)
    ("AG",   12): ("Core Markets", "Switzerland",           "B2B Trade"),       # Innendienst CH
    ("AG",   13): ("Core Markets", "Switzerland",           "B2B Trade"),       # E. Grigoreva CH → French Switzerland
    ("AG",   14): ("Core Markets", "Switzerland",           "B2B Trade"),       # E. Grigoreva Neukd CH

    # ── GmbH entity ──────────────────────────────────────────────────────
    # No sales employee / catch-all
    ("GmbH", -1): ("Core Markets", "Germany",               "B2B Trade"),       # -Kein Vertriebsmitarbeiter-

    # Germany
    ("GmbH",  2): ("Core Markets", "Germany",               "B2B Trade"),       # S. Wöhrle → Sibylle
    ("GmbH",  5): ("Core Markets", "Germany",               "B2B Trade"),       # Innendienst → DE Other
    ("GmbH", 11): ("Core Markets", "Germany",               "B2B Trade"),       # no GVL (inactive legacy)
    ("GmbH", 21): ("Core Markets", "Germany",               "B2B Trade"),       # Vertrieb
    ("GmbH", 23): ("Core Markets", "Germany",               "B2B Trade"),       # descomed → Interco (but within Germany P&L)
    ("GmbH", 25): ("Core Markets", "Germany",               "B2B Trade"),       # S. Wöhrle Neukd
    ("GmbH", 50): ("Core Markets", "Germany",               "B2B Trade"),       # Asia Cilek → Interco → Germany
    ("GmbH", 66): ("Core Markets", "Germany",               "B2B Trade"),       # K. Brunbauer → Kerstin
    ("GmbH", 67): ("Core Markets", "Germany",               "B2B Trade"),       # K. Brunbauer Neukd
    ("GmbH", 68): ("Core Markets", "Germany",               "Retail"),          # Steve Byrom-Chadd → Retail
    ("GmbH", 69): ("Core Markets", "Germany",               "B2B Trade"),       # U. Bensmann → Ulrike
    ("GmbH", 70): ("Core Markets", "Germany",               "B2B Trade"),       # U. Bensmann Neukd
    ("GmbH", 72): ("Core Markets", "Germany",               "B2B Trade"),       # Clare Morgan → Interco → Germany
    ("GmbH", 73): ("Core Markets", "Germany",               "B2B Trade"),       # M. Pfauch → Marina
    ("GmbH", 74): ("Core Markets", "Germany",               "B2B Trade"),       # M. Pfauch Neukd
    ("GmbH", 82): ("Core Markets", "Germany",               "B2B Trade"),       # M. Meiritz Neukd → Melanie
    ("GmbH", 84): ("Core Markets", "Germany",               "Retail"),          # Rainer Anskinewitsch → Retail
    ("GmbH", 87): ("Core Markets", "Germany",               "B2B Trade"),       # Larissa Scherer
    ("GmbH", 89): ("Core Markets", "Germany",               "B2B Trade"),       # Bayern
    ("GmbH", 91): ("Core Markets", "Germany",               "B2B Trade"),       # A. Gutierrez → Aracelli
    ("GmbH", 93): ("Core Markets", "Germany",               "B2B Trade"),       # A. Gutierrez Neukd
    ("GmbH", 99): ("Core Markets", "Germany",               "B2B Trade"),       # I. Papoulias → Iannis

    # Benelux
    ("GmbH", 36): ("Core Markets", "Benelux",               "B2B Trade"),       # Mark Stanlein BE (inactive)
    ("GmbH", 38): ("Core Markets", "Benelux",               "B2B Trade"),       # M. Mijnheer NL → Marjelein
    ("GmbH", 39): ("Core Markets", "Benelux",               "B2B Trade"),       # M. Mijnheer BE (inactive)
    ("GmbH", 46): ("Core Markets", "Benelux",               "B2B Trade"),       # A. Noran NL → Gabrielle
    ("GmbH", 47): ("Core Markets", "Benelux",               "B2B Trade"),       # A. Noran BE → Gabrielle
    ("GmbH", 53): ("Core Markets", "Benelux",               "B2B Trade"),       # A. Noran NL Neukd
    ("GmbH", 54): ("Core Markets", "Benelux",               "B2B Trade"),       # M. Mijnheer NL Neukd → Marjelein
    ("GmbH", 55): ("Core Markets", "Benelux",               "B2B Trade"),       # A. Noran BE Neukd
    ("GmbH", 85): ("Core Markets", "Benelux",               "B2B Trade"),       # G. van Eykern NL → Gabrielle
    ("GmbH", 86): ("Core Markets", "Benelux",               "B2B Trade"),       # G. van Eykern BE → Gabrielle
    ("GmbH", 90): ("Core Markets", "Benelux",               "B2B Trade"),       # G. van Eykern NL Neukd
    ("GmbH", 98): ("Core Markets", "Benelux",               "B2B Trade"),       # G. van Eykern BE Neukd

    # Other NL (Benelux catch-all in App)
    ("GmbH", 96): ("Core Markets", "Other NL",              "B2B Distributor"), # C. da Costa Campos → Benelux-Other
    ("GmbH", 92): ("Core Markets", "Other NL",              "B2B Distributor"), # C. da Costa Campos Neukd

    # Switzerland (GmbH-serviced CH accounts)
    # NB: Ch. Rose and Innendienst CH under GmbH entity = interco transfers
    # between QMS GmbH and QMS AG — entity_mappings classifies them as Interco.
    ("GmbH",  7): ("Core Markets", "Germany",               "B2C Online"),      # e-commerce — overridden below
    ("GmbH", 77): ("Core Markets", "Interco",               "Interco"),         # Ch. Rose Neukd (GmbH→AG interco)
    ("GmbH", 79): ("Core Markets", "Interco",               "Interco"),         # Ch. Rose (GmbH→AG interco)
    ("GmbH", 88): ("Core Markets", "Interco",               "Interco"),         # Innendienst CH (GmbH→AG interco)
    ("GmbH", 94): ("Core Markets", "Switzerland",           "B2B Trade"),       # E. Grigoreva CH → French Switzerland (GmbH)
    ("GmbH", 95): ("Core Markets", "Switzerland",           "B2B Trade"),       # E. Grigoreva Neukd CH → French Switzerland

    # France
    ("GmbH", 76): ("Core Markets", "France",                "B2B Trade"),       # G. Russo → France
    ("GmbH", 80): ("Core Markets", "France",                "B2B Trade"),       # Y. Januario FRA → France
    ("GmbH", 81): ("Core Markets", "France",                "B2B Trade"),       # Y. Januario Neukd FRA

    # Spain
    ("GmbH", 60): ("Core Markets", "Spain",                 "B2B Trade"),       # M. Calle Perez (inactive)
    ("GmbH", 75): ("Core Markets", "Spain",                 "B2B Trade"),       # M. G. Fernández → Montse
    ("GmbH", 97): ("Core Markets", "Spain",                 "B2B Trade"),       # M. G. Fernández Neukd

    # Italy
    ("GmbH", 100): ("Core Markets", "Italy",                "B2B Trade"),       # E. Grigoreva → Italy
    ("GmbH", 101): ("Core Markets", "Italy",                "B2B Trade"),       # E. Grigoreva Neukd

    # Nordics (no active revenue in March but mapping preserved)
    ("GmbH", 10): ("Core Markets", "Nordics",               "B2B Trade"),       # Nord (inactive)

    # eCommerce (GmbH/AG digital channels)
    # slp_code=7 "e-commerce" in App maps to eCommerce (excl. USA) / Core Markets
    # Override the entry above for slp_code=7 with proper channel
    # (handled separately — see note in _derive_slp_row)

    # Export / International
    ("GmbH", 83): ("Export",        "Export - Other ROW",   "B2B Distributor"), # Export (generic distributor bucket)
    ("GmbH", 78): ("Export",        "Distributor - Other ROW", "B2B Distributor"), # Solveig Loresco → Interco in App but Export geography
    ("GmbH", 56): ("Export",        "Distributor - Other ROW", "B2B Distributor"), # Solveig Loresco Neukd

    # Interco (GmbH)
    ("GmbH", 64): ("USA",           "Interco",              "Interco"),         # Interco (US-GmbH interco)
    ("GmbH", 32): ("UK",            "Interco",              "Interco"),         # Rowan → Interco UK
    ("GmbH", 11): ("Core Markets",  "Germany",              "Interco"),         # no GVL
}
# fmt: on

# Separate override for the e-commerce slp_code (channel differs from trade)
_ECOMMERCE_SLP: dict[tuple[str, int], tuple[str, str, str]] = {
    ("GmbH",  7): ("Core Markets", "eCommerce EU (incl. UK)", "B2C Online"),
    ("GmbH", 37): ("Core Markets", "Germany",                  "Retail"),        # KaDeWe Endverbr.
    ("GmbH", 22): ("Core Markets", "Germany",                  "Retail"),        # KaDeWe
}
SLP_REGION_MAP.update(_ECOMMERCE_SLP)

# UK entity salesperson mapping
_UK_SLP: dict[tuple[str, int], tuple[str, str, str]] = {
    ("UK",  -1): ("UK",  "UK",      "B2B Trade"),
    ("UK",   1): ("UK",  "UK",      "B2B Trade"),
    ("UK",   2): ("UK",  "UK",      "B2C Online"),    # e-commerce
}
SLP_REGION_MAP.update(_UK_SLP)

# US entity salesperson mapping (US sub-regions come from entity_mappings card_codes,
# not from salesperson — keep slp as pass-through, card_code join wins)
_US_SLP: dict[tuple[str, int], tuple[str, str, str]] = {
    ("US",  -1): ("USA", "USA",       "B2B Trade"),   # No Sales Employee / eCommerce
    ("US",   1): ("USA", "USA",       "B2B Trade"),   # Asia Cilek
    ("US",   2): ("USA", "Southeast", "B2B Trade"),   # Melissa Blamey
    ("US",   3): ("USA", "USA",       "B2B Trade"),   # Lisamarie DeLucia
    ("US",   4): ("USA", "Central",   "B2B Trade"),   # Amy Chasko
    ("US",   5): ("USA", "USA",       "B2B Trade"),   # Angela Caporaletti
    ("US",   6): ("USA", "USA",       "B2B Trade"),   # Bridget Lonergan
    ("US",   7): ("USA", "USA",       "B2B Trade"),   # Lisa Perez
    ("US",   8): ("USA", "USA",       "B2B Trade"),   # Nicole Tiberii
    ("US",   9): ("USA", "Northeast", "B2B Trade"),   # Emily Kasprowicz
    ("US",  10): ("USA", "West",      "B2B Trade"),   # Erin Shapiro
    ("US",  11): ("USA", "Southeast", "B2B Trade"),   # Bethany Phillips
    ("US",  12): ("USA", "Central",   "B2B Trade"),   # Marcus Howard
}
SLP_REGION_MAP.update(_US_SLP)


# ─────────────────────────────────────────────────────────────────────────────
# Display-name crosswalk: (entity_norm, slp_code) → App Sales_Employee_Cleaned
# Derived from entity_mappings.csv Sales_Employee_Cleaned column.
# Used to align ETL rep names to the App's display labels in drilldown reports.
# ─────────────────────────────────────────────────────────────────────────────
# fmt: off
SLP_DISPLAY_NAME: dict[tuple[str, int], str] = {
    # ── AG ───────────────────────────────────────────────────────────────
    ("AG",  -1): "Innendienst CH",
    ("AG",   2): "Christiane",          # G. Monopoli → Christiane
    ("AG",   4): "Other Switzerland",   # S. Loresco
    ("AG",   6): "Other Switzerland",   # S. Loresco Neukd
    ("AG",   9): "Christiane",          # Ch. Rose Neukd
    ("AG",  10): "Christiane",          # Ch. Rose
    ("AG",  11): "French Switzerland",  # Y. Januario
    ("AG",  12): "Innendienst CH",
    ("AG",  13): "French Switzerland",  # E. Grigoreva CH
    ("AG",  14): "French Switzerland",  # E. Grigoreva Neukd CH
    # ── GmbH Germany ────────────────────────────────────────────────────
    ("GmbH", -1): "DE Other",
    ("GmbH",  2): "Sibylle",            # S. Wöhrle
    ("GmbH",  5): "DE Other",           # Innendienst
    ("GmbH", 11): "DE Other",           # no GVL
    ("GmbH", 21): "DE Other",           # Vertrieb generic
    ("GmbH", 22): "Retail",             # KaDeWe direct
    ("GmbH", 23): "Interco",            # descomed
    ("GmbH", 25): "Sibylle",            # S. Wöhrle Neukd
    ("GmbH", 37): "Retail",             # KaDeWe Endverbraucher
    ("GmbH", 48): "Export",             # Jerome Dubarry
    ("GmbH", 50): "Interco",            # Asia Cilek
    ("GmbH", 66): "Kerstin",            # K. Brunbauer
    ("GmbH", 67): "Kerstin",            # K. Brunbauer Neukd
    ("GmbH", 68): "Retail",             # Steve Byrom-Chadd
    ("GmbH", 69): "Ulrike",             # U. Bensmann
    ("GmbH", 70): "Ulrike",             # U. Bensmann Neukd
    ("GmbH", 71): "DE Other",           # Ana Moll
    ("GmbH", 72): "Claire",             # Clare Morgan
    ("GmbH", 73): "Marina",             # M. Pfauch
    ("GmbH", 74): "Marina",             # M. Pfauch Neukd
    ("GmbH", 82): "Melanie",            # M. Meiritz Neukd
    ("GmbH", 84): "Retail",             # Rainer Anskinewitsch
    ("GmbH", 87): "Larissa",            # Larissa Scherer
    ("GmbH", 89): "Iannis",             # Bayern
    ("GmbH", 91): "Aracelli",           # A. Gutierrez
    ("GmbH", 93): "Aracelli",           # A. Gutierrez Neukd
    ("GmbH", 99): "Iannis",             # I. Papoulias
    # ── GmbH Benelux ────────────────────────────────────────────────────
    ("GmbH", 38): "Marjelein",          # M. Mijnheer NL
    ("GmbH", 46): "Gabrielle",          # A. Noran BE
    ("GmbH", 47): "Gabrielle",          # A. Noran NL
    ("GmbH", 53): "Gabrielle",          # A. Noran NL Neukd
    ("GmbH", 54): "Marjelein",          # M. Mijnheer NL Neukd
    ("GmbH", 55): "Gabrielle",          # A. Noran BE Neukd
    ("GmbH", 85): "Gabrielle",          # G. van Eykern NL
    ("GmbH", 86): "Gabrielle",          # G. van Eykern BE
    ("GmbH", 90): "Gabrielle",          # G. van Eykern NL Neukd
    ("GmbH", 92): "Benelux - Other",    # C. da Costa Campos Neukd
    ("GmbH", 96): "Benelux - Other",    # C. da Costa Campos
    ("GmbH", 98): "Gabrielle",          # G. van Eykern BE Neukd
    # ── GmbH Interco cross-entity ───────────────────────────────────────
    ("GmbH", 32): "Interco",            # Rowan (UK↔GmbH cross-entity)
    ("GmbH", 64): "Interco",            # USA↔GmbH interco
    ("GmbH", 77): "Interco",            # Ch. Rose Neukd (GmbH→AG)
    ("GmbH", 79): "Interco",            # Ch. Rose (GmbH→AG)
    ("GmbH", 88): "Interco",            # Innendienst CH (GmbH→AG)
    ("GmbH", 94): "French Switzerland", # E. Grigoreva CH
    ("GmbH", 95): "French Switzerland", # E. Grigoreva Neukd CH
    # ── GmbH France ─────────────────────────────────────────────────────
    ("GmbH", 76): "France",
    ("GmbH", 80): "France",             # Y. Januario FRA
    ("GmbH", 81): "France",             # Y. Januario Neukd FRA
    # ── GmbH Spain ──────────────────────────────────────────────────────
    ("GmbH", 75): "Montse",             # M. G. Fernández
    ("GmbH", 97): "Montse",             # M. G. Fernández Neukd
    # ── GmbH eCommerce ──────────────────────────────────────────────────
    ("GmbH",  7): "eCommerce EU (incl. UK)",  # matches App Sales_Employee_Cleaned label
    # ── GmbH Export ─────────────────────────────────────────────────────
    ("GmbH", 56): "Export",             # Solveig Loresco Neukd
    ("GmbH", 78): "Export",             # Solveig Loresco – all accounts are external distributors
    ("GmbH", 83): "Export",
    # ── GmbH Italy ──────────────────────────────────────────────────────
    ("GmbH", 100): "Italy",             # E. Grigoreva Italy
    ("GmbH", 101): "Italy",             # E. Grigoreva Neukd Italy
    # ── UK ──────────────────────────────────────────────────────────────
    ("UK",   -1): "UK Other",           # No Sales Employee UK
    ("UK",    1): "UK Other",           # placeholder
    ("UK",    2): "eCommerce UK",       # UK e-commerce
    # ── US / Inc. ────────────────────────────────────────────────────────
    ("US",   -1): "No Sales Employee",  # eCommerce / Amazon catch-all
    ("US",    1): "Asia",               # Asia Cilek
    ("US",    2): "Melissa",            # Melissa Blamey
    ("US",    3): "Lisamarie",          # Lisamarie DeLucia
    ("US",    4): "Amy",                # Amy Chasko
    ("US",    5): "Angela",             # Angela Caporaletti
    ("US",    6): "Bridget",            # Bridget Lonergan
    ("US",    7): "Lisa",               # Lisa Perez
    ("US",    8): "Nicole",             # Nicole Tiberii
    ("US",    9): "Emily",              # Emily Kasprowicz
    ("US",   10): "Erin",               # Erin Shapiro
    ("US",   11): "Bethany",            # Bethany Phillips
    ("US",   12): "Marcus",             # Marcus Howard
}
# fmt: on


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add market_group, region, channel to a dim_salesperson DataFrame."""
    df = df.copy()
    # Normalise entity so the enriched parquet uses the same keys as dim_customer
    # and the normalised fact_sales_daily entity (CH → AG).
    df["entity"] = df["entity"].astype(str).replace({"CH": "AG"})
    df["entity_norm"] = df["entity"]
    df["slp_code_int"] = pd.to_numeric(df["slp_code"], errors="coerce").fillna(-1).astype(int)

    def _lookup(row) -> tuple[str | None, str | None, str | None, str | None]:
        key = (row["entity_norm"], row["slp_code_int"])
        result = SLP_REGION_MAP.get(key)
        display = SLP_DISPLAY_NAME.get(key)
        if result:
            return (*result, display)
        return (None, None, None, display)

    enriched = df.apply(_lookup, axis=1, result_type="expand")
    enriched.columns = ["market_group", "region", "channel", "display_name"]

    df["market_group"]  = enriched["market_group"]
    df["region"]        = enriched["region"]
    df["channel"]       = enriched["channel"]
    df["display_name"]  = enriched["display_name"]

    coverage = df["market_group"].notna().sum()
    log.info("SLP enrichment coverage: %d/%d (%.1f%%)", coverage, len(df), coverage / len(df) * 100)
    return df


def transform(dry_run: bool = False) -> dict:
    """Read silver dim_salesperson, enrich, write back as latest_enriched.parquet."""
    client = get_container_client()

    raw = client.download_blob(SILVER_PATH).readall()
    df = pq.read_table(io.BytesIO(raw)).to_pandas()
    log.info("Loaded %d dim_salesperson rows from %s", len(df), SILVER_PATH)

    enriched = enrich(df)
    enriched["etl_load_timestamp"] = current_utc_timestamp()

    total     = len(enriched)
    mg_filled = enriched["market_group"].notna().sum()

    if dry_run:
        log.info("[DRY RUN] Would write %s (%d rows)", ENRICHED_PATH, total)
        return {"status": "dry_run", "total_rows": total, "mapped": int(mg_filled)}

    table = pa.Table.from_pandas(enriched)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    client.upload_blob(name=ENRICHED_PATH, data=buf.getvalue(), overwrite=True)
    log.info("Written %s (%.1f KB)", ENRICHED_PATH, len(buf.getvalue()) / 1024)

    return {
        "status": "ok",
        "pipeline": "enrich_dim_salesperson",
        "total_rows": total,
        "mapped": int(mg_filled),
        "pct": round(mg_filled / total * 100, 1),
        "unmapped": enriched[enriched["market_group"].isna()][["entity", "slp_code", "slp_name"]].to_dict("records"),
        "output_path": ENRICHED_PATH,
    }
