"""
Tests for src/transforms/enrich_dim_customer.py

Tests the rule-based enrichment logic: market_group, channel, region, company_group
derivation from entity, card_code prefix, group_name, territory_id, bill_to_country.
"""

import pandas as pd
import pytest

from src.transforms.enrich_dim_customer import enrich, _derive_row, ENTITY_COMPANY_MAP


# ═════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════

def _make_customer(**kwargs) -> pd.DataFrame:
    """Create a single-row dim_customer DataFrame with sensible defaults."""
    defaults = {
        "entity":          "GmbH",
        "card_code":       "20001",
        "card_name":       "Test Customer",
        "group_name":      "Kunden",
        "territory_id":    1,
        "bill_to_country": "DE",
        "is_active":       "Y",
    }
    defaults.update(kwargs)
    return pd.DataFrame([defaults])


# ═════════════════════════════════════════════
# _derive_row unit tests
# ═════════════════════════════════════════════

class TestDeriveRow:
    """Unit tests for the core rule engine."""

    # ── Entity shortcuts ──────────────────────────────────────────────────

    def test_us_entity_always_usa(self):
        mg, ch, reg, cg = _derive_row("US", "25001", "Customers", 3, "US")
        assert mg == "USA"
        assert reg == "USA"
        assert cg == ENTITY_COMPANY_MAP["US"]

    def test_ag_entity_dach(self):
        mg, ch, reg, cg = _derive_row("AG", "21001", "Kunden", 1, "CH")
        assert mg == "Germany"
        assert reg == "DACH"
        assert cg == ENTITY_COMPANY_MAP["AG"]

    def test_ag_entity_non_dach(self):
        mg, ch, reg, cg = _derive_row("AG", "21050", "Kunden", 1, "FR")
        assert mg == "Germany"
        assert reg == "International"

    def test_uk_entity(self):
        mg, ch, reg, cg = _derive_row("UK", "51101", "Customers", None, "GB")
        assert mg == "UK"
        assert reg == "UK"
        assert cg == ENTITY_COMPANY_MAP["UK"]

    # ── GmbH — card_code prefix ────────────────────────────────────────────

    def test_gmbh_40_prefix_b2c(self):
        mg, ch, reg, cg = _derive_row("GmbH", "40001", "Endverbraucher", 8, "DE")
        assert ch == "B2C Online"

    def test_gmbh_41_prefix_b2c(self):
        _, ch, _, _ = _derive_row("GmbH", "41500", "Endverbraucher", 8, "DE")
        assert ch == "B2C Online"

    def test_gmbh_46_prefix_b2c_eu(self):
        _, ch, _, _ = _derive_row("GmbH", "46010", "Endverbraucher", 8, "BE")
        assert ch == "B2C Online"

    def test_gmbh_10_prefix_distributor(self):
        _, ch, _, _ = _derive_row("GmbH", "10018", "Vertrieb", 26, "AT")
        assert ch == "B2B Distributor"

    def test_gmbh_30_prefix_internal(self):
        _, ch, _, _ = _derive_row("GmbH", "30001", "Mitarbeiter", None, "DE")
        assert ch == "Internal"

    def test_gmbh_31_prefix_internal(self):
        _, ch, _, _ = _derive_row("GmbH", "31001", "Mitarbeiter", None, "DE")
        assert ch == "Internal"

    # ── GmbH — territory_id ────────────────────────────────────────────────

    def test_gmbh_territory_8_is_dach(self):
        mg, _, reg, _ = _derive_row("GmbH", "40001", "Endverbraucher", 8, "DE")
        assert mg == "Germany"
        assert reg == "DACH"

    def test_gmbh_territory_3_export(self):
        mg, _, reg, _ = _derive_row("GmbH", "20001", "Kunden", 3, "US")
        assert mg == "Export"
        assert reg == "International"

    def test_gmbh_territory_11_benelux(self):
        mg, _, reg, _ = _derive_row("GmbH", "23001", "Kunden", 11, "NL")
        assert mg == "Core Markets"
        assert reg == "Benelux"

    def test_gmbh_territory_21_france(self):
        mg, _, reg, _ = _derive_row("GmbH", "23002", "Kunden", 21, "FR")
        assert mg == "Core Markets"
        assert reg == "France"

    def test_gmbh_territory_26_export_row(self):
        mg, _, reg, _ = _derive_row("GmbH", "10040", "Vertrieb", 26, "EE")
        assert mg == "Export"

    # ── GmbH — country fallback ────────────────────────────────────────────

    def test_country_gb_falls_back_to_uk(self):
        mg, _, reg, _ = _derive_row("GmbH", "51101", "Kunden", None, "GB")
        assert mg == "UK"
        assert reg == "UK"

    def test_country_de_falls_back_to_germany(self):
        mg, _, reg, _ = _derive_row("GmbH", "20001", "Kunden", None, "DE")
        assert mg == "Germany"

    def test_unknown_country_fallback(self):
        mg, _, reg, _ = _derive_row("GmbH", "20001", "Kunden", None, "ZZ")
        assert mg == "Export"
        assert reg == "International"

    # ── group_name → channel fallback ─────────────────────────────────────

    def test_endverbraucher_channel(self):
        _, ch, _, _ = _derive_row("GmbH", "20001", "Endverbraucher", 8, "DE")
        assert ch == "B2C Online"

    def test_kunden_channel(self):
        _, ch, _, _ = _derive_row("GmbH", "22001", "Kunden", 1, "DE")
        assert ch == "B2B Trade"

    def test_mitarbeiter_channel(self):
        _, ch, _, _ = _derive_row("GmbH", "30001", "Mitarbeiter", None, "DE")
        assert ch == "Internal"

    def test_vertrieb_channel(self):
        _, ch, _, _ = _derive_row("GmbH", "10018", "Vertrieb", 26, "HK")
        assert ch == "B2B Distributor"

    # ── company_group ─────────────────────────────────────────────────────

    def test_company_group_gmbh(self):
        _, _, _, cg = _derive_row("GmbH", "20001", "Kunden", 1, "DE")
        assert cg == "QMS Medicosmetics GmbH"

    def test_company_group_uk(self):
        _, _, _, cg = _derive_row("UK", "51101", "Customers", None, "GB")
        assert cg == "Descomed Ltd"


# ═════════════════════════════════════════════
# enrich() integration tests (no blob needed)
# ═════════════════════════════════════════════

class TestEnrich:
    """Integration tests for enrich() using in-memory DataFrames."""

    def test_adds_required_columns(self):
        df = _make_customer()
        enriched = enrich(df, None)
        for col in ["market_group", "channel", "region", "company_group"]:
            assert col in enriched.columns, f"Missing column: {col}"

    def test_no_nulls_in_output_columns(self):
        rows = [
            _make_customer(entity="GmbH", card_code="40001", group_name="Endverbraucher", territory_id=8,  bill_to_country="DE"),
            _make_customer(entity="UK",   card_code="51101", group_name="Customers",      territory_id=None, bill_to_country="GB"),
            _make_customer(entity="US",   card_code="25001", group_name="Customers",      territory_id=3,  bill_to_country="US"),
            _make_customer(entity="AG",   card_code="21001", group_name="Kunden",         territory_id=1,  bill_to_country="CH"),
        ]
        df = pd.concat(rows, ignore_index=True)
        enriched = enrich(df, None)
        for col in ["market_group", "channel", "region", "company_group"]:
            assert enriched[col].notna().all(), f"Nulls found in {col}"

    def test_entity_mappings_win_over_rules(self):
        """entity_mappings values should override rule-derived ones."""
        df = _make_customer(entity="GmbH", card_code="51101", group_name="Kunden", territory_id=1, bill_to_country="DE")
        em = pd.DataFrame([{
            "card_code":       "51101",
            "em_market_group": "UK",
            "em_channel":      "Retail",
            "em_region":       "UK",
            "em_company_group":"Descomed Ltd",
        }])
        enriched = enrich(df, em)
        assert enriched["market_group"].iloc[0] == "UK"
        assert enriched["channel"].iloc[0] == "Retail"

    def test_entity_mappings_missing_card_code_falls_back_to_rules(self):
        """If card_code not in entity_mappings, rules should fill in."""
        df = _make_customer(entity="US", card_code="25999", group_name="Customers", territory_id=3, bill_to_country="US")
        em = pd.DataFrame([{
            "card_code":       "99999",  # Different code
            "em_market_group": "UK",
            "em_channel":      "Retail",
            "em_region":       "UK",
            "em_company_group":"Descomed Ltd",
        }])
        enriched = enrich(df, em)
        assert enriched["market_group"].iloc[0] == "USA"

    def test_channel_whitespace_stripped(self):
        """entity_mappings channel values with trailing spaces are stripped."""
        df = _make_customer(entity="UK", card_code="51101", group_name="Customers", territory_id=None, bill_to_country="GB")
        em = pd.DataFrame([{
            "card_code":       "51101",
            "em_market_group": "UK",
            "em_channel":      "Spa ",   # trailing space
            "em_region":       "UK",
            "em_company_group":"Descomed Ltd",
        }])
        enriched = enrich(df, em)
        assert enriched["channel"].iloc[0] == "Spa"   # stripped

    def test_original_columns_preserved(self):
        """enrich() should not drop original dim_customer columns."""
        df = _make_customer()
        enriched = enrich(df, None)
        for col in ["entity", "card_code", "card_name", "group_name", "bill_to_country"]:
            assert col in enriched.columns
