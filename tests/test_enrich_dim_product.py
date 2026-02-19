"""
Tests for src/transforms/enrich_dim_product.py

Tests product_line normalisation, product_category derivation,
sku_type classification, and is_sellable logic.
"""

import pandas as pd
import pytest

from src.transforms.enrich_dim_product import (
    enrich,
    _normalise_product_line,
    _derive_product_category,
    _derive_sku_type,
    _is_sellable,
)


# ═════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════

def _make_product(**kwargs) -> pd.DataFrame:
    """Create a single-row dim_product DataFrame with sensible defaults."""
    defaults = {
        "entity":         "GmbH",
        "item_code":      "1001001",
        "description":    "Face Cream 50ml",
        "product_line":   "DERMA EXPERT",
        "is_active":      "Y",
        "webshop_active": "Y",
        "is_provisional": "N",
        "status":         "Active",
    }
    defaults.update(kwargs)
    return pd.DataFrame([defaults])


# ═════════════════════════════════════════════
# _normalise_product_line
# ═════════════════════════════════════════════

class TestNormaliseProductLine:

    def test_upper_normalised_to_canonical(self):
        assert _normalise_product_line("HYDROMAX") == "Hydromax"
        assert _normalise_product_line("AGE PREVENT") == "Age Prevent"
        assert _normalise_product_line("OFFERS") == "Offers"

    def test_mixed_case_variant(self):
        # "Hydromax" (title case from SAP) should match canonical
        assert _normalise_product_line("Hydromax") == "Hydromax"

    def test_sonstiges_maps_to_other(self):
        assert _normalise_product_line("SONSTIGES") == "Other"
        assert _normalise_product_line("-") == "Other"

    def test_none_and_empty(self):
        assert _normalise_product_line(None) == "Other"
        assert _normalise_product_line("") == "Other"
        assert _normalise_product_line("nan") == "Other"

    def test_packaging_variants(self):
        assert _normalise_product_line("VERPACKUNG") == "Packaging"
        assert _normalise_product_line("OLD PACKAGING") == "Packaging (Legacy)"

    def test_unknown_falls_back_to_title_case(self):
        result = _normalise_product_line("SOME NEW LINE")
        assert result == "Some New Line"


# ═════════════════════════════════════════════
# _derive_product_category
# ═════════════════════════════════════════════

class TestDeriveProductCategory:

    def test_treatment_lines(self):
        for pl in ["DERMA EXPERT", "AGE PREVENT", "HYDROMAX", "COLLAGEN SYSTEM",
                   "EPIGEN PROTECT", "PRECISION CARE", "BODY BALANCE"]:
            assert _derive_product_category(pl) == "Skincare Treatment", f"Failed for {pl}"

    def test_packaging(self):
        assert _derive_product_category("VERPACKUNG") == "Packaging"
        assert _derive_product_category("PACKAGING") == "Packaging"
        assert _derive_product_category("OLD PACKAGING") == "Packaging"

    def test_merchandising(self):
        assert _derive_product_category("INFOKARTE") == "Merchandising"
        assert _derive_product_category("AUFSTELLER") == "Merchandising"
        assert _derive_product_category("POSTER MIT POSTERSCHIENE") == "Merchandising"

    def test_promotional(self):
        assert _derive_product_category("OFFERS") == "Promotional"
        assert _derive_product_category("SETS") == "Promotional"

    def test_promotional_keyword_in_free_text(self):
        assert _derive_product_category("XMAS GIFT 2024") == "Promotional"
        assert _derive_product_category("TRAVEL SET") == "Promotional"

    def test_accessories(self):
        assert _derive_product_category("ACCESSORIES") == "Accessories"
        assert _derive_product_category("ACCESSOIRES") == "Accessories"
        assert _derive_product_category("PROFESSIONAL EQUIPMENT") == "Accessories"

    def test_uncategorised(self):
        assert _derive_product_category("SONSTIGES") == "Uncategorised"
        assert _derive_product_category("-") == "Uncategorised"
        assert _derive_product_category("OTHER") == "Uncategorised"

    def test_unknown_defaults_to_skincare_treatment(self):
        # Anything unrecognised defaults to the dominant category
        assert _derive_product_category("SOME UNKNOWN LINE") == "Skincare Treatment"


# ═════════════════════════════════════════════
# _derive_sku_type
# ═════════════════════════════════════════════

class TestDeriveSkuType:

    def test_a_prefix_is_internal(self):
        assert _derive_sku_type("A19001", "OFFERS", "N") == "internal"
        assert _derive_sku_type("A2001", "DERMA EXPERT", "N") == "internal"

    def test_short_numeric_code_is_service(self):
        assert _derive_sku_type("01", "SONSTIGES", "N") == "service"
        assert _derive_sku_type("2", "SONSTIGES", "N") == "service"
        assert _derive_sku_type("999", "SONSTIGES", "N") == "service"

    def test_service_by_description(self):
        assert _derive_sku_type("0002", "SONSTIGES", "N", "shipment cost") == "service"
        assert _derive_sku_type("0018", "SONSTIGES", "N", "Erstattung Frachtkosten") == "service"
        assert _derive_sku_type("0024", "SONSTIGES", "N", "Marketing/Promotion") == "service"

    def test_packaging_product_line(self):
        assert _derive_sku_type("1001111", "VERPACKUNG", "N") == "packaging"
        assert _derive_sku_type("1001112", "INFOKARTE", "N") == "packaging"
        assert _derive_sku_type("1001113", "OLD PACKAGING", "N") == "packaging"

    def test_promotional_product_line(self):
        assert _derive_sku_type("5050001", "OFFERS", "N") == "promotional"
        assert _derive_sku_type("5050002", "SETS", "N") == "promotional"

    def test_accessories_product_line(self):
        assert _derive_sku_type("7001001", "ACCESSORIES", "N") == "accessories"
        assert _derive_sku_type("7001002", "PROFESSIONAL EQUIPMENT", "N") == "accessories"

    def test_provisional_flag(self):
        assert _derive_sku_type("1001001", "DERMA EXPERT", "Y") == "provisional"

    def test_normal_product(self):
        assert _derive_sku_type("1001001", "DERMA EXPERT", "N") == "product"
        assert _derive_sku_type("5000101", "HYDROMAX", "N") == "product"


# ═════════════════════════════════════════════
# _is_sellable
# ═════════════════════════════════════════════

class TestIsSellable:

    def test_active_product_is_sellable(self):
        assert _is_sellable("Y", "product", "Y") is True

    def test_inactive_product_not_sellable(self):
        assert _is_sellable("N", "product", "N") is False

    def test_active_internal_not_sellable(self):
        assert _is_sellable("Y", "internal", "N") is False

    def test_active_packaging_not_sellable(self):
        assert _is_sellable("Y", "packaging", "N") is False

    def test_active_service_not_sellable(self):
        assert _is_sellable("Y", "service", "N") is False

    def test_active_promotional_is_sellable(self):
        assert _is_sellable("Y", "promotional", "N") is True

    def test_active_accessories_is_sellable(self):
        assert _is_sellable("Y", "accessories", "Y") is True

    def test_provisional_active_is_sellable(self):
        assert _is_sellable("Y", "provisional", "N") is True


# ═════════════════════════════════════════════
# enrich() integration tests
# ═════════════════════════════════════════════

class TestEnrich:

    def test_adds_required_columns(self):
        df = _make_product()
        enriched = enrich(df)
        for col in ["product_line_clean", "product_category", "sku_type", "is_sellable", "item_code_prefix"]:
            assert col in enriched.columns, f"Missing column: {col}"

    def test_product_line_clean_normalised(self):
        df = _make_product(product_line="HYDROMAX")
        enriched = enrich(df)
        assert enriched["product_line_clean"].iloc[0] == "Hydromax"

    def test_product_category_correct(self):
        df = _make_product(product_line="DERMA EXPERT")
        enriched = enrich(df)
        assert enriched["product_category"].iloc[0] == "Skincare Treatment"

    def test_sku_type_internal_for_a_prefix(self):
        df = _make_product(item_code="A19001", product_line="OFFERS", is_active="N")
        enriched = enrich(df)
        assert enriched["sku_type"].iloc[0] == "internal"
        assert enriched["is_sellable"].iloc[0] == False

    def test_sku_type_packaging(self):
        df = _make_product(item_code="1001111", product_line="VERPACKUNG", is_active="N")
        enriched = enrich(df)
        assert enriched["sku_type"].iloc[0] == "packaging"
        assert enriched["is_sellable"].iloc[0] == False

    def test_is_sellable_active_product(self):
        df = _make_product(item_code="1001001", product_line="DERMA EXPERT", is_active="Y")
        enriched = enrich(df)
        assert enriched["is_sellable"].iloc[0] == True

    def test_item_code_prefix_two_chars(self):
        df = _make_product(item_code="1001001")
        enriched = enrich(df)
        assert enriched["item_code_prefix"].iloc[0] == "10"

    def test_original_columns_preserved(self):
        df = _make_product()
        enriched = enrich(df)
        for col in ["entity", "item_code", "description", "product_line", "is_active"]:
            assert col in enriched.columns
