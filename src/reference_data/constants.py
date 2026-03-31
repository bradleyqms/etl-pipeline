"""Central reference constants for ETL transforms and validation scripts."""

from __future__ import annotations

# Actuals FX (local currency -> EUR)
FX_RATES: dict[str, float] = {
    "EUR": 1.00,
    "CHF": 1.08,
    "GBP": 1.20,
    "USD": 0.96,
}

# EOY-2025 management report FX (local currency -> EUR)
EOY_2025_FX_RATES: dict[str, float] = {
    "EUR": 1.00,
    "CHF": 1.08,
    "GBP": 1.21,
    "USD": 0.98,
}

# Budget FX (native budget currency -> EUR compare space)
BUDGET_FX_RATES: dict[str, float] = {
    "EUR": 1.0,
    "USD": 1.0 / 1.0757,
    "GBP": 1.0 / 1.20,
    "CHF": 1.0 / 1.05,
}

# Fact-sales entity -> ISO currency
ENTITY_CURRENCY: dict[str, str] = {
    "GmbH": "EUR",
    "AG": "CHF",
    "UK": "GBP",
    "US": "USD",
}

# Placeholder item-code buckets for customer metric calculations.
# Populate these lists once the business confirms exact SKU sets.
KEY_PRODUCTS: dict[str, list[str]] = {
    "algae_treatment": [],
    "collagen_pro": [],
    "exfoliant_pro": [],
    "collagen_retail": [],
}