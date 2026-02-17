"""
Tests for src/transforms/dim_product_to_parquet.py

Tests read_bronze_csv (comma separator, quoted fields) and clean_dataframe
(column renaming, date parsing, numeric downcasting, boolean flag normalisation).
"""

import io
import pandas as pd
import pytest

from tests.conftest import DIM_PRODUCT_CSV, MockContainerClient
from src.transforms.dim_product_to_parquet import read_bronze_csv, clean_dataframe


# ═════════════════════════════════════════════
# read_bronze_csv
# ═════════════════════════════════════════════

class TestReadBronzeCSV:
    """read_bronze_csv() should handle quoted-field product CSVs."""

    def test_parses_csv(self):
        client = MockContainerClient({"test.csv": DIM_PRODUCT_CSV})
        df = read_bronze_csv(client, "test.csv")

        assert df is not None
        assert len(df) == 3

    def test_correct_column_count(self):
        client = MockContainerClient({"test.csv": DIM_PRODUCT_CSV})
        df = read_bronze_csv(client, "test.csv")

        real_cols = [c for c in df.columns if not c.startswith("Unnamed")]
        assert len(real_cols) == 13

    def test_item_codes_parsed(self):
        client = MockContainerClient({"test.csv": DIM_PRODUCT_CSV})
        df = read_bronze_csv(client, "test.csv")

        codes = df["ItemCode"].tolist()
        assert "SKU-001" in codes
        assert "SKU-002" in codes
        assert "SKU-003" in codes


# ═════════════════════════════════════════════
# clean_dataframe
# ═════════════════════════════════════════════

class TestCleanDataframe:
    """clean_dataframe() normalises dim_product columns."""

    @pytest.fixture()
    def raw_df(self):
        client = MockContainerClient({"test.csv": DIM_PRODUCT_CSV})
        return read_bronze_csv(client, "test.csv")

    def test_renames_to_snake_case(self, raw_df):
        df = clean_dataframe(raw_df, "dim_product_master.csv")

        expected = [
            "entity", "item_code", "description", "item_group",
            "is_inventory", "is_sales_item", "is_active",
            "guidanceline", "kontrollfeld",
            "price_list_num", "price_list_name",
            "create_date", "update_date",
        ]
        for col in expected:
            assert col in df.columns, f"Missing column: {col}"

    def test_date_parsing(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")

        assert pd.api.types.is_datetime64_any_dtype(df["create_date"])
        assert pd.api.types.is_datetime64_any_dtype(df["update_date"])
        assert df["create_date"].iloc[0] == pd.Timestamp("2020-01-01")
        assert df["update_date"].iloc[2] == pd.Timestamp("2024-01-01")

    def test_numeric_downcast(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")

        for col in ["item_group", "price_list_num"]:
            assert df[col].dtype.name == "Int32", f"{col} should be Int32"

    def test_item_code_is_string(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")
        assert df["item_code"].dtype == object

    def test_entity_is_category(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")
        assert df["entity"].dtype.name == "category"

    def test_boolean_flag_normalisation_y(self, raw_df):
        """'Y' should stay 'Y'."""
        df = clean_dataframe(raw_df, "test.csv")

        assert df["is_inventory"].iloc[0] == "Y"
        assert df["is_sales_item"].iloc[0] == "Y"
        assert df["is_active"].iloc[0] == "Y"

    def test_boolean_flag_normalisation_n(self, raw_df):
        """'N' should stay 'N'."""
        df = clean_dataframe(raw_df, "test.csv")
        assert df["is_inventory"].iloc[2] == "N"

    def test_boolean_flag_normalisation_slash(self, raw_df):
        """SAP uses '/' to mean 'Y' — should be normalised to 'Y'."""
        df = clean_dataframe(raw_df, "test.csv")
        assert df["is_sales_item"].iloc[2] == "Y"

    def test_guidanceline_is_string(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")
        values = df["guidanceline"].tolist()
        assert "Premium" in values
        assert "Standard" in values

    def test_source_file_metadata(self, raw_df):
        df = clean_dataframe(raw_df, "dim_product/2026-02-17/dim_product_master.csv")
        assert all(df["_source_file"] == "dim_product_master.csv")
