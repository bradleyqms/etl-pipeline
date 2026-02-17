"""
Tests for src/transforms/dim_customer_to_parquet.py

Tests read_bronze_csv (comma separator with RFC 4180 quoted fields)
and clean_dataframe (column renaming, date parsing, downcasting, valid_for normalisation).
"""

import io
import pandas as pd
import pytest

from tests.conftest import DIM_CUSTOMER_CSV, MockContainerClient
from src.transforms.dim_customer_to_parquet import read_bronze_csv, clean_dataframe


# ═════════════════════════════════════════════
# read_bronze_csv
# ═════════════════════════════════════════════

class TestReadBronzeCSV:
    """read_bronze_csv() should handle RFC 4180 quoted CSV fields."""

    def test_parses_comma_separated_csv(self):
        client = MockContainerClient({"test.csv": DIM_CUSTOMER_CSV})
        df = read_bronze_csv(client, "test.csv")

        assert df is not None
        assert len(df) == 3

    def test_handles_embedded_commas_in_quotes(self):
        """'Schmidt, Mueller & Co.' should be a single CardName value."""
        client = MockContainerClient({"test.csv": DIM_CUSTOMER_CSV})
        df = read_bronze_csv(client, "test.csv")

        card_names = df["CardName"].tolist()
        assert "Schmidt, Mueller & Co." in card_names

    def test_handles_embedded_quotes(self):
        """'Test \"Quoted\" Name' should preserve the escaped quotes."""
        client = MockContainerClient({"test.csv": DIM_CUSTOMER_CSV})
        df = read_bronze_csv(client, "test.csv")

        card_names = df["CardName"].tolist()
        assert any("Quoted" in str(name) for name in card_names)

    def test_handles_embedded_commas_in_city(self):
        """'Frankfurt, Main' should be a single BillToCity value."""
        client = MockContainerClient({"test.csv": DIM_CUSTOMER_CSV})
        df = read_bronze_csv(client, "test.csv")

        cities = df["BillToCity"].tolist()
        assert "Frankfurt, Main" in cities

    def test_correct_column_count(self):
        client = MockContainerClient({"test.csv": DIM_CUSTOMER_CSV})
        df = read_bronze_csv(client, "test.csv")

        real_cols = [c for c in df.columns if not c.startswith("Unnamed")]
        assert len(real_cols) == 17


# ═════════════════════════════════════════════
# clean_dataframe
# ═════════════════════════════════════════════

class TestCleanDataframe:
    """clean_dataframe() normalises dim_customer columns."""

    @pytest.fixture()
    def raw_df(self):
        client = MockContainerClient({"test.csv": DIM_CUSTOMER_CSV})
        return read_bronze_csv(client, "test.csv")

    def test_renames_to_snake_case(self, raw_df):
        df = clean_dataframe(raw_df, "dim_customer_gmbh_extract.csv")

        expected = [
            "entity", "card_code", "card_name",
            "bill_to_street", "bill_to_city", "bill_to_zip", "bill_to_country",
            "ship_to_street", "ship_to_city", "ship_to_zip", "ship_to_country",
            "group_code", "territory", "slp_code",
            "create_date", "update_date", "valid_for",
        ]
        for col in expected:
            assert col in df.columns, f"Missing column: {col}"

    def test_date_parsing(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")

        assert pd.api.types.is_datetime64_any_dtype(df["create_date"])
        assert pd.api.types.is_datetime64_any_dtype(df["update_date"])
        assert df["create_date"].iloc[0] == pd.Timestamp("2022-06-01")
        assert df["update_date"].iloc[1] == pd.Timestamp("2023-11-20")

    def test_numeric_downcast_group_code(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")

        for col in ["group_code", "territory", "slp_code"]:
            assert df[col].dtype.name == "Int32", f"{col} should be Int32"

    def test_card_code_is_string(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")
        assert df["card_code"].dtype == object  # string

    def test_entity_is_category(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")
        assert df["entity"].dtype.name == "category"

    def test_valid_for_normalisation(self, raw_df):
        """Y stays Y, N stays N."""
        df = clean_dataframe(raw_df, "test.csv")

        values = df["valid_for"].tolist()
        assert values[0] == "Y"
        assert values[1] == "N"
        assert values[2] == "Y"

    def test_source_file_metadata(self, raw_df):
        df = clean_dataframe(raw_df, "dim_customer/2026-02-17/dim_customer_gmbh_extract.csv")
        assert all(df["_source_file"] == "dim_customer_gmbh_extract.csv")

    def test_preserves_embedded_commas_after_clean(self, raw_df):
        """Embedded commas in card_name should survive the clean step."""
        df = clean_dataframe(raw_df, "test.csv")
        assert "Schmidt, Mueller & Co." in df["card_name"].tolist()
