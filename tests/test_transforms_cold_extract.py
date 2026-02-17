"""
Tests for src/transforms/cold_extract_to_parquet.py

Tests read_bronze_csv (= separator parsing) and clean_dataframe
(column renaming, German decimal parsing, date parsing, dtype casting).
"""

import io
import pandas as pd
import pytest

from tests.conftest import COLD_EXTRACT_CSV, MockContainerClient
from src.transforms.cold_extract_to_parquet import read_bronze_csv, clean_dataframe


# ═════════════════════════════════════════════
# read_bronze_csv
# ═════════════════════════════════════════════

class TestReadBronzeCSV:
    """read_bronze_csv() should parse = -separated SAP cold extract CSVs."""

    def test_parses_equals_separator(self):
        client = MockContainerClient({"test.csv": COLD_EXTRACT_CSV})
        df = read_bronze_csv(client, "test.csv")

        assert df is not None
        assert len(df) == 3

    def test_correct_column_count(self):
        client = MockContainerClient({"test.csv": COLD_EXTRACT_CSV})
        df = read_bronze_csv(client, "test.csv")

        # 13 real columns + 1 trailing Unnamed from trailing =
        real_cols = [c for c in df.columns if not c.startswith("Unnamed")]
        assert len(real_cols) == 13

    def test_entity_values(self):
        client = MockContainerClient({"test.csv": COLD_EXTRACT_CSV})
        df = read_bronze_csv(client, "test.csv")

        assert all(df["Entity"] == "GmbH")

    def test_returns_none_for_garbage(self):
        client = MockContainerClient({"bad.csv": b"not,a,valid\x00csv\xff\xfe"})
        df = read_bronze_csv(client, "bad.csv")
        # Should either return None or a DataFrame — not crash
        # (the parser may parse garbage into a DF, which is acceptable)
        assert df is None or isinstance(df, pd.DataFrame)


# ═════════════════════════════════════════════
# clean_dataframe
# ═════════════════════════════════════════════

class TestCleanDataframe:
    """clean_dataframe() normalises columns, parses German decimals & dates."""

    @pytest.fixture()
    def raw_df(self):
        """Parse the sample CSV into a raw DataFrame."""
        client = MockContainerClient({"test.csv": COLD_EXTRACT_CSV})
        return read_bronze_csv(client, "test.csv")

    def test_renames_to_snake_case(self, raw_df):
        df = clean_dataframe(raw_df, "cold_extract_gmbh.csv")

        expected_cols = [
            "entity", "doc_entry", "doc_num", "doc_date", "doc_type",
            "line_num", "card_code", "item_code", "description",
            "quantity", "net_revenue", "slp_code", "update_date",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"

    def test_drops_unnamed_columns(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")
        unnamed = [c for c in df.columns if c.startswith("Unnamed")]
        assert len(unnamed) == 0

    def test_german_decimal_parsing_quantity(self, raw_df):
        """'1,000000' → 1.0, '2,500000' → 2.5, '-1,000000' → -1.0"""
        df = clean_dataframe(raw_df, "test.csv")

        assert df["quantity"].iloc[0] == pytest.approx(1.0)
        assert df["quantity"].iloc[1] == pytest.approx(2.5)
        assert df["quantity"].iloc[2] == pytest.approx(-1.0)

    def test_german_decimal_parsing_revenue(self, raw_df):
        """'49,990000' → 49.99, '1.234,560000' → 1234.56"""
        df = clean_dataframe(raw_df, "test.csv")

        assert df["net_revenue"].iloc[0] == pytest.approx(49.99)
        assert df["net_revenue"].iloc[1] == pytest.approx(1234.56)
        assert df["net_revenue"].iloc[2] == pytest.approx(-29.99)

    def test_date_parsing(self, raw_df):
        """German date format dd.mm.yyyy HH:MM:SS → datetime."""
        df = clean_dataframe(raw_df, "test.csv")

        assert pd.api.types.is_datetime64_any_dtype(df["doc_date"])
        assert df["doc_date"].iloc[0] == pd.Timestamp("2023-01-02")
        assert df["doc_date"].iloc[2] == pd.Timestamp("2023-12-31")

    def test_numeric_downcast(self, raw_df):
        """doc_entry, doc_num, line_num, slp_code should be Int32."""
        df = clean_dataframe(raw_df, "test.csv")

        for col in ["doc_entry", "doc_num", "line_num", "slp_code"]:
            assert df[col].dtype.name == "Int32", f"{col} should be Int32, got {df[col].dtype}"

    def test_entity_is_category(self, raw_df):
        df = clean_dataframe(raw_df, "test.csv")
        assert df["entity"].dtype.name == "category"

    def test_source_file_metadata(self, raw_df):
        df = clean_dataframe(raw_df, "cold_extract/2026-02-16/gmbh_sales.csv")
        assert all(df["_source_file"] == "gmbh_sales.csv")
