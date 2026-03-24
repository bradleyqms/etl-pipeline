from __future__ import annotations

import io
from unittest.mock import MagicMock

import pandas as pd
import pyarrow.parquet as pq

from tests.conftest import COLD_EXTRACT_CSV, DIM_CUSTOMER_CSV, DIM_PRODUCT_CSV, MockContainerClient
from src.core.alerting import (
    _compute_alert_key,
    build_failure_alert,
    send_failure_alert,
    should_send_failure_alert,
    _load_alert_dedupe,
)
from src.core.validation import add_etl_load_timestamp, validate_dataframe
from src.transforms import cold_extract_to_parquet, dim_tables_to_parquet
from src.transforms import dim_customer_to_parquet, dim_product_to_parquet


DIM_SALESPERSON_CSV = (
    '"Entity","SlpCode","SlpName","Active","Commission","Locked"\n'
    '"GmbH","7","Alice","Y","5.0","N"\n'
).encode("utf-8")

BAD_DIM_PRODUCT_CSV = (
    '"Entity","Description","ItemGroup","IsActive","ProductLine","CreateDate"\n'
    '"GmbH","Face Cream 50ml","100","Y","Skincare","2020-01-01"\n'
).encode("utf-8")


BAD_COLD_EXTRACT_CSV = (
    "Entity=DocEntry=DocNum=DocDate=DocType=LineNum=CardCode=ItemCode="
    "Dscription=Quantity=SlpCode=UpdateDate=\n"
    "GmbH=1001=5001=02.01.2023 00:00:00=I=0=10001=SKU-001="
    "Face Cream 50ml=1,000000=7=02.01.2023 00:00:00=\n"
).encode("utf-8")


class TestValidationHelpers:
    def test_validate_dataframe_flags_missing_columns(self):
        df = pd.DataFrame({"entity": ["GmbH"], "doc_entry": [1001]})

        result = validate_dataframe(
            df,
            required_columns={"entity", "doc_entry", "net_revenue"},
            non_null_columns={"entity", "doc_entry"},
        )

        assert result.is_valid is False
        assert result.errors[0]["code"] == "missing_required_columns"
        assert "net_revenue" in result.errors[0]["columns"]

    def test_add_etl_load_timestamp_adds_utc_column(self):
        df = pd.DataFrame({"entity": ["GmbH"]})

        stamped = add_etl_load_timestamp(df, "2026-03-24T10:15:00+00:00")

        assert "etl_load_timestamp" in stamped.columns
        assert stamped["etl_load_timestamp"].iloc[0] == pd.Timestamp("2026-03-24T10:15:00+00:00")


class TestAlertingHelpers:
    def test_should_alert_for_top_level_dead_letter(self):
        result = {"status": "ok", "dead_letter_files": [{"source_blob": "bad.csv"}]}
        assert should_send_failure_alert(result) is True

    def test_should_alert_for_nested_transform_dead_letter(self):
        result = {"status": "ok", "transform": {"status": "ok", "dead_letter_files": [{"source_blob": "bad.csv"}]}}
        assert should_send_failure_alert(result) is True


class TestColdExtractReliability:
    def test_transform_quarantines_invalid_files_and_continues(self, monkeypatch):
        client = MockContainerClient(
            {
                "cold_extract/2026-02-17/good.csv": COLD_EXTRACT_CSV,
                "cold_extract/2026-02-17/bad.csv": BAD_COLD_EXTRACT_CSV,
            }
        )
        monkeypatch.setattr(cold_extract_to_parquet, "get_container_client", lambda: client)

        result = cold_extract_to_parquet.transform(date="2026-02-17")

        assert result["status"] == "ok"
        assert len(result["dead_letter_files"]) == 1
        assert result["files_converted"] == 1
        assert any(name.startswith("dead_letter/2026-02-17/bad.csv") for name in client._blobs)
        assert any(name.startswith("silver/cold_extract/2026-02-17/") for name in client._blobs)

    def test_transform_writes_etl_timestamp_to_parquet(self, monkeypatch):
        client = MockContainerClient(
            {
                "cold_extract/2026-02-17/good.csv": COLD_EXTRACT_CSV,
            }
        )
        monkeypatch.setattr(cold_extract_to_parquet, "get_container_client", lambda: client)

        result = cold_extract_to_parquet.transform(date="2026-02-17")

        silver_blob = next(name for name in client._blobs if name.startswith("silver/cold_extract/2026-02-17/"))
        table = pq.read_table(io.BytesIO(client._blobs[silver_blob]))
        df = table.to_pandas()

        assert result["etl_load_timestamp"]
        assert "etl_load_timestamp" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["etl_load_timestamp"])
        assert df["etl_load_timestamp"].nunique() == 1


class TestDimTablesReliability:
    def test_transform_quarantines_invalid_dim_file_and_stamps_outputs(self, monkeypatch):
        client = MockContainerClient(
            {
                "dim_tables/2026-02-17/dim_customer_gmbh_extract.csv": DIM_CUSTOMER_CSV,
                "dim_tables/2026-02-17/dim_product_master.csv": BAD_DIM_PRODUCT_CSV,
                "dim_tables/2026-02-17/dim_salesperson.csv": DIM_SALESPERSON_CSV,
            }
        )
        seen_timestamps = []

        monkeypatch.setattr(dim_tables_to_parquet, "get_container_client", lambda: client)
        monkeypatch.setattr(
            dim_tables_to_parquet._enrich_cust,
            "transform",
            lambda dry_run=False, etl_load_timestamp=None: seen_timestamps.append(etl_load_timestamp) or {
                "status": "ok",
                "total_rows": 1,
                "output_path": "silver/dim_customer/latest_enriched.parquet",
                "market_group": {"pct": 100.0},
            },
        )
        monkeypatch.setattr(
            dim_tables_to_parquet._enrich_prod,
            "transform",
            lambda dry_run=False, etl_load_timestamp=None: seen_timestamps.append(etl_load_timestamp) or {
                "status": "ok",
                "total_rows": 0,
                "output_path": "silver/dim_product/latest_enriched.parquet",
                "sellable_rows": 0,
            },
        )

        result = dim_tables_to_parquet.transform(date="2026-02-17")

        assert result["status"] == "ok"
        assert len(result["dead_letter_files"]) == 1
        assert any(name.startswith("dead_letter/2026-02-17/dim_product_master.csv") for name in client._blobs)
        assert any(name == "silver/dim_customer/latest.parquet" for name in client._blobs)
        assert any(name == "silver/dim_salesperson/latest.parquet" for name in client._blobs)
        assert seen_timestamps
        assert all(ts == result["etl_load_timestamp"] for ts in seen_timestamps)

        customer_table = pq.read_table(io.BytesIO(client._blobs["silver/dim_customer/latest.parquet"]))
        customer_df = customer_table.to_pandas()
        assert "etl_load_timestamp" in customer_df.columns
        assert customer_df["etl_load_timestamp"].nunique() == 1


# ─────────────────────────────────────────────
# Bad fixtures for direct dim transform tests
# ─────────────────────────────────────────────

BAD_CUSTOMER_DIRECT_CSV = (
    '"Entity","CardName","BillToCity"\n'
    '"GmbH","Acme Corp","Berlin"\n'
).encode("utf-8")

BAD_PRODUCT_DIRECT_CSV = (
    '"Entity","Description","ItemGroup"\n'
    '"GmbH","Face Cream 50ml","100"\n'
).encode("utf-8")


class TestDirectDimTransformReliability:
    """Direct dim_customer / dim_product transform() entry points must honour DNR-72."""

    def test_dim_customer_quarantines_invalid_and_stamps_valid(self, monkeypatch):
        client = MockContainerClient(
            {
                "dim_customer/2026-02-17/good_customer.csv": DIM_CUSTOMER_CSV,
                "dim_customer/2026-02-17/bad_customer.csv": BAD_CUSTOMER_DIRECT_CSV,
            }
        )
        monkeypatch.setattr(dim_customer_to_parquet, "get_container_client", lambda: client)

        result = dim_customer_to_parquet.transform(date="2026-02-17")

        assert result["status"] == "ok"
        assert result["files_converted"] == 1
        assert len(result["dead_letter_files"]) == 1
        assert any(n.startswith("dead_letter/2026-02-17/bad_customer.csv") for n in client._blobs)
        assert result["etl_load_timestamp"]

        latest_blob = client._blobs["silver/dim_customer/latest.parquet"]
        table = pq.read_table(io.BytesIO(latest_blob))
        assert "etl_load_timestamp" in table.schema.names

    def test_dim_product_quarantines_invalid_and_stamps_valid(self, monkeypatch):
        client = MockContainerClient(
            {
                "dim_product/2026-02-17/good_product.csv": DIM_PRODUCT_CSV,
                "dim_product/2026-02-17/bad_product.csv": BAD_PRODUCT_DIRECT_CSV,
            }
        )
        monkeypatch.setattr(dim_product_to_parquet, "get_container_client", lambda: client)

        result = dim_product_to_parquet.transform(date="2026-02-17")

        assert result["status"] == "ok"
        assert result["files_converted"] == 1
        assert len(result["dead_letter_files"]) == 1
        assert any(n.startswith("dead_letter/2026-02-17/bad_product.csv") for n in client._blobs)
        assert result["etl_load_timestamp"]

        latest_blob = client._blobs["silver/dim_product/latest.parquet"]
        table = pq.read_table(io.BytesIO(latest_blob))
        assert "etl_load_timestamp" in table.schema.names

    def test_dim_customer_propagates_caller_timestamp(self, monkeypatch):
        """Caller can inject a fixed timestamp so all outputs share the same value."""
        client = MockContainerClient(
            {"dim_customer/2026-02-17/good_customer.csv": DIM_CUSTOMER_CSV}
        )
        monkeypatch.setattr(dim_customer_to_parquet, "get_container_client", lambda: client)
        fixed_ts = "2026-03-24T08:00:00+00:00"

        result = dim_customer_to_parquet.transform(date="2026-02-17", etl_load_timestamp=fixed_ts)

        assert result["etl_load_timestamp"] == fixed_ts
        table = pq.read_table(io.BytesIO(client._blobs["silver/dim_customer/latest.parquet"]))
        df = table.to_pandas()
        assert df["etl_load_timestamp"].iloc[0] == pd.Timestamp(fixed_ts)


class TestAlertDelivery:
    """Alert build and send path — tests Graph sendMail integration via mocks."""

    def test_build_failure_alert_includes_key_fields(self):
        result = {
            "errors": ["missing column net_revenue"],
            "dead_letter_files": [{"source_blob": "bad.csv", "reason": "validation_failed"}],
        }
        subject, body = build_failure_alert(
            pipeline_name="cold_extract",
            environment="prod",
            result=result,
            run_date="2026-03-24",
        )

        assert "cold_extract" in subject
        assert "prod" in subject
        assert "net_revenue" in body
        assert "bad.csv" in body
        assert "2026-03-24" in body

    def test_send_failure_alert_posts_to_graph_sendmail(self, monkeypatch):
        monkeypatch.setenv("ALERT_ENABLED", "true")
        monkeypatch.setenv("ALERT_EMAIL_TO", "ops@example.com")
        monkeypatch.setattr("src.core.alerting.get_graph_token", lambda cfg: "fake-token")

        captured = {}
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        def _mock_post(url, headers, json, timeout):
            captured["url"] = url
            captured["headers"] = headers
            captured["payload"] = json
            return mock_response

        monkeypatch.setattr("src.core.alerting.requests.post", _mock_post)

        cfg = {
            "mailbox": "sap-reports@example.com",
            "tenant_id": "fake-tenant",
            "client_id": "fake-client",
            "client_secret": "fake-secret",
        }
        sent = send_failure_alert(
            cfg,
            pipeline_name="cold_extract",
            result={"errors": ["col missing"], "dead_letter_files": []},
            environment="prod",
        )

        assert sent is True
        assert "sendMail" in captured["url"]
        assert "sap-reports@example.com" in captured["url"]
        assert captured["headers"]["Authorization"] == "Bearer fake-token"
        to_addr = captured["payload"]["message"]["toRecipients"][0]["emailAddress"]["address"]
        assert to_addr == "ops@example.com"

    def test_send_failure_alert_skipped_when_disabled(self, monkeypatch):
        monkeypatch.setenv("ALERT_ENABLED", "false")
        monkeypatch.setenv("ALERT_EMAIL_TO", "ops@example.com")
        cfg = {"mailbox": "x@x.com", "tenant_id": "t", "client_id": "c", "client_secret": "s"}

        sent = send_failure_alert(
            cfg,
            pipeline_name="cold_extract",
            result={"errors": ["err"]},
            environment="prod",
        )

        assert sent is False

    def test_send_failure_alert_skipped_when_no_recipients(self, monkeypatch):
        monkeypatch.setenv("ALERT_ENABLED", "true")
        monkeypatch.setenv("ALERT_EMAIL_TO", "")
        cfg = {"mailbox": "x@x.com", "tenant_id": "t", "client_id": "c", "client_secret": "s"}

        sent = send_failure_alert(
            cfg,
            pipeline_name="cold_extract",
            result={"errors": ["err"]},
            environment="prod",
        )

        assert sent is False

    def test_should_not_alert_for_clean_result(self):
        result = {"status": "ok", "errors": [], "dead_letter_files": []}
        assert should_send_failure_alert(result) is False

    def test_send_failure_alert_saves_dedupe_state_after_send(self, monkeypatch):
        """Dedupe state must be saved after a successful send."""
        monkeypatch.setenv("ALERT_ENABLED", "true")
        monkeypatch.setenv("ALERT_EMAIL_TO", "ops@example.com")
        monkeypatch.setattr("src.core.alerting.get_graph_token", lambda cfg: "fake-token")

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        monkeypatch.setattr("src.core.alerting.requests.post", lambda *a, **kw: mock_response)

        saved = {}
        monkeypatch.setattr("src.core.alerting._load_alert_dedupe", lambda cfg, name: {})
        monkeypatch.setattr(
            "src.core.alerting._save_alert_dedupe",
            lambda cfg, name, key: saved.update({"key": key}),
        )

        cfg = {"mailbox": "sap@example.com", "tenant_id": "t", "client_id": "c", "client_secret": "s"}
        send_failure_alert(
            cfg,
            pipeline_name="cold_extract",
            result={"errors": ["col missing"], "dead_letter_files": []},
            environment="prod",
            run_date="2026-03-24",
        )

        assert "key" in saved
        assert saved["key"].startswith("cold_extract:2026-03-24:")


class TestAlertDedupe:
    """Alert deduplication — identical failure keys must suppress retransmit."""

    def _result_with_dead_letter(self, source_blob: str = "cold_extract/2026-03-24/bad.csv") -> dict:
        return {
            "status": "ok",
            "errors": [],
            "dead_letter_files": [{"source_blob": source_blob, "reason": "validation_failed"}],
            "date": "2026-03-24",
        }

    def test_compute_alert_key_is_stable(self):
        """Same pipeline/date/result should always produce the same key."""
        result = self._result_with_dead_letter()
        k1 = _compute_alert_key("cold_extract", "2026-03-24", result)
        k2 = _compute_alert_key("cold_extract", "2026-03-24", result)
        assert k1 == k2
        assert k1.startswith("cold_extract:2026-03-24:")

    def test_compute_alert_key_differs_for_different_source_blobs(self):
        """A new failing file on the same date must yield a different key."""
        result_a = self._result_with_dead_letter("path/to/file_a.csv")
        result_b = self._result_with_dead_letter("path/to/file_b.csv")
        assert _compute_alert_key("cold_extract", "2026-03-24", result_a) != \
               _compute_alert_key("cold_extract", "2026-03-24", result_b)

    def test_compute_alert_key_differs_for_different_dates(self):
        result = self._result_with_dead_letter()
        assert _compute_alert_key("cold_extract", "2026-03-24", result) != \
               _compute_alert_key("cold_extract", "2026-03-25", result)

    def test_send_failure_alert_suppresses_duplicate_key(self, monkeypatch):
        """If the stored key matches, no POST should be made and return is False."""
        monkeypatch.setenv("ALERT_ENABLED", "true")
        monkeypatch.setenv("ALERT_EMAIL_TO", "ops@example.com")

        result = self._result_with_dead_letter()
        matching_key = _compute_alert_key("cold_extract", "2026-03-24", result)

        monkeypatch.setattr(
            "src.core.alerting._load_alert_dedupe",
            lambda cfg, name: {"last_key": matching_key},
        )

        post_called = []
        monkeypatch.setattr("src.core.alerting.requests.post", lambda *a, **kw: post_called.append(1))

        cfg = {"mailbox": "x@x.com", "tenant_id": "t", "client_id": "c", "client_secret": "s"}
        sent = send_failure_alert(
            cfg,
            pipeline_name="cold_extract",
            result=result,
            environment="prod",
            run_date="2026-03-24",
        )

        assert sent is False
        assert not post_called

    def test_send_failure_alert_fires_when_key_changes(self, monkeypatch):
        """A different key (new file failing) must not be suppressed."""
        monkeypatch.setenv("ALERT_ENABLED", "true")
        monkeypatch.setenv("ALERT_EMAIL_TO", "ops@example.com")
        monkeypatch.setattr("src.core.alerting.get_graph_token", lambda cfg: "t")
        monkeypatch.setattr("src.core.alerting._save_alert_dedupe", lambda *a: None)

        result = self._result_with_dead_letter("path/to/new_file.csv")
        # Stored key is from a different failing file
        old_key = _compute_alert_key("cold_extract", "2026-03-24", self._result_with_dead_letter("path/to/old_file.csv"))
        monkeypatch.setattr("src.core.alerting._load_alert_dedupe", lambda cfg, name: {"last_key": old_key})

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        monkeypatch.setattr("src.core.alerting.requests.post", lambda *a, **kw: mock_response)

        cfg = {"mailbox": "x@x.com", "tenant_id": "t", "client_id": "c", "client_secret": "s"}
        sent = send_failure_alert(
            cfg,
            pipeline_name="cold_extract",
            result=result,
            environment="prod",
            run_date="2026-03-24",
        )

        assert sent is True


class TestFileNameExtraction:
    """_extract_file_name (function_app helper) — verify dead-letter and empty cases."""

    def test_extracts_filename_from_dead_letter_source_blob(self):
        from azure_functions.function_app import _extract_file_name

        result = {
            "dead_letter_files": [
                {"source_blob": "cold_extract/2026-03-24/bad_file.csv", "reason": "validation_failed"},
                {"source_blob": "cold_extract/2026-03-24/another_bad.csv", "reason": "parse_error"},
            ]
        }
        assert _extract_file_name(result) == "bad_file.csv"

    def test_returns_none_when_no_dead_letters(self):
        from azure_functions.function_app import _extract_file_name

        assert _extract_file_name({"status": "ok", "errors": ["something"]}) is None

    def test_returns_none_for_empty_result(self):
        from azure_functions.function_app import _extract_file_name

        assert _extract_file_name({}) is None

