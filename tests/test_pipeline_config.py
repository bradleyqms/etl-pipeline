"""
Tests for src/pipelines/config.py — pipeline definitions & merging.
"""

import os
import pytest

from src.pipelines.config import get_pipeline, list_pipelines, PIPELINES


class TestGetPipeline:
    """get_pipeline() returns a fully-merged config dict."""

    def test_returns_all_required_keys(self, monkeypatch):
        """Every pipeline config must contain Graph + Blob credentials keys."""
        # Inject fake env vars so the merge picks them up
        monkeypatch.setenv("GRAPH_TENANT_ID", "fake-tenant")
        monkeypatch.setenv("GRAPH_CLIENT_ID", "fake-client")
        monkeypatch.setenv("GRAPH_CLIENT_SECRET", "fake-secret")
        monkeypatch.setenv("PA_MAILBOX_ADDRESS", "test@example.com")
        monkeypatch.setenv("DATALAKE_ACCOUNT_KEY", "fake-key")

        cfg = get_pipeline("cold_extract")

        required = [
            "tenant_id", "client_id", "client_secret", "mailbox",
            "storage_account", "storage_key", "container", "mail_folder",
            "subject_filter", "blob_prefix", "state_blob_path",
        ]
        for key in required:
            assert key in cfg, f"Missing key: {key}"
            assert cfg[key], f"Empty value for key: {key}"

    def test_merges_env_credentials(self, monkeypatch):
        """Env vars should override the base config."""
        monkeypatch.setenv("GRAPH_TENANT_ID", "my-tenant-123")
        monkeypatch.setenv("PA_MAILBOX_ADDRESS", "ops@test.com")

        cfg = get_pipeline("dim_customer")
        assert cfg["tenant_id"] == "my-tenant-123"
        assert cfg["mailbox"] == "ops@test.com"

    def test_pipeline_specific_fields(self, monkeypatch):
        """Pipeline-specific config (subject_filter, blob_prefix) must be present."""
        monkeypatch.setenv("GRAPH_TENANT_ID", "t")
        cfg = get_pipeline("dim_product")

        assert cfg["subject_filter"] == "dim_product_master"
        assert cfg["blob_prefix"] == "dim_product"
        assert "state_blob_path" in cfg

    def test_unknown_pipeline_raises_key_error(self):
        """Requesting a nonexistent pipeline should raise KeyError."""
        with pytest.raises(KeyError, match="Unknown pipeline"):
            get_pipeline("nonexistent_pipeline")


class TestListPipelines:
    """list_pipelines() returns all defined pipelines."""

    def test_returns_all_five_pipelines(self):
        result = list_pipelines()
        assert len(result) == 5

    def test_includes_active_pipelines(self):
        result = list_pipelines()
        for name in ["cold_extract", "fact_sales_daily", "dim_customer", "dim_product"]:
            assert name in result

    def test_includes_planned_pipelines(self):
        result = list_pipelines()
        for name in ["dim_salesperson"]:
            assert name in result
            assert "PLANNED" in result[name]

    def test_every_pipeline_has_description(self):
        result = list_pipelines()
        for name, desc in result.items():
            assert desc, f"Pipeline '{name}' has empty description"
