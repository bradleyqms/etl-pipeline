"""
pipelines/config.py — Centralised pipeline definitions

Each pipeline has:
  - subject_filter:   Graph API filter for email subject
  - mail_folder:      Mailbox folder to search
  - blob_prefix:      Bronze layer path prefix
  - state_blob_path:  Blob path for state JSON
  - state_file:       Local dev state file name
  - silver_prefix:    Silver layer path prefix (for transforms)

get_pipeline(name) merges env credentials into the pipeline dict.
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────
# Base credentials (injected at runtime from env)
# ─────────────────────────────────────────────
_BASE = {
    "tenant_id":       os.getenv("GRAPH_TENANT_ID", ""),
    "client_id":       os.getenv("GRAPH_CLIENT_ID", ""),
    "client_secret":   os.getenv("GRAPH_CLIENT_SECRET", ""),
    "mailbox":         os.getenv("PA_MAILBOX_ADDRESS", ""),
    "storage_account": os.getenv("DATALAKE_ACCOUNT_NAME", "stqmssaledatalakeprod"),
    "storage_key":     os.getenv("DATALAKE_ACCOUNT_KEY", ""),
    "container":       "bronze",
    "mail_folder":     "SAP Reports",
}

# State file root (local dev convenience)
_STATE_DIR = Path(__file__).parent.parent.parent  # etl_pipeline/

# ─────────────────────────────────────────────
# Pipeline definitions
# ─────────────────────────────────────────────
PIPELINES = {
    # ── Fact tables (sales transactions) ──────
    "cold_extract": {
        "subject_filter":  "Cold_Extract",
        "blob_prefix":     "cold_extract",
        "state_blob_path": "state/cold_extract_state.json",
        "state_file":      _STATE_DIR / ".cold_extract_state.json",
        "silver_prefix":   "silver/cold_extract",
        "transform_name":  "cold_extract",
        "description":     "Sales transactions (6-hourly cold extract)",
    },
    "fact_sales_daily": {
        "subject_filter":  "FACT_SALES_DAILY_INCREMENTAL",
        "blob_prefix":     "fact_sales_daily",
        "state_blob_path": "state/fact_sales_daily_state.json",
        "state_file":      _STATE_DIR / ".fact_sales_daily_state.json",
        "silver_prefix":   "silver/fact_sales_daily",
        "transform_name":  "fact_sales_daily",
        "description":     "Sales transactions (daily incremental, all entities)",
    },

    # ── Dimension tables ──────────────────────
    # All dim tables arrive in one email (subject: dim_tables) from SAP dispatcher.
    # The dim_tables transform routes each file to the correct silver subfolder.
    "dim_tables": {
        "subject_filter":  "dim_tables",
        "blob_prefix":     "dim_tables",
        "state_blob_path": "state/dim_tables_state.json",
        "state_file":      _STATE_DIR / ".dim_tables_state.json",
        "silver_prefix":   "silver/dim_tables",
        "transform_name":  "dim_tables",
        "description":     "All dimension tables: customer (4 entities), product, salesperson",
    },
}


def get_pipeline(name: str) -> dict:
    """Return a fully-merged pipeline config with env credentials.

    Raises KeyError if pipeline name is unknown.
    """
    if name not in PIPELINES:
        available = ", ".join(sorted(PIPELINES.keys()))
        raise KeyError(f"Unknown pipeline '{name}'. Available: {available}")

    # Re-read env vars each call (Azure Functions injects them late)
    base = {
        "tenant_id":       os.getenv("GRAPH_TENANT_ID", ""),
        "client_id":       os.getenv("GRAPH_CLIENT_ID", ""),
        "client_secret":   os.getenv("GRAPH_CLIENT_SECRET", ""),
        "mailbox":         os.getenv("PA_MAILBOX_ADDRESS", ""),
        "storage_account": os.getenv("DATALAKE_ACCOUNT_NAME", "stqmssaledatalakeprod"),
        "storage_key":     os.getenv("DATALAKE_ACCOUNT_KEY", ""),
        "container":       "bronze",
        "mail_folder":     "SAP Reports",
    }
    return {**base, **PIPELINES[name]}


def list_pipelines() -> dict:
    """Return all pipeline names and descriptions."""
    return {name: cfg.get("description", "") for name, cfg in PIPELINES.items()}
