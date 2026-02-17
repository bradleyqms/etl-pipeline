"""
core/blob_client.py — Azure Blob Storage helpers

Container client factory, upload, list, and state management.
State is blob-first (works in Azure Functions) with local file fallback.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


# ═════════════════════════════════════════════
# Container client factory
# ═════════════════════════════════════════════

def get_container_client(
    account: str | None = None,
    key: str | None = None,
    container: str = "bronze",
):
    """Create an Azure Blob Storage ContainerClient.

    Falls back to env vars if params not provided.
    """
    from azure.storage.blob import ContainerClient

    account = account or os.getenv("DATALAKE_ACCOUNT_NAME", "stqmssaledatalakeprod")
    key = key or os.getenv("DATALAKE_ACCOUNT_KEY", "")
    conn_str = os.getenv("DATALAKE_CONNECTION_STRING", "")

    if conn_str:
        return ContainerClient.from_connection_string(conn_str, container_name=container)
    if key:
        return ContainerClient(
            account_url=f"https://{account}.blob.core.windows.net",
            container_name=container,
            credential=key,
        )
    raise RuntimeError(
        "No Blob Storage credentials. "
        "Set DATALAKE_ACCOUNT_KEY or DATALAKE_CONNECTION_STRING."
    )


# ═════════════════════════════════════════════
# Upload / List
# ═════════════════════════════════════════════

def upload_to_blob(
    container_client,
    blob_path: str,
    content: bytes,
    overwrite: bool = True,
) -> str:
    """Upload content to a blob. Returns the blob URL."""
    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(content, overwrite=overwrite)
    url = blob_client.url
    log.info("  ✅ → %s (%s bytes)", blob_path, f"{len(content):,}")
    return url


def list_recent_blobs(container_client, prefix: str, limit: int = 5) -> list:
    """List most recent blobs under a prefix."""
    blobs = list(container_client.list_blobs(name_starts_with=prefix))
    blobs.sort(
        key=lambda b: b.last_modified or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return blobs[:limit]


# ═════════════════════════════════════════════
# State management (blob-first, local fallback)
# ═════════════════════════════════════════════

def _get_blob_client_for_state(cfg: dict, blob_path: str):
    """Build a BlobClient pointing at the state JSON blob."""
    from azure.storage.blob import BlobClient

    account = cfg.get("storage_account") or os.getenv("DATALAKE_ACCOUNT_NAME", "")
    key = cfg.get("storage_key") or os.getenv("DATALAKE_ACCOUNT_KEY", "")
    conn_str = os.getenv("DATALAKE_CONNECTION_STRING", "")
    container = cfg.get("container", "bronze")

    if conn_str:
        return BlobClient.from_connection_string(conn_str, container, blob_path)
    if key and account:
        return BlobClient(
            account_url=f"https://{account}.blob.core.windows.net",
            container_name=container,
            blob_name=blob_path,
            credential=key,
        )
    raise RuntimeError("No blob credentials for state")


def load_state(cfg: dict) -> dict:
    """Load processed email IDs — blob first, then local file fallback."""
    state_blob = cfg.get("state_blob_path", "state/state.json")
    state_file = cfg.get("state_file")

    # Try blob storage first (works in Azure Functions)
    try:
        blob = _get_blob_client_for_state(cfg, state_blob)
        data = blob.download_blob().readall()
        state = json.loads(data)
        log.info(
            "State loaded from blob %s (%d processed IDs)",
            state_blob, len(state.get("processed_ids", [])),
        )
        return state
    except Exception as e:
        log.debug("Blob state not available (%s), trying local file", e)

    # Fall back to local file (local dev)
    if state_file:
        p = Path(state_file)
        if p.exists():
            try:
                return json.loads(p.read_text())
            except (json.JSONDecodeError, OSError):
                log.warning("Corrupt state file, starting fresh")

    return {"processed_ids": [], "last_run": None}


def save_state(state: dict, cfg: dict) -> None:
    """Persist processed email IDs — blob + local file."""
    state_blob = cfg.get("state_blob_path", "state/state.json")
    state_file = cfg.get("state_file")

    state["last_run"] = datetime.now(timezone.utc).isoformat()
    state_json = json.dumps(state, indent=2)

    # Primary: blob storage
    try:
        blob = _get_blob_client_for_state(cfg, state_blob)
        blob.upload_blob(state_json, overwrite=True)
        log.info(
            "State saved to blob %s (%d processed IDs)",
            state_blob, len(state["processed_ids"]),
        )
    except Exception as e:
        log.warning("Could not save state to blob (%s), saving locally only", e)

    # Secondary: local file (convenience for local dev)
    if state_file:
        try:
            Path(state_file).write_text(state_json)
        except OSError:
            pass  # OK in Azure Functions — ephemeral filesystem
