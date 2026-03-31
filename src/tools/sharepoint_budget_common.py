"""
Shared helpers for SharePoint-based budget file discovery and download.
"""

from __future__ import annotations

import fnmatch
import logging
import os
from typing import Any

import requests

from ..core.graph_client import GRAPH_BASE, get_graph_token

log = logging.getLogger(__name__)


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def build_sharepoint_cfg() -> dict[str, str]:
    """Build Graph + SharePoint config from environment variables."""
    return {
        "tenant_id": _required_env("GRAPH_TENANT_ID"),
        "client_id": _required_env("GRAPH_CLIENT_ID"),
        "client_secret": _required_env("GRAPH_CLIENT_SECRET"),
        "sp_site_hostname": _required_env("SHAREPOINT_SITE_HOSTNAME"),
        "sp_site_path": _required_env("SHAREPOINT_SITE_PATH"),
        "sp_drive_name": os.getenv("SHAREPOINT_DOCUMENT_LIBRARY", "Documents").strip() or "Documents",
        "sp_folder_path": _required_env("SHAREPOINT_BUDGET_FOLDER"),
        "sp_pattern": os.getenv("SHAREPOINT_BUDGET_FILENAME_PATTERN", "*budget*.xlsx"),
    }


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _graph_get(token: str, url: str) -> dict[str, Any]:
    resp = requests.get(url, headers=_headers(token), timeout=30)
    resp.raise_for_status()
    return resp.json()


def resolve_site_id(token: str, hostname: str, site_path: str) -> str:
    # Example: /sites/Finance -> https://graph.../sites/{hostname}:/sites/Finance
    url = f"{GRAPH_BASE}/sites/{hostname}:{site_path}"
    data = _graph_get(token, url)
    return data["id"]


def resolve_drive_id(token: str, site_id: str, drive_name: str) -> str:
    url = f"{GRAPH_BASE}/sites/{site_id}/drives"
    data = _graph_get(token, url)
    for drive in data.get("value", []):
        if drive.get("name", "").lower() == drive_name.lower():
            return drive["id"]
    available = ", ".join(d.get("name", "?") for d in data.get("value", []))
    raise RuntimeError(f"Drive '{drive_name}' not found. Available: {available}")


def list_folder_files(token: str, drive_id: str, folder_path: str) -> list[dict[str, Any]]:
    clean = folder_path.strip("/")
    candidates = [clean]

    # Tolerate users providing full path with library prefix.
    lowered = clean.lower()
    if lowered.startswith("documents/"):
        candidates.append(clean.split("/", 1)[1])
    if lowered.startswith("shared documents/"):
        candidates.append(clean.split("/", 1)[1])

    last_exc: Exception | None = None
    for candidate in dict.fromkeys(candidates):
        try:
            url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{candidate}:/children?$top=200"
            data = _graph_get(token, url)
            return [item for item in data.get("value", []) if "file" in item]
        except Exception as exc:
            last_exc = exc

    raise RuntimeError(
        f"Could not resolve SharePoint folder path '{folder_path}' in drive '{drive_id}': {last_exc}"
    )


def select_latest_budget_file(files: list[dict[str, Any]], pattern: str) -> dict[str, Any]:
    matches = [f for f in files if fnmatch.fnmatch(f.get("name", ""), pattern)]
    if not matches:
        raise RuntimeError(f"No files matched pattern '{pattern}'")
    matches.sort(key=lambda x: x.get("lastModifiedDateTime", ""), reverse=True)
    return matches[0]


def download_file_bytes(token: str, drive_id: str, item_id: str) -> bytes:
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
    resp = requests.get(url, headers=_headers(token), timeout=60)
    resp.raise_for_status()
    return resp.content


def latest_sharepoint_budget_file() -> dict[str, Any]:
    """Resolve and return metadata of the latest matching budget file."""
    cfg = build_sharepoint_cfg()
    token = get_graph_token(cfg)
    site_id = resolve_site_id(token, cfg["sp_site_hostname"], cfg["sp_site_path"])
    drive_id = resolve_drive_id(token, site_id, cfg["sp_drive_name"])
    files = list_folder_files(token, drive_id, cfg["sp_folder_path"])
    latest = select_latest_budget_file(files, cfg["sp_pattern"])
    latest["_drive_id"] = drive_id
    latest["_site_id"] = site_id
    latest["_folder_path"] = cfg["sp_folder_path"]
    return latest
