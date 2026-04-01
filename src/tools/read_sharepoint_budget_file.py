"""
Download the latest budget workbook from SharePoint and run the cold v0->v2 conversion.

Usage:
  python -m src.tools.read_sharepoint_budget_file
  python -m src.tools.read_sharepoint_budget_file --no-convert
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from ..core.graph_client import get_graph_token
from ..transforms.us_budget_v0_to_parquet import transform as cold_transform
from ..transforms.us_budget_workbook_to_canonical import transform as full_workbook_transform
from .sharepoint_budget_common import (
    build_sharepoint_cfg,
    download_file_bytes,
    list_folder_files,
    resolve_drive_id,
    resolve_site_id,
    select_latest_budget_file,
)

log = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _state_path() -> Path:
    return _project_root() / ".sharepoint_budget_state.json"


def _input_dir() -> Path:
    p = _project_root() / "data" / "reference" / "v0_inputs" / "sharepoint"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_state() -> dict:
    path = _state_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            import logging
            logging.getLogger(__name__).warning("State file unreadable (%s) — starting fresh", exc)
    return {}


def _save_state(state: dict) -> None:
    _state_path().write_text(json.dumps(state, indent=2), encoding="utf-8")


def run(no_convert: bool = False, full_workbook: bool = True) -> dict:
    cfg = build_sharepoint_cfg()
    token = get_graph_token(cfg)
    site_id = resolve_site_id(token, cfg["sp_site_hostname"], cfg["sp_site_path"])
    drive_id = resolve_drive_id(token, site_id, cfg["sp_drive_name"])

    files = list_folder_files(token, drive_id, cfg["sp_folder_path"])
    latest = select_latest_budget_file(files, cfg["sp_pattern"])

    content = download_file_bytes(token, drive_id, latest["id"])
    digest = _sha256(content)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    local_name = f"{ts}_{latest['name']}"
    local_path = _input_dir() / local_name
    local_path.write_bytes(content)

    state = _load_state()
    state.update(
        {
            "last_item_id": latest.get("id"),
            "last_name": latest.get("name"),
            "last_modified": latest.get("lastModifiedDateTime"),
            "last_size": latest.get("size"),
            "last_sha256": digest,
            "last_download_path": str(local_path),
            "last_download_ts": ts,
        }
    )
    _save_state(state)

    result = {
        "status": "ok",
        "downloaded": {
            "name": latest.get("name"),
            "item_id": latest.get("id"),
            "last_modified": latest.get("lastModifiedDateTime"),
            "size": latest.get("size"),
            "sha256": digest,
            "local_path": str(local_path),
        },
        "cold_transform": None,
        "full_workbook_canonical": None,
    }

    if not no_convert:
        version_label = f"sharepoint_{ts}"
        result["cold_transform"] = cold_transform(
            xlsx_path=local_path,
            version_label=version_label,
            dry_run=False,
        )
        if full_workbook:
            result["full_workbook_canonical"] = full_workbook_transform(
                xlsx_path=local_path,
                version_label=version_label,
                dry_run=False,
            )

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Read latest SharePoint budget file")
    parser.add_argument("--no-convert", action="store_true", help="Only download file, skip cold transform")
    parser.add_argument(
        "--skip-full-workbook",
        action="store_true",
        help="Skip full-workbook canonical transform output",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    load_dotenv(_project_root() / ".env")
    result = run(
        no_convert=args.no_convert,
        full_workbook=not args.skip_full_workbook,
    )

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
