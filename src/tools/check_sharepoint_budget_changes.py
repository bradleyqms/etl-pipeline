"""
Check whether the latest SharePoint budget file changed since last processed state.

Usage:
  python -m src.tools.check_sharepoint_budget_changes
  python -m src.tools.check_sharepoint_budget_changes --download-on-change
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dotenv import load_dotenv

from .read_sharepoint_budget_file import run as download_and_convert
from .sharepoint_budget_common import latest_sharepoint_budget_file

log = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _state_path() -> Path:
    return _project_root() / ".sharepoint_budget_state.json"


def _load_state() -> dict:
    p = _state_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            import logging
            logging.getLogger(__name__).warning("State file unreadable (%s) — starting fresh", exc)
    return {}


def check_changes() -> dict:
    current = latest_sharepoint_budget_file()
    state = _load_state()

    changed = any(
        [
            state.get("last_item_id") != current.get("id"),
            state.get("last_modified") != current.get("lastModifiedDateTime"),
            state.get("last_size") != current.get("size"),
        ]
    )

    return {
        "status": "ok",
        "changed": changed,
        "current": {
            "id": current.get("id"),
            "name": current.get("name"),
            "last_modified": current.get("lastModifiedDateTime"),
            "size": current.get("size"),
        },
        "state": {
            "last_item_id": state.get("last_item_id"),
            "last_name": state.get("last_name"),
            "last_modified": state.get("last_modified"),
            "last_size": state.get("last_size"),
            "last_download_path": state.get("last_download_path"),
            "last_download_ts": state.get("last_download_ts"),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check SharePoint budget changes")
    parser.add_argument("--download-on-change", action="store_true", help="Download and transform when a change is detected")
    parser.add_argument("--no-convert", action="store_true", help="With --download-on-change, skip cold transform")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv(_project_root() / ".env")

    check = check_changes()
    output = {"check": check, "action": None}

    if args.download_on_change and check["changed"]:
        output["action"] = download_and_convert(no_convert=args.no_convert)

    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
