"""
Build canonical outputs from all budget workbooks downloaded to the local SharePoint folder mirror.

Usage:
  python -m src.tools.build_all_budget_canonical
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dotenv import load_dotenv

from ..transforms.budget_multi_workbook_canonical import build_canonical


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build canonical datasets from all budget workbooks")
    parser.add_argument("--dry-run", action="store_true", help="Profile and return stats without writing files")
    parser.add_argument("--version-label", default=None, help="Optional output version label")
    parser.add_argument(
        "--include-combined",
        action="store_true",
        help="Also write combined root-level parquet/csv outputs in addition to split regional review outputs",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv(_project_root() / ".env")

    result = build_canonical(
        version_label=args.version_label,
        dry_run=args.dry_run,
        include_combined=args.include_combined,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
