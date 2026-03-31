"""
transforms/us_budget_workbook_to_canonical.py

Full-workbook canonical pipeline for US budget workbooks.

This transform complements the customer-month fact extraction by converting
*all sheets* into canonical row datasets so layout and formula-driven tabs
remain queryable and auditable.

Outputs (under data/reference/v0_outputs/<version_label>/full_workbook/):
- workbook_sheet_rows.parquet / .csv
- workbook_definition_map.parquet / .csv
- workbook_list_map.parquet / .csv
- workbook_sheet_profiles.json
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FullOutputPaths:
    sheet_rows_parquet: Path
    sheet_rows_csv: Path
    definition_parquet: Path
    definition_csv: Path
    list_parquet: Path
    list_csv: Path
    profiles_json: Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_input_path() -> Path:
    return (
        _project_root()
        / "data"
        / "reference"
        / "v0_inputs"
        / "2026.01.22 US Spa Sales Budget 2026 FINAL.xlsx"
    )


def _default_output_root() -> Path:
    return _project_root() / "data" / "reference" / "v0_outputs"


def _clean_header(value: object, idx: int) -> str:
    text = "" if value is None else str(value).strip()
    if not text or text.lower().startswith("unnamed:"):
        return f"col_{idx:02d}"
    # Keep deterministic snake-ish headers with low punctuation noise.
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^0-9a-zA-Z_]+", "", text)
    text = text.strip("_").lower()
    return text or f"col_{idx:02d}"


def _detect_header_idx(raw: pd.DataFrame, scan_rows: int = 25) -> int:
    best_idx = 0
    best_score = -1
    limit = min(len(raw), scan_rows)
    for i in range(limit):
        row = raw.iloc[i]
        non_null = row.notna().sum()
        str_cells = sum(isinstance(v, str) and v.strip() != "" for v in row.tolist())
        score = int(non_null) + int(str_cells) * 2
        if score > best_score:
            best_score = score
            best_idx = i
    return best_idx


def _sheet_to_rows(xls: pd.ExcelFile, sheet_name: str, source_file_name: str) -> tuple[pd.DataFrame, dict]:
    raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    # Trim all-empty outer borders to reduce noise.
    raw = raw.dropna(axis=0, how="all").dropna(axis=1, how="all")
    if raw.empty:
        return pd.DataFrame(), {
            "sheet_name": sheet_name,
            "header_row": None,
            "raw_rows": 0,
            "raw_cols": 0,
            "data_rows": 0,
        }

    raw = raw.reset_index(drop=True)
    header_idx = _detect_header_idx(raw)

    headers = [_clean_header(v, i + 1) for i, v in enumerate(raw.iloc[header_idx].tolist())]

    # De-duplicate repeated header names by suffixing _n.
    seen: dict[str, int] = {}
    deduped = []
    for h in headers:
        seen[h] = seen.get(h, 0) + 1
        deduped.append(h if seen[h] == 1 else f"{h}_{seen[h]}")
    headers = deduped

    data = raw.iloc[header_idx + 1 :].copy()
    data.columns = headers
    data = data.dropna(axis=0, how="all").reset_index(drop=True)

    if data.empty:
        return pd.DataFrame(), {
            "sheet_name": sheet_name,
            "header_row": int(header_idx + 1),
            "raw_rows": int(len(raw)),
            "raw_cols": int(len(raw.columns)),
            "data_rows": 0,
        }

    # Add sheet-level metadata.
    data.insert(0, "sheet_name", sheet_name)
    data.insert(1, "source_file_name", source_file_name)
    data.insert(2, "header_row", int(header_idx + 1))
    data.insert(3, "row_number", data.index + int(header_idx + 2))

    profile = {
        "sheet_name": sheet_name,
        "header_row": int(header_idx + 1),
        "raw_rows": int(len(raw)),
        "raw_cols": int(len(raw.columns)),
        "data_rows": int(len(data)),
    }
    return data, profile


def _extract_definition_map(sheet_rows: pd.DataFrame) -> pd.DataFrame:
    if sheet_rows.empty:
        return pd.DataFrame(columns=["cluster", "definition"]) 

    subset = sheet_rows[sheet_rows["sheet_name"] == "Definition"].copy()
    if subset.empty:
        return pd.DataFrame(columns=["cluster", "definition"]) 

    cluster_col = next((c for c in subset.columns if c.endswith("cluster")), None)
    definition_col = next((c for c in subset.columns if c.endswith("definition")), None)
    if not cluster_col or not definition_col:
        return pd.DataFrame(columns=["cluster", "definition"]) 

    out = subset[[cluster_col, definition_col]].copy()
    out.columns = ["cluster", "definition"]
    out = out.dropna(how="all").drop_duplicates().reset_index(drop=True)
    return out


def _extract_list_map(sheet_rows: pd.DataFrame) -> pd.DataFrame:
    if sheet_rows.empty:
        return pd.DataFrame(columns=["active_flag", "growth_class"]) 

    subset = sheet_rows[sheet_rows["sheet_name"] == "List"].copy()
    if subset.empty:
        return pd.DataFrame(columns=["active_flag", "growth_class"]) 

    active_col = next((c for c in subset.columns if c.endswith("yes") or c.endswith("no") or c == "col_01"), None)
    # Prefer explicit growth-like columns, otherwise first non-meta column fallback.
    growth_candidates = [c for c in subset.columns if "grow" in c or "maintain" in c or "lose" in c]
    growth_col = growth_candidates[0] if growth_candidates else None
    if growth_col is None:
        user_cols = [c for c in subset.columns if c not in {"sheet_name", "source_file_name", "header_row", "row_number"}]
        growth_col = user_cols[-1] if user_cols else None

    if not growth_col:
        return pd.DataFrame(columns=["active_flag", "growth_class"]) 

    out = pd.DataFrame(
        {
            "active_flag": subset[active_col] if active_col else None,
            "growth_class": subset[growth_col],
        }
    )
    out = out.dropna(how="all").drop_duplicates().reset_index(drop=True)
    return out


def _build_output_paths(output_root: Path, version_label: str) -> FullOutputPaths:
    out_dir = output_root / version_label / "full_workbook"
    out_dir.mkdir(parents=True, exist_ok=True)
    return FullOutputPaths(
        sheet_rows_parquet=out_dir / "workbook_sheet_rows.parquet",
        sheet_rows_csv=out_dir / "workbook_sheet_rows.csv",
        definition_parquet=out_dir / "workbook_definition_map.parquet",
        definition_csv=out_dir / "workbook_definition_map.csv",
        list_parquet=out_dir / "workbook_list_map.parquet",
        list_csv=out_dir / "workbook_list_map.csv",
        profiles_json=out_dir / "workbook_sheet_profiles.json",
    )


def _safe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce mixed object columns to nullable strings for robust parquet writes."""
    out = df.copy()
    if out.empty:
        return out

    for col in out.columns:
        if out[col].dtype == "object":
            out[col] = out[col].map(
                lambda v: None if pd.isna(v) else str(v)
            )
    return out


def transform(
    xlsx_path: str | Path | None = None,
    output_root: str | Path | None = None,
    version_label: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Convert full workbook into canonical per-sheet datasets."""
    source_path = Path(xlsx_path) if xlsx_path else _default_input_path()
    out_root = Path(output_root) if output_root else _default_output_root()

    if not source_path.exists():
        return {"status": "error", "message": f"Input workbook not found: {source_path}"}

    if not version_label:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        version_label = f"us_budget_full_{stamp}"

    xls = pd.ExcelFile(source_path)
    all_rows: list[pd.DataFrame] = []
    profiles: list[dict] = []

    for sheet in xls.sheet_names:
        rows, profile = _sheet_to_rows(xls, sheet, source_file_name=source_path.name)
        profiles.append(profile)
        if not rows.empty:
            all_rows.append(rows)

    sheet_rows = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

    # Append global metadata for lineage.
    if not sheet_rows.empty:
        sheet_rows["extract_type"] = "us_budget_v2_full_workbook_rows"
        sheet_rows["load_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    definition_map = _extract_definition_map(sheet_rows)
    if not definition_map.empty:
        definition_map["extract_type"] = "us_budget_v2_definition_map"

    list_map = _extract_list_map(sheet_rows)
    if not list_map.empty:
        list_map["extract_type"] = "us_budget_v2_list_map"

    paths = _build_output_paths(out_root, version_label)

    if not dry_run:
        sheet_rows_out = _safe_for_parquet(sheet_rows)
        definition_out = _safe_for_parquet(definition_map)
        list_out = _safe_for_parquet(list_map)

        sheet_rows_out.to_parquet(paths.sheet_rows_parquet, index=False)
        sheet_rows_out.to_csv(paths.sheet_rows_csv, index=False)

        definition_out.to_parquet(paths.definition_parquet, index=False)
        definition_out.to_csv(paths.definition_csv, index=False)

        list_out.to_parquet(paths.list_parquet, index=False)
        list_out.to_csv(paths.list_csv, index=False)

        paths.profiles_json.write_text(json.dumps(profiles, indent=2), encoding="utf-8")

    return {
        "status": "dry_run" if dry_run else "ok",
        "pipeline": "us_budget_full_workbook_canonical",
        "source_file": str(source_path),
        "version_label": version_label,
        "sheet_count": len(xls.sheet_names),
        "sheet_row_records": int(len(sheet_rows)),
        "definition_rows": int(len(definition_map)),
        "list_rows": int(len(list_map)),
        "outputs": {
            "sheet_rows_parquet": str(paths.sheet_rows_parquet),
            "sheet_rows_csv": str(paths.sheet_rows_csv),
            "definition_parquet": str(paths.definition_parquet),
            "definition_csv": str(paths.definition_csv),
            "list_parquet": str(paths.list_parquet),
            "list_csv": str(paths.list_csv),
            "profiles_json": str(paths.profiles_json),
        },
    }
