"""
Canonicalize a folder of budget workbooks (US/UK/Core/Export/Group) into
verification-friendly datasets.
"""

from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

# Keep this for internal validation comparisons that require Swiss CHF->EUR mapping.
CHF_TO_EUR_RATE = 1.05

# EUR compare-space mapping intentionally avoids hardcoded non-EUR rates.
# Keep native values as source-of-truth; EUR compare is additive metadata.
# FX rates to EUR used for budget_amount_eur_compare and non-EUR report normalisation.
# USD rate matches the app's budget_USA_spa_2026 implied conversion (86,060 kUSD => 79,974 kEUR).
FX_RATE_TO_EUR: dict[str, float | None] = {
    "EUR": 1.0,
    "USD": 1.0 / 1.0757,   # ~0.9296 — aligns with sales report app FX assumption
    "GBP": 1.0 / 1.20,    # ~0.8333 — standard budget rate
    "CHF": 1.0 / 1.05,    # ~0.9524
}


@dataclass(frozen=True)
class CanonicalOutputPaths:
    base_dir: Path
    catalog_csv: Path
    report_monthly_parquet: Path
    report_monthly_csv: Path
    sales_monthly_parquet: Path
    sales_monthly_csv: Path
    sales_customer_parquet: Path
    sales_customer_csv: Path
    group_lines_parquet: Path
    group_lines_csv: Path
    ecommerce_report_monthly_parquet: Path
    ecommerce_report_monthly_csv: Path
    regional_views_dir: Path
    validation_summary_csv: Path
    validation_summary_md: Path
    reference_comparison_csv: Path
    reference_comparison_md: Path
    pbix_fact_csv: Path
    source_inventory_csv: Path
    qa_market_month_summary_csv: Path
    readme_md: Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_input_dir() -> Path:
    return _project_root() / "data" / "reference" / "v0_inputs" / "sharepoint" / "all_budget_files"


def _default_output_root() -> Path:
    return _project_root() / "data" / "reference" / "v0_outputs" / "all_budget_canonical"


def _default_reference_budget_dir() -> Path:
    return _project_root().parent / "sales_report_v2_independent" / "data" / "inputs" / "budget"


def _classify_workbook(name: str, sheet_names: list[str]) -> str:
    n = name.lower()
    s = {x.lower() for x in sheet_names}
    us_regions = {"northeast", "central", "west", "southeast"}
    us_region_hits = sum(1 for region in us_regions if region in s)

    if "initiative log - detailed" in s or "bau log - detailed" in s or "qms detailed budget" in n:
        return "group_budget"
    if us_region_hits >= 2 or "us spa sales budget" in n:
        return "us_budget"
    if "uk spa budget" in s or "uk retail budget" in s:
        return "uk_budget"
    if "region review" in s and "summary" in s:
        return "core_markets_budget"
    if "budget" in s and "performance data" in s:
        return "export_budget"
    return "unknown"


# ---------------------------------------------------------------------------
# Per-sheet context: maps workbook_type → sheet_name → enrichment fields.
# Used by _extract_sales_from_sheet to stamp region/currency onto every row.
# ---------------------------------------------------------------------------
_SHEET_CONTEXT: dict[str, dict[str, dict]] = {
    "core_markets_budget": {
        "North":             {"sub_region": "North",            "region": "Germany",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Kerstin"},
        "North East":        {"sub_region": "North East",       "region": "Germany",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Aracelli"},
        "NRW Marina":        {"sub_region": "NRW - Marina",     "region": "Germany",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Marina"},
        "NRW Ulrike":        {"sub_region": "NRW - Ulrike",     "region": "Germany",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Ulrike"},
        "South West":        {"sub_region": "South West",       "region": "Germany",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Sibylle"},
        "Bayern":            {"sub_region": "Bayern",           "region": "Germany",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "I. Papoulias"},
        "Golden Girls DE":   {"sub_region": "DE Other",         "region": "Germany",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "-Kein Vertriebsmitarbeiter-"},
        "Retail":            {"sub_region": "Retail",           "region": "Germany",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Aracelli"},
        "NL Central":        {"sub_region": "NL Central",       "region": "Benelux",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Marjelein"},
        "NL Other_Belgium":  {"sub_region": "NL Other + BL",    "region": "Benelux",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Gabrielle"},
        "Golden Girls NL":   {"sub_region": "NL Other",         "region": "Benelux",      "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "-Kein Vertriebsmitarbeiter-"},
        "Switzerland DE":    {"sub_region": "German Switzerland", "region": "Switzerland", "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "CHF", "sales_person": "Christiane"},
        "Switzerland FR":    {"sub_region": "French Switzerland", "region": "Switzerland", "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Elena"},
        "Golden Girls CH":   {"sub_region": "Other Switzerland", "region": "Switzerland", "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "-Kein Vertriebsmitarbeiter-"},
        "France South":      {"sub_region": "France South",     "region": "France",       "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Yannick"},
        "France North":      {"sub_region": "France North",     "region": "France",       "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Elena"},
        "Italy":             {"sub_region": "Italy",            "region": "Italy",        "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Elena"},
        "Spain":             {"sub_region": "Spain",            "region": "Spain",        "market_group": "Core Markets", "company_group": "Company 1", "currency_code": "EUR", "sales_person": "Montse"},
    },
    "us_budget": {
        "Northeast": {"sub_region": "Northeast", "region": "Northeast", "market_group": "USA", "company_group": "Company 1", "currency_code": "USD"},
        "Central":   {"sub_region": "Central",   "region": "Central",   "market_group": "USA", "company_group": "Company 1", "currency_code": "USD"},
        "West":      {"sub_region": "West",      "region": "West",      "market_group": "USA", "company_group": "Company 1", "currency_code": "USD"},
        "Southeast": {"sub_region": "Southeast", "region": "Southeast", "market_group": "USA", "company_group": "Company 1", "currency_code": "USD"},
        "Other":     {"sub_region": "Other",     "region": "Other",     "market_group": "USA", "company_group": "Company 1", "currency_code": "USD"},
    },
    "uk_budget": {
        "UK Spa Budget":          {"sub_region": None, "region": "Spa",              "market_group": "UK", "company_group": "Company 1", "currency_code": "GBP", "channel": "Spa"},
        "UK Retail Budget":       {"sub_region": None, "region": "Retail",           "market_group": "UK", "company_group": "Company 1", "currency_code": "GBP", "channel": "Retail"},
        "Global eTailer Budget":  {"sub_region": None, "region": "Global eTailers",  "market_group": "UK", "company_group": "Company 3", "currency_code": "GBP", "channel": "Global eTailers"},
    },
    "export_budget": {
        "Budget": {"sub_region": None, "region": "Export", "market_group": "Export", "company_group": "Company 1", "currency_code": "EUR"},
        "2026B":  {"sub_region": None, "region": "Export", "market_group": "Export", "company_group": "Company 1", "currency_code": "EUR"},
    },
}


def _to_snake(name: object) -> str:
    text = "" if name is None else str(name).strip()
    if not text or text.lower().startswith("unnamed:"):
        return ""
    out = []
    for ch in text:
        if ch.isalnum():
            out.append(ch.lower())
        else:
            out.append("_")
    text = "".join(out)
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def _read_sheet_with_header_detection(path: Path, sheet_name: str) -> tuple[pd.DataFrame, int]:
    raw = pd.read_excel(path, sheet_name=sheet_name, header=None)
    raw = raw.dropna(axis=1, how="all")
    if raw.empty:
        return pd.DataFrame(), 0

    best_i = 0
    best_score = -1
    for i in range(min(len(raw), 30)):
        vals = ["" if pd.isna(v) else str(v).strip().lower() for v in raw.iloc[i].tolist()]
        non_empty = sum(1 for v in vals if v)
        score = non_empty
        if any("customer code" in v or "customer no" in v for v in vals):
            score += 50
        if any(v.startswith("202") for v in vals):
            score += 3
        if score > best_score:
            best_score = score
            best_i = i

    df = pd.read_excel(path, sheet_name=sheet_name, header=best_i)
    df = df.dropna(axis=1, how="all")
    keep = [c for c in df.columns if not str(c).startswith("Unnamed:")]
    df = df[keep].copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df, best_i + 1


def _detect_customer_col(columns: list[str]) -> str | None:
    lower = {c.lower().strip(): c for c in columns}
    for key in ["customer code", "customer no.", "customer no", "customer number"]:
        if key in lower:
            return lower[key]
    return None


def _detect_customer_name_col(columns: list[str]) -> str | None:
    for c in columns:
        if "customer name" in c.lower():
            return c
    return None


def _extract_budget_version_token(text: Any) -> str | None:
    value = _strip_or_none(text)
    if not value:
        return None

    match = re.search(r"(20\d{2}[._-]\d{2}[._-]\d{2}|20\d{6})", value)
    if not match:
        return None

    token = match.group(1)
    if re.fullmatch(r"20\d{6}", token):
        return f"{token[:4]}.{token[4:6]}.{token[6:8]}"
    return token.replace("-", ".").replace("_", ".")


def _apply_budget_version_column(frame: pd.DataFrame, workbook_col: str = "workbook_name") -> pd.DataFrame:
    if frame.empty:
        return frame

    out = frame.copy()
    if workbook_col not in out.columns:
        out["budget_version"] = None
        return out

    def _resolve_value(cell: Any) -> str | None:
        parts = [p.strip() for p in str(cell).split("|") if _strip_or_none(p)]
        tokens = sorted({t for p in parts for t in [_extract_budget_version_token(p)] if t})
        if not tokens:
            return None
        return "|".join(tokens)

    out["budget_version"] = out[workbook_col].map(_resolve_value)
    return out


def _strip_or_none(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _resolve_fx_rate_to_eur(currency_code: Any) -> float | None:
    code = _strip_or_none(currency_code)
    if not code:
        return None
    return FX_RATE_TO_EUR.get(code.upper())


def _apply_native_eur_columns(
    frame: pd.DataFrame,
    *,
    amount_col: str,
    currency_col: str,
) -> pd.DataFrame:
    """Add budget_amount_native / budget_amount_eur_compare / fx_rate_to_eur columns.

    Native amount is a direct alias of the supplied amount column to keep source
    behavior untouched. EUR compare is populated when an FX rate is available.
    """
    if frame.empty:
        return frame

    out = frame.copy()
    if amount_col not in out.columns:
        out["budget_amount_native"] = None
        out["fx_rate_to_eur"] = None
        out["budget_amount_eur_compare"] = None
        return out

    out[amount_col] = pd.to_numeric(out[amount_col], errors="coerce")
    out["budget_amount_native"] = out[amount_col]

    if currency_col in out.columns:
        out["fx_rate_to_eur"] = out[currency_col].map(_resolve_fx_rate_to_eur)
    else:
        out["fx_rate_to_eur"] = None

    out["budget_amount_eur_compare"] = pd.to_numeric(out["fx_rate_to_eur"], errors="coerce") * out["budget_amount_native"]
    out.loc[out["fx_rate_to_eur"].isna(), "budget_amount_eur_compare"] = None
    return out


def _detect_month_cols(df: pd.DataFrame) -> list[str]:
    out: list[str] = []
    rename: dict[object, str] = {}
    for col in df.columns:
        if isinstance(col, (datetime, pd.Timestamp)):
            month = pd.Timestamp(col).strftime("%Y-%m-01")
            rename[col] = month
            out.append(month)
            continue
        s = str(col).strip()
        ts = pd.to_datetime(s, errors="coerce")
        if pd.notna(ts) and ts.year >= 2000:
            month = pd.Timestamp(ts).strftime("%Y-%m-01")
            rename[col] = month
            out.append(month)
    if rename:
        df.rename(columns=rename, inplace=True)
    return sorted(set(out))


def _extract_sales_from_sheet(
    path: Path,
    sheet_name: str,
    workbook_name: str,
    workbook_type: str,
    context: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract customer-level monthly budget rows from a single workbook sheet.

    ``context`` is a dict from ``_SHEET_CONTEXT`` and supplies per-sheet
    enrichment: ``currency_code``, ``market_group``, ``region``,
    ``sub_region``, and optionally ``channel``.
    """
    ctx = context or {}

    df, header_row = _read_sheet_with_header_detection(path, sheet_name)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    cust_col = _detect_customer_col(list(df.columns))
    name_col = _detect_customer_name_col(list(df.columns))
    month_cols = _detect_month_cols(df)

    if not cust_col or not month_cols:
        return pd.DataFrame(), pd.DataFrame()

    # ── Numeric SAP-coded customers (existing / new-with-code) ─────────────
    work_numeric = df.copy()
    work_numeric[cust_col] = pd.to_numeric(work_numeric[cust_col], errors="coerce")
    work_numeric = work_numeric[work_numeric[cust_col].notna()].copy()
    if name_col:
        work_numeric = work_numeric[
            ~work_numeric[name_col].astype(str).str.lower().str.startswith("total")
        ].copy()
    work_numeric["door_type"] = None

    # ── NKD placeholder rows (NKD25_*, NKD26_*) — new doors without SAP codes ──
    # Business rule: Total Active Existing Doors + Total New Doors 2025 = Existing
    #                Total New Doors 2026                               = New
    nkd_mask = df[cust_col].astype(str).str.upper().str.startswith("NKD")
    work_nkd = df[nkd_mask].copy()
    if not work_nkd.empty:
        work_nkd["door_type"] = work_nkd[cust_col].astype(str).apply(
            lambda v: "new_2026" if "26" in v.upper() else "new_2025"
        )
        # Use distinct negative synthetic codes so the (workbook, sheet, customer, month)
        # dedup key stays unique across NKD25 / NKD26 rows; avoids collapsing both
        # to a single row with keep="last" when customer_code=None.
        work_nkd[cust_col] = work_nkd[cust_col].astype(str).apply(
            lambda v: -26 if "26" in v.upper() else -25
        )

    work = (
        pd.concat([work_numeric, work_nkd], ignore_index=True)
        if not work_nkd.empty
        else work_numeric
    )
    if work.empty:
        return pd.DataFrame(), pd.DataFrame()

    work[cust_col] = pd.array(work[cust_col], dtype="Int64")

    # Deduplicate column names — some workbooks have parallel phasing/growth columns
    # that share the same month date label as the budget columns.  Keep only the
    # first occurrence (which is the actual budget amount); the second is usually a
    # weight/rate and causes NKD placeholder rows to be misread.
    if work.columns.duplicated().any():
        work = work.loc[:, ~work.columns.duplicated(keep="first")]
        month_cols = [c for c in month_cols if c in work.columns]

    # Sales monthly fact
    id_vars = [cust_col]
    for c in [name_col, "Region", "Country", "Sales person", "Customer Map", "2026 Cluster", "2025 Cluster", "Active?", "door_type"]:
        if c and c in work.columns and c not in id_vars:
            id_vars.append(c)

    fact = work.melt(id_vars=id_vars, value_vars=month_cols, var_name="budget_month", value_name="budget_amount")
    fact["budget_month"] = pd.to_datetime(fact["budget_month"], errors="coerce")
    fact["budget_amount"] = pd.to_numeric(fact["budget_amount"], errors="coerce")
    fact = fact.dropna(subset=["budget_month", "budget_amount"]).copy()

    fact.rename(columns={cust_col: "customer_code", name_col: "customer_name"} if name_col else {cust_col: "customer_code"}, inplace=True)
    fact["customer_code"] = fact["customer_code"].astype("Int64")
    fact["sheet_name"] = sheet_name
    fact["workbook_name"] = workbook_name
    fact["workbook_type"] = workbook_type
    fact["header_row"] = header_row

    # Context-driven enrichment — prefer explicit context over default fallbacks
    fact["currency_code"] = ctx.get("currency_code", "USD")
    fact["market_group"] = ctx.get("market_group")
    fact["region"] = ctx.get("region")
    fact["sub_region"] = ctx.get("sub_region")
    fact["company_group"] = ctx.get("company_group")
    if "channel" in ctx:
        fact["channel"] = ctx["channel"]

    if "Sales person" not in fact.columns:
        fact["Sales person"] = None
    fact["Sales person"] = fact["Sales person"].map(_strip_or_none)
    if ctx.get("sales_person"):
        fact["Sales person"] = fact["Sales person"].fillna(ctx["sales_person"])
    fact["sales_person"] = fact["Sales person"]

    fact["load_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Customer snapshot from same rows
    snapshot_cols = [
        "customer_code", "customer_name", "Region", "Country",
        "Sales person", "sales_person", "Customer Map", "2026 Cluster", "2025 Cluster", "Active?",
        "market_group", "region", "sub_region", "company_group", "door_type",
    ]
    if "channel" in fact.columns:
        snapshot_cols.append("channel")
    customer_cols = [c for c in snapshot_cols if c in fact.columns]
    customer = fact[customer_cols + ["sheet_name", "workbook_name", "workbook_type"]].drop_duplicates().copy()

    return fact, customer


def _extract_group_lines(path: Path, sheet_names: list[str], workbook_name: str, workbook_type: str) -> pd.DataFrame:
    records: list[dict] = []
    for sheet in sheet_names:
        df, header_row = _read_sheet_with_header_detection(path, sheet)
        if df.empty:
            continue

        # Keep first 250 rows max per sheet for verification-scale dataset.
        df = df.head(250).copy()
        for i, row in df.iterrows():
            payload = { _to_snake(k): (None if pd.isna(v) else str(v)) for k, v in row.items() if _to_snake(k) }
            if not payload:
                continue
            records.append(
                {
                    "workbook_name": workbook_name,
                    "workbook_type": workbook_type,
                    "sheet_name": sheet,
                    "header_row": header_row,
                    "row_number": int(i + header_row + 1),
                    "payload_json": json.dumps(payload, ensure_ascii=True),
                }
            )

    return pd.DataFrame(records)


def _output_paths(output_root: Path, version_label: str) -> CanonicalOutputPaths:
    out = output_root / version_label
    out.mkdir(parents=True, exist_ok=True)
    return CanonicalOutputPaths(
        base_dir=out,
        catalog_csv=out / "workbook_catalog.csv",
        report_monthly_parquet=out / "report_budget_monthly_canonical.parquet",
        report_monthly_csv=out / "report_budget_monthly_canonical.csv",
        sales_monthly_parquet=out / "sales_budget_monthly_canonical.parquet",
        sales_monthly_csv=out / "sales_budget_monthly_canonical.csv",
        sales_customer_parquet=out / "sales_budget_customer_canonical.parquet",
        sales_customer_csv=out / "sales_budget_customer_canonical.csv",
        group_lines_parquet=out / "group_budget_lines_canonical.parquet",
        group_lines_csv=out / "group_budget_lines_canonical.csv",
        ecommerce_report_monthly_parquet=out / "ecommerce_report_budget_monthly_canonical.parquet",
        ecommerce_report_monthly_csv=out / "ecommerce_report_budget_monthly_canonical.csv",
        regional_views_dir=out / "regional_views",
        validation_summary_csv=out / "regional_validation_summary.csv",
        validation_summary_md=out / "regional_validation_summary.md",
        reference_comparison_csv=out / "reference_budget_comparison.csv",
        reference_comparison_md=out / "reference_budget_comparison.md",
        pbix_fact_csv=out / "budget_pbix_fact.csv",
        source_inventory_csv=out / "budget_sources_inventory.csv",
        qa_market_month_summary_csv=out / "budget_qa_market_month_summary.csv",
        readme_md=out / "README.md",
    )


def _safe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce mixed object columns to strings for reliable parquet writes."""
    if df.empty:
        return df

    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].apply(lambda v: None if pd.isna(v) else str(v))
    return out


def _view_prefix(workbook_type: str) -> str:
    prefix = workbook_type.strip().lower()
    if prefix.endswith("_budget"):
        prefix = prefix[: -len("_budget")]
    return prefix


def _cleanup_legacy_view_outputs(base_dir: Path, dataset_name: str) -> None:
    for path in [
        base_dir / f"{dataset_name}.parquet",
        base_dir / f"{dataset_name}.csv",
    ]:
        if path.exists():
            path.unlink()


def _write_view_outputs(base_dir: Path, file_prefix: str, dataset_name: str, frame: pd.DataFrame) -> dict[str, str]:
    _cleanup_legacy_view_outputs(base_dir, dataset_name)

    file_stem = f"{file_prefix}_{dataset_name}"
    parquet_path = base_dir / f"{file_stem}.parquet"
    csv_path = base_dir / f"{file_stem}.csv"
    frame.to_parquet(parquet_path, index=False)
    frame.to_csv(csv_path, index=False)
    return {
        "parquet": str(parquet_path),
        "csv": str(csv_path),
    }


def _month_coverage_metrics(fact_view: pd.DataFrame) -> tuple[str | None, str | None, int]:
    if fact_view.empty or "budget_month" not in fact_view.columns:
        return None, None, 0

    months = pd.to_datetime(fact_view["budget_month"], errors="coerce").dropna()
    if months.empty:
        return None, None, 0

    return (
        months.min().strftime("%Y-%m-%d"),
        months.max().strftime("%Y-%m-%d"),
        int(months.dt.strftime("%Y-%m-%d").nunique()),
    )


def _normalize_label(value: object) -> str:
    text = "" if value is None or pd.isna(value) else str(value).strip().lower()
    return " ".join(text.split())


def _round_half_up(value: float) -> int:
    if pd.isna(value):
        return 0
    return int(math.floor(float(value) + 0.5))


def _find_month_header_row(raw: pd.DataFrame, anchor_text: str | None = None) -> int | None:
    candidates: list[int] = []
    for idx in range(len(raw)):
        month_hits = 0
        for value in raw.iloc[idx].tolist():
            ts = pd.to_datetime(value, errors="coerce")
            if pd.notna(ts) and ts.year >= 2026:
                month_hits += 1
        if month_hits >= 3:
            candidates.append(idx)

    if not candidates:
        return None

    if not anchor_text:
        return candidates[0]

    anchor = anchor_text.lower()
    for idx in candidates:
        text_window: list[str] = []
        for probe in range(max(0, idx - 2), idx + 1):
            text_window.extend(_normalize_label(v) for v in raw.iloc[probe].tolist() if _normalize_label(v))
        if any(anchor in text for text in text_window):
            return idx

    return candidates[0]


def _extract_month_map(raw: pd.DataFrame, header_row: int, section_anchor: str | None = None, max_months: int = 12) -> dict[int, pd.Timestamp]:
    anchor_col: int | None = None
    if section_anchor:
        anchor = section_anchor.lower()
        for probe in range(max(0, header_row - 2), header_row + 1):
            for col_idx, value in enumerate(raw.iloc[probe].tolist()):
                text = _normalize_label(value)
                if anchor in text:
                    anchor_col = col_idx
                    break
            if anchor_col is not None:
                break

    month_map: dict[int, pd.Timestamp] = {}
    seen_months: set[pd.Timestamp] = set()
    for col_idx, value in enumerate(raw.iloc[header_row].tolist()):
        if anchor_col is not None and col_idx <= anchor_col:
            continue
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts) or ts.year < 2026:
            continue
        month = pd.Timestamp(ts).normalize().replace(day=1)
        if month in seen_months:
            continue
        month_map[col_idx] = month
        seen_months.add(month)
        if len(month_map) >= max_months:
            break
    return month_map


def _extract_monthly_values_by_label(
    raw: pd.DataFrame,
    header_row: int,
    label_map: dict[str, dict[str, str]],
    workbook_name: str,
    workbook_type: str,
    sheet_name: str,
    market_group: str,
    currency_code: str,
    source_section: str,
    month_section_anchor: str | None = None,
    value_divisor: float = 1.0,
    stop_when_labels: set[str] | None = None,
) -> pd.DataFrame:
    month_map = _extract_month_map(raw, header_row, section_anchor=month_section_anchor)
    if not month_map:
        return pd.DataFrame()

    wanted = {_normalize_label(label): meta for label, meta in label_map.items()}
    stop_labels = {_normalize_label(v) for v in (stop_when_labels or set())}
    records: list[dict[str, Any]] = []

    for row_idx in range(header_row + 1, len(raw)):
        label = _normalize_label(raw.iat[row_idx, 1] if raw.shape[1] > 1 else None)
        if label in stop_labels:
            break
        if label not in wanted:
            continue

        meta = wanted[label]
        for col_idx, month in month_map.items():
            raw_value = pd.to_numeric(raw.iat[row_idx, col_idx], errors="coerce")
            if pd.isna(raw_value):
                continue
            scaled_value = float(raw_value) / value_divisor

            records.append(
                {
                    "workbook_name": workbook_name,
                    "workbook_type": workbook_type,
                    "sheet_name": sheet_name,
                    "source_section": source_section,
                    "source_label": str(raw.iat[row_idx, 1]).strip(),
                    "budget_month": month,
                    "market_group": market_group,
                    "region": meta["region"],
                    "company_group": meta["company_group"],
                    "currency_code": currency_code,
                    "budget_amount_raw": scaled_value,
                    "budget_amount_report_k": _round_half_up(scaled_value),
                    "load_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                }
            )

    return pd.DataFrame(records)


def _aggregate_report_budget_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    grouped = (
        frame.groupby(
            [
                "workbook_type",
                "budget_month",
                "market_group",
                "region",
                "company_group",
                "currency_code",
            ],
            as_index=False,
        )
        .agg(
            workbook_name=("workbook_name", lambda values: "|".join(sorted({str(v) for v in values if str(v)}))),
            sheet_name=("sheet_name", lambda values: "|".join(sorted({str(v) for v in values if str(v)}))),
            source_section=("source_section", lambda values: "|".join(sorted({str(v) for v in values if str(v)}))),
            budget_amount_raw=("budget_amount_raw", "sum"),
            source_label=("source_label", lambda values: "|".join(sorted({str(v) for v in values if str(v)}))),
        )
    )
    grouped["budget_amount_report_k"] = grouped["budget_amount_raw"].map(_round_half_up)
    grouped["load_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return grouped


def _extract_us_report_budget(path: Path, workbook_name: str) -> pd.DataFrame:
    try:
        raw = pd.read_excel(path, sheet_name="Summary", header=None)
    except ValueError:
        return pd.DataFrame()
    header_row = _find_month_header_row(raw, anchor_text="total sales")
    if header_row is None:
        return pd.DataFrame()

    return _extract_monthly_values_by_label(
        raw=raw,
        header_row=header_row,
        label_map={
            "Northeast": {"region": "Northeast", "company_group": "Company 1"},
            "Central": {"region": "Central", "company_group": "Company 1"},
            "West": {"region": "West", "company_group": "Company 1"},
            "Southeast": {"region": "Southeast", "company_group": "Company 1"},
        },
        workbook_name=workbook_name,
        workbook_type="us_budget",
        sheet_name="Summary",
        market_group="USA",
        currency_code="USD",
        source_section="summary_total_sales",
        month_section_anchor="total sales",
        value_divisor=1000.0,
    )


def _extract_uk_report_budget(path: Path, workbook_name: str) -> pd.DataFrame:
    try:
        raw = pd.read_excel(path, sheet_name="Summary", header=None)
    except ValueError:
        return pd.DataFrame()
    header_row = _find_month_header_row(raw, anchor_text="eur")
    if header_row is None:
        return pd.DataFrame()

    frame = _extract_monthly_values_by_label(
        raw=raw,
        header_row=header_row,
        label_map={
            "Spa": {"region": "Retail", "company_group": "Company 1"},
            "Retail": {"region": "Spa", "company_group": "Company 1"},
            "Global eTailer": {"region": "Global eTailers", "company_group": "Company 3"},
        },
        workbook_name=workbook_name,
        workbook_type="uk_budget",
        sheet_name="Summary",
        market_group="UK",
        currency_code="EUR",
        source_section="summary_eur_total_sales",
        month_section_anchor="total sales",
        value_divisor=1000.0,
    )
    if frame.empty:
        return frame

    retail_mask = frame["region"] == "Retail"
    spa_mask = frame["region"] == "Spa"
    frame.loc[retail_mask, "budget_amount_report_k"] = frame.loc[retail_mask, "budget_amount_raw"].map(lambda v: int(math.ceil(float(v))))
    frame.loc[spa_mask, "budget_amount_report_k"] = frame.loc[spa_mask, "budget_amount_raw"].map(lambda v: int(math.floor(float(v))))
    return frame


def _extract_core_report_budget(path: Path, workbook_name: str) -> pd.DataFrame:
    try:
        raw = pd.read_excel(path, sheet_name="Summary", header=None)
    except ValueError:
        return pd.DataFrame()
    header_row = _find_month_header_row(raw, anchor_text="total sales")
    if header_row is None:
        return pd.DataFrame()

    main_detail = _extract_monthly_values_by_label(
        raw=raw,
        header_row=header_row,
        label_map={
            "North": {"region": "Germany", "company_group": "Company 1"},
            "North East": {"region": "Germany", "company_group": "Company 1"},
            "Bayern": {"region": "Germany", "company_group": "Company 1"},
            "South West": {"region": "Germany", "company_group": "Company 1"},
            "Retail": {"region": "Germany", "company_group": "Company 1"},
            "DE Other": {"region": "Germany", "company_group": "Company 1"},
            "NL Central": {"region": "Benelux", "company_group": "Company 1"},
            "NL Other + BL": {"region": "Benelux", "company_group": "Company 1"},
            "NL Other": {"region": "Benelux", "company_group": "Company 1"},
            "German Switzerland": {"region": "Switzerland", "company_group": "Company 1"},
            "French Switzerland": {"region": "Switzerland", "company_group": "Company 1"},
            "Other Switzerland": {"region": "Switzerland", "company_group": "Company 1"},
            "Spain": {"region": "Spain", "company_group": "Company 1"},
            "France North": {"region": "France", "company_group": "Company 1"},
            "France South": {"region": "France", "company_group": "Company 1"},
            "Italy": {"region": "Italy", "company_group": "Company 1"},
        },
        workbook_name=workbook_name,
        workbook_type="core_markets_budget",
        sheet_name="Summary",
        market_group="Core Markets",
        currency_code="EUR",
        source_section="summary_total_sales",
        month_section_anchor="total sales",
        value_divisor=1000.0,
        stop_when_labels={"Other EU"},
    )

    nrw_marina = _extract_core_nrw_split(
        path,
        workbook_name,
        "NRW Marina",
        "NRW - Marina",
        included_total_labels={"Total Active Existing Doors", "Total New Doors 2025", "Total New Doors 2026"},
    )
    nrw_ulrike = _extract_core_nrw_split(
        path,
        workbook_name,
        "NRW Ulrike",
        "NRW - Ulrike",
        included_total_labels={"Total Active Existing Doors", "Total New Doors 2026"},
    )

    frames = [f for f in [main_detail, nrw_marina, nrw_ulrike] if not f.empty]
    detail = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return _aggregate_report_budget_rows(detail)


def _extract_export_report_budget(path: Path, workbook_name: str) -> pd.DataFrame:
    try:
        raw = pd.read_excel(path, sheet_name="2026B", header=None)
    except ValueError:
        return pd.DataFrame()
    header_row = _find_month_header_row(raw, anchor_text="keur")
    if header_row is None:
        return pd.DataFrame()

    return _extract_monthly_values_by_label(
        raw=raw,
        header_row=header_row,
        label_map={
            "Distributor - Austria": {"region": "Distributor - Austria", "company_group": "Company 1"},
            "Distributor - South Africa": {"region": "Distributor - South Africa", "company_group": "Company 1"},
            "Distributor - Russia": {"region": "Distributor - Russia", "company_group": "Company 1"},
            "Distributor - Other EU": {"region": "Distributor - Other EU", "company_group": "Company 1"},
            "Distributor - Other ROW": {"region": "Distributor - Other ROW", "company_group": "Company 1"},
            "Distributor - New": {"region": "Distributor - New", "company_group": "Company 1"},
            "Export - Direct business": {"region": "Export - Direct business", "company_group": "Company 1"},
            "Distributor - China": {"region": "Distributor - China", "company_group": "Company 2"},
            "Distributor - Middle East": {"region": "Distributor - Middle East", "company_group": "Company 2"},
            "Distributor - APAC": {"region": "Distributor - APAC", "company_group": "Company 2"},
        },
        workbook_name=workbook_name,
        workbook_type="export_budget",
        sheet_name="2026B",
        market_group="Export",
        currency_code="EUR",
        source_section="group_2026b",
        month_section_anchor="keur",
        value_divisor=1.0,
    )


def _extract_export_customer_monthly_from_phasing(path: Path, workbook_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build customer-level monthly export rows from annual amount * monthly phasing."""
    try:
        raw = pd.read_excel(path, sheet_name="Co. 1_2 Export Sales", header=None)
    except ValueError:
        return pd.DataFrame(), pd.DataFrame()

    if raw.empty or raw.shape[1] <= 27:
        return pd.DataFrame(), pd.DataFrame()

    month_cols: dict[int, pd.Timestamp] = {}
    header_row_idx: int | None = None
    for probe in range(min(15, len(raw))):
        probe_map: dict[int, pd.Timestamp] = {}
        for col_idx in range(16, min(28, raw.shape[1])):
            ts = pd.to_datetime(raw.iat[probe, col_idx], errors="coerce")
            if pd.notna(ts) and ts.year >= 2026:
                probe_map[col_idx] = pd.Timestamp(ts).normalize().replace(day=1)
        if len(probe_map) >= 6:
            month_cols = probe_map
            header_row_idx = probe
            break

    if not month_cols or header_row_idx is None:
        return pd.DataFrame(), pd.DataFrame()

    records: list[dict[str, Any]] = []
    annual_col_idx = 7
    for row_idx in range(header_row_idx + 1, len(raw)):
        customer_name = _strip_or_none(raw.iat[row_idx, 1] if raw.shape[1] > 1 else None)
        if not customer_name:
            continue

        label_norm = _normalize_label(customer_name)
        if label_norm.startswith("total"):
            continue

        annual_amount = pd.to_numeric(raw.iat[row_idx, annual_col_idx], errors="coerce")
        if pd.isna(annual_amount) or float(annual_amount) == 0.0:
            continue

        for col_idx, month_value in month_cols.items():
            phase_ratio = pd.to_numeric(raw.iat[row_idx, col_idx], errors="coerce")
            if pd.isna(phase_ratio):
                continue

            budget_amount = float(annual_amount) * float(phase_ratio)
            if budget_amount == 0.0:
                continue

            records.append(
                {
                    "customer_code": -200000 - int(row_idx),
                    "customer_name": customer_name,
                    "budget_month": month_value,
                    "budget_amount": budget_amount,
                    "sheet_name": "Co. 1_2 Export Sales",
                    "workbook_name": workbook_name,
                    "workbook_type": "export_budget",
                    "header_row": int(header_row_idx + 1),
                    "currency_code": "EUR",
                    "market_group": "Export",
                    "region": customer_name,
                    "sub_region": customer_name,
                    "company_group": "Company 1",
                    "sales_person": None,
                    "door_type": None,
                    "source_section": "group_export_phasing",
                    "load_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                }
            )

    if not records:
        return pd.DataFrame(), pd.DataFrame()

    fact = pd.DataFrame(records)
    fact["customer_code"] = pd.array(fact["customer_code"], dtype="Int64")

    customer_cols = [
        "customer_code",
        "customer_name",
        "sales_person",
        "market_group",
        "region",
        "sub_region",
        "company_group",
        "door_type",
        "sheet_name",
        "workbook_name",
        "workbook_type",
    ]
    customer = fact[customer_cols].drop_duplicates().copy()
    return fact, customer


def _extract_core_nrw_split(
    path: Path,
    workbook_name: str,
    sheet_name: str,
    source_label: str,
    included_total_labels: set[str] | None = None,
) -> pd.DataFrame:
    try:
        raw = pd.read_excel(path, sheet_name=sheet_name, header=None)
    except ValueError:
        return pd.DataFrame()

    header_row = _find_month_header_row(raw)
    if header_row is None:
        return pd.DataFrame()

    month_map = _extract_month_map(raw, header_row)
    if not month_map:
        return pd.DataFrame()

    component_labels = {
        _normalize_label("Total Active Existing Doors"),
        _normalize_label("Total New Doors 2025"),
        _normalize_label("Total New Doors 2026"),
    }
    if included_total_labels:
        component_labels = {_normalize_label(v) for v in included_total_labels}
    row_indices: list[int] = []
    for row_idx in range(header_row + 1, len(raw)):
        label = _normalize_label(raw.iat[row_idx, 1] if raw.shape[1] > 1 else None)
        if label in component_labels:
            row_indices.append(row_idx)

    if not row_indices:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for col_idx, month in month_map.items():
        total_eur = 0.0
        for row_idx in row_indices:
            value = pd.to_numeric(raw.iat[row_idx, col_idx], errors="coerce")
            if pd.notna(value):
                total_eur += float(value)

        if total_eur == 0.0:
            continue

        total_k = total_eur / 1000.0
        records.append(
            {
                "workbook_name": workbook_name,
                "workbook_type": "core_markets_budget",
                "sheet_name": sheet_name,
                "source_section": "nrw_sheet_totals",
                "source_label": source_label,
                "budget_month": month,
                "market_group": "Core Markets",
                "region": "Germany",
                "company_group": "Company 1",
                "currency_code": "EUR",
                "budget_amount_raw": total_k,
                "budget_amount_report_k": _round_half_up(total_k),
                "load_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )

    return pd.DataFrame(records)


def _derive_report_budget_from_facts(sales_fact_df: pd.DataFrame) -> pd.DataFrame:
    """Build the report-grain monthly budget by aggregating from customer-level facts.

    This is the authoritative report view: it sums customer budget amounts from
    the workbook sheets (which are the source of truth) rather than reading
    Summary tabs, which can contain errors (e.g. incorrect NRW subtotals).

    Values in the xlsx customer sheets are in the native currency (EUR/USD/GBP).
    Divide by 1 000 to express kCurrency in the report output, consistent with
    the conventions used in the budget CSV reference files.
    """
    required = {"workbook_type", "workbook_name", "sheet_name", "budget_month", "budget_amount",
                "market_group", "region", "currency_code", "company_group"}
    if sales_fact_df.empty or not required.issubset(sales_fact_df.columns):
        return pd.DataFrame()

    df = sales_fact_df.dropna(subset=["budget_amount", "region"]).copy()
    df["budget_amount"] = pd.to_numeric(df["budget_amount"], errors="coerce")
    df = df.dropna(subset=["budget_amount"])

    group_keys = ["workbook_type", "budget_month", "market_group", "region", "currency_code", "company_group"]
    agg = (
        df.groupby(group_keys, as_index=False, dropna=False)
        .agg(
            workbook_name=("workbook_name", lambda s: "|".join(sorted({str(v) for v in s if pd.notna(v)}))),
            sheet_name=("sheet_name", lambda s: "|".join(sorted({str(v) for v in s if pd.notna(v)}))),
            budget_amount_sum=("budget_amount", "sum"),
        )
    )

    # Existing = Active Existing Doors + New Doors 2025 (door_type != "new_2026")
    # New      = New Doors 2026         (door_type == "new_2026")
    if "door_type" in df.columns:
        new_2026_mask = df["door_type"] == "new_2026"
        agg_new = (
            df[new_2026_mask].groupby(group_keys, as_index=False, dropna=False)
            .agg(budget_new_sum=("budget_amount", "sum"))
        )
        agg_existing = (
            df[~new_2026_mask].groupby(group_keys, as_index=False, dropna=False)
            .agg(budget_existing_sum=("budget_amount", "sum"))
        )
        agg = agg.merge(agg_existing, on=group_keys, how="left")
        agg = agg.merge(agg_new, on=group_keys, how="left")
        agg["budget_existing_raw"] = agg["budget_existing_sum"].fillna(0.0) / 1000.0
        agg["budget_new_raw"] = agg["budget_new_sum"].fillna(0.0) / 1000.0
        agg["budget_existing_k"] = agg["budget_existing_raw"].map(_round_half_up)
        agg["budget_new_k"] = agg["budget_new_raw"].map(_round_half_up)
        agg = agg.drop(columns=["budget_existing_sum", "budget_new_sum"], errors="ignore")

    # Customer-level cells are in raw currency units; normalise to kCurrency
    agg["budget_amount_raw"] = agg["budget_amount_sum"] / 1000.0
    agg["budget_amount_report_k"] = agg["budget_amount_raw"].map(_round_half_up)
    agg["source_section"] = "derived_from_customer_facts"
    agg["source_label"] = agg["region"]
    agg["load_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    agg = agg.drop(columns=["budget_amount_sum"])
    agg = _apply_native_eur_columns(
        agg,
        amount_col="budget_amount_raw",
        currency_col="currency_code",
    )
    return agg


def _extract_group_ecommerce_report_budget(path: Path, workbook_name: str) -> pd.DataFrame:
    """Extract eCommerce and USA Retail monthly budget lines from 'Sales Budget Summary'.

    The sheet layout (0-based row indices, values already in kEUR):
      Row 1  : header  — columns 11-22 are Jan-Dec 2026 monthly dates
      Row 36 : eCommerce EU (incl. UK)  — Company 3, EUR
      Row 37 : eCommerce USA            — Company 3, EUR
      Row 38 : Amazon                   — Company 3, EUR
      Row 39 : Global etailers          — Company 3, EUR
      Row 16 : Retail (USA section)     — Company 1, EUR  (scoped between UK/USA totals)
    """
    try:
        raw = pd.read_excel(path, sheet_name="Sales Budget Summary", header=None)
    except (ValueError, KeyError):
        return pd.DataFrame()

    header_row = _find_month_header_row(raw)
    if header_row is None:
        return pd.DataFrame()

    month_map = _extract_month_map(raw, header_row)
    if not month_map:
        return pd.DataFrame()

    col1 = 1  # label column index

    # ------------------------------------------------------------------
    # 1. eCommerce lines — unique labels, safe to do a full-sheet scan.
    # ------------------------------------------------------------------
    _ECOMM_LABEL_MAP: dict[str, dict] = {
        "ecommerce eu (incl. uk)": {
            "source_label": "eCommerce EU (incl. UK)",
            "region": "eCommerce EU",
            "market_group": "eCommerce",
            "company_group": "Company 3",
        },
        "ecommerce usa": {
            "source_label": "eCommerce USA",
            "region": "eCommerce USA",
            "market_group": "eCommerce",
            "company_group": "Company 3",
        },
        "amazon": {
            "source_label": "Amazon",
            "region": "Amazon",
            "market_group": "eCommerce",
            "company_group": "Company 3",
        },
        "global etailers": {
            "source_label": "Global eTailers",
            "region": "Global eTailers",
            "market_group": "eCommerce",
            "company_group": "Company 3",
        },
    }

    records: list[dict[str, Any]] = []
    for row_idx in range(header_row + 1, len(raw)):
        norm = _normalize_label(raw.iat[row_idx, col1] if raw.shape[1] > col1 else None)
        if norm not in _ECOMM_LABEL_MAP:
            continue
        meta = _ECOMM_LABEL_MAP[norm]
        for col_idx, month in month_map.items():
            val = pd.to_numeric(raw.iat[row_idx, col_idx], errors="coerce")
            if pd.isna(val):
                continue
            records.append(
                {
                    "workbook_name": workbook_name,
                    "workbook_type": "group_budget",
                    "sheet_name": "Sales Budget Summary",
                    "source_section": "group_sales_budget_summary",
                    "source_label": meta["source_label"],
                    "budget_month": month,
                    "market_group": meta["market_group"],
                    "region": meta["region"],
                    "company_group": meta["company_group"],
                    "currency_code": "EUR",
                    "budget_amount_raw": float(val),
                    "budget_amount_report_k": _round_half_up(float(val)),
                    "load_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                }
            )

    # ------------------------------------------------------------------
    # 2. USA Retail — "Retail" label also appears in UK section so we
    #    scope extraction to the block between "uk" total and "usa" total.
    # ------------------------------------------------------------------
    in_usa_section = False
    for row_idx in range(header_row + 1, len(raw)):
        norm = _normalize_label(raw.iat[row_idx, col1] if raw.shape[1] > col1 else None)

        # Enter USA section after the UK subtotal row
        if norm == "uk":
            in_usa_section = True
            continue

        # Leave USA section once we hit the USA subtotal row
        if in_usa_section and norm == "usa":
            break

        if in_usa_section and norm == "retail":
            for col_idx, month in month_map.items():
                val = pd.to_numeric(raw.iat[row_idx, col_idx], errors="coerce")
                if pd.isna(val):
                    continue
                records.append(
                    {
                        "workbook_name": workbook_name,
                        "workbook_type": "group_budget",
                        "sheet_name": "Sales Budget Summary",
                        "source_section": "group_sales_budget_summary",
                        "source_label": "Retail USA",
                        "budget_month": month,
                        "market_group": "USA",
                        "region": "Retail",
                        "company_group": "Company 1",
                        "currency_code": "EUR",
                        "budget_amount_raw": float(val),
                        "budget_amount_report_k": _round_half_up(float(val)),
                        "load_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    }
                )

    return pd.DataFrame(records)


def _extract_report_budget(path: Path, wb_type: str) -> pd.DataFrame:
    if wb_type == "us_budget":
        return _extract_us_report_budget(path, path.name)
    if wb_type == "uk_budget":
        return _extract_uk_report_budget(path, path.name)
    if wb_type == "core_markets_budget":
        return _extract_core_report_budget(path, path.name)
    if wb_type == "group_budget":
        return _extract_export_report_budget(path, path.name)
    return pd.DataFrame()


def _coerce_numeric_text(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")


def _normalize_sub_region_label(value: Any) -> str | None:
    text = _strip_or_none(value)
    if text == "Other DE":
        return "DE Other"
    return text


def _finalize_reference_comparison(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return detail_df

    out = detail_df.copy()
    out["budget_month"] = pd.to_datetime(out["budget_month"], errors="coerce")
    out["canonical_present"] = out["canonical_amount_compare"].notna()
    out["reference_present"] = out["reference_amount_compare"].notna()
    out["match_status"] = "matched"
    out.loc[out["canonical_present"] & ~out["reference_present"], "match_status"] = "canonical_only"
    out.loc[~out["canonical_present"] & out["reference_present"], "match_status"] = "reference_only"
    out.loc[out["canonical_present"] & out["reference_present"], "delta_compare"] = (
        out["canonical_amount_compare"] - out["reference_amount_compare"]
    )
    out.loc[~(out["canonical_present"] & out["reference_present"]), "delta_compare"] = None

    currency_col = "canonical_currency_code" if "canonical_currency_code" in out.columns else "currency_code"
    out = _apply_native_eur_columns(
        out,
        amount_col="canonical_amount_raw",
        currency_col=currency_col,
    )
    return out


def _build_processed_management_comparison(
    report_df: pd.DataFrame,
    fallback_report_df: pd.DataFrame,
    reference_budget_dir: Path,
) -> pd.DataFrame:
    path = reference_budget_dir / "budget_2026_processed.csv"
    if not path.exists():
        return pd.DataFrame()

    proc = pd.read_csv(path)
    if proc.empty:
        return pd.DataFrame()

    proc["Date"] = pd.to_datetime(proc.get("Date"), dayfirst=True, errors="coerce")
    proc["Value_kEUR"] = pd.to_numeric(proc.get("Value_kEUR"), errors="coerce")
    proc = proc[(proc["Date"].dt.year == 2026) & (proc.get("Metric").astype(str) == "Budget")].copy()
    if proc.empty:
        return pd.DataFrame()

    reference = (
        proc.groupby(["Market_Group", "Region", "Company_Group", "Currency", "Date"], as_index=False, dropna=False)["Value_kEUR"]
        .sum()
        .rename(columns={
            "Market_Group": "market_group",
            "Region": "region",
            "Company_Group": "company_group",
            "Currency": "currency_code",
            "Date": "budget_month",
            "Value_kEUR": "reference_amount_compare",
        })
    )

    derived_views = report_df.copy() if not report_df.empty else pd.DataFrame()
    if not derived_views.empty:
        derived_views["budget_month"] = pd.to_datetime(derived_views["budget_month"], errors="coerce")
        derived_views = derived_views[
            (derived_views["budget_month"].dt.year == 2026)
            & (derived_views["workbook_type"].isin(["core_markets_budget", "export_budget"]))
            & (derived_views["currency_code"] == "EUR")
        ].copy()
        derived_views["canonical_amount_compare"] = pd.to_numeric(derived_views["budget_amount_report_k"], errors="coerce")
        derived_views["canonical_amount_raw"] = pd.to_numeric(derived_views["budget_amount_raw"], errors="coerce")
        derived_views["canonical_compare_basis"] = "derived_report_k"

    uk_eur_view = fallback_report_df.copy() if not fallback_report_df.empty else pd.DataFrame()
    if not uk_eur_view.empty:
        uk_eur_view["budget_month"] = pd.to_datetime(uk_eur_view["budget_month"], errors="coerce")
        uk_eur_view = uk_eur_view[
            (uk_eur_view["budget_month"].dt.year == 2026)
            & (uk_eur_view["workbook_type"] == "uk_budget")
            & (uk_eur_view["currency_code"] == "EUR")
        ].copy()
        uk_eur_view["canonical_amount_compare"] = pd.to_numeric(uk_eur_view["budget_amount_report_k"], errors="coerce")
        uk_eur_view["canonical_amount_raw"] = pd.to_numeric(uk_eur_view["budget_amount_raw"], errors="coerce")
        uk_eur_view["canonical_compare_basis"] = "uk_summary_eur_report_k"

    canonical = pd.concat([frame for frame in [derived_views, uk_eur_view] if not frame.empty], ignore_index=True) if (not derived_views.empty or not uk_eur_view.empty) else pd.DataFrame()
    if canonical.empty:
        return pd.DataFrame()

    canonical = canonical[
        [
            "workbook_type", "market_group", "region", "company_group", "currency_code", "budget_month",
            "canonical_amount_raw", "canonical_amount_compare", "canonical_compare_basis",
        ]
    ].copy()

    joined = canonical.merge(
        reference,
        on=["market_group", "region", "company_group", "currency_code", "budget_month"],
        how="outer",
    )
    joined["comparison_family"] = "processed_management"
    joined["reference_file"] = path.name
    joined["comparison_grain"] = "report_region_month"
    joined["sub_region"] = None
    joined["sales_person"] = None
    joined["reference_amount_raw"] = joined["reference_amount_compare"]
    joined["reference_compare_basis"] = "processed_value_keur"
    joined["normalization_method"] = None
    joined["notes"] = joined["workbook_type"].map(
        lambda value: "UK compared using workbook EUR summary section." if value == "uk_budget" else None
    )
    return _finalize_reference_comparison(joined)


def _build_usa_region_comparison(report_df: pd.DataFrame, reference_budget_dir: Path) -> pd.DataFrame:
    path = reference_budget_dir / "budget_USA_spa_2026.csv"
    if not path.exists() or report_df.empty:
        return pd.DataFrame()

    usa = pd.read_csv(path)
    if usa.empty:
        return pd.DataFrame()

    usa["Date"] = pd.to_datetime(usa.get("Date"), dayfirst=True, errors="coerce")
    usa["Value_kUSD"] = _coerce_numeric_text(usa.get("Value_kUSD"))
    usa = usa[(usa["Date"].dt.year == 2026) & (usa.get("Metric").astype(str) == "Budget")].copy()
    if usa.empty:
        return pd.DataFrame()

    # Source file labels West/Southeast swapped relative to workbook region tabs.
    usa["normalization_method"] = ""
    swap_map = {"West": "Southeast", "Southeast": "West"}
    swap_mask = usa["Region"].isin(swap_map)
    if swap_mask.any():
        usa.loc[swap_mask, "Region"] = usa.loc[swap_mask, "Region"].map(swap_map)
        usa.loc[swap_mask, "normalization_method"] = "region swap fix: USA West <-> Southeast"

    reference = (
        usa.groupby(["Market_Group", "Region", "Company_Group", "Currency", "Date"], as_index=False, dropna=False)["Value_kUSD"]
        .sum()
        .rename(columns={
            "Market_Group": "market_group",
            "Region": "region",
            "Company_Group": "company_group",
            "Currency": "currency_code",
            "Date": "budget_month",
            "Value_kUSD": "reference_amount_raw",
        })
    )
    reference["reference_amount_compare"] = reference["reference_amount_raw"] / 1000.0
    if "normalization_method" in usa.columns:
        norm = (
            usa.groupby(["Market_Group", "Region", "Company_Group", "Currency", "Date"], as_index=False, dropna=False)
            .agg(normalization_method=("normalization_method", lambda s: "|".join(sorted({str(v) for v in s if _strip_or_none(v)}))))
            .rename(columns={
                "Market_Group": "market_group",
                "Region": "region",
                "Company_Group": "company_group",
                "Currency": "currency_code",
                "Date": "budget_month",
            })
        )
        reference = reference.merge(
            norm,
            on=["market_group", "region", "company_group", "currency_code", "budget_month"],
            how="left",
        )

    canonical = report_df.copy()
    canonical["budget_month"] = pd.to_datetime(canonical["budget_month"], errors="coerce")
    canonical = canonical[
        (canonical["workbook_type"] == "us_budget")
        & (canonical["budget_month"].dt.year == 2026)
        & (canonical["currency_code"] == "USD")
    ].copy()
    if canonical.empty:
        return pd.DataFrame()

    canonical["canonical_amount_raw"] = pd.to_numeric(canonical["budget_amount_raw"], errors="coerce")
    canonical["canonical_amount_compare"] = canonical["canonical_amount_raw"]
    canonical["canonical_compare_basis"] = "native_usd_k_from_customer_facts"
    canonical = canonical[
        [
            "workbook_type", "market_group", "region", "company_group", "currency_code", "budget_month",
            "canonical_amount_raw", "canonical_amount_compare", "canonical_compare_basis",
        ]
    ].copy()

    joined = canonical.merge(
        reference,
        on=["market_group", "region", "company_group", "currency_code", "budget_month"],
        how="outer",
    )
    joined["comparison_family"] = "usa_region"
    joined["reference_file"] = path.name
    joined["comparison_grain"] = "region_month"
    joined["sub_region"] = None
    joined["sales_person"] = None
    joined["reference_compare_basis"] = "value_kusd_scaled_to_true_kusd"
    joined["normalization_method"] = joined.get("normalization_method")
    joined["normalization_method"] = joined["normalization_method"].fillna("")
    joined["normalization_method"] = joined["normalization_method"].map(
        lambda s: "|".join([x for x in [str(s).strip("|"), "reference_amount_compare = Value_kUSD / 1000"] if x])
    )
    joined["notes"] = "USA source column is labeled kUSD but behaves like whole USD values."
    return _finalize_reference_comparison(joined)


def _build_core_gvl_comparison(sales_fact_df: pd.DataFrame, reference_budget_dir: Path) -> pd.DataFrame:
    path = reference_budget_dir / "budget_GVL_2026.csv"
    if not path.exists() or sales_fact_df.empty:
        return pd.DataFrame()

    gvl = pd.read_csv(path)
    if gvl.empty:
        return pd.DataFrame()

    gvl["Date"] = pd.to_datetime(gvl.get("Date"), dayfirst=True, errors="coerce")
    gvl["Value_kEUR"] = _coerce_numeric_text(gvl.get("Value_kEUR"))
    gvl["Existing_Budget_EUR"] = _coerce_numeric_text(gvl.get("Existing_Budget_EUR"))
    gvl["New_Budget_EUR"] = _coerce_numeric_text(gvl.get("New_Budget_EUR"))
    gvl["Sub Region"] = gvl.get("Sub Region").map(_normalize_sub_region_label)
    gvl["Sales Employee / Account"] = gvl.get("Sales Employee / Account").map(_strip_or_none)
    gvl = gvl[(gvl["Date"].dt.year == 2026) & (gvl.get("Metric").astype(str) == "Budget")].copy()
    if gvl.empty:
        return pd.DataFrame()

    gvl["normalization_method"] = ""
    gvl["notes"] = ""

    # Business rule: Italy belongs to Region=Italy (GVL occasionally labels it as France)
    italy_fix = (gvl["Sub Region"] == "Italy") & (gvl["Region"].astype(str).str.strip() == "France")
    if italy_fix.any():
        gvl.loc[italy_fix, "Region"] = "Italy"
        gvl.loc[italy_fix, "normalization_method"] += "|region remap: France/Italy -> Italy/Italy"

    # Business rule: France North blank salesperson belongs to Elena
    france_north_blank = (gvl["Sub Region"] == "France North") & (gvl["Sales Employee / Account"].isna())
    if france_north_blank.any():
        gvl.loc[france_north_blank, "Sales Employee / Account"] = "Elena"
        gvl.loc[france_north_blank, "normalization_method"] += "|salesperson fill: France North blank -> Elena"

    # Ignore explicit zero rows for Other Switzerland in reference validation
    drop_zero_other_ch = (gvl["Sub Region"] == "Other Switzerland") & (gvl["Value_kEUR"].fillna(0) == 0)
    if drop_zero_other_ch.any():
        gvl = gvl[~drop_zero_other_ch].copy()

    reference = (
        gvl.groupby(
            ["Market_Group", "Region", "Sub Region", "Sales Employee / Account", "Company_Group", "Currency", "Date"],
            as_index=False,
            dropna=False,
        )
        .agg(
            reference_amount_compare=("Value_kEUR", "sum"),
            reference_existing_budget_eur=("Existing_Budget_EUR", "sum"),
            reference_new_budget_eur=("New_Budget_EUR", "sum"),
            normalization_method=("normalization_method", lambda s: "|".join(sorted({str(v).strip("|") for v in s if _strip_or_none(v)}))),
        )
        .rename(columns={
            "Market_Group": "market_group",
            "Region": "region",
            "Sub Region": "sub_region",
            "Sales Employee / Account": "sales_person",
            "Company_Group": "company_group",
            "Currency": "currency_code",
            "Date": "budget_month",
        })
    )
    reference["reference_existing_amount_compare"] = reference["reference_existing_budget_eur"] / 1000.0
    reference["reference_new_amount_compare"] = reference["reference_new_budget_eur"] / 1000.0

    canonical = sales_fact_df.copy()
    canonical["budget_month"] = pd.to_datetime(canonical["budget_month"], errors="coerce")
    canonical = canonical[
        (canonical["workbook_type"] == "core_markets_budget")
        & (canonical["budget_month"].dt.year == 2026)
    ].copy()
    if canonical.empty:
        return pd.DataFrame()

    canonical = canonical.rename(columns={"currency_code": "canonical_currency_code"})
    canonical["sub_region"] = canonical.get("sub_region").map(_normalize_sub_region_label)
    canonical["sales_person"] = canonical.get("sales_person").map(_strip_or_none)
    canonical["budget_amount"] = pd.to_numeric(canonical["budget_amount"], errors="coerce")
    canonical = canonical.dropna(subset=["budget_amount", "sub_region"])
    canonical = (
        canonical.groupby(
            ["workbook_type", "market_group", "region", "sub_region", "sales_person", "company_group", "canonical_currency_code", "budget_month"],
            as_index=False,
            dropna=False,
        )["budget_amount"]
        .sum()
    )
    canonical["canonical_amount_raw"] = canonical["budget_amount"] / 1000.0
    canonical["canonical_amount_compare"] = canonical["canonical_amount_raw"]
    canonical["canonical_compare_basis"] = "native_eur_k_from_customer_facts"
    canonical["normalization_method"] = None

    swiss_de_chf = (canonical["sub_region"] == "German Switzerland") & (canonical["canonical_currency_code"] == "CHF")
    if swiss_de_chf.any():
        canonical.loc[swiss_de_chf, "canonical_amount_compare"] = (
            canonical.loc[swiss_de_chf, "canonical_amount_raw"] * CHF_TO_EUR_RATE
        )
        canonical.loc[swiss_de_chf, "canonical_compare_basis"] = "chf_to_eur_k_from_customer_facts"
        canonical.loc[swiss_de_chf, "normalization_method"] = (
            f"canonical_amount_compare = canonical_amount_raw * CHF_TO_EUR_RATE({CHF_TO_EUR_RATE})"
        )

    # Compare in reference currency space (GVL is EUR).
    canonical["comparison_currency_code"] = canonical["canonical_currency_code"]
    canonical.loc[swiss_de_chf, "comparison_currency_code"] = "EUR"
    canonical = canonical.drop(columns=["budget_amount"])

    joined = canonical.merge(
        reference,
        left_on=["market_group", "region", "sub_region", "sales_person", "company_group", "comparison_currency_code", "budget_month"],
        right_on=["market_group", "region", "sub_region", "sales_person", "company_group", "currency_code", "budget_month"],
        how="outer",
        suffixes=("", "_reference"),
    )
    joined["currency_code"] = joined["comparison_currency_code"].combine_first(joined["currency_code"])
    if "normalization_method_reference" in joined.columns:
        joined["normalization_method"] = joined[["normalization_method", "normalization_method_reference"]].apply(
            lambda row: "|".join([x for x in [
                _strip_or_none(row.iloc[0]),
                _strip_or_none(row.iloc[1]),
            ] if x]),
            axis=1,
        )
        joined = joined.drop(columns=["normalization_method_reference"])
    joined["comparison_family"] = "core_gvl_salesperson"
    joined["reference_file"] = path.name
    joined["comparison_grain"] = "region_sub_region_salesperson_month"
    joined["reference_amount_raw"] = joined["reference_amount_compare"]
    joined["reference_compare_basis"] = "gvl_value_keur"
    joined["notes"] = (
        "GVL reference includes explicit existing/new budget visibility; "
        "comparison uses total Value_kEUR as the primary compare amount."
    )
    return _finalize_reference_comparison(joined)


def _build_reference_budget_comparison(
    report_df: pd.DataFrame,
    sales_fact_df: pd.DataFrame,
    fallback_report_df: pd.DataFrame,
    reference_budget_dir: Path | None,
) -> pd.DataFrame:
    if reference_budget_dir is None or not reference_budget_dir.exists():
        return pd.DataFrame()

    frames = [
        _build_processed_management_comparison(report_df, fallback_report_df, reference_budget_dir),
        _build_usa_region_comparison(report_df, reference_budget_dir),
        _build_core_gvl_comparison(sales_fact_df, reference_budget_dir),
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()

    detail_df = pd.concat(frames, ignore_index=True)
    if "budget_month" in detail_df.columns:
        detail_df["budget_month"] = pd.to_datetime(detail_df["budget_month"], errors="coerce")
    return detail_df.sort_values([
        "comparison_family", "workbook_type", "market_group", "region", "sub_region", "sales_person", "budget_month"
    ]).reset_index(drop=True)


def _summarize_reference_budget_comparison(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()

    summary = (
        detail_df.groupby(["comparison_family", "workbook_type", "reference_file", "comparison_grain"], as_index=False, dropna=False)
        .agg(
            matched_rows=("match_status", lambda s: int((s == "matched").sum())),
            canonical_only_rows=("match_status", lambda s: int((s == "canonical_only").sum())),
            reference_only_rows=("match_status", lambda s: int((s == "reference_only").sum())),
            canonical_total_compare=("canonical_amount_compare", "sum"),
            reference_total_compare=("reference_amount_compare", "sum"),
            delta_total_compare=("delta_compare", "sum"),
            normalization_method=("normalization_method", lambda s: "|".join(sorted({str(v) for v in s if _strip_or_none(v)}))),
        )
    )
    return summary


def _build_validation_summary(report_df: pd.DataFrame, sales_fact_df: pd.DataFrame, sales_cust_df: pd.DataFrame, group_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    sales_view_types = [
        "us_budget",
        "uk_budget",
        "core_markets_budget",
        "export_budget",
        "unknown",
    ]
    for workbook_type in sales_view_types:
        report_view = report_df[report_df["workbook_type"] == workbook_type].copy() if not report_df.empty else pd.DataFrame()
        fact_view = sales_fact_df[sales_fact_df["workbook_type"] == workbook_type].copy() if not sales_fact_df.empty else pd.DataFrame()
        cust_view = sales_cust_df[sales_cust_df["workbook_type"] == workbook_type].copy() if not sales_cust_df.empty else pd.DataFrame()
        if report_view.empty and fact_view.empty and cust_view.empty:
            continue

        report_month_min, report_month_max, report_distinct_month_count = _month_coverage_metrics(report_view)
        month_min, month_max, distinct_month_count = _month_coverage_metrics(fact_view)
        duplicate_customer_key_rows = 0
        duplicate_customer_key_count = 0
        if not cust_view.empty and "customer_code" in cust_view.columns:
            duplicates = cust_view[cust_view["customer_code"].duplicated(keep=False)]
            duplicate_customer_key_rows = int(len(duplicates))
            duplicate_customer_key_count = int(duplicates["customer_code"].nunique()) if not duplicates.empty else 0

        rows.append(
            {
                "regional_view": workbook_type,
                "dataset_family": "sales_budget",
                "report_monthly_rows": int(len(report_view)),
                "report_month_min": report_month_min,
                "report_month_max": report_month_max,
                "report_distinct_month_count": report_distinct_month_count,
                "sales_monthly_rows": int(len(fact_view)),
                "sales_customer_rows": int(len(cust_view)),
                "group_lines_rows": 0,
                "sales_monthly_null_customer_code_rows": int(fact_view["customer_code"].isna().sum()) if "customer_code" in fact_view.columns else 0,
                "sales_monthly_null_budget_month_rows": int(fact_view["budget_month"].isna().sum()) if "budget_month" in fact_view.columns else 0,
                "sales_monthly_null_budget_amount_rows": int(fact_view["budget_amount"].isna().sum()) if "budget_amount" in fact_view.columns else 0,
                "sales_monthly_null_sales_person_rows": int(fact_view["sales_person"].isna().sum()) if "sales_person" in fact_view.columns else None,
                "sales_customer_null_customer_code_rows": int(cust_view["customer_code"].isna().sum()) if "customer_code" in cust_view.columns else 0,
                "sales_customer_null_customer_name_rows": int(cust_view["customer_name"].isna().sum()) if "customer_name" in cust_view.columns else 0,
                "sales_customer_null_sales_person_rows": int(cust_view["sales_person"].isna().sum()) if "sales_person" in cust_view.columns else None,
                "duplicate_customer_key_rows": duplicate_customer_key_rows,
                "duplicate_customer_key_count": duplicate_customer_key_count,
                "month_min": month_min,
                "month_max": month_max,
                "distinct_month_count": distinct_month_count,
            }
        )

    if not group_df.empty:
        rows.append(
            {
                "regional_view": "group_budget",
                "dataset_family": "group_budget",
                "report_monthly_rows": 0,
                "report_month_min": None,
                "report_month_max": None,
                "report_distinct_month_count": None,
                "sales_monthly_rows": 0,
                "sales_customer_rows": 0,
                "group_lines_rows": int(len(group_df)),
                "sales_monthly_null_customer_code_rows": None,
                "sales_monthly_null_budget_month_rows": None,
                "sales_monthly_null_budget_amount_rows": None,
                "sales_monthly_null_sales_person_rows": None,
                "sales_customer_null_customer_code_rows": None,
                "sales_customer_null_customer_name_rows": None,
                "sales_customer_null_sales_person_rows": None,
                "duplicate_customer_key_rows": None,
                "duplicate_customer_key_count": None,
                "month_min": None,
                "month_max": None,
                "distinct_month_count": None,
            }
        )

    return pd.DataFrame(rows)


def _write_validation_summary(paths: CanonicalOutputPaths, validation_df: pd.DataFrame) -> None:
    validation_df.to_csv(paths.validation_summary_csv, index=False)

    lines = [
        "# Regional Validation Summary",
        "",
        "Validation checks include nulls, duplicate customer keys, and month coverage for sales-oriented regional views.",
        "",
    ]
    for row in validation_df.to_dict(orient="records"):
        lines.append(f"## {row['regional_view']}")
        lines.append("")
        lines.append(f"- dataset_family: {row['dataset_family']}")
        lines.append(f"- report_monthly_rows: {row['report_monthly_rows']}")
        lines.append(f"- report_month_min: {row['report_month_min']}")
        lines.append(f"- report_month_max: {row['report_month_max']}")
        lines.append(f"- report_distinct_month_count: {row['report_distinct_month_count']}")
        lines.append(f"- sales_monthly_rows: {row['sales_monthly_rows']}")
        lines.append(f"- sales_customer_rows: {row['sales_customer_rows']}")
        lines.append(f"- group_lines_rows: {row['group_lines_rows']}")
        lines.append(f"- sales_monthly_null_customer_code_rows: {row['sales_monthly_null_customer_code_rows']}")
        lines.append(f"- sales_monthly_null_budget_month_rows: {row['sales_monthly_null_budget_month_rows']}")
        lines.append(f"- sales_monthly_null_budget_amount_rows: {row['sales_monthly_null_budget_amount_rows']}")
        lines.append(f"- sales_monthly_null_sales_person_rows: {row['sales_monthly_null_sales_person_rows']}")
        lines.append(f"- sales_customer_null_customer_code_rows: {row['sales_customer_null_customer_code_rows']}")
        lines.append(f"- sales_customer_null_customer_name_rows: {row['sales_customer_null_customer_name_rows']}")
        lines.append(f"- sales_customer_null_sales_person_rows: {row['sales_customer_null_sales_person_rows']}")
        lines.append(f"- duplicate_customer_key_rows: {row['duplicate_customer_key_rows']}")
        lines.append(f"- duplicate_customer_key_count: {row['duplicate_customer_key_count']}")
        lines.append(f"- month_min: {row['month_min']}")
        lines.append(f"- month_max: {row['month_max']}")
        lines.append(f"- distinct_month_count: {row['distinct_month_count']}")
        lines.append("")

    paths.validation_summary_md.write_text("\n".join(lines), encoding="utf-8")


def _write_reference_budget_comparison(paths: CanonicalOutputPaths, detail_df: pd.DataFrame) -> pd.DataFrame:
    detail_df.to_csv(paths.reference_comparison_csv, index=False)

    summary_df = _summarize_reference_budget_comparison(detail_df)
    lines = [
        "# Reference Budget Comparison",
        "",
        "These comparisons keep management/processed, USA regional, and Core GVL salesperson validations separate.",
        "No values are overwritten; any source normalization used for comparison is recorded explicitly per comparison family.",
        "",
    ]

    if summary_df.empty:
        lines.append("No reference budget comparison rows were generated.")
    else:
        for row in summary_df.to_dict(orient="records"):
            lines.append(f"## {row['comparison_family']} / {row['workbook_type']}")
            lines.append("")
            lines.append(f"- reference_file: {row['reference_file']}")
            lines.append(f"- comparison_grain: {row['comparison_grain']}")
            lines.append(f"- matched_rows: {row['matched_rows']}")
            lines.append(f"- canonical_only_rows: {row['canonical_only_rows']}")
            lines.append(f"- reference_only_rows: {row['reference_only_rows']}")
            lines.append(f"- canonical_total_compare: {row['canonical_total_compare']}")
            lines.append(f"- reference_total_compare: {row['reference_total_compare']}")
            lines.append(f"- delta_total_compare: {row['delta_total_compare']}")
            lines.append(f"- normalization_method: {row['normalization_method']}")
            lines.append("")

    paths.reference_comparison_md.write_text("\n".join(lines), encoding="utf-8")
    return summary_df


def _write_pbix_budget_fact(paths: CanonicalOutputPaths, sales_fact_df: pd.DataFrame) -> dict[str, Any]:
    if sales_fact_df.empty:
        pd.DataFrame().to_csv(paths.pbix_fact_csv, index=False)
        return {"rows": 0, "path": str(paths.pbix_fact_csv)}

    fact = sales_fact_df.copy()
    fact["budget_month"] = pd.to_datetime(fact["budget_month"], errors="coerce")
    fact = fact[fact["budget_month"].dt.year == 2026].copy()

    cols = [
        "workbook_type",
        "workbook_name",
        "sheet_name",
        "budget_version",
        "customer_code",
        "customer_name",
        "market_group",
        "region",
        "sub_region",
        "sales_person",
        "channel",
        "door_type",
        "budget_month",
        "currency_code",
        "budget_amount_native",
        "fx_rate_to_eur",
        "budget_amount_eur_compare",
    ]
    for col in cols:
        if col not in fact.columns:
            fact[col] = None

    out = fact[cols].copy()
    out = out.sort_values(["market_group", "region", "customer_name", "budget_month"], kind="stable")
    out.to_csv(paths.pbix_fact_csv, index=False)
    return {"rows": int(len(out)), "path": str(paths.pbix_fact_csv)}


def _write_budget_sources_inventory(paths: CanonicalOutputPaths, catalog: pd.DataFrame, sales_fact_df: pd.DataFrame) -> dict[str, Any]:
    workbook_order = [
        "us_budget",
        "uk_budget",
        "core_markets_budget",
        "export_budget",
        "group_budget",
    ]
    static_meta = {
        "us_budget": {
            "market_group": "USA",
            "grain": "customer_month",
            "currency_home": "USD",
            "monthly_available": True,
            "notes": "Monthly customer rows sourced from US regional tabs.",
        },
        "uk_budget": {
            "market_group": "UK",
            "grain": "customer_month",
            "currency_home": "GBP",
            "monthly_available": True,
            "notes": "Monthly customer rows sourced from UK budget tabs.",
        },
        "core_markets_budget": {
            "market_group": "Core Markets",
            "grain": "customer_month",
            "currency_home": "EUR/CHF",
            "monthly_available": True,
            "notes": "Monthly customer rows sourced from regional tabs; Swiss DE is in CHF.",
        },
        "export_budget": {
            "market_group": "Export",
            "grain": "customer_month + report_month",
            "currency_home": "EUR",
            "monthly_available": True,
            "notes": "2026 customer monthly rows derived from group phasing and annual plan.",
        },
        "group_budget": {
            "market_group": "Group",
            "grain": "line_level_json + report_month",
            "currency_home": "EUR",
            "monthly_available": True,
            "notes": "Source workbook for group report lines and export phasing inputs.",
        },
    }

    records: list[dict[str, Any]] = []
    sales = sales_fact_df.copy()
    sales["budget_month"] = pd.to_datetime(sales.get("budget_month"), errors="coerce")
    sales_2026 = sales[sales["budget_month"].dt.year == 2026].copy() if not sales.empty else pd.DataFrame()

    for wb_type in workbook_order:
        cat_slice = catalog[catalog.get("workbook_type") == wb_type].copy() if not catalog.empty else pd.DataFrame()
        sales_slice = sales_2026[sales_2026.get("workbook_type") == wb_type].copy() if not sales_2026.empty else pd.DataFrame()
        meta = static_meta[wb_type]

        workbook_names = "|".join(sorted({str(v) for v in cat_slice.get("workbook_name", pd.Series(dtype=object)).dropna().tolist()}))
        sheet_names = "|".join(sorted({str(v) for v in sales_slice.get("sheet_name", pd.Series(dtype=object)).dropna().tolist()})) if not sales_slice.empty else None
        salesperson_available = bool(not sales_slice.empty and sales_slice.get("sales_person").dropna().astype(str).str.strip().ne("").any())

        records.append(
            {
                "workbook_type": wb_type,
                "market_group": meta["market_group"],
                "workbook_file": workbook_names or None,
                "sheet_name": sheet_names,
                "grain": meta["grain"],
                "currency_home": meta["currency_home"],
                "monthly_available": meta["monthly_available"],
                "salesperson_available": salesperson_available,
                "rows_2026": int(len(sales_slice)),
                "notes": meta["notes"],
            }
        )

    out = pd.DataFrame(records)
    out.to_csv(paths.source_inventory_csv, index=False)
    return {"rows": int(len(out)), "path": str(paths.source_inventory_csv)}


def _write_budget_qa_market_month_summary(paths: CanonicalOutputPaths, sales_fact_df: pd.DataFrame) -> dict[str, Any]:
    cols = [
        "market_group",
        "budget_month",
        "row_count",
        "distinct_customers",
        "zero_budget_rows",
        "total_budget_native",
    ]

    if sales_fact_df.empty:
        pd.DataFrame(columns=cols).to_csv(paths.qa_market_month_summary_csv, index=False)
        return {"rows": 0, "path": str(paths.qa_market_month_summary_csv)}

    fact = sales_fact_df.copy()
    fact["budget_month"] = pd.to_datetime(fact["budget_month"], errors="coerce")
    fact = fact[fact["budget_month"].dt.year == 2026].copy()
    if fact.empty:
        pd.DataFrame(columns=cols).to_csv(paths.qa_market_month_summary_csv, index=False)
        return {"rows": 0, "path": str(paths.qa_market_month_summary_csv)}

    fact["budget_amount_native"] = pd.to_numeric(fact.get("budget_amount_native"), errors="coerce")
    fact["budget_month"] = fact["budget_month"].dt.strftime("%Y-%m-01")

    out = (
        fact.groupby(["market_group", "budget_month"], dropna=False)
        .agg(
            row_count=("customer_code", "size"),
            distinct_customers=("customer_code", "nunique"),
            zero_budget_rows=("budget_amount_native", lambda s: int(s.fillna(0).eq(0).sum())),
            total_budget_native=("budget_amount_native", "sum"),
        )
        .reset_index()
        .sort_values(["market_group", "budget_month"], kind="stable")
    )

    out.to_csv(paths.qa_market_month_summary_csv, index=False)
    return {"rows": int(len(out)), "path": str(paths.qa_market_month_summary_csv)}


def _write_readme(paths: CanonicalOutputPaths, regional_view_outputs: dict[str, dict[str, Any]], include_combined: bool) -> None:
    lines = [
        "# Budget Canonical Review Outputs",
        "",
        "This folder contains verification-friendly canonical outputs generated from the SharePoint budget workbooks.",
        "",
        "## Structure",
        "",
        "- workbook_catalog.csv: workbook-level inventory with classification and sheet names.",
        "- report_budget_monthly_canonical.parquet/csv: combined report-grain monthly budget facts when combined outputs are enabled.",
        "- regional_validation_summary.csv: machine-readable validation summary by regional view.",
        "- regional_validation_summary.md: human-readable validation summary by regional view.",
        "- reference_budget_comparison.csv: machine-readable comparison against budget reference CSVs.",
        "- reference_budget_comparison.md: human-readable comparison summary against budget reference CSVs.",
        "- budget_pbix_fact.csv: unified customer-month budget fact table for PBIX (2026 rows).",
        "- budget_sources_inventory.csv: source inventory for grain/currency/monthly/salesperson availability.",
        "- budget_qa_market_month_summary.csv: tiny QA table with row counts by market/month for refresh regression checks.",
        "- regional_views/: split outputs for each workbook family used for review.",
        "",
        "## Regional Views",
        "",
        "- us_budget: US report-grain monthly budget facts plus customer-detail review outputs.",
        "- uk_budget: UK report-grain monthly budget facts plus customer-detail review outputs.",
        "- core_markets_budget: Core Markets report-grain monthly budget facts plus customer-detail review outputs.",
        "- export_budget: Export report-grain monthly budget facts sourced from the group budget workbook plus customer-detail review outputs.",
        "- group_budget: Group planning lines captured as row-level JSON payloads.",
        "",
        "## Run Mode",
        "",
        f"- combined_outputs_included: {include_combined}",
        "- default behavior is split-only for review runs; combined outputs are optional.",
        "",
        "## Generated Views",
        "",
    ]

    for view_name, info in regional_view_outputs.items():
        lines.append(f"- {view_name}: {json.dumps({k: v for k, v in info.items() if k != 'outputs'}, ensure_ascii=True)}")

    if include_combined:
        lines.extend(
            [
                "",
                "## Combined Outputs",
                "",
                "- report_budget_monthly_canonical.parquet/csv: combined report-grain monthly budget facts across workbook families.",
                "- sales_budget_monthly_canonical.parquet/csv: combined sales facts across non-group workbook families.",
                "- sales_budget_customer_canonical.parquet/csv: combined customer snapshot across non-group workbook families.",
                "- group_budget_lines_canonical.parquet/csv: combined group planning lines.",
            ]
        )

    paths.readme_md.write_text("\n".join(lines), encoding="utf-8")


def _cleanup_combined_outputs(paths: CanonicalOutputPaths) -> None:
    for path in [
        paths.report_monthly_parquet,
        paths.report_monthly_csv,
        paths.sales_monthly_parquet,
        paths.sales_monthly_csv,
        paths.sales_customer_parquet,
        paths.sales_customer_csv,
        paths.group_lines_parquet,
        paths.group_lines_csv,
        paths.ecommerce_report_monthly_parquet,
        paths.ecommerce_report_monthly_csv,
    ]:
        if path.exists():
            path.unlink()


def _write_regional_view_outputs(
    paths: CanonicalOutputPaths,
    report_df: pd.DataFrame,
    sales_fact_df: pd.DataFrame,
    sales_cust_df: pd.DataFrame,
    group_df: pd.DataFrame,
    ecommerce_report_df: pd.DataFrame | None = None,
) -> dict[str, dict[str, Any]]:
    paths.regional_views_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, dict[str, Any]] = {}

    sales_view_types = [
        "us_budget",
        "uk_budget",
        "core_markets_budget",
        "export_budget",
        "unknown",
    ]
    for workbook_type in sales_view_types:
        report_view = report_df[report_df["workbook_type"] == workbook_type].copy() if not report_df.empty else pd.DataFrame()
        fact_view = sales_fact_df[sales_fact_df["workbook_type"] == workbook_type].copy() if not sales_fact_df.empty else pd.DataFrame()
        cust_view = sales_cust_df[sales_cust_df["workbook_type"] == workbook_type].copy() if not sales_cust_df.empty else pd.DataFrame()
        if report_view.empty and fact_view.empty and cust_view.empty:
            continue

        view_dir = paths.regional_views_dir / workbook_type
        view_dir.mkdir(parents=True, exist_ok=True)
        file_prefix = _view_prefix(workbook_type)
        outputs[workbook_type] = {
            "report_monthly_rows": int(len(report_view)),
            "sales_monthly_rows": int(len(fact_view)),
            "sales_customer_rows": int(len(cust_view)),
            "outputs": {
                "report_monthly": _write_view_outputs(view_dir, file_prefix, "report_budget_monthly_canonical", report_view),
                "sales_monthly": _write_view_outputs(view_dir, file_prefix, "sales_budget_monthly_canonical", fact_view),
                "sales_customer": _write_view_outputs(view_dir, file_prefix, "sales_budget_customer_canonical", cust_view),
            },
        }

    if not group_df.empty:
        view_dir = paths.regional_views_dir / "group_budget"
        view_dir.mkdir(parents=True, exist_ok=True)
        file_prefix = _view_prefix("group_budget")
        outputs["group_budget"] = {
            "group_lines_rows": int(len(group_df)),
            "outputs": {
                "group_lines": _write_view_outputs(view_dir, file_prefix, "budget_lines_canonical", group_df),
            },
        }

    _ecomm = ecommerce_report_df if ecommerce_report_df is not None else pd.DataFrame()
    if not _ecomm.empty:
        view_dir = paths.regional_views_dir / "ecommerce_budget"
        view_dir.mkdir(parents=True, exist_ok=True)
        outputs["ecommerce_budget"] = {
            "report_monthly_rows": int(len(_ecomm)),
            "outputs": {
                "report_monthly": _write_view_outputs(view_dir, "ecommerce", "report_budget_monthly_canonical", _ecomm),
            },
        }

    return outputs


def _upload_budget_canonical_to_blob(paths: CanonicalOutputPaths, version_label: str) -> list[str]:
    """Upload all canonical parquet/csv files to Azure silver/budget/canonical/{version}/."""
    try:
        from src.core.blob_client import get_container_client, upload_to_blob
    except ImportError:
        log.warning("blob_client not available — skipping Azure upload")
        return []

    try:
        container_client = get_container_client()
    except RuntimeError as exc:
        log.warning("Azure credentials unavailable — skipping blob upload: %s", exc)
        return []

    prefix = f"silver/budget/canonical/{version_label}/"
    uploaded: list[str] = []

    # Upload all parquet and csv files from the canonical output directory (flat + regional views)
    search_dirs = [paths.base_dir, paths.regional_views_dir]
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        for file_path in sorted(search_dir.rglob("*.parquet")):
            rel = file_path.relative_to(paths.base_dir)
            blob_path = prefix + rel.as_posix()
            url = upload_to_blob(container_client, blob_path, file_path.read_bytes())
            uploaded.append(url)

    log.info("Azure upload complete: %d files → silver/budget/canonical/%s/", len(uploaded), version_label)
    return uploaded


def build_canonical(
    input_dir: str | Path | None = None,
    output_root: str | Path | None = None,
    version_label: str | None = None,
    dry_run: bool = False,
    include_combined: bool = False,
    reference_budget_dir: str | Path | None = None,
) -> dict:
    src = Path(input_dir) if input_dir else _default_input_dir()
    dst = Path(output_root) if output_root else _default_output_root()
    ref_budget_dir = Path(reference_budget_dir) if reference_budget_dir else _default_reference_budget_dir()

    if not src.exists():
        return {"status": "error", "message": f"Input folder not found: {src}"}

    files = sorted([p for p in src.iterdir() if p.suffix.lower() == ".xlsx" and not p.name.startswith("~$")])
    if not files:
        return {"status": "error", "message": f"No xlsx files in: {src}"}

    if not version_label:
        version_label = datetime.now(timezone.utc).strftime("all_budget_%Y%m%dT%H%M%SZ")

    catalog_rows: list[dict] = []
    report_budgets: list[pd.DataFrame] = []
    sales_facts: list[pd.DataFrame] = []
    sales_customers: list[pd.DataFrame] = []
    group_lines: list[pd.DataFrame] = []
    ecommerce_report_budgets: list[pd.DataFrame] = []

    for path in files:
        xls = pd.ExcelFile(path)
        wb_type = _classify_workbook(path.name, xls.sheet_names)
        catalog_rows.append(
            {
                "workbook_name": path.name,
                "workbook_type": wb_type,
                "sheet_count": len(xls.sheet_names),
                "sheet_names": "|".join(xls.sheet_names),
            }
        )

        # Compatibility fallback: summary-tab report extraction remains available
        # for workbook fixtures that do not include customer-level sheet rows.
        report_budget = _extract_report_budget(path, wb_type)
        if not report_budget.empty:
            report_budgets.append(report_budget)

        if wb_type in {"us_budget", "uk_budget", "core_markets_budget", "export_budget", "unknown"}:
            for sheet in xls.sheet_names:
                sh_l = sheet.lower()
                # Skip non-transactional / summary tabs
                if any(k in sh_l for k in ["summary", "definition", "performance", "kpi", "mapping", "list", "review", "phasing", "tracker", "initiative", "bau"]):
                    continue
                ctx = _SHEET_CONTEXT.get(wb_type, {}).get(sheet)
                fact, cust = _extract_sales_from_sheet(path, sheet, path.name, wb_type, context=ctx)
                if not fact.empty:
                    sales_facts.append(fact)
                if not cust.empty:
                    sales_customers.append(cust)

        if wb_type == "group_budget":
            grp = _extract_group_lines(path, xls.sheet_names, path.name, wb_type)
            if not grp.empty:
                group_lines.append(grp)

            export_fact, export_cust = _extract_export_customer_monthly_from_phasing(path, path.name)
            if not export_fact.empty:
                sales_facts.append(export_fact)
            if not export_cust.empty:
                sales_customers.append(export_cust)

            ecomm_frame = _extract_group_ecommerce_report_budget(path, path.name)
            if not ecomm_frame.empty:
                ecommerce_report_budgets.append(ecomm_frame)

    catalog = pd.DataFrame(catalog_rows)
    sales_fact_df = pd.concat(sales_facts, ignore_index=True) if sales_facts else pd.DataFrame()
    sales_cust_df = pd.concat(sales_customers, ignore_index=True) if sales_customers else pd.DataFrame()
    group_df = pd.concat(group_lines, ignore_index=True) if group_lines else pd.DataFrame()
    ecommerce_report_df = pd.concat(ecommerce_report_budgets, ignore_index=True) if ecommerce_report_budgets else pd.DataFrame()
    ecommerce_report_df = _safe_for_parquet(ecommerce_report_df)
    if not ecommerce_report_df.empty:
        ecommerce_report_df = _apply_native_eur_columns(
            ecommerce_report_df, amount_col="budget_amount_raw", currency_col="currency_code"
        )

    # Derive report-grain budget by summing customer-level facts — this is authoritative
    # because it reflects the actual per-customer, per-salesperson worksheet data rather
    # than Summary tab totals, which can contain aggregation errors.
    report_df = _derive_report_budget_from_facts(sales_fact_df)
    fallback_report_df = pd.concat(report_budgets, ignore_index=True) if report_budgets else pd.DataFrame()

    # If derived rows are partial (e.g. limited customer fixtures), preserve prior
    # behavior by adding only missing keys from legacy summary extraction.
    if report_df.empty:
        report_df = fallback_report_df
    elif not fallback_report_df.empty:
        key_cols = [
            "workbook_type",
            "budget_month",
            "market_group",
            "region",
            "currency_code",
            "company_group",
        ]
        usable_keys = [c for c in key_cols if c in report_df.columns and c in fallback_report_df.columns]
        if usable_keys:
            derived_keys = report_df[usable_keys].astype(str).drop_duplicates()
            fallback_keys = fallback_report_df[usable_keys].astype(str)
            missing_mask = ~fallback_keys.apply(tuple, axis=1).isin(derived_keys.apply(tuple, axis=1))
            fallback_only = fallback_report_df[missing_mask].copy()
            if not fallback_only.empty:
                report_df = pd.concat([report_df, fallback_only], ignore_index=True)

    if not report_df.empty:
        report_df = report_df.drop_duplicates(subset=["workbook_type", "region", "budget_month", "currency_code"], keep="last")
    if not sales_fact_df.empty:
        sales_fact_df = sales_fact_df.drop_duplicates(subset=["workbook_name", "sheet_name", "customer_code", "budget_month"], keep="last")
    if not sales_cust_df.empty:
        sales_cust_df = sales_cust_df.drop_duplicates(subset=["workbook_name", "customer_code"], keep="last")

    report_df = _apply_budget_version_column(report_df)
    sales_fact_df = _apply_budget_version_column(sales_fact_df)
    sales_cust_df = _apply_budget_version_column(sales_cust_df)

    report_df = _safe_for_parquet(report_df)
    sales_fact_df = _safe_for_parquet(sales_fact_df)
    sales_cust_df = _safe_for_parquet(sales_cust_df)
    group_df = _safe_for_parquet(group_df)

    report_df = _apply_native_eur_columns(
        report_df,
        amount_col="budget_amount_raw",
        currency_col="currency_code",
    )
    # For non-EUR rows, override budget_amount_report_k with the FX-converted EUR value
    # so that all report rows are comparable in EUR regardless of source currency.
    if not report_df.empty and "budget_amount_eur_compare" in report_df.columns:
        non_eur_mask = (
            report_df["currency_code"].str.upper().ne("EUR")
            & report_df["budget_amount_eur_compare"].notna()
        )
        if non_eur_mask.any():
            report_df.loc[non_eur_mask, "budget_amount_raw"] = report_df.loc[non_eur_mask, "budget_amount_eur_compare"]
            report_df.loc[non_eur_mask, "budget_amount_report_k"] = (
                report_df.loc[non_eur_mask, "budget_amount_eur_compare"].map(_round_half_up)
            )

    # UK double-count fix: when both EUR and GBP rows exist for the same
    # (region, budget_month) within the uk_budget workbook_type, keep only the
    # EUR row.  Scoped to uk_budget only so other workbooks (e.g. CHF German
    # Switzerland in core_markets_budget) are not affected.
    if not report_df.empty and "currency_code" in report_df.columns:
        uk_mask = report_df["workbook_type"].eq("uk_budget")
        uk_eur_keys = set(
            zip(
                report_df.loc[uk_mask & report_df["currency_code"].str.upper().eq("EUR"), "region"],
                report_df.loc[uk_mask & report_df["currency_code"].str.upper().eq("EUR"), "budget_month"],
            )
        )
        if uk_eur_keys:
            uk_non_eur = (
                uk_mask
                & report_df["currency_code"].str.upper().ne("EUR")
                & pd.Series(
                    list(zip(report_df["region"], report_df["budget_month"])),
                    index=report_df.index,
                ).isin(uk_eur_keys)
            )
            report_df = report_df[~uk_non_eur].reset_index(drop=True)

    sales_fact_df = _apply_native_eur_columns(
        sales_fact_df,
        amount_col="budget_amount",
        currency_col="currency_code",
    )

    validation_df = _build_validation_summary(report_df, sales_fact_df, sales_cust_df, group_df)
    reference_detail_df = _build_reference_budget_comparison(report_df, sales_fact_df, fallback_report_df, ref_budget_dir)

    paths = _output_paths(dst, version_label)

    if not dry_run:
        catalog.to_csv(paths.catalog_csv, index=False)

        regional_view_outputs = _write_regional_view_outputs(
                paths, report_df, sales_fact_df, sales_cust_df, group_df,
                ecommerce_report_df=ecommerce_report_df if not ecommerce_report_df.empty else None,
            )
        _write_validation_summary(paths, validation_df)
        reference_summary_df = _write_reference_budget_comparison(paths, reference_detail_df)
        pbix_fact_info = _write_pbix_budget_fact(paths, sales_fact_df)
        source_inventory_info = _write_budget_sources_inventory(paths, catalog, sales_fact_df)
        qa_market_month_info = _write_budget_qa_market_month_summary(paths, sales_fact_df)
        _write_readme(paths, regional_view_outputs, include_combined)

        if include_combined:
            report_df.to_parquet(paths.report_monthly_parquet, index=False)
            report_df.to_csv(paths.report_monthly_csv, index=False)

            sales_fact_df.to_parquet(paths.sales_monthly_parquet, index=False)
            sales_fact_df.to_csv(paths.sales_monthly_csv, index=False)

            sales_cust_df.to_parquet(paths.sales_customer_parquet, index=False)
            sales_cust_df.to_csv(paths.sales_customer_csv, index=False)

            group_df.to_parquet(paths.group_lines_parquet, index=False)
            group_df.to_csv(paths.group_lines_csv, index=False)

            if not ecommerce_report_df.empty:
                ecommerce_report_df.to_parquet(paths.ecommerce_report_monthly_parquet, index=False)
                ecommerce_report_df.to_csv(paths.ecommerce_report_monthly_csv, index=False)
        else:
            _cleanup_combined_outputs(paths)

        # Upload canonical parquets to Azure after all local files are written
        _upload_budget_canonical_to_blob(paths, version_label)
        regional_view_outputs = {}
        reference_summary_df = _summarize_reference_budget_comparison(reference_detail_df)
        pbix_fact_info = {}
        source_inventory_info = {}
        qa_market_month_info = {}

    outputs = {
        "catalog_csv": str(paths.catalog_csv),
        "regional_views_dir": str(paths.regional_views_dir),
        "validation_summary_csv": str(paths.validation_summary_csv),
        "validation_summary_md": str(paths.validation_summary_md),
        "reference_comparison_csv": str(paths.reference_comparison_csv),
        "reference_comparison_md": str(paths.reference_comparison_md),
        "pbix_fact_csv": str(paths.pbix_fact_csv),
        "source_inventory_csv": str(paths.source_inventory_csv),
        "qa_market_month_summary_csv": str(paths.qa_market_month_summary_csv),
        "readme_md": str(paths.readme_md),
    }
    if include_combined:
        outputs.update(
            {
                "report_monthly_parquet": str(paths.report_monthly_parquet),
                "report_monthly_csv": str(paths.report_monthly_csv),
                "sales_monthly_parquet": str(paths.sales_monthly_parquet),
                "sales_monthly_csv": str(paths.sales_monthly_csv),
                "sales_customer_parquet": str(paths.sales_customer_parquet),
                "sales_customer_csv": str(paths.sales_customer_csv),
                "group_lines_parquet": str(paths.group_lines_parquet),
                "group_lines_csv": str(paths.group_lines_csv),
                "ecommerce_report_monthly_parquet": str(paths.ecommerce_report_monthly_parquet),
                "ecommerce_report_monthly_csv": str(paths.ecommerce_report_monthly_csv),
            }
        )

    return {
        "status": "dry_run" if dry_run else "ok",
        "version_label": version_label,
        "include_combined": include_combined,
        "workbook_count": int(len(catalog)),
        "report_monthly_rows": int(len(report_df)),
        "sales_monthly_rows": int(len(sales_fact_df)),
        "sales_customer_rows": int(len(sales_cust_df)),
        "group_lines_rows": int(len(group_df)),
        "ecommerce_report_monthly_rows": int(len(ecommerce_report_df)),
        "outputs": outputs,
        "validation_summary": validation_df.to_dict(orient="records"),
        "reference_comparison_summary": reference_summary_df.to_dict(orient="records"),
        "regional_views": regional_view_outputs,
        "pbix_fact": pbix_fact_info,
        "source_inventory": source_inventory_info,
        "qa_market_month_summary": qa_market_month_info,
    }
