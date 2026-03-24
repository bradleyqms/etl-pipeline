from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[dict] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)


def current_utc_timestamp() -> str:
    """Return a single-run UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def add_etl_load_timestamp(df: pd.DataFrame, etl_load_timestamp: str) -> pd.DataFrame:
    """Stamp a DataFrame with the ETL load timestamp."""
    stamped = df.copy()
    stamped["etl_load_timestamp"] = pd.Timestamp(etl_load_timestamp)
    return stamped


def validate_dataframe(
    df: pd.DataFrame,
    *,
    required_columns: set[str] | list[str] | tuple[str, ...],
    datetime_columns: set[str] | list[str] | tuple[str, ...] = (),
    numeric_columns: set[str] | list[str] | tuple[str, ...] = (),
    non_null_columns: set[str] | list[str] | tuple[str, ...] = (),
) -> ValidationResult:
    """Validate a cleaned DataFrame before parquet writes."""
    required_columns = set(required_columns)
    datetime_columns = set(datetime_columns)
    numeric_columns = set(numeric_columns)
    non_null_columns = set(non_null_columns)

    errors: list[dict] = []
    warnings: list[dict] = []

    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        errors.append(
            {
                "code": "missing_required_columns",
                "message": f"Missing required columns: {', '.join(missing_columns)}",
                "columns": missing_columns,
            }
        )

    for column in sorted(datetime_columns & set(df.columns)):
        if not pd.api.types.is_datetime64_any_dtype(df[column]):
            errors.append(
                {
                    "code": "invalid_datetime_dtype",
                    "message": f"Column '{column}' must be datetime after cleaning",
                    "column": column,
                    "dtype": str(df[column].dtype),
                }
            )

    for column in sorted(numeric_columns & set(df.columns)):
        if not pd.api.types.is_numeric_dtype(df[column]):
            errors.append(
                {
                    "code": "invalid_numeric_dtype",
                    "message": f"Column '{column}' must be numeric after cleaning",
                    "column": column,
                    "dtype": str(df[column].dtype),
                }
            )

    for column in sorted(non_null_columns & set(df.columns)):
        null_count = int(df[column].isna().sum())
        if null_count:
            errors.append(
                {
                    "code": "null_in_required_column",
                    "message": f"Column '{column}' contains {null_count} null value(s)",
                    "column": column,
                    "null_count": null_count,
                }
            )

    if len(df) == 0:
        warnings.append(
            {
                "code": "empty_dataframe",
                "message": "Cleaned DataFrame is empty",
            }
        )

    return ValidationResult(is_valid=not errors, errors=errors, warnings=warnings)
