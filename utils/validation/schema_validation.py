"""Schema checks for source DataFrames."""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import yaml


logger = logging.getLogger(__name__)


VALIDATION_RULES_PATH = Path(__file__).resolve().parents[2] / "config" / "validation_rules.yaml"


def _normalize_column_name(name: Any) -> str:
    """Normalize a column label for comparison without changing the DataFrame."""
    return " ".join(str(name).strip().casefold().split())


def validate_schema(
    df: pd.DataFrame | pl.DataFrame,
    required_columns: list[str] | tuple[str, ...] | None = None,
    *,
    retailer: str,
    file_path: str | Path = "",
) -> tuple[bool, str | None]:
    """Validate required, unexpected, and duplicate DataFrame columns.
    """
    if isinstance(df, (pd.DataFrame, pl.DataFrame)):
        actual_columns = list(df.columns)
    else:
        return False, f"Unsupported DataFrame type: {type(df).__name__}"

    retailer_key = retailer.strip().casefold()
    if required_columns is None:
        try:
            with VALIDATION_RULES_PATH.open("r", encoding="utf-8") as rules_file:
                validation_rules = yaml.safe_load(rules_file) or {}
        except (OSError, yaml.YAMLError) as exc:
            message = f"Could not load validation rules from {VALIDATION_RULES_PATH}: {exc}"
            logger.error(message)
            return False, message

        retailer_rules = validation_rules.get(retailer_key, {})
        required_columns = retailer_rules.get("schema", {}).get("essential_columns")
        if not required_columns:
            message = f"No essential-column schema rules configured for retailer {retailer!r}"
            logger.error(message)
            return False, message

    normalized_actual = [_normalize_column_name(name) for name in actual_columns]
    normalized_required = [_normalize_column_name(name) for name in required_columns]
    actual_counts = Counter(normalized_actual)

    duplicates = sorted(
        name for name, count in actual_counts.items() if count > 1
    )
    missing = sorted(set(normalized_required) - set(normalized_actual))
    unexpected = sorted(set(normalized_actual) - set(normalized_required))

    context = " ".join(
        value for value in (retailer, Path(file_path).name if file_path else "") if value
    )
    label = f" for {context}" if context else ""

    if unexpected:
        logger.warning(
            "Unexpected column(s)%s: %s",
            label,
            ", ".join(unexpected),
        )

    if duplicates or missing:
        problems = []
        if duplicates:
            problems.append(f"duplicate column names after normalization: {duplicates}")
        if missing:
            problems.append(f"missing required columns: {missing}")
        message = f"Schema validation failed{label}: {'; '.join(problems)}"
        logger.error(message)
        return False, message

    logger.info("Schema validation passed%s", label)
    return True, None
