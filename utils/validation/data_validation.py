"""Data-value validation helpers for pandas and Polars DataFrames."""

from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl


logger = logging.getLogger(__name__)


def validate_numeric_columns(
    df: pd.DataFrame | pl.DataFrame,
    columns: list[str] | tuple[str, ...],
    *,
    retailer: str = "",
    file_path: str | Path = "",
) -> tuple[bool, dict[str, dict[str, int | float]], str | None]:
    """Measure nulls, blanks, and non-numeric values in selected columns.
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        return False, {}, f"Unsupported DataFrame type: {type(df).__name__}"

    missing_columns = [column for column in columns if column not in df.columns]
    if missing_columns:
        context = f" for {retailer}" if retailer else ""
        location = f" in {Path(file_path).name}" if file_path else ""
        message = (
            f"Numeric validation cannot find required column(s){context}{location}: "
            f"{', '.join(missing_columns)}"
        )
        return False, {}, message

    row_count = len(df)
    metrics: dict[str, dict[str, int | float]] = {}
    invalid_columns: list[str] = []

    for column in columns:
        if isinstance(df, pd.DataFrame):
            values = df[column]
            source_nulls = values.isna()
            blank_strings = values.astype("string").str.strip().eq("").fillna(False)
            parsed = pd.to_numeric(values, errors="coerce")
            invalid = ~(source_nulls | blank_strings) & parsed.isna()

            null_count = int(source_nulls.sum())
            blank_count = int(blank_strings.sum())
            invalid_count = int(invalid.sum())
        else:
            values = df.get_column(column)
            text_values = pl.col(column).cast(pl.String).str.strip_chars()
            is_blank = pl.col(column).is_not_null() & (text_values == "")
            parsed_values = pl.col(column).cast(pl.Float64, strict=False)
            is_invalid = (
                pl.col(column).is_not_null()
                & ~is_blank
                & parsed_values.is_null()
            )
            counts = df.select(
                pl.col(column).is_null().sum().alias("null_count"),
                is_blank.sum().alias("blank_count"),
                is_invalid.sum().alias("invalid_count"),
            ).row(0, named=True)
            null_count = int(counts["null_count"] or 0)
            blank_count = int(counts["blank_count"] or 0)
            invalid_count = int(counts["invalid_count"] or 0)

        denominator = row_count or 1
        metrics[column] = {
            "row_count": row_count,
            "null_count": null_count,
            "blank_count": blank_count,
            "invalid_parse_count": invalid_count,
            "null_rate": (null_count + blank_count) / denominator,
            "invalid_parse_rate": invalid_count / denominator,
        }
        if invalid_count:
            invalid_columns.append(f"{column}: {invalid_count}")

    context = f" for {retailer}" if retailer else ""
    location = f" in {Path(file_path).name}" if file_path else ""
    if invalid_columns:
        message = (
            f"Non-numeric value(s) found{context}{location}: "
            f"{'; '.join(invalid_columns)}"
        )
        return False, metrics, message

    logger.info("Numeric validation passed%s%s: %s", context, location, metrics)
    return True, metrics, None


def validate_identifier_columns(
    df: pd.DataFrame | pl.DataFrame,
    columns: list[str] | tuple[str, ...],
    *,
    retailer: str = "",
    file_path: str | Path = "",
) -> tuple[bool, dict[str, dict[str, int | float]], str | None]:
    """Count null and blank identifiers without modifying the DataFrame.

    Returns ``(passed, metrics, error_message)``. A column with missing
    identifiers makes ``passed`` false; the caller chooses whether to fail,
    quarantine, or drop the affected rows.
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        return False, {}, f"Unsupported DataFrame type: {type(df).__name__}"

    missing_columns = [column for column in columns if column not in df.columns]
    if missing_columns:
        context = f" for {retailer}" if retailer else ""
        location = f" in {Path(file_path).name}" if file_path else ""
        message = (
            f"Identifier validation cannot find required column(s){context}{location}: "
            f"{', '.join(missing_columns)}"
        )
        logger.error(message)
        return False, {}, message

    row_count = len(df)
    denominator = row_count or 1
    metrics: dict[str, dict[str, int | float]] = {}
    missing_identifiers: list[str] = []

    for column in columns:
        if isinstance(df, pd.DataFrame):
            values = df[column]
            null_count = int(values.isna().sum())
            blank_count = int(
                (values.notna() & values.astype("string").str.strip().eq("").fillna(False)).sum()
            )
        else:
            text_values = pl.col(column).cast(pl.String).str.strip_chars()
            counts = df.select(
                pl.col(column).is_null().sum().alias("null_count"),
                (
                    pl.col(column).is_not_null() & (text_values == "")
                ).sum().alias("blank_count"),
            ).row(0, named=True)
            null_count = int(counts["null_count"] or 0)
            blank_count = int(counts["blank_count"] or 0)

        missing_count = null_count + blank_count
        metrics[column] = {
            "row_count": row_count,
            "null_count": null_count,
            "blank_count": blank_count,
            "missing_count": missing_count,
            "missing_rate": missing_count / denominator,
        }
        if missing_count:
            missing_identifiers.append(
                f"{column}: {missing_count} ({missing_count / denominator:.2%})"
            )

    context = f" for {retailer}" if retailer else ""
    location = f" in {Path(file_path).name}" if file_path else ""
    if missing_identifiers:
        message = (
            f"Null or blank identifier(s) found{context}{location}: "
            f"{'; '.join(missing_identifiers)}"
        )
        logger.warning(message)
        return False, metrics, message

    logger.info("Identifier validation passed%s%s: %s", context, location, metrics)
    return True, metrics, None


def calculate_filter_impact(
    before_df: pd.DataFrame | pl.DataFrame,
    after_df: pd.DataFrame | pl.DataFrame,
    *,
    amount_column: str,
    quantity_column: str,
    retailer: str = "",
    filter_name: str,
) -> dict[str, int | float | str]:
    """Report rows and measure totals before/after a row filter.

    This helper assumes the filter only removes rows and does not change the
    amount or quantity values. It does not modify either DataFrame.
    """
    if not isinstance(before_df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError(f"Unsupported before_df type: {type(before_df).__name__}")
    if not isinstance(after_df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError(f"Unsupported after_df type: {type(after_df).__name__}")
    if type(before_df) is not type(after_df):
        raise TypeError("before_df and after_df must use the same DataFrame library")

    required_columns = (amount_column, quantity_column)
    missing_before = [column for column in required_columns if column not in before_df.columns]
    missing_after = [column for column in required_columns if column not in after_df.columns]
    if missing_before or missing_after:
        raise ValueError(
            f"Measure column(s) missing; before: {missing_before}, after: {missing_after}"
        )

    def measure_total(df: pd.DataFrame | pl.DataFrame, column: str) -> float:
        if isinstance(df, pd.DataFrame):
            numeric_values = pd.to_numeric(df[column], errors="coerce")
            return float(numeric_values.sum(skipna=True))

        total = df.select(
            pl.col(column).cast(pl.Float64, strict=False).sum()
        ).item()
        return float(total or 0)

    amount_before = measure_total(before_df, amount_column)
    amount_after = measure_total(after_df, amount_column)
    quantity_before = measure_total(before_df, quantity_column)
    quantity_after = measure_total(after_df, quantity_column)

    result: dict[str, int | float | str] = {
        "retailer": retailer,
        "filter_name": filter_name,
        "rows_before": len(before_df),
        "rows_after": len(after_df),
        "rows_removed": len(before_df) - len(after_df),
        "amount_before": amount_before,
        "amount_after": amount_after,
        "amount_removed": amount_before - amount_after,
        "quantity_before": quantity_before,
        "quantity_after": quantity_after,
        "quantity_removed": quantity_before - quantity_after,
    }
    logger.info("Filter impact: %s", result)
    return result


def validate_parquet_output(
    df: pd.DataFrame | pl.DataFrame,
    parquet_path: str | Path,
    *,
    amount_column: str = "Sales Amount",
    quantity_column: str = "Sales Quantity",
    retailer: str = "",
    tolerance: float = 1e-6,
) -> tuple[bool, dict[str, int | float], str | None]:
    """Read a written Parquet file back and compare rows and measure totals."""
    path = Path(parquet_path)
    if not path.is_file():
        message = f"Silver Parquet file does not exist: {path}"
        return False, {}, message
    if path.stat().st_size == 0:
        message = f"Silver Parquet file is empty: {path}"
        return False, {}, message
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        message = f"Unsupported DataFrame type: {type(df).__name__}"
        return False, {}, message

    measure_columns = (amount_column, quantity_column)
    missing_columns = [column for column in measure_columns if column not in df.columns]
    if missing_columns:
        message = f"Measure column(s) missing from output DataFrame: {missing_columns}"
        return False, {}, message

    def dataframe_total(column: str) -> float:
        if isinstance(df, pd.DataFrame):
            return float(pd.to_numeric(df[column], errors="coerce").sum(skipna=True))
        total = df.select(
            pl.col(column).cast(pl.Float64, strict=False).sum()
        ).item()
        return float(total or 0)

    try:
        saved = (
            pl.scan_parquet(str(path))
            .select(
                pl.len().alias("row_count"),
                pl.col(amount_column)
                .cast(pl.Float64, strict=False)
                .sum()
                .alias("amount_total"),
                pl.col(quantity_column)
                .cast(pl.Float64, strict=False)
                .sum()
                .alias("quantity_total"),
            )
            .collect()
            .row(0, named=True)
        )
    except Exception as exc:
        message = (
            f"Could not read back Silver Parquet for {retailer or 'retailer'} "
            f"at {path}: {type(exc).__name__}: {exc}"
        )
        return False, {}, message

    expected_rows = len(df)
    expected_amount = dataframe_total(amount_column)
    expected_quantity = dataframe_total(quantity_column)
    actual_rows = int(saved["row_count"])
    actual_amount = float(saved["amount_total"] or 0)
    actual_quantity = float(saved["quantity_total"] or 0)

    metrics: dict[str, int | float] = {
        "expected_rows": expected_rows,
        "parquet_rows": actual_rows,
        "expected_amount": expected_amount,
        "parquet_amount": actual_amount,
        "expected_quantity": expected_quantity,
        "parquet_quantity": actual_quantity,
    }
    differences = []
    if expected_rows != actual_rows:
        differences.append(f"row count expected {expected_rows}, found {actual_rows}")
    if not math.isclose(
        expected_amount, actual_amount, rel_tol=1e-9, abs_tol=tolerance
    ):
        differences.append(
            f"amount total expected {expected_amount}, found {actual_amount}"
        )
    if not math.isclose(
        expected_quantity, actual_quantity, rel_tol=1e-9, abs_tol=tolerance
    ):
        differences.append(
            f"quantity total expected {expected_quantity}, found {actual_quantity}"
        )

    if differences:
        message = (
            f"Silver Parquet validation failed for {retailer or 'retailer'} "
            f"({path.name}): {'; '.join(differences)}"
        )
        return False, metrics, message

    logger.info("Silver Parquet validation passed for %s: %s", path.name, metrics)
    return True, metrics, None
