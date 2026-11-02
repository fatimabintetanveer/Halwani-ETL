"""Retailer-specific layout rules and a generic source-layout checker.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


logger = logging.getLogger(__name__)


VALIDATION_RULES_PATH = Path(__file__).resolve().parents[2] / "config" / "validation_rules.yaml"


def _normalize(value: Any) -> str:
    """Normalize a cell value for comparison without changing source data."""
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).strip().casefold().split())


def _excel_cell_position(cell_address: str) -> tuple[int, int]:
    """Convert an A1-style cell address to one-based row and column indexes."""
    letters = "".join(char for char in cell_address if char.isalpha())
    digits = "".join(char for char in cell_address if char.isdigit())
    if not letters or not digits or len(letters) + len(digits) != len(cell_address):
        raise ValueError(f"Invalid Excel cell address: {cell_address!r}")

    column_number = 0
    for char in letters.upper():
        column_number = column_number * 26 + ord(char) - ord("A") + 1
    return int(digits), column_number


def _cell_value(rows: list[list[Any]], cell_address: str) -> Any:
    row_number, column_number = _excel_cell_position(cell_address)
    if row_number > len(rows) or column_number > len(rows[row_number - 1]):
        return None
    return rows[row_number - 1][column_number - 1]


def check_layout(
    file_path: str | Path,
    retailer: str,
    *,
    sheet_name: str | None = None,
    engine: str = "calamine",
) -> tuple[bool, str | None]:
    """Check a small source preview against that retailer's layout rules.

    Excel sheet validation is separate. Pass the selected sheet name returned
    by ``check_expected_sheet``. Row numbers in the rules are one-based.
    """
    path = Path(file_path)
    retailer_key = retailer.strip().casefold()
    try:
        with VALIDATION_RULES_PATH.open("r", encoding="utf-8") as rules_file:
            validation_rules = yaml.safe_load(rules_file) or {}
    except (OSError, yaml.YAMLError) as exc:
        message = f"Could not load layout rules from {VALIDATION_RULES_PATH}: {exc}"
        logger.error(message)
        return False, message

    rules = validation_rules.get(retailer_key, {}).get("layout")
    if rules is None:
        message = f"No layout rules configured for retailer {retailer!r}"
        logger.error(message)
        return False, message

    try:
        if path.suffix.lower() == ".csv":
            rows = []
            with path.open(
                "r", encoding=rules.get("encoding", "utf-8-sig"), newline=""
            ) as csv_file:
                reader = csv.reader(csv_file, delimiter=rules.get("delimiter", ","))
                for _ in range(20):
                    try:
                        rows.append(next(reader))
                    except StopIteration:
                        break
        else:
            if path.suffix.lower() not in {".xlsx", ".xlsb"}:
                raise ValueError(
                    f"Unsupported layout-check file extension: {path.suffix or '(none)'}"
                )
            if not sheet_name:
                raise ValueError(
                    "sheet_name is required for Excel layout checks; "
                    "run check_expected_sheet first"
                )
            preview = pd.read_excel(
                path,
                sheet_name=sheet_name,
                header=None,
                nrows=20,
                engine=engine,
            )
            rows = preview.values.tolist()
    except FileNotFoundError:
        message = f"Source file not found during layout check: {path}"
        return False, message
    except PermissionError:
        message = f"Access denied during layout check: {path}"
        return False, message
    except Exception as exc:
        message = (
            f"Could not inspect {retailer} layout in {path.name}: "
            f"{type(exc).__name__}: {exc}"
        )
        return False, message

    if not rows:
        message = f"Unknown {retailer} layout in {path.name}: file preview is empty"
        return False, message

    selected_layout: str | None = None
    selected_rules = rules
    variants = rules.get("variants", {})
    if variants:
        matched_variants = []
        for variant_name, variant_rules in variants.items():
            header_row_number = int(variant_rules["header_row"])
            if header_row_number > len(rows):
                continue
            header_values = {
                _normalize(value)
                for value in rows[header_row_number - 1]
                if _normalize(value)
            }
            header_anchors = variant_rules.get("header_anchors", ())
            row_one_values = {
                _normalize(value) for value in rows[0] if _normalize(value)
            }
            if (
                all(_normalize(anchor) in header_values for anchor in header_anchors)
                and all(
                    _normalize(value) in row_one_values
                    for value in variant_rules.get("required_values_in_row", {}).get(1, ())
                )
            ):
                matched_variants.append(variant_name)

        if len(matched_variants) != 1:
            message = (
                f"Unknown or ambiguous {retailer} layout in {path.name}; "
                f"matched variants: {matched_variants or 'none'}"
            )
            return False, message

        selected_layout = matched_variants[0]
        selected_rules = variants[selected_layout]

    header_row = int(
        selected_rules.get("header_row", max(rules.get("header_rows", (1,))))
    )

    header_anchors = tuple(rules.get("header_anchors", ()))
    header_values = (
        {
            _normalize(value)
            for value in rows[header_row - 1]
            if _normalize(value)
        }
        if header_row <= len(rows)
        else set()
    )
    if header_anchors and not all(
        _normalize(anchor) in header_values for anchor in header_anchors
    ):
        message = (
            f"Unexpected {retailer} layout in {path.name}: expected header anchors "
            f"{header_anchors} on row {header_row}"
        )
        return False, message

    for row_number, anchors in rules.get("required_values_in_row", {}).items():
        row_values = (
            {
                _normalize(value)
                for value in rows[row_number - 1]
                if _normalize(value)
            }
            if row_number <= len(rows)
            else set()
        )
        if row_number > len(rows) or not all(
            _normalize(anchor) in row_values for anchor in anchors
        ):
            message = (
                f"Unexpected {retailer} layout in {path.name}: expected values "
                f"{anchors} on row {row_number}"
            )
            return False, message

    for cell_address, expected_value in rules.get("fixed_cells", {}).items():
        actual_value = _cell_value(rows, cell_address)
        if _normalize(actual_value) != _normalize(expected_value):
            message = (
                f"Unexpected {retailer} layout in {path.name}: cell {cell_address} "
                f"should contain {expected_value!r}, found {actual_value!r}"
            )
            return False, message

    for cell_address in rules.get("expected_blank_cells", ()):
        actual_value = _cell_value(rows, cell_address)
        if _normalize(actual_value):
            message = (
                f"Unexpected {retailer} layout in {path.name}: expected cell "
                f"{cell_address} to be blank, found {actual_value!r}"
            )
            return False, message

    actual_column_count = len(rows[header_row - 1])
    expected_column_count = selected_rules.get(
        "expected_column_count", rules.get("expected_column_count")
    )

    if expected_column_count is not None and actual_column_count != expected_column_count:

        logger.warning(
            "%s layout column-count change in %s: expected %s, found %d",
            retailer,
            path.name,
            expected_column_count,
            actual_column_count,
        )

    logger.info(
        "Layout check passed for %s file %s%s: header row=%d, columns=%d",
        retailer,
        path.name,
        f" ({selected_layout} layout)" if selected_layout else "",
        header_row,
        actual_column_count,
    )
    return True, None
