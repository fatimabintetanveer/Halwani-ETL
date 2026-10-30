"""Validation checks for retailer source files.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from pathlib import Path
import pandas as pd


logger = logging.getLogger(__name__)

_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

_MONTH_PATTERN = "|".join(sorted(_MONTHS, key=len, reverse=True))
_PERIOD_PATTERN = re.compile(
    rf"(?<![A-Za-z])(?P<month>{_MONTH_PATTERN})\b[\s_.'-]*"
    rf"(?P<year>20\d{{2}}|\d{{2}})(?!\d)",
    re.IGNORECASE,
)


def check_source_directory(source_dir: str | Path, retailer: str = "") -> bool:
    """Return whether the configured path exists and is a directory."""
    directory = Path(source_dir).expanduser()
    label = f" for {retailer}" if retailer else ""

    if not directory.exists():
        logger.error("Source directory is missing%s: %s", label, directory)
        return False
    if not directory.is_dir():
        logger.error("Configured source path is not a directory%s: %s", label, directory)
        return False

    logger.info("Source directory is available%s: %s", label, directory)
    return True


def validate_file_extensions(
    files: list[str | Path] | tuple[str | Path, ...],
    expected_extensions: tuple[str, ...] | list[str],
    retailer: str = "",
) -> list[Path]:
    """Return candidate files with an unexpected extension."""
    allowed = {
        ext.strip().lower() if ext.strip().startswith(".") else f".{ext.strip().lower()}"
        for ext in expected_extensions
    }
    if not allowed:
        raise ValueError("expected_extensions must contain at least one extension")

    return [
        Path(file)
        for file in files
        if Path(file).suffix.lower() not in allowed
    ]


def parse_source_period(file_path: str | Path) -> tuple[int, int] | None:
    """Return ``(year, month_number)`` parsed from a filename, or ``None``.

    Two-digit years are interpreted as 2000-2099.
    """
    matches = list(_PERIOD_PATTERN.finditer(Path(file_path).stem))
    if not matches:
        return None

    match = matches[-1]
    year_text = match.group("year")
    year = int(year_text) + (2000 if len(year_text) == 2 else 0)
    month_number = _MONTHS[match.group("month").lower()]
    return year, month_number


def find_duplicate_periods(
    files: list[str | Path] | tuple[str | Path, ...],
    retailer: str = "",
) -> dict[str, tuple[Path, ...]]:
    """Return files grouped by duplicate year/month and log each duplicate."""
    files_by_period: dict[tuple[int, int], list[Path]] = defaultdict(list)
    for value in files:
        path = Path(value)
        period = parse_source_period(path)
        if period is not None:
            files_by_period[period].append(path)

    duplicates: dict[str, tuple[Path, ...]] = {}
    for (year, month_number), paths in sorted(files_by_period.items()):
        if len(paths) < 2:
            continue

        period_label = f"{year}-{month_number:02d}"
        duplicates[period_label] = tuple(paths)

    if not duplicates:
        logger.info(
            "No duplicate retailer/month files detected%s",
            f" for {retailer}" if retailer else "",
        )
    return duplicates

def check_expected_sheet(
    file_path: str | Path,
    *,
    expected_sheets: tuple[str, ...],
    retailer: str = "",
    engine: str = "calamine",
) -> tuple[str | None, str | None]:
    """Return the matching sheet name, or an error message if none matches.

    Plain entries in expected_sheets are exact names. Entries prefixed with
    're:' are regular expressions, for example r're:DayStoreProduct_\\d+'.
    """
    path = Path(file_path)
    retailer_label = f" for {retailer}" if retailer else ""

    if not expected_sheets:
        raise ValueError("expected_sheets must contain at least one name or pattern")

    try:
        with pd.ExcelFile(path, engine=engine) as workbook:
            available_sheets = workbook.sheet_names
    except FileNotFoundError:
        message = f"Workbook not found{retailer_label}: {path}"
        return None, message
    
    except PermissionError:
        message = f"Access denied while opening workbook{retailer_label}: {path}"
        return None, message
    
    except ImportError as exc:
        message = f"Excel reader engine '{engine}' is unavailable{retailer_label}: {exc}"
        return None, message
    
    except Exception as exc:
        message = (
            f"Could not inspect workbook{retailer_label}: {path}; "
            f"{type(exc).__name__}: {exc}"
        )
        return None, message

    matched_sheets = []

    for expected in expected_sheets:
        if expected.startswith("re:"):
            pattern = expected[3:]

            try:
                compiled_pattern = re.compile(pattern, flags=re.IGNORECASE)
            except re.error as exc:
                raise ValueError(
                    f"Invalid expected sheet pattern {expected!r}: {exc}"
                ) from exc

            for sheet_name in available_sheets:
                if compiled_pattern.fullmatch(sheet_name):
                    matched_sheets.append(sheet_name)

        elif expected in available_sheets:
            matched_sheets.append(expected)

    if len(matched_sheets) == 1:
        sheet_name = matched_sheets[0]
        logger.info(
            "Found expected sheet '%s'%s in %s",
            sheet_name,
            retailer_label,
            path.name,
        )
        return sheet_name, None

    if len(matched_sheets) > 1:
        message = (
            f"Multiple expected sheet matches found{retailer_label} "
            f"in {path.name}: {matched_sheets}"
        )
        return None, message

    message = (
        f"No expected sheet found{retailer_label} in {path.name}. "
        f"Expected names/patterns: {expected_sheets}; "
        f"available sheets: {available_sheets}"
    )
    return None, message
