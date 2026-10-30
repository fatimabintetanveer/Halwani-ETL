import duckdb
import polars as pl
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


from utils.validation.source_files_validation import (
    check_source_directory,
    find_duplicate_periods,
    parse_source_period,
    validate_file_extensions,
)

def find_file(
    source_dir: str,
    month_label: str,
    year: int,
    extensions: tuple = (".xlsb", ".xlsx", ".csv"),
    retailer: str = "",
) -> tuple[str | None, str | None]:
    """
    Search source_dir for a file whose name contains month_label.
    Works for any retailer -- the caller specifies extensions.
    Parameters:
        source_dir:  root folder to search
        month_label: month name to match, e.g. "May", "July", "Mar"
        year:          year to match
        extensions:  file extensions to consider
        retailer:    retailer name for logging and validation purposes
        
    Returns the full file path, or None if not found.
    """
    source = Path(source_dir)

    # 1. Stop if the configured location is missing or isn't a directory.
    if not check_source_directory(source, retailer):
        return None, f"Source directory is missing or invalid: {source}"

    # Discover files directly inside the source directory.
    all_files = sorted(
        (
            path for path in source.iterdir()
            if path.is_file() and not path.name.startswith("~$")
        ),
        key=lambda path: str(path).casefold(),
    )

    # Report files whose extensions do not match this retailer's expected format.
    extension_mismatches = validate_file_extensions(all_files, extensions, retailer)

    if extension_mismatches:
        message = (
            f"[WARNING] Found unexpected file extension(s) for {retailer or 'retailer'}; "
            f"expected {', '.join(extensions)}. Files: "
            f"{', '.join(path.name for path in extension_mismatches)}"
        )
        logger.warning(message)

    # Parse the requested month and year, e.g. "Apr 2026" -> (2026, 4).
    requested_period = parse_source_period(Path(f"{month_label} {year}"))
    if requested_period is None:
        message = f"[ERROR] Unrecognized year/month: {month_label} {year}"
        logger.error("%s for %s", message, retailer)
        return None, message

    duplicate_periods = find_duplicate_periods(all_files, retailer)
    requested_period_key = (
        f"{requested_period[0]}-{requested_period[1]:02d}"
    )

    if requested_period_key in duplicate_periods:
        paths = duplicate_periods[requested_period_key]
        message = (
            f"[ERROR] Duplicate source files detected for {retailer} {requested_period_key}: "
            f"{', '.join(path.name for path in paths)}"
        )
        logger.error("%s; manual review required", message)
        return None, message

    allowed_extensions = {
        ext.strip().lower()
        if ext.strip().startswith(".")
        else f".{ext.strip().lower()}"
        for ext in extensions
    }

    matching_files = []
    for file_path in all_files:
        if file_path.suffix.lower() not in allowed_extensions:
            continue

        if parse_source_period(file_path) == requested_period:
            matching_files.append(file_path)


    if not matching_files:
        message = f" No eligible {retailer} file found for {requested_period_key}"
        logger.warning("%s", message)
        return None, message

    selected_file = matching_files[0]
    return str(selected_file), None



def extract_year_month(file_path: str) -> tuple[int, str]:
    """
    Extract year and month from the last segment of a filename.
    Works for all retailers -- the month+year always appears after
    the last dash or underscore.

    "Othaim Data - May 2026.xlsb"       → (2026, "May")
    "Halwani Data_July 2026.xlsx"       → (2026, "July")
    "Halawani Brothers - Mar 2026.csv"  → (2026, "Mar")
    "Othaim Data - July_26.xlsb"        → (2026, "July")
    "Othaim Data - Aug'26.xlsb"         → (2026, "Aug")
    """
    stem = Path(file_path).stem
    # Normalize all delimiters to spaces, then take the last two words
    last_part = stem.replace("_", " ").replace("'", " ").rsplit(" ", 2)[-2:]
    # last_part is now e.g. ["May", "2026"] or ["July", "26"]
    if len(last_part) < 2:
        return pd.Timestamp.now().year, last_part[0] if last_part else "Unknown"
    month = last_part[0]
    year_str = last_part[1] if last_part[1].isdigit() else str(pd.Timestamp.now().year)
    year = int(year_str) if len(year_str) == 4 else 2000 + int(year_str)
    return year, month


def discover_files(
    source_dir: str,
    extensions: tuple = (".xlsb",),
) -> list[tuple[str, int, str]]:
    """
    Walk all year subfolders and find every file matching the given extensions.
    Returns list of (file_path, year, month) for each file found.
    Used by --all mode for full refresh across all retailers.

    Parameters:
        source_dir:  base directory (contains year subfolders like 2026/)
        extensions:  file extensions to include
    """
    files = []
    base = Path(source_dir)
    if not base.exists():
        return files

    for year_dir in base.iterdir():
        if not year_dir.is_dir():
            continue
        for file_path in year_dir.iterdir():
            if file_path.suffix not in extensions:
                continue
            if file_path.name.startswith("~$"):
                continue
            year, month = extract_year_month(str(file_path))
            files.append((str(file_path), year, month))
    return files


def write_parquet(df: pl.DataFrame | pd.DataFrame, output_path: str) -> str:
    """
    Write a pandas or Polars DataFrame to a Parquet file.
    Creates parent directories if they don't exist.
    Overwrites if file already exists → idempotent.

    Returns the output path.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(df, pd.DataFrame):
        df.to_parquet(str(out), index=False)
    elif isinstance(df, pl.DataFrame):
        df.write_parquet(str(out))

    return str(out)


def log_pipeline_run(
    db_path: str,
    retailer: str,
    year: int,
    month: str,
    stage: str,
    status: str,
    rows: int = 0,
    file_path: str = "",
    error_msg: str = None,
) -> int:
    """
    Insert a row into the pipeline_runs table in DuckDB.
    Creates the table if it doesn't exist yet.

    status: 'success' | 'failed' | 'skipped'
    """
    con = duckdb.connect(db_path)

# Create table and sequence once (IF NOT EXISTS makes this safe every call)
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS pipeline_run_seq START 1
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id        INTEGER PRIMARY KEY DEFAULT nextval('pipeline_run_seq'),
            retailer      VARCHAR NOT NULL,
            year          INTEGER NOT NULL,
            month         VARCHAR NOT NULL,
            stage         VARCHAR NOT NULL,
            status        VARCHAR NOT NULL,
            rows_processed INTEGER,
            file_path     VARCHAR,
            error_message VARCHAR,
            started_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert the run record and return its generated ID.
    result = con.execute("""
        INSERT INTO pipeline_runs
            (retailer, year, month, stage, status, rows_processed, file_path, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?,?)
        RETURNING run_id
    """, [retailer, year, month, stage, status, rows, file_path, error_msg]).fetchone()

    con.close()
    return result[0]


def update_pipeline_run(
    db_path: str,
    run_id: int,
    status: str,
    error_msg: str | None = None,
) -> None:
    """Update a pipeline_runs row when a stage finishes.

    Sets ``completed_at`` to the current time and updates the status. If an
    error message is provided, it replaces the row's existing error message.
    """
    allowed_statuses = {"running", "success", "failed"}
    if status not in allowed_statuses:
        raise ValueError(
            f"Unsupported pipeline status {status!r}; "
            f"expected one of {sorted(allowed_statuses)}"
        )

    con = duckdb.connect(db_path)
    try:
        result = con.execute(
            """
            UPDATE pipeline_runs
            SET status = ?,
                error_message = COALESCE(?, error_message),
                completed_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            RETURNING run_id
            """,
            [status, error_msg, run_id],
        ).fetchone()

        if result is None:
            raise ValueError(f"No pipeline_runs row found for run_id={run_id}")
    finally:
        con.close()
