import os
import time
import duckdb
import polars as pl
import pandas as pd
from pathlib import Path


def find_file(
    source_dir: str,
    month_label: str,
    extensions: tuple = (".xlsb", ".xlsx", ".csv"),
    recursive: bool = True,
) -> str | None:
    """
    Search source_dir for a file whose name contains month_label.
    Works for any retailer -- the caller specifies extensions and recursion.

    Parameters:
        source_dir:  root folder to search
        month_label: month name to match, e.g. "May", "July", "Mar"
        extensions:  file extensions to consider
        recursive:   if True, search subfolders (for Othaim's year/ layout);
                     if False, only look in source_dir directly (for Tamimi, Panda)

    Returns the full file path, or None if not found.
    """
    source = Path(source_dir)
    if not source.exists():
        return None

    # Pick the right walker based on recursive flag
    walker = source.rglob("*") if recursive else source.iterdir()

    for file_path in walker:
        if not file_path.is_file():
            continue
        if file_path.suffix not in extensions:
            continue
        if file_path.name.startswith("~$"):
            continue

        if month_label.lower() in file_path.stem.lower():
            return str(file_path)

    return None


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
        df.to_parquet(str(out))
    elif isinstance(df, pl.DataFrame):
        df.write_parquet(str(out))

    return str(out)


def log_pipeline_run(
    db_path: str,
    retailer: str,
    year: int,
    month: str,
    status: str,
    rows: int = 0,
    file_path: str = "",
    error_msg: str = None,
) -> None:
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
            status        VARCHAR NOT NULL,
            rows_processed INTEGER,
            file_path     VARCHAR,
            error_message VARCHAR,
            started_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert the run record
    con.execute("""
        INSERT INTO pipeline_runs
            (retailer, year, month, status, rows_processed, file_path, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [retailer, year, month, status, rows, file_path, error_msg])

    con.close()