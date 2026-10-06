import sys
from pathlib import Path
import pandas as pd
import yaml

# Add project root to path so utils imports work
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.io import find_file, write_parquet, log_pipeline_run, discover_files
from utils.schema_validator import validate_columns
from utils.grammage import extract_grammage


def _detect_format(raw_df: pd.DataFrame) -> str:
    """
    Check whether the header is at row 0 (new format) or row 1 (old format).

    new: row 0 contains real column names like "StoreFormat", "StoreNumber"
    old: row 0 is "Values" noise, row 1 has column names
    """
    row0_values = raw_df.iloc[0].dropna().astype(str).tolist()
    if "StoreFormat" in row0_values:
        return "new"
    return "old"


def _set_header(raw_df: pd.DataFrame, fmt: str) -> pd.DataFrame:
    """
    Assign column names from the correct header row and trim header rows
    from the data body.

    old: header = row 1, data starts at row 2 (row 0 is "Values" noise)
    new: header = row 0, data starts at row 1
    """
    if fmt == "old":
        raw_df.columns = raw_df.iloc[1]       # row 1 becomes column names
        df = raw_df.iloc[2:].reset_index(drop=True)  # data from row 2 onward
    else:
        raw_df.columns = raw_df.iloc[0]       # row 0 becomes column names
        df = raw_df.iloc[1:].reset_index(drop=True)  # data from row 1 onward
    return df


def _remove_grand_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop the footer row where the first column contains "Grand Total".
    Uses the first column because that's where it appears in both formats
    (R_WEEK_NO_IN_YEAR in May/June, StoreFormat in July).
    """
    first_col = df.columns[0]
    mask = df[first_col].astype(str).str.contains("Grand Total", case=False, na=False)
    return df[~mask]


def process(year: int, month_label: str, config_path: str, settings_path: str) -> None:

    # ----- Load config -----
    settings = yaml.safe_load(open(settings_path))
    config = yaml.safe_load(open(f"{config_path}/othaim.yaml"))
    source_dir = Path(settings["source"]["othaim"]) / str(year)
    silver_dir = Path(settings["output"]["silver"]) / "Othaim"
    db_path = settings["duckdb"]["path"]

    # ----- 1. Find file (we are already inside the year folder, so no recursion needed) -----
    file_path = find_file(str(source_dir), month_label, extensions=(".xlsb",), recursive=False)
    if file_path is None:
        print(f"[SKIP] No file found for Othaim {year} - {month_label}")
        log_pipeline_run(db_path, "Othaim", year, month_label, "skipped",
                         error_msg=f"No file found for {year} - {month_label}")
        return

    print(f"[READ] {file_path}")

    # ----- 2. Read raw -----
    raw = pd.read_excel(file_path, header=None, engine="calamine")

    # ----- 3. Detect format + set header -----
    fmt = _detect_format(raw)
    df = _set_header(raw, fmt)

    # ----- 4. Validate columns -----
    ok, err = validate_columns(df, config["essential_columns"], file_path)
    if not ok:
        print(f"[FAIL] {err}")
        log_pipeline_run(db_path, "Othaim", year, month_label, "failed", error_msg=err)
        return
    
    # ----- 5. Remove Grand Total -----
    df = _remove_grand_total(df)

    # ----- 6. Rename columns -----
    df = df.rename(columns=config["rename_map"])

    # ----- 7. Extract grammage -----
    df = extract_grammage(df, source_column="SKU_DESCRIPTION", retailer="Othaim")

    # ----- 8. Select output columns -----
    df = df[["STORE_FORMAT_ENGLISH","STORE_NAME","STORE_NUMBER","D2_DEPARTMENT_NAME","D3_SUB_DEPARTMENT_NAME","D4_CLASS_NAME","D5_SUB_CLASS_NAME","SKU","SKU_DESCRIPTION","VENDOR_NAME_WITH_NUMBER","Sales Quantity","Sales Amount","SizeDesc"]]

    # ----- 9. Write Parquet -----
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_path = silver_dir / f"{month_label}_{year}.parquet"
    write_parquet(df, str(output_path))

    print(f"[DONE] {len(df)} rows → {output_path}")

    # ----- 10. Log success -----
    log_pipeline_run(db_path, "Othaim", year, month_label, "success",
                     rows=len(df), file_path=file_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Othaim Silver layer ETL")
    parser.add_argument("--year", type=int, help="Year to process, e.g. 2026")
    parser.add_argument("--month", type=str, help="Month label, e.g. May, June, July")
    parser.add_argument("--all", action="store_true", help="Process all files in all year folders")
    parser.add_argument("--config", default="config/schemas")
    parser.add_argument("--settings", default="config/settings.yaml")
    args = parser.parse_args()

    if args.all:
        # Full refresh: discover and process every file
        settings = yaml.safe_load(open(args.settings))
        source_dir = settings["source"]["othaim"]
        files = discover_files(source_dir, extensions=(".xlsb",))
        print(f"Full refresh: found {len(files)} file(s) across all years")
        for file_path, year, month in files:
            print(f"  {year} - {month}")
            process(year, month, args.config, args.settings)
    elif args.year and not args.month:
        # All months in a specific year
        settings = yaml.safe_load(open(args.settings))
        source_dir = Path(settings["source"]["othaim"]) / str(args.year)
        files = discover_files(str(source_dir), extensions=(".xlsb",))
        print(f"Processing Othaim {args.year}: found {len(files)} file(s)")
        for file_path, year, month in files:
            print(f"  {month}")
            process(year, month, args.config, args.settings)
    elif args.year and args.month:
        print(f"Processing Othaim {args.year} - {args.month}")
        process(args.year, args.month, args.config, args.settings)
    else:
        parser.error("Use --all, --year only, or both --year and --month")