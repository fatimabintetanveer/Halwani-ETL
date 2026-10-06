import sys
import re
from pathlib import Path
import pandas as pd
import yaml

# Add project root to path so utils imports work
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.io import find_file, write_parquet, log_pipeline_run, discover_files
from utils.schema_validator import validate_columns
from utils.grammage import extract_grammage


# ---------------------------------------------------------------------------
# Tamimi-specific cleaning functions
# ---------------------------------------------------------------------------

def _strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Remove leading/trailing whitespace from key text columns."""
    cols = ["Store", "Product", "Product ID", "H1:Category", "Brand"]
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df


def _merge_sales_qty(df: pd.DataFrame) -> pd.DataFrame:
    """
    KG items use Volume; unit items (EA, PAK, CS, ZM) use Units.
    Creates a single 'Sales Qty' column.
    """
    df["Sales Qty"] = df.apply(
        lambda row: row["Volume"]
        if str(row["Selling_Unit"]).strip().upper() == "KG"
        else row["Units"],
        axis=1,
    )
    return df


def _clean_store(df: pd.DataFrame) -> pd.DataFrame:
    """
    "S101 - CORNICHE AL-KHOBAR [S101]"  →  "S101 - CORNICHE AL-KHOBAR"
    Also removes any trailing comma (some store names have stray commas).
    """
    df["Store"] = df["Store"].str.replace(r"\s\[\w+\]$", "", regex=True)
    df["Store"] = df["Store"].str.rstrip(",").str.strip()
    return df


def _clean_product(df: pd.DataFrame) -> pd.DataFrame:
    """
    "HALWANI SMOKED TURKEY BREAST 1kgs KG /1 [119231_KG]"
     → "HALWANI SMOKED TURKEY BREAST 1kgs"
    """
    df["Product"] = df["Product"].str.replace(
        r"\s[A-Z]{2,3}\s/[\d]+\s\[\d+_[A-Z]{2,3}\]$", "", regex=True
    )
    return df


def _clean_product_id(df: pd.DataFrame) -> pd.DataFrame:
    """ "119231_KG" → "119231" """
    df["Product ID"] = df["Product ID"].str.split("_").str[0]
    return df


def _split_composite_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    "5125 - COLD MEAT"     →  CATEGORY_CODE="5125",  CATEGORY_NAME="COLD MEAT"
    "04308 - HALWANI BROS" →  BRAND_CODE="04308",    BRAND_NAME="HALWANI BROS"
    """
    if "H1:Category" in df.columns:
        split = df["H1:Category"].str.split(" - ", n=1, expand=True)
        df["CATEGORY_CODE"] = split[0].str.strip()
        df["CATEGORY_NAME"] = split[1].str.strip()
        df = df.drop(columns=["H1:Category"])

    if "Brand" in df.columns:
        split = df["Brand"].str.split(" - ", n=1, expand=True)
        df["BRAND_CODE"] = split[0].str.strip()
        df["BRAND_NAME"] = split[1].str.strip()

    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process(year: int, month_label: str, config_path: str, settings_path: str) -> None:
    """Process one Tamimi file for a given year and month."""
    settings = yaml.safe_load(open(settings_path))
    config = yaml.safe_load(open(f"{config_path}/tamimi.yaml"))
    source_dir = Path(settings["source"]["tamimi"]) / str(year)
    silver_dir = Path(settings["output"]["silver"]) / "Tamimi"
    db_path = settings["duckdb"]["path"]

    # 1. Find file
    file_path = find_file(str(source_dir), month_label, extensions=(".xlsx",), recursive=False)
    if file_path is None:
        print(f"[SKIP] No file found for Tamimi {year} - {month_label}")
        log_pipeline_run(db_path, "Tamimi", year, month_label, "skipped",
                         error_msg=f"No file found for {year} - {month_label}")
        return

    print(f"[READ] {file_path}")

    # 2. Read raw
    df = pd.read_excel(file_path)

    # 3. Validate
    ok, err = validate_columns(df, config["essential_columns"], file_path)
    if not ok:
        print(f"[FAIL] {err}")
        log_pipeline_run(db_path, "Tamimi", year, month_label, "failed", error_msg=err)
        return

    # 4. Strip whitespace
    df = _strip_whitespace(df)

    # 5. Merge Units/Volume into Sales Qty
    df = _merge_sales_qty(df)

    # 6. Clean store names
    df = _clean_store(df)

    # 7. Clean product names
    df = _clean_product(df)

    # 8. Clean product IDs
    df = _clean_product_id(df)

    # 9. Split composite columns
    df = _split_composite_columns(df)

    # 10. Rename columns
    df = df.rename(columns=config["rename_map"])

    # 11. Extract grammage
    df = extract_grammage(df, source_column="SKU_DESCRIPTION", retailer="Tamimi")

    # 12. Select output columns
    df = df[['STORE_DESCRIPTION','CATEGORY_CODE','CATEGORY_NAME','SKU','SKU_DESCRIPTION','BRAND_CODE','BRAND_NAME','Sales_Quantity','SALES_UNIT','Sales_Amount','SizeDesc']]


    # 13. Write Parquet
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_path = silver_dir / f"{month_label}_{year}.parquet"
    write_parquet(df, str(output_path))

    print(f"[DONE] {len(df)} rows → {output_path}")

    # 14. Log success
    log_pipeline_run(db_path, "Tamimi", year, month_label, "success",
                     rows=len(df), file_path=file_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Tamimi Silver layer ETL")
    parser.add_argument("--year", type=int, help="Year to process, e.g. 2026")
    parser.add_argument("--month", type=str, help="Month label, e.g. Jan, Feb, Mar")
    parser.add_argument("--all", action="store_true", help="Process all files in all year folders")
    parser.add_argument("--config", default="config/schemas")
    parser.add_argument("--settings", default="config/settings.yaml")
    args = parser.parse_args()

    if args.all:
        settings = yaml.safe_load(open(args.settings))
        source_dir = settings["source"]["tamimi"]
        files = discover_files(source_dir, extensions=(".xlsx",))
        print(f"Full refresh: found {len(files)} file(s) across all years")
        for file_path, year, month in files:
            print(f"  {year} - {month}")
            process(year, month, args.config, args.settings)
    elif args.year and not args.month:
        # All months in a specific year
        settings = yaml.safe_load(open(args.settings))
        source_dir = Path(settings["source"]["tamimi"]) / str(args.year)
        files = discover_files(str(source_dir), extensions=(".xlsx",))
        print(f"Processing Tamimi {args.year}: found {len(files)} file(s)")
        for file_path, year, month in files:
            print(f"  {month}")
            process(year, month, args.config, args.settings)
    elif args.year and args.month:
        print(f"Processing Tamimi {args.year} - {args.month}")
        process(args.year, args.month, args.config, args.settings)
    else:
        parser.error("Use --all, --year only, or both --year and --month")