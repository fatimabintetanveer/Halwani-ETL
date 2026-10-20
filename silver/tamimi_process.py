import sys
from pathlib import Path
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.io import find_file, write_parquet, log_pipeline_run, discover_files, extract_year_month
from utils.schema_validator import validate_columns
from utils.grammage import extract_grammage


def process(year: int, month_label: str, config_path: str, settings_path: str) -> None:

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

    # 3. Validate essential columns
    ok, err = validate_columns(df, config["essential_columns"], file_path)
    if not ok:
        print(f"[FAIL] {err}")
        log_pipeline_run(db_path, "Tamimi", year, month_label, "failed", error_msg=err)
        return

    # 4. Strip column name whitespace
    df.columns = df.columns.str.strip()

    # 5. Drop rows with missing SKU
    df = df.dropna(subset=["Product ID"])

    # 6. Strip whitespace from text columns
    for col in ["Store", "Product", "Product ID", "H1:Category", "Brand"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # 7. Merge Units/Volume into Sales Qty (KG items use Volume, others use Units)
    df["Sales Qty"] = df.apply(
        lambda row: row["Volume"]
        if str(row["Selling_Unit"]).strip().upper() == "KG"
        else row["Units"],
        axis=1,
    )

    # 8. Clean store names: strip trailing "[S101]" code and commas
    df["Store"] = df["Store"].str.replace(r"\s\[\w+\]$", "", regex=True)
    df["Store"] = df["Store"].str.rstrip(",").str.strip()

    # 9. Clean product names: strip trailing "KG /1 [119231_KG]" SKU code
    df["Product"] = df["Product"].str.replace(
        r"\s[A-Z]{2,3}\s/[\d]+\s\[\d+_[A-Z]{2,3}\]$", "", regex=True
    )

    # 10. Clean product IDs: "119231_KG" → "119231"
    df["Product ID"] = df["Product ID"].str.split("_").str[0]

    # 11. Split composite columns: "5125 - COLD MEAT" → code + name
    if "H1:Category" in df.columns:
        split = df["H1:Category"].str.split(" - ", n=1, expand=True)
        df["CATEGORY_CODE"] = split[0].str.strip()
        df["CATEGORY_NAME"] = split[1].str.strip()
        df = df.drop(columns=["H1:Category"])

    if "Brand" in df.columns:
        split = df["Brand"].str.split(" - ", n=1, expand=True)
        df["BRAND_CODE"] = split[0].str.strip()
        df["BRAND_NAME"] = split[1].str.strip()

    # 12. Extract grammage (before rename, column is still "Product")
    df = extract_grammage(df, source_column="Product", retailer="Tamimi")

    # 13. Rename columns
    df = df.rename(columns=config["rename_map"])

    # 14. Add month column
    df["Month"] = f"{month_label}-{str(year)[-2:]}"

    # 15. Select output columns
    df = df[['Month','Store Description','CATEGORY_CODE','CATEGORY_NAME','SKU','SKU Description','BRAND_CODE','BRAND_NAME','Sales_Quantity','Sales Unit','Sales_Amount','SizeDesc']]

    # 16. Write Parquet
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_path = silver_dir / str(year) / f"{month_label}-{year}.parquet"
    write_parquet(df, str(output_path))

    print(f"[DONE] {len(df)} rows → {output_path}")

    # 17. Log success
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
        settings = yaml.safe_load(open(args.settings))
        source_dir = Path(settings["source"]["tamimi"]) / str(args.year)
        files = [(str(f), args.year, extract_year_month(str(f))[1])
                 for f in source_dir.glob("*.xlsx") if not f.name.startswith("~$")]
        print(f"Processing Tamimi {args.year}: found {len(files)} file(s)")
        for file_path, year, month in files:
            print(f"  {month}")
            process(year, month, args.config, args.settings)
    elif args.year and args.month:
        print(f"Processing Tamimi {args.year} - {args.month}")
        process(args.year, args.month, args.config, args.settings)
    else:
        parser.error("Use --all, --year only, or both --year and --month")