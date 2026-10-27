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
    config = yaml.safe_load(open(f"{config_path}/othaim.yaml"))
    source_dir = Path(settings["source"]["othaim"]) / str(year)
    silver_dir = Path(settings["output"]["silver"]) / "Othaim"
    db_path = settings["duckdb"]["path"]

    # 1. Find file
    file_path = find_file(str(source_dir), month_label, extensions=(".xlsb",), recursive=False)
    if file_path is None:
        print(f"[SKIP] No file found for Othaim {year} - {month_label}")
        log_pipeline_run(db_path, "Othaim", year, month_label, "skipped",
                         error_msg=f"No file found for {year} - {month_label}")
        return

    print(f"[READ] {file_path}")

    # 2. Read raw
    raw = pd.read_excel(file_path, header=None, engine="calamine")

    # 3. Detect format (old: "Values" noise at row 0, new: header at row 0)
    row0_values = raw.iloc[0].dropna().astype(str).tolist()
    fmt = "new" if "StoreFormat" in row0_values else "old"

    # 4. Set header and trim header rows
    if fmt == "old":
        raw.columns = raw.iloc[1]
        df = raw.iloc[2:].reset_index(drop=True)
    else:
        raw.columns = raw.iloc[0]
        df = raw.iloc[1:].reset_index(drop=True)

    # 5. Validate essential columns
    ok, err = validate_columns(df, config["essential_columns"], file_path)
    if not ok:
        print(f"[FAIL] {err}")
        log_pipeline_run(db_path, "Othaim", year, month_label, "failed", error_msg=err)
        return

    # 6. Remove Grand Total footer row
    first_col = df.columns[0]
    df = df[~df[first_col].astype(str).str.contains("Grand Total", case=False, na=False)]

    # 7. Strip column name whitespace
    df.columns = df.columns.str.strip()

    # 8. Drop rows with missing SKU
    df = df.dropna(subset=["ItemID"])

    # 9. Extract grammage 
    df = extract_grammage(df, source_column="ItemEnglishDescriptionWithNumber", retailer="Othaim")

    # 10. Build Key column
    df["Key"] = df.apply(
        lambda row: "Othaim" + str(row["ItemID"]) + ("-" + str(row["SizeDesc"]) if row["SizeDesc"] else ""),
        axis=1
    )

    # 11. Rename columns
    df = df.rename(columns=config["rename_map"])

    # 12. Add Retailer column
    df["Retailer"] = "Othaim"

    # 13. Add month column
    df["Month"] = f"{month_label}-{str(year)[-2:]}"

    # 14. Select output columns
    df = df[["Retailer","Key","Month","StoreType","Store Num","STORE_NAME_ARABIC","D2_DEPARTMENT_NAME","D3_SUB_DEPARTMENT_NAME","D4_CLASS_NAME","D5_SUB_CLASS_NAME","SKU","SKU Description","VENDOR_NAME_WITH_NUMBER","Sales Quantity","Sales Amount","SizeDesc"]]

    # 15. Write Parquet
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_path = silver_dir / str(year) / f"{month_label}-{year}.parquet"
    write_parquet(df, str(output_path))

    print(f"[DONE] {len(df)} rows → {output_path}")

    # 16. Log success
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
        settings = yaml.safe_load(open(args.settings))
        source_dir = settings["source"]["othaim"]
        files = discover_files(source_dir, extensions=(".xlsb",))
        print(f"Full refresh: found {len(files)} file(s) across all years")
        for file_path, year, month in files:
            print(f"  {year} - {month}")
            process(year, month, args.config, args.settings)
    elif args.year and not args.month:
        settings = yaml.safe_load(open(args.settings))
        source_dir = Path(settings["source"]["othaim"]) / str(args.year)
        files = [(str(f), args.year, extract_year_month(str(f))[1])
                 for f in source_dir.glob("*.xlsb") if not f.name.startswith("~$")]
        print(f"Processing Othaim {args.year}: found {len(files)} file(s)")
        for file_path, year, month in files:
            print(f"  {month}")
            process(year, month, args.config, args.settings)
    elif args.year and args.month:
        print(f"Processing Othaim {args.year} - {args.month}")
        process(args.year, args.month, args.config, args.settings)
    else:
        parser.error("Use --all, --year only, or both --year and --month")