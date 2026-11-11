import sys
import logging
from pathlib import Path
import pandas as pd
import yaml





sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.io import find_file, update_pipeline_run, write_parquet, log_pipeline_run, discover_files, extract_year_month
from utils.validation.schema_validation import validate_schema
from utils.validation.data_validation import (
    calculate_filter_impact,
    validate_numeric_columns,
    validate_parquet_output,
)
from utils.validation.source_files_validation import check_expected_sheet, format_read_error
from utils.grammage import extract_grammage
from utils.validation.layout_validation import check_layout

logger = logging.getLogger(__name__)


def process(year: int, month_label: str, config_path: str, settings_path: str) -> None:

    settings = yaml.safe_load(open(settings_path))
    source_dir = Path(settings["source"]["tamimi"]) / str(year)
    silver_dir = Path(settings["output"]["silver"]) / "Tamimi"
    db_path = settings["duckdb"]["path"]

    # 1. Find file
    discover_run_id = log_pipeline_run(
        db_path, "Tamimi", year, month_label,
        stage="discover_file", status="running",
    )
    try:
        file_path, error_message = find_file(
            str(source_dir), month_label, year,
            extensions=(".xlsx",), retailer="Tamimi",
        )
    except Exception as exc:
        error_message = (
            f"Tamimi file discovery failed for {year} - {month_label}: "
            f"{type(exc).__name__}: {exc}"
        )
        update_pipeline_run(db_path, discover_run_id, "failed", error_msg=error_message)
        return

    if file_path is None:
        update_pipeline_run(db_path, discover_run_id, "failed", error_msg=error_message)
        return

    source_file_name = Path(file_path).name
    update_pipeline_run(
        db_path, discover_run_id, "success", source_file_name=source_file_name,
    )

    sheet_name, error = check_expected_sheet(file_path,expected_sheets=("Sheet1", r"re:DayStoreProduct_\d+"),retailer="Tamimi")
    if sheet_name is None:
        print(f"[FAIL] {error}")
        return

    layout, layout_error = check_layout(file_path,"Tamimi",sheet_name=sheet_name)
    if not layout:
        print(f"[FAIL] {layout_error}")
        return


    print(f"[READ] {file_path}")

    # 2. Read raw
    read_run_id = log_pipeline_run(
        db_path, "Tamimi", year, month_label,
        stage="source_read", status="running", source_file_name=source_file_name,
    )
    try:
        df = pd.read_excel(file_path)
    except Exception as exc:
        error = format_read_error(exc, retailer="Tamimi", file_path=file_path)
        logger.exception(error)
        update_pipeline_run(db_path, read_run_id, "failed", error_msg=error)
        return
    update_pipeline_run(db_path, read_run_id, "success")

    transform_run_id = log_pipeline_run(
        db_path, "Tamimi", year, month_label,
        stage="silver_transform", status="running", source_file_name=source_file_name,
    )

    # 3. Validate essential columns
    ok, err = validate_schema(df, retailer="Tamimi", file_path=file_path)
    if not ok:
        print(f"[FAIL] {err}")
        update_pipeline_run(db_path, transform_run_id, "failed", error_msg=err)
        return

    # 4. Strip column name whitespace
    df.columns = df.columns.str.strip()

    # 5. Validate numeric source fields before filters and quantity calculations
    numeric_ok, numeric_metrics, numeric_error = validate_numeric_columns(
        df,
        ("Sales Exc Tax", "Units", "Volume"),
        retailer="Tamimi",
        file_path=file_path,
    )
    print(f"[NUMERIC VALIDATION] {numeric_metrics}")
    if not numeric_ok:
        print(f"[FAIL] {numeric_error}")
        update_pipeline_run(
            db_path, transform_run_id, "failed", error_msg=numeric_error
        )
        return

    # 6. Calculate quantity before measuring rows removed for missing SKU
    df["Sales Qty"] = df.apply(
        lambda row: row["Volume"]
        if str(row["Selling_Unit"]).strip().upper() == "KG"
        else row["Units"],
        axis=1,
    )

    # 7. Drop rows with missing SKU and measure the removed totals
    before_missing_sku_filter = df
    df = df.dropna(subset=["Product ID"])
    missing_sku_impact = calculate_filter_impact(
        before_missing_sku_filter,
        df,
        amount_column="Sales Exc Tax",
        quantity_column="Sales Qty",
        retailer="Tamimi",
        filter_name="missing_product_id",
    )
    print(f"[FILTER IMPACT] {missing_sku_impact}")

    # 8. Strip whitespace from text columns
    for col in ["Store", "Product", "Product ID", "H1:Category", "Brand"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # 9. Clean store names: strip trailing "[S101]" code and commas
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
    df = df.rename(columns={
        "Store": "Store Description",
        "Product ID": "SKU",
        "Product": "SKU Description",
        "Selling_Unit": "Sales Unit",
        "Sales Exc Tax": "Sales Amount",
        "Sales Qty": "Sales Quantity",
    })

    # 14. Build Key column (SKU + Sales Unit + SizeDesc)
    df["Key"] = df.apply(
        lambda row: "Tamimi" + str(row["SKU"]) + str(row["Sales Unit"]) + ("-" + str(row["SizeDesc"]) if row["SizeDesc"] else ""),
        axis=1
    )

    # 15. Add Retailer column
    df["Retailer"] = "Tamimi"

    # 16. Add month column
    df["Month"] = f"{month_label}-{str(year)[-2:]}"

    # 17. Select output columns
    df = df[['Retailer','Key','Month','Store Description','CATEGORY_CODE','CATEGORY_NAME','SKU','SKU Description','BRAND_CODE','BRAND_NAME','Sales Quantity','Sales Unit','Sales Amount','SizeDesc']]

    # 18. Write Parquet
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_path = silver_dir / str(year) / f"{month_label}-{year}.parquet"
    write_parquet(df, str(output_path))

    output_ok, output_metrics, output_error = validate_parquet_output(
        df, output_path, retailer="Tamimi"
    )
    print(f"[SILVER OUTPUT VALIDATION] {output_metrics}")
    if not output_ok:
        print(f"[FAIL] {output_error}")
        update_pipeline_run(
            db_path, transform_run_id, "failed", error_msg=output_error
        )
        return

    print(f"[DONE] {len(df)} rows → {output_path}")

    # 19. Log success
    update_pipeline_run(db_path, transform_run_id, "success")


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
