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
from utils.grammage import extract_grammage
from utils.validation.source_files_validation import check_expected_sheet, format_read_error
from utils.validation.layout_validation import check_layout

logger = logging.getLogger(__name__)

def process(year: int, month_label: str, config_path: str, settings_path: str) -> None:

    settings = yaml.safe_load(open(settings_path))
    source_dir = Path(settings["source"]["othaim"]) / str(year)
    silver_dir = Path(settings["output"]["silver"]) / "Othaim"
    db_path = settings["duckdb"]["path"]

    # 1. Find file
    discover_run_id = log_pipeline_run(
        db_path, "Othaim", year, month_label,
        stage="discover_file", status="running",
    )
    try:
        file_path, error_message = find_file(
            str(source_dir), month_label, year,
            extensions=(".xlsb",), retailer="Othaim",
        )
    except Exception as exc:
        error_message = (
            f"Othaim file discovery failed for {year} - {month_label}: "
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

    sheet_name, error = check_expected_sheet(file_path, expected_sheets=("Sheet1",), retailer="Othaim")
    if sheet_name is None:
        print(f"[FAIL] {error}")
        return
    
    layout, layout_error = check_layout(file_path,"Othaim",sheet_name=sheet_name)
    if not layout:
        print(f"[FAIL] {layout_error}")
        return


    print(f"[READ] {file_path}")

    # 2. Read raw
    read_run_id = log_pipeline_run(
        db_path, "Othaim", year, month_label,
        stage="source_read", status="running", source_file_name=source_file_name,
    )
    try:
        raw = pd.read_excel(file_path, header=None, engine="calamine")
    except Exception as exc:
        error = format_read_error(exc, retailer="Othaim", file_path=file_path)
        logger.exception(error)
        update_pipeline_run(db_path, read_run_id, "failed", error_msg=error)
        return
    update_pipeline_run(db_path, read_run_id, "success")

    transform_run_id = log_pipeline_run(
        db_path, "Othaim", year, month_label,
        stage="silver_transform", status="running", source_file_name=source_file_name,
    )

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
    ok, err = validate_schema(df, retailer="Othaim", file_path=file_path)
    if not ok:
        print(f"[FAIL] {err}")
        update_pipeline_run(db_path, transform_run_id, "failed", error_msg=err)
        return

    numeric_ok, numeric_metrics, numeric_error = validate_numeric_columns(
        df,
        ("Sum of Total_NetSale", "Sum of Total_Quantity"),
        retailer="Othaim",
        file_path=file_path,
    )
    print(f"[NUMERIC VALIDATION] {numeric_metrics}")
    if not numeric_ok:
        print(f"[FAIL] {numeric_error}")
        update_pipeline_run(
            db_path, transform_run_id, "failed", error_msg=numeric_error
        )
        return

    # 6. Remove Grand Total footer row
    first_col = df.columns[0]
    df = df[~df[first_col].astype(str).str.contains("Grand Total", case=False, na=False)]

    # 7. Strip column name whitespace
    df.columns = df.columns.str.strip()

    # 8. Drop rows with missing SKU
    before_missing_sku_filter = df
    df = df.dropna(subset=["ItemID"])
    missing_sku_impact = calculate_filter_impact(
        before_missing_sku_filter,
        df,
        amount_column="Sum of Total_NetSale",
        quantity_column="Sum of Total_Quantity",
        retailer="Othaim",
        filter_name="missing_item_id",
    )
    print(f"[FILTER IMPACT] {missing_sku_impact}")

    # 9. Extract grammage 
    df = extract_grammage(df, source_column="ItemEnglishDescriptionWithNumber", retailer="Othaim")

    # 10. Build Key column
    df["Key"] = df.apply(
        lambda row: "Othaim" + str(row["ItemID"]) + ("-" + str(row["SizeDesc"]) if row["SizeDesc"] else ""),
        axis=1
    )

    # 11. Rename columns
    df = df.rename(columns={
        "ItemID": "SKU",
        "ItemEnglishDescriptionWithNumber": "SKU Description",
        "VendorNameWithNumber": "VENDOR_NAME_WITH_NUMBER",
        "D5_SubClassName": "D5_SUB_CLASS_NAME",
        "D4_ClassName": "D4_CLASS_NAME",
        "D3_SubDepartmentName": "D3_SUB_DEPARTMENT_NAME",
        "D2_DepartmentName": "D2_DEPARTMENT_NAME",
        "StoreNumber": "Store Num",
        "StoreFormat": "StoreType",
        "StoreArabicName": "STORE_NAME_ARABIC",
        "Sum of Total_NetSale": "Sales Amount",
        "Sum of Total_Quantity": "Sales Quantity",
    })

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

    output_ok, output_metrics, output_error = validate_parquet_output(
        df, output_path, retailer="Othaim"
    )
    print(f"[SILVER OUTPUT VALIDATION] {output_metrics}")
    if not output_ok:
        print(f"[FAIL] {output_error}")
        update_pipeline_run(
            db_path, transform_run_id, "failed", error_msg=output_error
        )
        return

    print(f"[DONE] {len(df)} rows → {output_path}")

    # 16. Log success
    update_pipeline_run(db_path, transform_run_id, "success")


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
