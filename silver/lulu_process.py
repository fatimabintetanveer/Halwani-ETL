import sys
import time
import logging
from pathlib import Path
import pandas as pd
import yaml


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.io import find_file, update_pipeline_run, write_parquet, log_pipeline_run, discover_files, extract_year_month, next_pipeline_id
from utils.validation.schema_validation import validate_schema
from utils.validation.data_validation import (
    calculate_filter_impact,
    validate_identifier_columns,
    validate_numeric_columns,
    validate_parquet_output,
)
from utils.grammage import extract_grammage
from utils.validation.source_files_validation import check_expected_sheet, format_read_error
from utils.validation.layout_validation import check_layout

logger = logging.getLogger(__name__)


def process(
    year: int,
    month_label: str,
    config_path: str,
    settings_path: str,
    p_id: str | None = None,
) -> None:
    t0 = time.time()
    settings = yaml.safe_load(open(settings_path))
    source_dir = Path(settings["source"]["lulu"]) / str(year) 
    silver_dir = Path(settings["output"]["silver"]) / "Lulu"
    db_path = settings["duckdb"]["path"]
    p_id = p_id or next_pipeline_id(db_path)

    # 1. Find file
    discover_run_id = log_pipeline_run(
        db_path, "Lulu", year, month_label,
        stage="discover_file", status="running", p_id=p_id,
    )
    try:
        file_path, error_message = find_file(
            str(source_dir), month_label, year,
            extensions=(".xlsx",), retailer="Lulu",
        )
    except Exception as exc:
        error_message = (
            f"Lulu file discovery failed for {year} - {month_label}: "
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

    sheet_name, error = check_expected_sheet(file_path, expected_sheets=("Sheet1",), retailer="Lulu")
    if sheet_name is None:
        print(f"[FAIL] {error}")
        return

    layout, layout_error = check_layout(file_path,"Lulu",sheet_name=sheet_name)
    if not layout:
        print(f"[FAIL] {layout_error}")
        return

    print(f"[READ] {Path(file_path).name}")

    # 2. Read raw 
    read_run_id = log_pipeline_run(
        db_path, "Lulu", year, month_label,
        stage="source_read", status="running", p_id=p_id,
        source_file_name=source_file_name,
    )
    try:
        df = pd.read_excel(file_path, sheet_name="Sheet1", header=None, engine="calamine")
    except Exception as exc:
        error = format_read_error(exc, retailer="Lulu", file_path=file_path)
        logger.exception(error)
        update_pipeline_run(db_path, read_run_id, "failed", error_msg=error)
        return
    update_pipeline_run(db_path, read_run_id, "success")


        # Start tracking the Silver transformation stage
    transform_run_id = log_pipeline_run(
        db_path,
        "Lulu",
        year,
        month_label,
        stage="silver_transform",
        status="running",
        p_id=p_id,
        source_file_name=source_file_name,
    )


    # 3. Fix missing header labels in row 3
    df.loc[3, 1] = "Date"
    df.loc[3, 3] = "Material Group Name"
    df.loc[3, 5] = "Product Name"

    # 4. Forward-fill section labels in row 0 ("Qty Sold (B.Un)" / "Sales Value")
    df.iloc[0] = df.iloc[0].ffill()

    # 5. Merge 4 header rows into one composite header (fill NaN → "nan" string)
    r0 = df.iloc[0].fillna("nan").astype(str)
    r1 = df.iloc[1].fillna("nan").astype(str)
    r2 = df.iloc[2].fillna("nan").astype(str)
    r3 = df.iloc[3].fillna("nan").astype(str)
    df.iloc[3] = r1 + "|" + r2 + "|" + r3 + "|" + r0

    # 6. Trim top 3 header rows and bottom 3 footer rows
    df = df[3:-3]

    # 7. Set composite header as column names, drop header row from data
    df.columns = df.iloc[0]
    df = df[1:]

    # 8. Forward-fill metadata (only on first row of each group)
    for col in ["nan|nan|Calendar Year/Month|nan",
                "nan|nan|Date|nan",
                "nan|nan|Material Group|nan",
                "nan|nan|Material Group Name|nan"]:
        if col in df.columns:
            df[col] = df[col].ffill()

    # 9. Drop columns and rows containing "Result" (embedded subtotals)
    df = df.drop(columns=[c for c in df.columns if "Result" in str(c)], errors="ignore")
    df = df[df["nan|nan|Material|nan"].astype(str) != "Result"]
    df = df[df["nan|nan|Material Group|nan"].astype(str) != "Result"]

    # 10. Rename fixed columns
    df = df.rename(columns={
        "nan|nan|Calendar Year/Month|nan": "Calendar Year/Month",
        "nan|nan|Material Group|nan": "Material Group",
        "nan|nan|Material Group Name|nan": "Material Group Name",
        "nan|nan|Material|nan": "Material",
        "nan|nan|Product Name|nan": "Product Name",
        "nan|nan|EAN/UPC|nan": "EAN/UPC",
        "Plant|nan|Brand|nan": "Brand",
    })

    # 11. Unpivot store columns into long format
    id_vars = ["Calendar Year/Month", "Material Group", "Material Group Name",
               "Material", "Product Name", "EAN/UPC", "Brand"]
    unpivoted = pd.melt(df, id_vars=id_vars, var_name="Attribute", value_name="Value_Qty")

    # 12. Split composite Attribute → Store Num, Store Name, section
    split = unpivoted["Attribute"].str.split("|", expand=True)
    unpivoted["Store Num"] = split[0].astype(str)
    unpivoted["Store Name"] = split[1].astype(str)
    unpivoted["_section"] = split[3].astype(str)

    # 13. Re-pivot to make Qty and Value separate columns
    pivot_index = ["Calendar Year/Month", "Material Group", "Material Group Name",
                   "Material", "Product Name", "EAN/UPC", "Brand", "Store Num", "Store Name"]
    df = unpivoted.pivot_table(
        index=pivot_index, columns="_section", values="Value_Qty", aggfunc="sum"
    ).reset_index()

    # 14. Ensure both Qty and Value columns exist (section order varies per month)
    if "Qty Sold (B.Un)" not in df.columns:
        df["Qty Sold (B.Un)"] = 0
    if "Sales Value" not in df.columns:
        df["Sales Value"] = 0

    # 15. Validate essential columns before validating their values
    ok, err = validate_schema(df, retailer="Lulu", file_path=file_path)
    if not ok:
        print(f"[FAIL] {err}")
        update_pipeline_run(db_path, transform_run_id, "failed", error_msg=err)
        return

    numeric_ok, numeric_metrics, numeric_error = validate_numeric_columns(
        df,
        ("Sales Value", "Qty Sold (B.Un)"),
        retailer="Lulu",
        file_path=file_path,
    )
    print(f"[NUMERIC VALIDATION] {numeric_metrics}")
    if not numeric_ok:
        print(f"[FAIL] {numeric_error}")
        update_pipeline_run(
            db_path, transform_run_id, "failed", error_msg=numeric_error
        )
        return

    identifier_ok, identifier_metrics, identifier_error = validate_identifier_columns(
        df,
        ("Material", "Store Num"),
        retailer="Lulu",
        file_path=file_path,
    )
    print(f"[IDENTIFIER VALIDATION] {identifier_metrics}")
    if not identifier_ok:
        print(f"[WARNING] {identifier_error}")

    df["Qty Sold (B.Un)"] = pd.to_numeric(df["Qty Sold (B.Un)"], errors="coerce")
    df["Sales Value"] = pd.to_numeric(df["Sales Value"], errors="coerce")

    # 16. Strip column name whitespace
    df.columns = [c.strip() for c in df.columns]

    # 17. Drop rows with missing SKU
    df = df.dropna(subset=["Material"])

    # 18. Drop zero qty rows
    before_zero_qty_filter = df
    df = df[df["Qty Sold (B.Un)"] > 0]
    zero_qty_impact = calculate_filter_impact(
        before_zero_qty_filter,
        df,
        amount_column="Sales Value",
        quantity_column="Qty Sold (B.Un)",
        retailer="Lulu",
        filter_name="zero_quantity",
    )
    print(f"[FILTER IMPACT] {zero_qty_impact}")

    # 19. Extract grammage from Product Name 
    df = extract_grammage(df, source_column="Product Name", retailer="Lulu")

    # 20. Rename columns
    df = df.rename(columns={
        "Material": "SKU",
        "Product Name": "SKU Description",
        "Material Group Name": "Sub-Department",
        "Sales Value": "Sales Amount",
        "Qty Sold (B.Un)": "Sales Quantity",
    })

    # 21. Build Key column (SKU + SizeDesc)
    df["Key"] = df.apply(
        lambda row: "Lulu" + str(row["SKU"]) + ("-" + str(row["SizeDesc"]) if row["SizeDesc"] else ""),
        axis=1
    )

    # 22. Add Retailer column
    df["Retailer"] = "Lulu"

    # 23. Add month column
    df["Month"] = f"{month_label}-{str(year)[-2:]}"

    # 24. Select output columns
    df = df[["Retailer","Key","Month", "SKU", "SKU Description", "Sub-Department", "Material Group", "SizeDesc", "Store Num", "Store Name", "Sales Quantity", "Sales Amount"]]

    # 25. Write Parquet
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_path = silver_dir / str(year) / f"{month_label}-{year}.parquet"
    write_parquet(df, str(output_path))

    output_ok, output_metrics, output_error = validate_parquet_output(
        df, output_path, retailer="Lulu"
    )
    print(f"[SILVER OUTPUT VALIDATION] {output_metrics}")
    if not output_ok:
        print(f"[FAIL] {output_error}")
        update_pipeline_run(
            db_path, transform_run_id, "failed", error_msg=output_error
        )
        return

    print(f"[DONE] {len(df)} rows → {output_path}")
    print(f"Time: {time.time() - t0:.1f}s")

    # 26. Log success
    update_pipeline_run(db_path, transform_run_id, "success")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Lulu Silver layer ETL")
    parser.add_argument("--year", type=int, help="Year to process, e.g. 2026")
    parser.add_argument("--month", type=str, help="Month label, e.g. May, June, July")
    parser.add_argument("--all", action="store_true", help="Process all files in all years")
    parser.add_argument("--config", default="config/schemas")
    parser.add_argument("--settings", default="config/settings.yaml")
    args = parser.parse_args()

    if args.all:
        settings = yaml.safe_load(open(args.settings))
        base = Path(settings["source"]["lulu"])
        files = []
        for year_dir in base.iterdir():
            if not year_dir.is_dir():
                continue
            data_dir = year_dir 
            if data_dir.exists():
                for f in data_dir.glob("*.xlsx"):
                    if f.name.startswith("~$"):
                        continue
                    month = f.stem.split("_")[-1].split()[0]
                    files.append((str(f), int(year_dir.name), month))
        print(f"Full refresh: found {len(files)} file(s) across all years")
        for file_path, year, month in files:
            print(f"  {year} - {month}")
            process(year, month, args.config, args.settings)
    elif args.year and not args.month:
        settings = yaml.safe_load(open(args.settings))
        source_dir = Path(settings["source"]["lulu"]) / str(args.year) / "Data"
        files = [(str(f), args.year, f.stem.split("_")[-1].split()[0])
                 for f in source_dir.glob("*.xlsx") if not f.name.startswith("~$")]
        print(f"Processing Lulu {args.year}: found {len(files)} file(s)")
        for file_path, year, month in files:
            print(f"  {month}")
            process(year, month, args.config, args.settings)
    elif args.year and args.month:
        print(f"Processing Lulu {args.year} - {args.month}")
        process(args.year, args.month, args.config, args.settings)
    else:
        parser.error("Use --all, --year only, or both --year and --month")
