import sys
import time
import logging
from pathlib import Path
import polars as pl
import yaml



sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.io import find_file, update_pipeline_run, write_parquet, log_pipeline_run, discover_files, extract_year_month
from utils.validation.schema_validation import validate_schema
from utils.validation.data_validation import (
    calculate_filter_impact,
    validate_numeric_columns,
    validate_parquet_output,
)
from utils.validation.source_files_validation import format_read_error
from utils.validation.layout_validation import check_layout

logger = logging.getLogger(__name__)


def process(year: int, month_label: str, config_path: str, settings_path: str) -> None:
    """Process one Panda CSV file for a given year and month."""
    t0 = time.time()

    settings = yaml.safe_load(open(settings_path))
    source_dir = Path(settings["source"]["panda"]) / str(year)
    silver_dir = Path(settings["output"]["silver"]) / "Panda"
    db_path = settings["duckdb"]["path"]

    # 1. Find file
    file_path, error_message = find_file(
        str(source_dir), month_label, year, extensions=(".csv",), retailer="Panda"
    )
    if file_path is None:
        log_pipeline_run(
            db_path, "Panda", year, month_label, "discover_file", "failed",
            error_msg=error_message,
        )
        return

    log_pipeline_run(
        db_path, "Panda", year, month_label, "discover_file", "success",
        file_path=file_path,
    )

    layout, layout_error = check_layout(file_path,"Panda")
    if not layout:
        print(f"[FAIL] {layout_error}")
        return

    print(f"[READ] {Path(file_path).name}")

    # 2. Read raw
    read_run_id = log_pipeline_run(
        db_path, "Panda", year, month_label,
        stage="source_read", status="running", file_path=file_path,
    )
    try:
        df = pl.read_csv(
            file_path,
            low_memory=False,       # use more RAM, fewer passes
            schema_overrides={
                "Sku": pl.Utf8,
                "BARCODE": pl.Utf8,
                "Store Num": pl.Utf8,
                "Month": pl.Utf8,
            }
            #ignore_errors=True,
        )
    except Exception as exc:
        error = format_read_error(exc, retailer="Panda", file_path=file_path)
        logger.exception(error)
        update_pipeline_run(db_path, read_run_id, "failed", error_msg=error)
        return
    update_pipeline_run(db_path, read_run_id, "success")

    transform_run_id = log_pipeline_run(
        db_path, "Panda", year, month_label,
        stage="silver_transform", status="running", file_path=file_path,
    )

    # 3. Validate
    ok, err = validate_schema(df, retailer="Panda", file_path=file_path)
    if not ok:
        print(f"[FAIL] {err}")
        update_pipeline_run(db_path, transform_run_id, "failed", error_msg=err)
        return

    # 4. Strip column name whitespace
    df.columns = [c.strip() for c in df.columns]

    # 5. Validate source amount and quantity before filtering
    numeric_ok, numeric_metrics, numeric_error = validate_numeric_columns(
        df,
        ("Sales TY With VAT", "Qty TY"),
        retailer="Panda",
        file_path=file_path,
    )
    print(f"[NUMERIC VALIDATION] {numeric_metrics}")
    if not numeric_ok:
        print(f"[FAIL] {numeric_error}")
        update_pipeline_run(
            db_path, transform_run_id, "failed", error_msg=numeric_error
        )
        return

    # 6. Filter zero/negative qty
    before_nonpositive_qty_filter = df
    df = df.filter(pl.col("Qty TY") > 0)
    nonpositive_qty_impact = calculate_filter_impact(
        before_nonpositive_qty_filter,
        df,
        amount_column="Sales TY With VAT",
        quantity_column="Qty TY",
        retailer="Panda",
        filter_name="nonpositive_quantity",
    )
    print(f"[FILTER IMPACT] {nonpositive_qty_impact}")

    # 6. Strip whitespace from string columns
    string_cols = ["SizeDesc", "Store Name", "StoreType", "City", "Region", "Promo Offer Code"]
    for col in string_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.strip_chars())

    # 6b. Normalize Store Name: remove dots, collapse spaces
    if "Store Name" in df.columns:
        df = df.with_columns(
            pl.col("Store Name")
            .str.replace_all(r"\.", " ")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
        )

    # 7. Rename columns
    df = df.rename({k: v for k, v in {
        "Sku": "SKU",
        "Sales TY With VAT": "Sales Amount",
        "Qty TY": "Sales Quantity",
    }.items() if k in df.columns})

    # 8. Build Key column (SKU + SizeDesc)
    df = df.with_columns(
        pl.concat_str(
            pl.lit("Panda"),
            pl.col("SKU"),
            pl.when(pl.col("SizeDesc").is_not_null() & (pl.col("SizeDesc") != ""))
            .then(pl.lit("-") + pl.col("SizeDesc"))
            .otherwise(pl.lit(""))
        ).alias("Key")
    )

    # 9. Add Retailer column + month column + drop old Month
    df = df.with_columns(pl.lit("Panda").alias("Retailer"))
    df = df.drop("Month") if "Month" in df.columns else df
    df = df.with_columns(pl.lit(f"{month_label}-{str(year)[-2:]}").alias("Month"))

    # 10. Select output columns
    df = df[["Retailer","Key","Month", "SKU", "SizeDesc", "Store Num", "Store Name", "StoreType",
             "City", "Region", "Promo Week", "Promo Offer Code",
             "Sales Quantity", "Sales Amount"]]

    # 12. Write Parquet
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_path = silver_dir / str(year) / f"{month_label}-{year}.parquet"
    write_parquet(df, str(output_path))

    output_ok, output_metrics, output_error = validate_parquet_output(
        df, output_path, retailer="Panda"
    )
    print(f"[SILVER OUTPUT VALIDATION] {output_metrics}")
    if not output_ok:
        print(f"[FAIL] {output_error}")
        update_pipeline_run(
            db_path, transform_run_id, "failed", error_msg=output_error
        )
        return

    print(f"[DONE] {len(df)} rows → {output_path}")

    # 12. Log success
    update_pipeline_run(db_path, transform_run_id, "success")

    print(f"Time: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Panda Silver layer ETL")
    parser.add_argument("--year", type=int, help="Year to process, e.g. 2026")
    parser.add_argument("--month", type=str, help="Month label, e.g. Mar, Jul")
    parser.add_argument("--all", action="store_true", help="Process all files in all years")
    parser.add_argument("--config", default="config/schemas")
    parser.add_argument("--settings", default="config/settings.yaml")
    args = parser.parse_args()

    if args.all:
        settings = yaml.safe_load(open(args.settings))
        source_dir = settings["source"]["panda"]
        files = discover_files(source_dir, extensions=(".csv",))
        print(f"Full refresh: found {len(files)} file(s) across all years")
        for file_path, year, month in files:
            print(f"  {year} - {month}")
            process(year, month, args.config, args.settings)
    elif args.year and not args.month:
        settings = yaml.safe_load(open(args.settings))
        source_dir = Path(settings["source"]["panda"]) / str(args.year)
        files = [(str(f), args.year, extract_year_month(str(f))[1])
                 for f in source_dir.glob("*.csv") if not f.name.startswith("~$")]
        print(f"Processing Panda {args.year}: found {len(files)} file(s)")
        for file_path, year, month in files:
            print(f"  {month}")
            process(year, month, args.config, args.settings)
    elif args.year and args.month:
        print(f"Processing Panda {args.year} - {args.month}")
        process(args.year, args.month, args.config, args.settings)
    else:
        parser.error("Use --all, --year only, or both --year and --month")
