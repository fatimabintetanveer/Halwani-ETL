import sys
import time
from pathlib import Path
import polars as pl
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.io import find_file, write_parquet, log_pipeline_run, discover_files, extract_year_month
from utils.schema_validator import validate_columns


def process(year: int, month_label: str, config_path: str, settings_path: str) -> None:
    """Process one Panda CSV file for a given year and month."""
    t0 = time.time()

    settings = yaml.safe_load(open(settings_path))
    config = yaml.safe_load(open(f"{config_path}/panda.yaml"))
    source_dir = Path(settings["source"]["panda"]) / str(year)
    silver_dir = Path(settings["output"]["silver"]) / "Panda"
    db_path = settings["duckdb"]["path"]

    # 1. Find file
    file_path = find_file(str(source_dir), month_label, extensions=(".csv",), recursive=False)
    if file_path is None:
        print(f"[SKIP] No file found for Panda {year} - {month_label}")
        log_pipeline_run(db_path, "Panda", year, month_label, "skipped",
                         error_msg=f"No file found for {year} - {month_label}")
        return

    print(f"[READ] {Path(file_path).name}")

    # 2. Read raw
    df = pl.read_csv(
        file_path,
        low_memory=False,       # use more RAM, fewer passes
        schema_overrides={
            "Sku": pl.Utf8,
            "BARCODE": pl.Utf8,
            "Store Num": pl.Utf8,
            "Month": pl.Utf8,
        },
        ignore_errors=True,
    )

    # 3. Validate
    ok, err = validate_columns(df, config["essential_columns"], file_path)
    if not ok:
        print(f"[FAIL] {err}")
        log_pipeline_run(db_path, "Panda", year, month_label, "failed", error_msg=err)
        return

    # 4. Strip column name whitespace
    df.columns = [c.strip() for c in df.columns]

    # 5. Filter zero/negative qty
    df = df.filter(pl.col("Qty TY") > 0)

    # 6. Strip whitespace from string columns
    string_cols = ["SizeDesc", "Store Name", "StoreType", "City", "Region", "Promo Offer Code"]
    for col in string_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.strip_chars())

    # 7. Rename columns
    rename_map = config["rename_map"]
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    # 8. Add month column + drop old Month
    df = df.drop("Month") if "Month" in df.columns else df
    df = df.with_columns(pl.lit(f"{month_label}-{str(year)[-2:]}").alias("Month"))

    # 9. Select output columns
    df = df[["Month", "SKU", "SizeDesc", "Store Num", "Store Name", "StoreType",
             "City", "Region", "Promo Week", "Promo Offer Code",
             "Sales Quantity", "Sales Amount"]]

    # 10. Write Parquet
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_path = silver_dir / str(year) / f"{month_label}-{year}.parquet"
    write_parquet(df, str(output_path))

    print(f"[DONE] {len(df)} rows → {output_path}")

    # 11. Log success
    log_pipeline_run(db_path, "Panda", year, month_label, "success",
                     rows=len(df), file_path=file_path)

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