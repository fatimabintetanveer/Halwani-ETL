import sys
import argparse
from pathlib import Path
import yaml

# Add project root to path so silver imports work
sys.path.insert(0, str(Path(__file__).resolve().parent))

from silver.othaim_process import process as othaim_process
from silver.tamimi_process import process as tamimi_process
from utils.io import discover_files

# Registry: retailer name → process function
ADAPTERS = {
    "Othaim": othaim_process,
    "Tamimi": tamimi_process,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Data pipeline orchestrator")
    parser.add_argument("--retailer", required=True, choices=list(ADAPTERS.keys()),
                        help="Retailer to process")
    parser.add_argument("--year", type=int, help="Year to process, e.g. 2026")
    parser.add_argument("--month", type=str, help="Month label, e.g. May, June, July")
    parser.add_argument("--all", action="store_true", help="Process all files in all years")
    parser.add_argument("--config", default="config/schemas")
    parser.add_argument("--settings", default="config/settings.yaml")
    args = parser.parse_args()

    process_fn = ADAPTERS[args.retailer]
    retailer_key = args.retailer.lower()

    if args.all:
        settings = yaml.safe_load(open(args.settings))
        source_dir = settings["source"][retailer_key]
        files = discover_files(source_dir, extensions=(".xlsb", ".xlsx", ".csv"))
        print(f"Full refresh for {args.retailer}: found {len(files)} file(s)")
        for file_path, year, month in files:
            print(f"  {year} - {month}")
            process_fn(year, month, args.config, args.settings)
    elif args.year and not args.month:
        settings = yaml.safe_load(open(args.settings))
        source_dir = Path(settings["source"][retailer_key]) / str(args.year)
        files = discover_files(str(source_dir), extensions=(".xlsb", ".xlsx", ".csv"))
        print(f"Processing {args.retailer} {args.year}: found {len(files)} file(s)")
        for file_path, year, month in files:
            print(f"  {month}")
            process_fn(year, month, args.config, args.settings)
    elif args.year and args.month:
        print(f"Processing {args.retailer} {args.year} - {args.month}")
        process_fn(args.year, args.month, args.config, args.settings)
    else:
        parser.error("Use --all, --year only, or both --year and --month")


if __name__ == "__main__":
    main()
