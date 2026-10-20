import duckdb
from pathlib import Path


def refresh_silver_views(db_path: str, silver_dir: str, retailer: str = None) -> None:
    """
    Build one DuckDB view per retailer that UNIONs all month Parquet files.
    If retailer is given, only refresh that one retailer's view.
    Run this after Silver adapters finish to refresh the views.
    """
    con = duckdb.connect(db_path)
    base = Path(silver_dir).resolve()

    dirs = [base / retailer] if retailer else sorted(base.iterdir())

    for retailer_dir in dirs:
        if not retailer_dir or not retailer_dir.is_dir():
            continue

        name = retailer_dir.name
        view_name = f"silver_{name.lower()}"
        parquet_files = sorted(retailer_dir.rglob("*.parquet"))

        if not parquet_files:
            continue

        parts = []
        for f in parquet_files:
            year = f.parent.name
            month = f.stem.split("-")[0]
            abs_path = str(f).replace("\\", "/")
            parts.append(f"SELECT *, '{month}' AS month, {year} AS year FROM '{abs_path}'")

        sql = f"CREATE OR REPLACE VIEW {view_name} AS " + "\nUNION ALL\n".join(parts)
        con.execute(sql)
        print(f"[VIEW] {view_name} → {len(parts)} months")

    con.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/halwani_warehouse.db")
    parser.add_argument("--silver", default="data/silver")
    parser.add_argument("--retailer", default=None, help="Only refresh one retailer")
    args = parser.parse_args()
    refresh_silver_views(args.db, args.silver, args.retailer)