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
            abs_path = str(f).replace("\\", "/")
            parts.append(f"SELECT * FROM '{abs_path}'")

        sql = f"CREATE OR REPLACE VIEW {view_name} AS " + "\nUNION ALL\n".join(parts)
        con.execute(sql)
        print(f"[VIEW] {view_name} → {len(parts)} months")

    # Build unified table with a Retailer column
    columns = set()
    retailer_views = ['silver_othaim', 'silver_tamimi', 'silver_lulu', 'silver_panda']

    # Step 1: collect all unique column names across all 4 views
    for view in retailer_views:
        try:
            cols = con.execute(f"SELECT * FROM {view} LIMIT 0").description
            columns.update([c[0] for c in cols])
        except Exception:
            continue

    # Step 2: build UNION query with NULL for missing columns
    parts = []
    for view in retailer_views:
        try:
            view_cols = [c[0] for c in con.execute(f"SELECT * FROM {view} LIMIT 0").description]
        except Exception:
            continue
        retailer_name = view.replace('silver_', '').title()
        selects = []
        for col in sorted(columns):
            if col in view_cols:
                selects.append(f'"{col}"')
            else:
                selects.append(f'NULL AS "{col}"')
        parts.append(f"SELECT '{retailer_name}' AS Retailer, {', '.join(selects)} FROM {view}")

    if parts:
        sql = "CREATE OR REPLACE TABLE silver_unified AS " + "\nUNION ALL\n".join(parts)
        con.execute(sql)
        print(f"[TABLE] silver_unified → {len(parts)} retailers, {len(columns)} columns")

    con.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/halwani_warehouse.db")
    parser.add_argument("--silver", default="data/silver")
    parser.add_argument("--retailer", default=None, help="Only refresh one retailer")
    args = parser.parse_args()
    refresh_silver_views(args.db, args.silver, args.retailer)