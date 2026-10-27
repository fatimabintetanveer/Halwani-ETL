import duckdb
from pathlib import Path

DB_PATH = r"D:\Halwani\Halwani-ETL\data\halwani_warehouse.db"
EXPORT_DIR = Path(r"D:\Halwani\Halwani-ETL\data\exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(DB_PATH, read_only=True)

tables = ["dim_product", "dim_store", "fct_sales"]

for table in tables:
    path = EXPORT_DIR / f"{table}.parquet"
    con.execute(f"COPY (SELECT * FROM main_gold.{table}) TO '{path}' (FORMAT 'parquet')")
    print(f"Exported: {path}")

con.close()
print("\nDone. All 3 tables exported.")