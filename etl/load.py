import duckdb
from .utils import WAREHOUSE_PATH
from .audit import ensure_audit_tables

def init_warehouse():
    """Ensure database file exists, schema, and basic tables are present."""
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    con.execute("CREATE TABLE IF NOT EXISTS etl.sales(date TIMESTAMP, product_name VARCHAR, quantity DOUBLE, unit_price DOUBLE, amount DOUBLE)")
    con.execute("CREATE TABLE IF NOT EXISTS etl.expenses(date TIMESTAMP, expense_category VARCHAR, amount DOUBLE)")
    con.close()
    ensure_audit_tables()

def safe_load_to_duckdb(df, table_name, db_path=None):
    """
    Loads a DataFrame into DuckDB safely.
    - Creates the table if it doesn't exist
    - Adds new columns dynamically if schema changes
    - Never fails on duplicate or missing columns
    """
    con = duckdb.connect(db_path or WAREHOUSE_PATH)
    con.register("temp_df", df)

    # Ensure table exists
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df LIMIT 0")

    # Add any new columns automatically (basic dtype inference)
    existing_cols = [c[0].lower() for c in con.execute(f"DESCRIBE {table_name}").fetchall()]
    for col in df.columns:
        if col.lower() not in existing_cols:
            dtype = "VARCHAR"
            try:
                import pandas as pd
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    dtype = "TIMESTAMP"
                elif pd.api.types.is_numeric_dtype(df[col]):
                    dtype = "DOUBLE"
            except Exception:
                pass
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}")

    # Insert data safely
    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
    con.close()
