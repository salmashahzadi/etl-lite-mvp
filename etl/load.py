import duckdb
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import time
from .utils import WAREHOUSE_PATH
from .audit import ensure_audit_tables


# ---------------------------------------------------------------------
# ✅ Initialize Warehouse (DuckDB)
# ---------------------------------------------------------------------
def init_warehouse():
    """Ensure DuckDB file exists and base tables are created."""
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS etl.sales(
            date TIMESTAMP,
            product_name VARCHAR,
            quantity DOUBLE,
            unit_price DOUBLE,
            amount DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS etl.expenses(
            date TIMESTAMP,
            expense_category VARCHAR,
            amount DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS etl.appointments(
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            service VARCHAR,
            staff VARCHAR,
            customer VARCHAR,
            branch VARCHAR,
            notes VARCHAR,
            amount DOUBLE
        )
    """)
    con.close()
    ensure_audit_tables()


# ---------------------------------------------------------------------
# ✅ Safe Load into DuckDB (auto schema adjust)
# ---------------------------------------------------------------------
def safe_load_to_duckdb(df, table_name, db_path=None):
    """
    Loads a DataFrame into DuckDB safely:
      - Creates the table if it doesn't exist
      - Adds missing columns dynamically
      - Inserts new rows safely
    """
    con = duckdb.connect(db_path or WAREHOUSE_PATH)
    con.register("temp_df", df)

    # Ensure table exists
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df LIMIT 0")

    # Add missing columns automatically
    existing_cols = [c[0].lower() for c in con.execute(f"DESCRIBE {table_name}").fetchall()]
    for col in df.columns:
        if col.lower() not in existing_cols:
            dtype = "VARCHAR"
            try:
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


# ---------------------------------------------------------------------
# 🌐 External Database Connections (MySQL / PostgreSQL)
# ---------------------------------------------------------------------
def read_from_mysql(host, user, password, database, query):
    """Read data from a MySQL database into a DataFrame."""
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    return pd.read_sql(query, engine)


def read_from_postgres(host, user, password, database, query):
    """Read data from a PostgreSQL database into a DataFrame."""
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
    return pd.read_sql(query, engine)
