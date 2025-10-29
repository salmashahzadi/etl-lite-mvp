# app.py — ETL Lite v1.9 (full)
# ------------------------------------------------------------
import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from etl.utils import WAREHOUSE_PATH, APP_PASSWORD, MAX_UPLOAD_MB
from etl.ingest import suggest_column_mapping
from etl.transform import transform_with_mapping, clean_dataframe
from etl.validate import validate_dataframe, summarize_errors
from etl.schema import ENTITY_SPECS
from etl.load import safe_load_to_duckdb

# ============================================================
# Toast helper
# ============================================================
def toast(msg, type="info", delay=3):
    colors = {"success": "#16a34a", "error": "#dc2626", "warning": "#f59e0b", "info": "#2563eb"}
    st.markdown(
        f"""
        <div style="position:fixed;bottom:30px;right:30px;
        background-color:{colors.get(type,'#2563eb')};
        color:white;padding:12px 20px;border-radius:8px;
        box-shadow:0 2px 8px rgba(0,0,0,0.3);
        z-index:9999;font-size:15px;
        animation:fadein 0.3s, fadeout 0.5s {delay}s forwards;">
          {msg}
        </div>
        <style>
        @keyframes fadein {{from{{opacity:0}}to{{opacity:1}}}}
        @keyframes fadeout {{from{{opacity:1}}to{{opacity:0}}}}
        </style>
        """,
        unsafe_allow_html=True,
    )

# ============================================================
# Rerun helpers (one-shot refresh after updating active df)
# ============================================================
def set_active_df(df: pd.DataFrame, label: str):
    st.session_state["df"] = df
    st.session_state["df_label"] = label
    st.session_state["last_loaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state["_needs_refresh"] = True

def maybe_rerun_once():
    if st.session_state.get("_needs_refresh"):
        st.session_state["_needs_refresh"] = False
        st.rerun()

# ============================================================
# DuckDB helpers (extra: safe replace + column-align append)
# ============================================================
def load_replace_to_duckdb(df: pd.DataFrame, table_name: str):
    """
    Create or REPLACE the table with df. This avoids binder errors when
    a new CSV has a different set/order of columns than a previous one.
    """
    con = duckdb.connect(WAREHOUSE_PATH)
    con.register("temp_df", df)
    # CREATE OR REPLACE is supported by DuckDB
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {table_name.split(".")[0]};')
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
    con.close()

def append_with_alignment(df: pd.DataFrame, table_name: str):
    """
    Try to align df to existing duckdb table columns (adds missing columns as NULL)
    then INSERT. If the table does not exist, it will be created.
    """
    con = duckdb.connect(WAREHOUSE_PATH)
    con.register("temp_df", df)

    # If table doesn't exist, create it
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df LIMIT 0")

    # Get existing columns
    existing_cols = [c[0] for c in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
    # Add missing columns to df
    for col in existing_cols:
        if col not in df.columns:
            df[col] = None
    # Reorder df to match table
    df = df[existing_cols]
    con.unregister("temp_df")
    con.register("temp_df", df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
    con.close()

# ============================================================
# Page setup + theme
# ============================================================
st.set_page_config(page_title="ETL Lite v1.9", layout="wide", page_icon="🟧")
st.title("🟧 ETL Lite MVP — Universal Data Loader & Dashboard")

if APP_PASSWORD:
    if "_pw_ok" not in st.session_state:
        st.session_state["_pw_ok"] = False
    if not st.session_state["_pw_ok"]:
        pw = st.text_input("Password", type="password")
        if pw == APP_PASSWORD:
            st.session_state["_pw_ok"] = True
            st.rerun()
        st.stop()

if "dark_mode" not in st.session_state:
    st.session_state["dark_mode"] = False
dark_mode = st.toggle("🌙 Dark Mode", value=st.session_state["dark_mode"])
st.session_state["dark_mode"] = dark_mode
bg, fg, accent = ("#18181b", "#eaeaea", "#60a5fa") if dark_mode else ("#f9fafb", "#1f2937", "#2563eb")
st.markdown(
    f"""
    <style>
    [data-testid="stAppViewContainer"] {{
        background-color:{bg};color:{fg};font-family:'Inter',sans-serif;
    }}
    .stButton>button {{
        background:{accent};color:white;border:none;border-radius:8px;font-weight:600;
    }}
    button[data-baseweb="tab"][aria-selected="true"] {{
        color:{accent};border-bottom:2px solid {accent};
    }}
    </style>
    """,
    unsafe_allow_html=True,
)
plotly_theme = "plotly_dark" if dark_mode else "plotly_white"

# ============================================================
# Tabs
# ============================================================
tabs = st.tabs([
    "🚀 Quickstart",
    "📂 Data Ingestion",
    "📊 Dashboard",
    "💼 Business Summary",
    "📈 Metrics"
])

# ============================================================
# Quickstart
# ============================================================
with tabs[0]:
    st.header("🚀 Quickstart")
    c1, c2, c3 = st.columns(3)
    if c1.button("Load Sales Sample"):
        df = pd.read_csv("data/samples/sales_sample.csv")
        load_replace_to_duckdb(df, "etl.sales_sample_staging")
        set_active_df(df, "sample:sales")
        toast("Loaded Sales Sample", "success")
    if c2.button("Load Expenses Sample"):
        df = pd.read_csv("data/samples/expenses_sample.csv")
        load_replace_to_duckdb(df, "etl.expenses_sample_staging")
        set_active_df(df, "sample:expenses")
        toast("Loaded Expenses Sample", "success")
    if c3.button("Load Appointments Sample"):
        df = pd.read_csv("data/samples/appointments_sample.csv")
        load_replace_to_duckdb(df, "etl.appointments_sample_staging")
        set_active_df(df, "sample:appointments")
        toast("Loaded Appointments Sample", "success")

maybe_rerun_once()

# ============================================================
# Data Ingestion
# ============================================================
with tabs[1]:
    st.header("📂 Data Ingestion")
    src = st.radio(
        "Select Data Source",
        ["Upload File", "Google Sheets URL", "Database (MySQL / PostgreSQL / Snowflake)"]
    )
    df = None

    # 1) Upload file
    if src == "Upload File":
        up = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"])
        if up:
            try:
                if up.size > MAX_UPLOAD_MB * 1024 * 1024:
                    toast(f"File too large (max {MAX_UPLOAD_MB} MB)", "error")
                    st.stop()
                df = pd.read_csv(up) if up.name.endswith(".csv") else pd.read_excel(up)

                # Use REPLACE for the quick staging table to avoid schema mismatch
                load_replace_to_duckdb(df, "etl.uploaded_data")
                set_active_df(df, f"upload:{up.name}")
                toast(f"Uploaded {len(df)} rows from {up.name}", "success")
            except Exception as e:
                toast(f"Upload failed – {e}", "error")

    # 2) Google Sheets
    elif src == "Google Sheets URL":
        url = st.text_input("Paste Google Sheet link (Anyone→Viewer)")
        if url:
            try:
                # if it's a view link, convert to /export?format=csv
                if "spreadsheets/d/" in url:
                    sid = url.split("/d/")[1].split("/")[0]
                    url = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv"
                df = pd.read_csv(url)
                load_replace_to_duckdb(df, "etl.uploaded_data")
                set_active_df(df, "google_sheets")
                toast(f"Loaded {len(df)} rows from Google Sheets", "success")
            except Exception as e:
                toast(f"Google Sheets load failed – {e}", "error")

    # 3) Database connections (MySQL / PostgreSQL / Snowflake)
    else:
        db_type = st.selectbox("Select Database Type", ["MySQL", "PostgreSQL", "Snowflake"])
        host = st.text_input("Host / Account Identifier")
        user = st.text_input("Username")
        password = st.text_input("Password", type="password")
        database = st.text_input("Database name")
        schema = st.text_input("Schema (optional)")
        warehouse = st.text_input("Warehouse name (Snowflake only)") if db_type == "Snowflake" else None

        # Prepare state holders
        if "connected" not in st.session_state:
            st.session_state["connected"] = False
        if "tables" not in st.session_state:
            st.session_state["tables"] = []
        if "active_con" not in st.session_state:
            st.session_state["active_con"] = None

        if st.button("Connect to Database"):
            try:
                if db_type == "MySQL":
                    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
                    st.session_state["tables"] = pd.read_sql("SHOW TABLES", engine).iloc[:, 0].tolist()
                    st.session_state["active_con"] = engine

                elif db_type == "PostgreSQL":
                    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
                    tables = pd.read_sql(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
                        engine,
                    )
                    st.session_state["tables"] = tables["table_name"].tolist()
                    st.session_state["active_con"] = engine

                elif db_type == "Snowflake":
                    import snowflake.connector
                    con = snowflake.connector.connect(
                        user=user,
                        password=password,
                        account=host,
                        warehouse=warehouse or None,
                        database=database or None,
                        schema=schema or None,
                    )
                    cur = con.cursor()

                    # Defensive context switching (ignore if missing / no perms)
                    ctx_cmds = [
                        (warehouse, f'USE WAREHOUSE "{warehouse}"'),
                        (database, f'USE DATABASE "{database}"'),
                        (schema,    f'USE SCHEMA "{schema}"'),
                    ]
                    for val, cmd in ctx_cmds:
                        if val:
                            try:
                                cur.execute(cmd)
                            except Exception:
                                pass  # don't block connection, let preview handle FQN usage

                    tables = pd.read_sql("SHOW TABLES", con)
                    # Snowflake SHOW TABLES returns mixed cases; standardize on NAME column
                    col_name = [c for c in tables.columns if c.lower() == "name"][0]
                    st.session_state["tables"] = tables[col_name].astype(str).tolist()
                    st.session_state["active_con"] = con

                st.session_state["connected"] = True
                toast(f"Connected successfully to {db_type}", "success")
            except Exception as e:
                st.session_state["connected"] = False
                st.session_state["tables"] = []
                st.session_state["active_con"] = None
                toast(f"Connection failed – {e}", "error")

        # Preview any table in the connected source
        if st.session_state.get("connected") and st.session_state.get("tables"):
            st.subheader("Select and Preview Table")
            selected_table = st.selectbox("Select a table", st.session_state["tables"])
            if st.button("Preview Table"):
                try:
                    con = st.session_state["active_con"]
                    if db_type == "Snowflake":
                        # Build fully-qualified name if schema/database provided
                        if database and schema:
                            fqn = f'"{database}"."{schema}"."{selected_table}"'
                        elif schema:
                            fqn = f'"{schema}"."{selected_table}"'
                        else:
                            fqn = f'"{selected_table}"'
                        query = f"SELECT * FROM {fqn} LIMIT 100"
                    elif db_type == "PostgreSQL":
                        fqn = f'"public"."{selected_table}"'
                        query = f"SELECT * FROM {fqn} LIMIT 100"
                    else:  # MySQL
                        query = f"SELECT * FROM `{selected_table}` LIMIT 100"

                    df = pd.read_sql(query, con)
                    set_active_df(df, f"db:{db_type}:{selected_table}")
                    toast(f"Loaded {len(df)} rows from {selected_table}", "success")
                except Exception as e:
                    toast(f"Error previewing table – {e}", "error")

maybe_rerun_once()

# ============================================================
# Dashboard (always reflects current active df)
# ============================================================
with tabs[2]:
    st.header("📊 Dashboard")
    df = st.session_state.get("df")
    if df is None:
        st.info("Upload or connect data first.")
    else:
        meta = st.session_state.get("df_label", "(unknown)")
        ts = st.session_state.get("last_loaded_at", "")
        st.caption(f"Source: {meta} • Loaded at: {ts} • Rows: {len(df):,}")
        # Summary table
        st.dataframe(df.head(50), use_container_width=True)

        num = df.select_dtypes(include=np.number).columns.tolist()
        cat = df.select_dtypes(exclude=np.number).columns.tolist()

        if num:
            st.subheader("Numeric Distribution")
            c = st.selectbox("Numeric column", num, key="dash_num_col")
            st.plotly_chart(px.histogram(df, x=c, nbins=30, template=plotly_theme), use_container_width=True)

        if cat:
            st.subheader("Categorical Breakdown")
            c = st.selectbox("Categorical column", cat, key="dash_cat_col")
            cnt = df[c].astype(str).value_counts().reset_index()
            cnt.columns = [c, "count"]
            st.plotly_chart(px.bar(cnt, x=c, y="count", template=plotly_theme), use_container_width=True)

# ============================================================
# Business Summary (auto-updates with active df)
# ============================================================
with tabs[3]:
    st.header("💼 Business Summary")
    df = st.session_state.get("df")
    if df is None:
        st.info("Upload or connect data first.")
    else:
        # Try to find amount-like column
        amt_col = next((c for c in df.columns if "amount" in c.lower() or "total" in c.lower()), None)
        if amt_col:
            s = pd.to_numeric(df[amt_col], errors="coerce").fillna(0)
            st.metric("💰 Total Revenue", f"PKR {s.sum():,.0f}")
            st.metric("🧾 Avg Bill", f"PKR {s.mean():,.0f}")
        else:
            st.warning("No numeric column containing 'amount' or 'total' found.")

        # Optional: load into warehouse (etl.<entity>) with validate pipeline
        st.divider()
        st.subheader("Transform, Validate & Load into Warehouse")
        entity = st.selectbox("Select entity", list(ENTITY_SPECS.keys()), key="entity_for_load")
        mode = st.radio("Load Mode", ["append", "replace"], horizontal=True, key="entity_load_mode")
        if st.button("⚙️ Transform, Validate & Load", type="primary", key="do_etl_load"):
            with st.spinner("Processing data ..."):
                try:
                    tdf = transform_with_mapping(df.copy(), entity, suggest_column_mapping(clean_dataframe(df.copy()), entity))
                    tdf, errs = validate_dataframe(entity, tdf)
                    if errs:
                        toast("Validation issues found", "warning")
                        st.json(summarize_errors(errs))
                    else:
                        toast("Validation Passed", "success")
                    # append with alignment or replace into etl.<entity>
                    target = f"etl.{entity}"
                    if mode == "replace":
                        load_replace_to_duckdb(tdf, target)
                    else:
                        append_with_alignment(tdf, target)
                    toast(f"Loaded {len(tdf)} rows into {target}", "success")
                except Exception as e:
                    toast(f"Load failed – {e}", "error")

# ============================================================
# Metrics (auto-updates with active df)
# ============================================================
with tabs[4]:
    st.header("📈 Metrics")
    df = st.session_state.get("df")
    if df is None:
        st.info("No data loaded yet.")
    else:
        # Trend — weekly sum if a date column exists
        date_col = next((c for c in df.columns if c.lower() == "date"), None)
        amt_col = next((c for c in df.columns if "amount" in c.lower() or "total" in c.lower()), None)
        if date_col and amt_col:
            dd = df.copy()
            dd[date_col] = pd.to_datetime(dd[date_col], errors="coerce")
            trend = dd.groupby(pd.Grouper(key=date_col, freq="W"))[amt_col].sum().reset_index()
            st.subheader("📅 Weekly Trend")
            st.plotly_chart(px.line(trend, x=date_col, y=amt_col, template=plotly_theme), use_container_width=True)

        # Top products
        prod_col = next((c for c in df.columns if "product" in c.lower()), None)
        if prod_col and amt_col:
            top = df.groupby(prod_col)[amt_col].sum().reset_index().sort_values(by=amt_col, ascending=False).head(10)
            st.subheader("🏆 Top Products")
            st.plotly_chart(px.bar(top, x=prod_col, y=amt_col, template=plotly_theme), use_container_width=True)
