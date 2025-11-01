# app.py — ETL Lite v1.9.5 Stable (Full integrations)
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
    colors = {
        "success": "#16a34a",
        "error": "#dc2626",
        "warning": "#f59e0b",
        "info": "#2563eb"
    }
    st.markdown(
        f"""
        <div style="
            position:fixed;bottom:30px;right:30px;
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
# Caching & session helpers
# ============================================================
@st.cache_data(show_spinner=False)
def load_csv_cached(file):
    return pd.read_csv(file)

@st.cache_data(show_spinner=False)
def load_excel_cached(file):
    return pd.read_excel(file)

def set_active_df(df, label):
    st.session_state["df"] = df
    st.session_state["df_label"] = label
    st.session_state["last_loaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# DuckDB helpers
# ============================================================
def load_replace_to_duckdb(df: pd.DataFrame, table_name: str):
    con = duckdb.connect(WAREHOUSE_PATH)
    try:
        con.register("temp_df", df)
        schema_name = table_name.split(".")[0]
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
    finally:
        con.close()


# ============================================================
# Page setup
# ============================================================
st.set_page_config(page_title="ETL Lite v1.9.5", layout="wide", page_icon="🟧")
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


# ============================================================
# Theme toggle
# ============================================================
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
tabs = st.tabs(["🚀 Quickstart", "📂 Data Ingestion", "📊 Dashboard", "💼 Business Summary", "📈 Metrics"])


# ============================================================
# TAB 0 — Quickstart
# ============================================================
with tabs[0]:
    st.header("🚀 Quickstart")
    st.caption("Load sample datasets instantly into DuckDB staging.")
    if st.button("Load Sales Sample"):
        df = pd.read_csv("data/samples/sales_sample.csv")
        load_replace_to_duckdb(df, "etl.sales_sample_staging")
        set_active_df(df, "sample:sales")
        toast("✅ Sales sample loaded", "success")
        st.success("Sample data loaded. Go to Dashboard tab!")


# ============================================================
# TAB 1 — Data Ingestion
# ============================================================
with tabs[1]:
    st.header("📂 Data Ingestion")
    src = st.radio(
        "Select Data Source",
        ["Upload File", "Google Sheets URL", "Database (MySQL / PostgreSQL / Snowflake)"]
    )

    # ---------------- Upload ----------------
    if src == "Upload File":
        up = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"])
        if up:
            try:
                if up.size > MAX_UPLOAD_MB * 1024 * 1024:
                    st.error(f"File too large (max {MAX_UPLOAD_MB} MB)")
                    st.stop()
                df = load_csv_cached(up) if up.name.endswith(".csv") else load_excel_cached(up)
                st.dataframe(df.head(), use_container_width=True)
                load_replace_to_duckdb(df, "etl.uploaded_data")
                set_active_df(df, f"upload:{up.name}")
                st.success(f"✅ Uploaded {len(df)} rows from {up.name}")
            except Exception as e:
                st.error(f"Upload failed – {e}")

    # ---------------- Google Sheets ----------------
    elif src == "Google Sheets URL":
        url = st.text_input("Paste Google Sheet link (Anyone → Viewer)")
        if url:
            try:
                if "spreadsheets/d/" in url:
                    sid = url.split("/d/")[1].split("/")[0]
                    url = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv"
                df = pd.read_csv(url)
                st.dataframe(df.head(), use_container_width=True)
                load_replace_to_duckdb(df, "etl.uploaded_data")
                set_active_df(df, "google_sheets")
                st.success(f"✅ Loaded {len(df)} rows from Google Sheets")
            except Exception as e:
                st.error(f"Google Sheets load failed – {e}")

    # ---------------- Databases ----------------
    else:
        db_type = st.selectbox("Select Database Type", ["MySQL", "PostgreSQL", "Snowflake"])
        host = st.text_input("Host / Account Identifier")
        user = st.text_input("Username")
        password = st.text_input("Password", type="password")
        database = st.text_input("Database name")
        schema = st.text_input("Schema (optional)")
        warehouse = st.text_input("Warehouse (Snowflake only)") if db_type == "Snowflake" else None

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
                    tables = pd.read_sql("SHOW TABLES", engine)
                    st.session_state["tables"] = tables.iloc[:, 0].astype(str).tolist()
                    st.session_state["active_con"] = engine

                elif db_type == "PostgreSQL":
                    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
                    tables = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema='public'", engine)
                    st.session_state["tables"] = tables["table_name"].astype(str).tolist()
                    st.session_state["active_con"] = engine

                elif db_type == "Snowflake":
                    import snowflake.connector
                    con = snowflake.connector.connect(
                        user=user, password=password, account=host,
                        warehouse=warehouse or None, database=database or None, schema=schema or None
                    )
                    tables = pd.read_sql("SHOW TABLES", con)
                    name_col = [c for c in tables.columns if c.lower() == "name"][0]
                    st.session_state["tables"] = tables[name_col].astype(str).tolist()
                    st.session_state["active_con"] = con

                st.session_state["connected"] = True
                toast(f"✅ Connected to {db_type}", "success")
            except Exception as e:
                st.session_state.update({"connected": False, "tables": [], "active_con": None})
                st.error(f"Connection failed – {e}")

        if st.session_state.get("connected") and st.session_state.get("tables"):
            st.subheader("Select and Preview Table")
            selected = st.selectbox("Select a table", st.session_state["tables"])
            if st.button("Preview Table"):
                try:
                    con = st.session_state["active_con"]
                    if db_type == "Snowflake":
                        query = f'SELECT * FROM "{database}"."{schema}"."{selected}" LIMIT 100'
                    elif db_type == "PostgreSQL":
                        query = f'SELECT * FROM public."{selected}" LIMIT 100'
                    else:
                        query = f"SELECT * FROM `{selected}` LIMIT 100"
                    df = pd.read_sql(query, con)
                    st.dataframe(df.head(), use_container_width=True)
                    load_replace_to_duckdb(df, f"etl.{db_type.lower()}_{selected}")
                    set_active_df(df, f"db:{db_type}:{selected}")
                    st.success(f"✅ Loaded {len(df)} rows from {selected}")
                except Exception as e:
                    st.error(f"Preview failed – {e}")


# ============================================================
# TAB 2 — Dashboard
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
        st.dataframe(df.head(50), use_container_width=True)

        num_cols = df.select_dtypes(include=np.number).columns.tolist()
        cat_cols = df.select_dtypes(exclude=np.number).columns.tolist()

        if num_cols:
            st.subheader("Numeric Distribution")
            c = st.selectbox("Numeric column", num_cols)
            st.plotly_chart(px.histogram(df, x=c, nbins=30, template=plotly_theme), use_container_width=True)

        if cat_cols:
            st.subheader("Categorical Breakdown")
            c = st.selectbox("Categorical column", cat_cols)
            cnt = df[c].astype(str).value_counts().reset_index()
            cnt.columns = [c, "count"]
            st.plotly_chart(px.bar(cnt, x=c, y="count", template=plotly_theme), use_container_width=True)


# ============================================================
# TAB 3 — Business Summary
# ============================================================
with tabs[3]:
    st.header("💼 Business Summary")
    df = st.session_state.get("df")
    if df is None:
        st.info("Upload or connect data first.")
    else:
        amt_col = next((c for c in df.columns if "amount" in c.lower() or "total" in c.lower()), None)
        if amt_col:
            s = pd.to_numeric(df[amt_col], errors="coerce").fillna(0)
            st.metric("💰 Total Revenue", f"PKR {s.sum():,.0f}")
            st.metric("🧾 Avg Bill", f"PKR {s.mean():,.0f}")
        else:
            st.warning("No numeric column containing 'amount' or 'total' found.")


# ============================================================
# TAB 4 — Metrics
# ============================================================
with tabs[4]:
    st.header("📈 Metrics")
    df = st.session_state.get("df")
    if df is None:
        st.info("No data loaded yet.")
    else:
        date_col = next((c for c in df.columns if "date" in c.lower()), None)
        amt_col = next((c for c in df.columns if "amount" in c.lower() or "total" in c.lower()), None)
        prod_col = next((c for c in df.columns if "product" in c.lower()), None)

        if date_col and amt_col:
            dd = df.copy()
            dd[date_col] = pd.to_datetime(dd[date_col], errors="coerce")
            trend = dd.groupby(pd.Grouper(key=date_col, freq="W"))[amt_col].sum().reset_index()
            st.subheader("📅 Weekly Trend")
            st.plotly_chart(px.line(trend, x=date_col, y=amt_col, template=plotly_theme), use_container_width=True)

        if prod_col and amt_col:
            top = df.groupby(prod_col)[amt_col].sum().reset_index().sort_values(by=amt_col, ascending=False).head(10)
            st.subheader("🏆 Top Products")
            st.plotly_chart(px.bar(top, x=prod_col, y=amt_col, template=plotly_theme), use_container_width=True)
