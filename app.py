import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
import numpy as np
from datetime import datetime
from etl.utils import WAREHOUSE_PATH, APP_PASSWORD, MAX_UPLOAD_MB
from etl.ingest import suggest_column_mapping
from etl.transform import transform_with_mapping, clean_dataframe
from etl.validate import validate_dataframe, summarize_errors
from etl.schema import ENTITY_SPECS
from etl.load import safe_load_to_duckdb

# ------------------------------------------------------------
# 💬 Toast Notification Utility (icon-free for success/warning)
# ------------------------------------------------------------
def toast(msg, type="info", delay=3):
    colors = {
        "success": "#16a34a",
        "error": "#dc2626",
        "warning": "#f59e0b",
        "info": "#2563eb"
    }
    icons = {
        "error": "❌",
        "info": "💡"
    }
    icon = icons.get(type, "")  # no icons for success/warning

    html = f"""
    <div style="
        position:fixed;bottom:30px;right:30px;
        background-color:{colors.get(type,'#2563eb')};
        color:white;padding:12px 20px;border-radius:8px;
        box-shadow:0 2px 8px rgba(0,0,0,0.3);
        z-index:9999;font-size:15px;
        animation:fadein 0.3s, fadeout 0.5s {delay}s forwards;">
        {icon} {msg}
    </div>
    <style>
    @keyframes fadein {{from{{opacity:0;}}to{{opacity:1;}}}}
    @keyframes fadeout {{from{{opacity:1;}}to{{opacity:0;}}}}
    </style>
    """
    st.markdown(html, unsafe_allow_html=True)

# ------------------------------------------------------------
# 🎨 Page Setup
# ------------------------------------------------------------
st.set_page_config(page_title="ETL Lite v1.3", layout="wide", page_icon="🟧")
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

# ------------------------------------------------------------
# 🌗 Theme Toggle
# ------------------------------------------------------------
if "dark_mode" not in st.session_state:
    st.session_state["dark_mode"] = False
dark_mode = st.toggle("🌙 Dark Mode", value=st.session_state["dark_mode"])
st.session_state["dark_mode"] = dark_mode

# ------------------------------------------------------------
# 💅 Theme CSS
# ------------------------------------------------------------
if dark_mode:
    bg, fg, accent = "#18181b", "#eaeaea", "#60a5fa"
else:
    bg, fg, accent = "#f9fafb", "#1f2937", "#2563eb"

st.markdown(f"""
<style>
[data-testid="stAppViewContainer"] {{
    background-color:{bg};color:{fg};
    font-family:'Inter',sans-serif;font-size:15px;
}}
h1,h2,h3,h4,h5,h6 {{color:{fg};font-weight:600;}}
.stButton>button {{
    background:{accent};color:white;border:none;
    border-radius:8px;font-weight:600;transition:all .2s;
}}
.stButton>button:hover {{background-color:#1e40af;}}
button[data-baseweb="tab"][aria-selected="true"] {{
    color:{accent};border-bottom:2px solid {accent};
}}
</style>
""", unsafe_allow_html=True)
plotly_theme = "plotly_dark" if dark_mode else "plotly_white"

# ------------------------------------------------------------
# 🧱 Tabs
# ------------------------------------------------------------
tabs = st.tabs(["🚀 Quickstart", "📂 Data Ingestion",
                "📊 Dashboard", "💼 Business Summary", "📈 Metrics"])

# ------------------------------------------------------------
# 🚀 Quickstart
# ------------------------------------------------------------
with tabs[0]:
    st.header("🚀 Quickstart")
    st.write("Load ready-made sample data for a quick demo.")
    c1, c2, c3 = st.columns(3)
    if c1.button("Load Sales Sample"):
        sdf = pd.read_csv("data/samples/sales_sample.csv")
        con = duckdb.connect(WAREHOUSE_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
        safe_load_to_duckdb(sdf, "etl.sales")
        con.close()
        toast("Sample Sales Data Loaded", "success")
        st.dataframe(sdf.head())
    if c2.button("Load Expenses Sample"):
        edf = pd.read_csv("data/samples/expenses_sample.csv")
        con = duckdb.connect(WAREHOUSE_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
        safe_load_to_duckdb(edf, "etl.expenses")
        con.close()
        toast("Expenses Data Inserted", "success")
        st.dataframe(edf.head())
    if c3.button("Load Appointments Sample"):
        adf = pd.read_csv("data/samples/appointments_sample.csv")
        con = duckdb.connect(WAREHOUSE_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
        safe_load_to_duckdb(adf, "etl.appointments")
        con.close()
        toast("Appointments Sample Loaded", "success")
        st.dataframe(adf.head())

# ------------------------------------------------------------
# 📂 Data Ingestion (MySQL + PostgreSQL + Snowflake)
# ------------------------------------------------------------
with tabs[1]:
    st.header("📂 Data Ingestion")
    src = st.radio("Select Data Source",
                   ["Upload File", "Google Sheets URL", "Database (MySQL / PostgreSQL / Snowflake)"])
    df = None

    # ---- Upload CSV/Excel ----
    if src == "Upload File":
        up = st.file_uploader("Upload CSV or Excel file", type=["csv", "xlsx"])
        if up:
            if up.size > MAX_UPLOAD_MB * 1024 * 1024:
                toast(f"File too large (max {MAX_UPLOAD_MB} MB)", "error"); st.stop()
            df = pd.read_csv(up) if up.name.endswith(".csv") else pd.read_excel(up)
            toast(f"Loaded {len(df)} rows from {up.name}", "success")
            st.dataframe(df.head())

    # ---- Google Sheets ----
    elif src == "Google Sheets URL":
        url = st.text_input("Paste Google Sheet link (Anyone→Viewer)")
        if url:
            if "spreadsheets/d/" in url:
                sid = url.split("/d/")[1].split("/")[0]
                url = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv"
            try:
                df = pd.read_csv(url)
                toast(f"Loaded {len(df)} rows from Google Sheets", "success")
                st.dataframe(df.head())
            except Exception as e:
                toast(f"Could not load Google Sheet – {e}", "error")

    # ---- Database Simplified ----
    elif src == "Database (MySQL / PostgreSQL / Snowflake)":
        st.subheader("🔗 Connect to your database (no coding needed)")
        db_type = st.selectbox("Select Database Type", ["MySQL", "PostgreSQL", "Snowflake"])

        if db_type == "Snowflake":
            account = st.text_input("Account (e.g. xy12345.eu-west-1)")
            user = st.text_input("Username")
            password = st.text_input("Password", type="password")
            warehouse = st.text_input("Warehouse name")
            database = st.text_input("Database name")
            schema = st.text_input("Schema", "PUBLIC")
        else:
            host = st.text_input("Host", "localhost")
            user = st.text_input("Username")
            password = st.text_input("Password", type="password")
            database = st.text_input("Database name")

        if st.button("Connect to Database"):
            try:
                if db_type == "MySQL":
                    from sqlalchemy import create_engine
                    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
                elif db_type == "PostgreSQL":
                    from sqlalchemy import create_engine
                    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
                else:  # Snowflake
                    import snowflake.connector
                    con = snowflake.connector.connect(
                        user=user,
                        password=password,
                        account=account,
                        warehouse=warehouse,
                        database=database,
                        schema=schema,
                    )
                    tables = pd.read_sql("SHOW TABLES", con)
                    st.session_state["sf_con"] = con
                    st.session_state["tables"] = tables["name"].tolist()
                    toast("Connected to Snowflake", "success")
                    con.close()
                    st.stop()

                if db_type != "Snowflake":
                    tables = pd.read_sql(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')",
                        engine
                    )
                    st.session_state["engine"] = engine
                    st.session_state["tables"] = tables["table_name"].tolist()
                    toast(f"Connected to {db_type}", "success")

            except Exception as e:
                toast(f"Connection failed – {e}", "error")

        if "tables" in st.session_state:
            table = st.selectbox("Select a table to preview", st.session_state["tables"])
            if st.button("Preview Table"):
                try:
                    if db_type == "Snowflake":
                        con = snowflake.connector.connect(
                            user=user, password=password, account=account,
                            warehouse=warehouse, database=database, schema=schema
                        )
                        df = pd.read_sql(f'SELECT * FROM "{schema}"."{table}" LIMIT 100', con)
                        con.close()
                    else:
                        df = pd.read_sql(f"SELECT * FROM {table} LIMIT 100", st.session_state["engine"])

                    st.session_state["df"] = df
                    st.dataframe(df.head())
                    toast(f"Loaded {len(df)} rows from {table}", "success")

                except Exception as e:
                    toast(f"Error previewing table – {e}", "error")

    # ---- Transform, Validate, Load ----
    if df is not None:
        st.divider()
        entity = st.selectbox("Select entity", list(ENTITY_SPECS.keys()))
        mode = st.radio("Load Mode", ["append", "replace"], horizontal=True)
        mapping = suggest_column_mapping(clean_dataframe(df.copy()), entity)
        if st.button("⚙️ Transform, Validate & Load", type="primary"):
            with st.spinner("Processing data ..."):
                tdf = transform_with_mapping(df.copy(), entity, mapping)
                tdf, errs = validate_dataframe(entity, tdf)
                if errs:
                    toast("Validation issues found", "warning")
                    st.json(summarize_errors(errs))
                else:
                    toast("Validation Passed", "success")
                safe_load_to_duckdb(tdf, f"etl.{entity}")
                st.session_state["df"] = tdf
                toast(f"Loaded {len(tdf)} rows into etl.{entity}", "success")

# ------------------------------------------------------------
# 📊 Dashboard
# ------------------------------------------------------------
with tabs[2]:
    st.header("📊 Dashboard")
    if "df" not in st.session_state:
        st.warning("Upload or connect data first.")
    else:
        df = st.session_state["df"]
        num = df.select_dtypes(include=np.number).columns.tolist()
        cat = df.select_dtypes(exclude=np.number).columns.tolist()
        st.dataframe(df.describe(include="all").transpose(), use_container_width=True)
        if num:
            st.subheader("Numeric Distribution")
            c = st.selectbox("Select numeric column", num)
            st.plotly_chart(px.histogram(df, x=c, nbins=30, template=plotly_theme),
                            use_container_width=True)
        if cat:
            st.subheader("Categorical Breakdown")
            c = st.selectbox("Select categorical column", cat)
            cnt = df[c].value_counts().reset_index()
            cnt.columns = [c, "count"]
            st.plotly_chart(px.bar(cnt, x=c, y="count", template=plotly_theme),
                            use_container_width=True)

# ------------------------------------------------------------
# 💼 Business Summary
# ------------------------------------------------------------
with tabs[3]:
    st.header("💼 Business Summary")
    start = st.date_input("Start date")
    end = st.date_input("End date")
    use_saved = st.toggle("Use saved warehouse data", value=False, key="use_saved_summary")

    if "df" not in st.session_state and not use_saved:
        st.info("Upload data or enable saved mode.")
    elif use_saved:
        try:
            con = duckdb.connect(WAREHOUSE_PATH)
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='etl';").fetchdf()
            if "sales" not in tables["table_name"].tolist():
                toast("No saved sales data found", "warning")
            else:
                where, params = [], []
                if start: where.append("date::date >= ?"); params.append(start)
                if end: where.append("date::date <= ?"); params.append(end)
                ws = (" WHERE " + " AND ".join(where)) if where else ""
                total = con.execute(
                    f"SELECT COALESCE(SUM(amount),0) FROM etl.sales{ws}", params).fetchone()[0]
                avg = con.execute(
                    f"SELECT COALESCE(AVG(amount),0) FROM etl.sales{ws}", params).fetchone()[0]
                if total == 0:
                    toast("Sales table found but no data", "warning")
                else:
                    st.metric("💰 Total Revenue", f"PKR {total:,.0f}")
                    st.metric("🧾 Avg Bill", f"PKR {avg:,.0f}")
                    toast("Warehouse metrics loaded", "success")
            con.close()
        except Exception as e:
            toast(f"Database error – {e}", "error")
    else:
        df = st.session_state["df"]
        amt_col = next((c for c in ["amount", "total"] if c in df.columns), None)
        if amt_col:
            total = float(pd.to_numeric(df[amt_col], errors="coerce").fillna(0).sum())
            avg = float(pd.to_numeric(df[amt_col], errors="coerce").fillna(0).mean())
            st.metric("💰 Total Revenue", f"PKR {total:,.0f}")
            st.metric("🧾 Avg Bill", f"PKR {avg:,.0f}")

# ------------------------------------------------------------
# 📈 Metrics
# ------------------------------------------------------------
with tabs[4]:
    st.header("📈 Metrics Dashboard")
    use_saved = st.toggle("Use saved warehouse data", value=False, key="use_saved_metrics")

    if not use_saved:
        st.info("Enable saved data to view metrics."); st.stop()

    def q(sql):
        con = duckdb.connect(WAREHOUSE_PATH)
        dfq = con.execute(sql).fetch_df()
        con.close()
        return dfq

    con = duckdb.connect(WAREHOUSE_PATH)
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='etl';").fetchdf()
    con.close()
    if "sales" not in tables["table_name"].tolist():
        toast("No saved sales data found", "warning"); st.stop()

    cards = q("""
        WITH ranges AS (
            SELECT current_date AS today,
                   date_trunc('month', current_date)::date AS m_start,
                   date_trunc('quarter', current_date)::date AS q_start,
                   date_trunc('year', current_date)::date AS y_start)
        SELECT 
            SUM(CASE WHEN date::date BETWEEN m_start AND today THEN amount END) AS mtd,
            SUM(CASE WHEN date::date BETWEEN q_start AND today THEN amount END) AS qtd,
            SUM(CASE WHEN date::date BETWEEN y_start AND today THEN amount END) AS ytd
        FROM etl.sales, ranges""")
    if not cards.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("MTD Sales", f"PKR {cards['mtd'][0]:,.0f}")
        col2.metric("QTD Sales", f"PKR {cards['qtd'][0]:,.0f}")
        col3.metric("YTD Sales", f"PKR {cards['ytd'][0]:,.0f}")
        toast("Metrics calculated successfully", "success")

    trend = q("""
        SELECT date_trunc('week', date)::date AS week, SUM(amount) AS total
        FROM etl.sales GROUP BY 1 ORDER BY 1""")
    if not trend.empty:
        st.subheader("📅 Weekly Trend")
        st.plotly_chart(px.line(trend, x="week", y="total",
                                template=plotly_theme), use_container_width=True)
    top = q("SELECT product_name, SUM(amount) AS total FROM etl.sales GROUP BY 1 ORDER BY total DESC LIMIT 10")
    if not top.empty:
        st.subheader("🏆 Top Products")
        st.plotly_chart(px.bar(top, x="product_name", y="total",
                               template=plotly_theme), use_container_width=True)
