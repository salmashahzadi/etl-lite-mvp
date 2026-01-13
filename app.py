# app.py — ETL Lite v2.0 (Power BI / Superset-style Dashboard)
# ------------------------------------------------------------
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
# Page Configuration
# ============================================================
st.set_page_config(
    page_title="ETL Lite Analytics",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="expanded"
)


# ============================================================
# Custom CSS for Power BI / Superset-like UI
# ============================================================
def inject_custom_css(dark_mode=False):
    if dark_mode:
        # Dark theme colors - Purple/Indigo aesthetic
        bg_primary = "#0f0f1a"
        bg_secondary = "#1a1a2e"
        bg_card = "#1e1e32"
        sidebar_bg = "linear-gradient(180deg, #1a1a2e 0%, #0f0f1a 100%)"
        sidebar_text = "#ffffff"
        sidebar_text_muted = "rgba(255,255,255,0.6)"
        sidebar_accent = "#8b5cf6"
        text_primary = "#f1f5f9"
        text_secondary = "#cbd5e1"
        text_muted = "#64748b"
        accent = "#8b5cf6"
        accent_hover = "#a78bfa"
        accent_light = "rgba(139, 92, 246, 0.15)"
        border_color = "#2d2d44"
        success = "#10b981"
        warning = "#f59e0b"
        error = "#ef4444"
        info = "#06b6d4"
        header_bg = "linear-gradient(135deg, #312e81 0%, #4c1d95 50%, #581c87 100%)"
        nav_hover = "rgba(139, 92, 246, 0.2)"
        nav_active = "rgba(139, 92, 246, 0.3)"
    else:
        # Light theme - Clean, Professional, Calm
        bg_primary = "#f8fafc"
        bg_secondary = "#ffffff"
        bg_card = "#ffffff"
        sidebar_bg = "linear-gradient(180deg, #ffffff 0%, #f1f5f9 100%)"
        sidebar_text = "#1e293b"
        sidebar_text_muted = "#64748b"
        sidebar_accent = "#0f766e"
        text_primary = "#1e293b"
        text_secondary = "#475569"
        text_muted = "#94a3b8"
        accent = "#0f766e"  # Teal - calm, professional, trustworthy
        accent_hover = "#0d9488"
        accent_light = "rgba(15, 118, 110, 0.1)"
        border_color = "#e2e8f0"
        success = "#059669"
        warning = "#d97706"
        error = "#dc2626"
        info = "#0284c7"
        header_bg = "linear-gradient(135deg, #0f766e 0%, #0d9488 50%, #14b8a6 100%)"
        nav_hover = "rgba(15, 118, 110, 0.08)"
        nav_active = "rgba(15, 118, 110, 0.12)"

    st.markdown(f"""
    <style>
        /* Import fonts */
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        
        /* Root variables */
        :root {{
            --bg-primary: {bg_primary};
            --bg-secondary: {bg_secondary};
            --bg-card: {bg_card};
            --text-primary: {text_primary};
            --text-secondary: {text_secondary};
            --text-muted: {text_muted};
            --accent: {accent};
            --accent-hover: {accent_hover};
            --accent-light: {accent_light};
            --border: {border_color};
            --success: {success};
            --warning: {warning};
            --error: {error};
            --info: {info};
        }}
        
        /* Main app container */
        .stApp {{
            background: var(--bg-primary);
            font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif;
        }}
        
        /* Sidebar styling */
        [data-testid="stSidebar"] {{
            background: {sidebar_bg};
            border-right: 1px solid {border_color};
        }}
        
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {{
            color: {sidebar_text};
        }}
        
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {{
            color: {sidebar_text_muted} !important;
        }}
        
        [data-testid="stSidebar"] .stRadio > label {{
            color: {sidebar_text_muted} !important;
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 600;
        }}
        
        [data-testid="stSidebar"] .stRadio > div {{
            background: transparent !important;
            border: none !important;
            padding: 0 !important;
            gap: 0.25rem;
        }}
        
        [data-testid="stSidebar"] .stRadio > div > label {{
            background: transparent;
            border-radius: 8px;
            padding: 0.65rem 0.85rem;
            margin: 0;
            transition: all 0.15s ease;
            border: none;
            color: {sidebar_text} !important;
            font-weight: 500;
            font-size: 0.9rem;
        }}
        
        [data-testid="stSidebar"] .stRadio > div > label:hover {{
            background: {nav_hover};
        }}
        
        [data-testid="stSidebar"] .stRadio > div > label[data-checked="true"] {{
            background: {nav_active};
            color: {sidebar_accent} !important;
            font-weight: 600;
        }}
        
        [data-testid="stSidebar"] .stRadio > div > label > div:first-child {{
            display: none;
        }}
        
        /* Hide default streamlit elements */
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
        
        /* Main content area */
        .main .block-container {{
            padding: 1.5rem 2.5rem 2rem 2.5rem;
            max-width: 100%;
        }}
        
        /* Custom header */
        .dashboard-header {{
            background: {header_bg};
            border-radius: 16px;
            padding: 1.75rem 2rem;
            margin-bottom: 1.75rem;
            box-shadow: 0 4px 24px rgba(0,0,0,0.12);
        }}
        
        .dashboard-header h1 {{
            color: white;
            font-size: 1.65rem;
            font-weight: 700;
            margin: 0;
            letter-spacing: -0.01em;
        }}
        
        .dashboard-header p {{
            color: rgba(255,255,255,0.85);
            margin: 0.4rem 0 0 0;
            font-size: 0.9rem;
            font-weight: 400;
        }}
        
        /* KPI Cards */
        .kpi-card {{
            background: var(--bg-card);
            border-radius: 14px;
            padding: 1.35rem 1.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05), 0 1px 2px rgba(0,0,0,0.03);
            border: 1px solid var(--border);
            transition: all 0.2s ease;
            height: 100%;
        }}
        
        .kpi-card:hover {{
            box-shadow: 0 8px 25px rgba(0,0,0,0.08);
            transform: translateY(-2px);
            border-color: var(--accent);
        }}
        
        .kpi-icon {{
            width: 50px;
            height: 50px;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.4rem;
            margin-bottom: 1rem;
        }}
        
        .kpi-icon.teal {{ background: linear-gradient(135deg, #0f766e 0%, #14b8a6 100%); }}
        .kpi-icon.blue {{ background: linear-gradient(135deg, #0284c7 0%, #0ea5e9 100%); }}
        .kpi-icon.emerald {{ background: linear-gradient(135deg, #059669 0%, #10b981 100%); }}
        .kpi-icon.amber {{ background: linear-gradient(135deg, #d97706 0%, #f59e0b 100%); }}
        .kpi-icon.rose {{ background: linear-gradient(135deg, #be123c 0%, #f43f5e 100%); }}
        .kpi-icon.violet {{ background: linear-gradient(135deg, #7c3aed 0%, #a78bfa 100%); }}
        .kpi-icon.slate {{ background: linear-gradient(135deg, #475569 0%, #64748b 100%); }}
        
        .kpi-label {{
            color: var(--text-muted);
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            margin-bottom: 0.35rem;
        }}
        
        .kpi-value {{
            color: var(--text-primary);
            font-size: 1.65rem;
            font-weight: 700;
            line-height: 1.2;
            margin-bottom: 0.5rem;
        }}
        
        .kpi-change {{
            font-size: 0.75rem;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 0.25rem;
            padding: 0.3rem 0.6rem;
            border-radius: 6px;
        }}
        
        .kpi-change.positive {{
            color: var(--success);
            background: rgba(5, 150, 105, 0.1);
        }}
        
        .kpi-change.negative {{
            color: var(--error);
            background: rgba(220, 38, 38, 0.1);
        }}
        
        /* Chart containers */
        .chart-container {{
            background: var(--bg-card);
            border-radius: 14px;
            padding: 1.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            border: 1px solid var(--border);
            margin-bottom: 1rem;
        }}
        
        .chart-title {{
            color: var(--text-primary);
            font-size: 0.95rem;
            font-weight: 600;
            margin-bottom: 1rem;
            padding-bottom: 0.75rem;
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        /* Data table styling */
        .dataframe-container {{
            background: var(--bg-card);
            border-radius: 14px;
            padding: 1rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            border: 1px solid var(--border);
            overflow: hidden;
        }}
        
        /* Streamlit dataframe */
        [data-testid="stDataFrame"] {{
            border-radius: 10px;
            overflow: hidden;
        }}
        
        /* Buttons */
        .stButton > button {{
            background: var(--accent);
            color: white;
            border: none;
            border-radius: 10px;
            padding: 0.6rem 1.5rem;
            font-weight: 600;
            font-size: 0.875rem;
            transition: all 0.2s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        }}
        
        .stButton > button:hover {{
            background: var(--accent-hover);
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(15, 118, 110, 0.25);
        }}
        
        /* Selectbox & inputs */
        .stSelectbox > div > div {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 10px;
        }}
        
        .stTextInput > div > div > input {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 10px;
            color: var(--text-primary);
        }}
        
        .stTextInput > div > div > input:focus {{
            border-color: var(--accent);
            box-shadow: 0 0 0 3px var(--accent-light);
        }}
        
        /* Tabs styling */
        .stTabs {{
            background: transparent;
        }}
        
        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.25rem;
            background: var(--bg-card);
            border-radius: 12px;
            padding: 0.35rem;
            border: 1px solid var(--border);
        }}
        
        .stTabs [data-baseweb="tab"] {{
            border-radius: 8px;
            padding: 0.65rem 1.25rem;
            font-weight: 500;
            font-size: 0.875rem;
            color: var(--text-secondary);
            background: transparent;
        }}
        
        .stTabs [data-baseweb="tab"]:hover {{
            color: var(--text-primary);
            background: var(--accent-light);
        }}
        
        .stTabs [aria-selected="true"] {{
            background: var(--accent) !important;
            color: white !important;
        }}
        
        .stTabs [data-baseweb="tab-highlight"] {{
            display: none;
        }}
        
        .stTabs [data-baseweb="tab-border"] {{
            display: none;
        }}
        
        /* Section headers */
        .section-header {{
            color: var(--text-primary);
            font-size: 1rem;
            font-weight: 600;
            margin: 1.75rem 0 1rem 0;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        .section-header::after {{
            content: '';
            flex: 1;
            height: 1px;
            background: var(--border);
            margin-left: 1rem;
        }}
        
        /* Metrics in sidebar */
        .sidebar-metric {{
            background: var(--accent-light);
            border-radius: 10px;
            padding: 0.85rem 1rem;
            margin-bottom: 0.5rem;
            border: 1px solid {"rgba(139, 92, 246, 0.2)" if dark_mode else "rgba(15, 118, 110, 0.15)"};
        }}
        
        .sidebar-metric-label {{
            color: {sidebar_text_muted};
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 600;
        }}
        
        .sidebar-metric-value {{
            color: {sidebar_text};
            font-size: 1.35rem;
            font-weight: 700;
        }}
        
        /* File uploader */
        [data-testid="stFileUploader"] {{
            background: var(--bg-card);
            border: 2px dashed var(--border);
            border-radius: 14px;
            padding: 1.25rem;
        }}
        
        [data-testid="stFileUploader"]:hover {{
            border-color: var(--accent);
            background: var(--accent-light);
        }}
        
        /* Expander */
        .streamlit-expanderHeader {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 10px;
        }}
        
        /* Alert boxes */
        .stAlert {{
            border-radius: 10px;
        }}
        
        /* Empty state */
        .empty-state {{
            text-align: center;
            padding: 3.5rem 2rem;
            color: var(--text-muted);
        }}
        
        .empty-state-icon {{
            font-size: 3.5rem;
            margin-bottom: 1.25rem;
            opacity: 0.6;
        }}
        
        .empty-state-title {{
            font-size: 1.15rem;
            font-weight: 600;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }}
        
        /* Toggle styling */
        [data-testid="stSidebar"] .stCheckbox {{
            padding: 0.5rem 0;
        }}
        
        /* Scrollbar */
        ::-webkit-scrollbar {{
            width: 8px;
            height: 8px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: var(--bg-primary);
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: var(--border);
            border-radius: 4px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: var(--text-muted);
        }}
        
        /* Plotly chart backgrounds */
        .js-plotly-plot .plotly .main-svg {{
            background: transparent !important;
        }}
        
        /* Logo area */
        .sidebar-logo {{
            text-align: center;
            padding: 1.25rem 1rem 1.5rem 1rem;
        }}
        
        .sidebar-logo-icon {{
            width: 52px;
            height: 52px;
            background: {"linear-gradient(135deg, #8b5cf6 0%, #a78bfa 100%)" if dark_mode else "linear-gradient(135deg, #0f766e 0%, #14b8a6 100%)"};
            border-radius: 14px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 0.75rem auto;
            font-size: 1.5rem;
            box-shadow: 0 4px 12px {"rgba(139, 92, 246, 0.3)" if dark_mode else "rgba(15, 118, 110, 0.25)"};
        }}
        
        .sidebar-logo h2 {{
            color: {sidebar_text};
            margin: 0;
            font-size: 1.25rem;
            font-weight: 700;
        }}
        
        .sidebar-logo p {{
            color: {sidebar_text_muted};
            font-size: 0.8rem;
            margin: 0.25rem 0 0 0;
        }}
        
        .sidebar-divider {{
            height: 1px;
            background: {border_color};
            margin: 0.75rem 0 1.25rem 0;
        }}
        
        .sidebar-section-label {{
            color: {sidebar_text_muted};
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 600;
            margin-bottom: 0.5rem;
            padding: 0 0.25rem;
        }}
    </style>
    """, unsafe_allow_html=True)


# ============================================================
# Chart Color Palettes
# ============================================================
def get_chart_colors(dark_mode=False):
    if dark_mode:
        return {
            'primary': ['#8b5cf6', '#a78bfa', '#c4b5fd', '#ddd6fe'],
            'categorical': ['#8b5cf6', '#06b6d4', '#10b981', '#f59e0b', '#f43f5e', '#ec4899', '#6366f1', '#14b8a6'],
            'sequential': ['#1e1b4b', '#312e81', '#4338ca', '#6366f1', '#818cf8', '#a5b4fc', '#c7d2fe', '#e0e7ff'],
            'diverging': ['#f43f5e', '#fb7185', '#fda4af', '#fecdd3', '#d1fae5', '#6ee7b7', '#34d399', '#10b981'],
            'bg': '#1e1e32',
            'paper_bg': '#1a1a2e',
            'grid': 'rgba(255,255,255,0.08)',
            'text': '#f1f5f9'
        }
    else:
        # Teal-based professional palette for light mode
        return {
            'primary': ['#0f766e', '#14b8a6', '#5eead4', '#99f6e4'],
            'categorical': ['#0f766e', '#0284c7', '#059669', '#d97706', '#be123c', '#7c3aed', '#0891b2', '#65a30d'],
            'sequential': ['#f0fdfa', '#ccfbf1', '#99f6e4', '#5eead4', '#2dd4bf', '#14b8a6', '#0d9488', '#0f766e'],
            'diverging': ['#be123c', '#e11d48', '#f43f5e', '#fecdd3', '#d1fae5', '#6ee7b7', '#34d399', '#059669'],
            'bg': '#ffffff',
            'paper_bg': '#ffffff',
            'grid': 'rgba(0,0,0,0.05)',
            'text': '#1e293b'
        }


def create_chart_layout(title="", dark_mode=False, height=350):
    colors = get_chart_colors(dark_mode)
    return dict(
        title=dict(text=title, font=dict(size=14, color=colors['text'], family="Inter")),
        font=dict(family="Inter", size=12, color=colors['text']),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=height,
        margin=dict(l=40, r=40, t=50, b=40),
        xaxis=dict(
            gridcolor=colors['grid'],
            linecolor=colors['grid'],
            tickfont=dict(size=11),
            title_font=dict(size=12)
        ),
        yaxis=dict(
            gridcolor=colors['grid'],
            linecolor=colors['grid'],
            tickfont=dict(size=11),
            title_font=dict(size=12)
        ),
        legend=dict(
            bgcolor='rgba(0,0,0,0)',
            font=dict(size=11)
        ),
        hoverlabel=dict(
            bgcolor=colors['bg'],
            font_size=12,
            font_family="Inter"
        )
    )


# ============================================================
# KPI Card Components
# ============================================================
def render_kpi_card(icon, label, value, change=None, change_type="positive", color="teal"):
    change_html = ""
    if change is not None:
        arrow = "↑" if change_type == "positive" else "↓"
        change_html = f'<div class="kpi-change {change_type}">{arrow} {change}</div>'
    
    return f"""
    <div class="kpi-card">
        <div class="kpi-icon {color}">{icon}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {change_html}
    </div>
    """


def render_section_header(icon, title):
    return f'<div class="section-header">{icon} {title}</div>'


def render_empty_state(icon, title, message):
    return f"""
    <div class="empty-state">
        <div class="empty-state-icon">{icon}</div>
        <div class="empty-state-title">{title}</div>
        <p>{message}</p>
    </div>
    """


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
# Password Check
# ============================================================
if APP_PASSWORD:
    if "_pw_ok" not in st.session_state:
        st.session_state["_pw_ok"] = False
    if not st.session_state["_pw_ok"]:
        st.markdown("""
        <div style="display:flex;justify-content:center;align-items:center;min-height:80vh;">
            <div style="background:white;padding:2rem;border-radius:16px;box-shadow:0 4px 20px rgba(0,0,0,0.1);width:360px;text-align:center;">
                <div style="font-size:3rem;margin-bottom:1rem;">🔐</div>
                <h2 style="color:#1e293b;margin-bottom:1.5rem;">ETL Lite Analytics</h2>
            </div>
        </div>
        """, unsafe_allow_html=True)
        pw = st.text_input("Enter Password", type="password", key="pw_input")
        if pw == APP_PASSWORD:
            st.session_state["_pw_ok"] = True
            st.rerun()
        st.stop()


# ============================================================
# Initialize theme
# ============================================================
if "dark_mode" not in st.session_state:
    st.session_state["dark_mode"] = False

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Dashboard"


# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    # Logo and branding
    st.markdown(f"""
    <div class="sidebar-logo">
        <div class="sidebar-logo-icon">📊</div>
        <h2>ETL Lite</h2>
        <p>Analytics Dashboard</p>
    </div>
    <div class="sidebar-divider"></div>
    """, unsafe_allow_html=True)
    
    # Navigation
    st.markdown('<div class="sidebar-section-label">Navigation</div>', unsafe_allow_html=True)
    
    page = st.radio(
        "Menu",
        ["◉ Dashboard", "↓ Data Import", "⊞ Analytics", "◈ Business", "⚡ Quick Load"],
        label_visibility="collapsed"
    )
    st.session_state["current_page"] = page
    
    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    
    # Theme toggle
    st.markdown('<div class="sidebar-section-label">Appearance</div>', unsafe_allow_html=True)
    
    dark_mode = st.toggle("Dark Mode", value=st.session_state["dark_mode"])
    st.session_state["dark_mode"] = dark_mode
    
    # Data status
    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-section-label">Data Status</div>', unsafe_allow_html=True)
    
    df = st.session_state.get("df")
    if df is not None:
        st.markdown(f"""
        <div class="sidebar-metric">
            <div class="sidebar-metric-label">Rows</div>
            <div class="sidebar-metric-value">{len(df):,}</div>
        </div>
        <div class="sidebar-metric">
            <div class="sidebar-metric-label">Columns</div>
            <div class="sidebar-metric-value">{len(df.columns)}</div>
        </div>
        """, unsafe_allow_html=True)
        source_label = st.session_state.get('df_label', 'Unknown')
        st.caption(f"Source: {source_label}")
    else:
        st.markdown(f"""
        <div class="sidebar-metric" style="text-align:center;">
            <div class="sidebar-metric-label">Status</div>
            <div style="font-size:0.9rem;color:var(--text-muted);margin-top:0.25rem;">No data loaded</div>
        </div>
        """, unsafe_allow_html=True)


# Apply custom CSS
inject_custom_css(dark_mode)
colors = get_chart_colors(dark_mode)


# ============================================================
# Dashboard Page
# ============================================================
if "Dashboard" in page or page == "◉ Dashboard":
    # Header
    st.markdown("""
    <div class="dashboard-header">
        <h1>📊 Analytics Dashboard</h1>
        <p>Real-time insights and data visualization</p>
    </div>
    """, unsafe_allow_html=True)
    
    df = st.session_state.get("df")
    
    if df is None:
        st.markdown(render_empty_state(
            "📂",
            "No Data Loaded",
            "Load data from the Quick Load or Data Ingestion page to see your analytics dashboard."
        ), unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("⚡ Quick Load Sample Data", use_container_width=True):
                sample_df = pd.read_csv("data/samples/sales_sample.csv")
                load_replace_to_duckdb(sample_df, "etl.sales_sample_staging")
                set_active_df(sample_df, "sample:sales")
                st.rerun()
    else:
        # Identify columns
        num_cols = df.select_dtypes(include=np.number).columns.tolist()
        cat_cols = df.select_dtypes(exclude=np.number).columns.tolist()
        date_col = next((c for c in df.columns if "date" in c.lower()), None)
        amt_col = next((c for c in df.columns if "amount" in c.lower() or "total" in c.lower()), None)
        qty_col = next((c for c in df.columns if "quantity" in c.lower() or "qty" in c.lower()), None)
        prod_col = next((c for c in df.columns if "product" in c.lower()), None)
        cat_col = next((c for c in df.columns if "category" in c.lower()), None)
        customer_col = next((c for c in df.columns if "customer" in c.lower()), None)
        branch_col = next((c for c in df.columns if "branch" in c.lower()), None)
        
        # KPI Cards Row
        st.markdown(render_section_header("📌", "Key Metrics"), unsafe_allow_html=True)
        
        kpi_cols = st.columns(4)
        
        # Total Revenue
        if amt_col:
            total_revenue = pd.to_numeric(df[amt_col], errors="coerce").fillna(0).sum()
            with kpi_cols[0]:
                st.markdown(render_kpi_card("💰", "Total Revenue", f"PKR {total_revenue:,.0f}", None, "positive", "teal"), unsafe_allow_html=True)
        
        # Total Transactions
        with kpi_cols[1]:
            st.markdown(render_kpi_card("📦", "Transactions", f"{len(df):,}", None, "positive", "blue"), unsafe_allow_html=True)
        
        # Average Transaction
        if amt_col:
            avg_transaction = pd.to_numeric(df[amt_col], errors="coerce").fillna(0).mean()
            with kpi_cols[2]:
                st.markdown(render_kpi_card("📊", "Avg Transaction", f"PKR {avg_transaction:,.0f}", None, "positive", "emerald"), unsafe_allow_html=True)
        
        # Unique Products/Categories
        if prod_col:
            unique_products = df[prod_col].nunique()
            with kpi_cols[3]:
                st.markdown(render_kpi_card("🏷️", "Products", f"{unique_products}", None, "positive", "amber"), unsafe_allow_html=True)
        elif cat_col:
            unique_cats = df[cat_col].nunique()
            with kpi_cols[3]:
                st.markdown(render_kpi_card("📁", "Categories", f"{unique_cats}", None, "positive", "amber"), unsafe_allow_html=True)
        else:
            with kpi_cols[3]:
                st.markdown(render_kpi_card("📋", "Columns", f"{len(df.columns)}", None, "positive", "slate"), unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Charts Row 1
        chart_col1, chart_col2 = st.columns(2)
        
        # Revenue Trend or Numeric Distribution
        with chart_col1:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<div class="chart-title">📈 Revenue Trend</div>', unsafe_allow_html=True)
            
            if date_col and amt_col:
                trend_df = df.copy()
                trend_df[date_col] = pd.to_datetime(trend_df[date_col], errors="coerce")
                trend_df = trend_df.dropna(subset=[date_col])
                trend_agg = trend_df.groupby(pd.Grouper(key=date_col, freq="D"))[amt_col].sum().reset_index()
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=trend_agg[date_col],
                    y=trend_agg[amt_col],
                    mode='lines+markers',
                    fill='tozeroy',
                    line=dict(color=colors['categorical'][0], width=2),
                    marker=dict(size=6, color=colors['categorical'][0]),
                    fillcolor=f"rgba({59 if not dark_mode else 139}, {130 if not dark_mode else 92}, {246 if not dark_mode else 246}, 0.1)"
                ))
                fig.update_layout(**create_chart_layout("", dark_mode, 300))
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            elif num_cols:
                selected_num = st.selectbox("Select metric", num_cols, key="dashboard_num")
                fig = px.histogram(
                    df, x=selected_num, nbins=20,
                    color_discrete_sequence=[colors['categorical'][0]]
                )
                fig.update_layout(**create_chart_layout("", dark_mode, 300))
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Category Distribution (Donut Chart)
        with chart_col2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<div class="chart-title">🍩 Category Distribution</div>', unsafe_allow_html=True)
            
            if cat_col and amt_col:
                cat_agg = df.groupby(cat_col)[amt_col].sum().reset_index()
                fig = go.Figure(data=[go.Pie(
                    labels=cat_agg[cat_col],
                    values=cat_agg[amt_col],
                    hole=0.55,
                    marker=dict(colors=colors['categorical']),
                    textinfo='label+percent',
                    textposition='outside',
                    textfont=dict(size=11, color=colors['text'])
                )])
                fig.update_layout(**create_chart_layout("", dark_mode, 300))
                fig.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.15))
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            elif cat_cols:
                selected_cat = st.selectbox("Select category", cat_cols, key="dashboard_cat")
                cat_counts = df[selected_cat].astype(str).value_counts().head(8).reset_index()
                cat_counts.columns = [selected_cat, "count"]
                fig = go.Figure(data=[go.Pie(
                    labels=cat_counts[selected_cat],
                    values=cat_counts["count"],
                    hole=0.55,
                    marker=dict(colors=colors['categorical']),
                    textinfo='label+percent',
                    textposition='outside',
                    textfont=dict(size=11, color=colors['text'])
                )])
                fig.update_layout(**create_chart_layout("", dark_mode, 300))
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Charts Row 2
        chart_col3, chart_col4 = st.columns(2)
        
        # Top Products Bar Chart
        with chart_col3:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<div class="chart-title">🏆 Top Performers</div>', unsafe_allow_html=True)
            
            if prod_col and amt_col:
                top_products = df.groupby(prod_col)[amt_col].sum().reset_index()
                top_products = top_products.sort_values(by=amt_col, ascending=True).tail(8)
                
                fig = go.Figure(data=[go.Bar(
                    x=top_products[amt_col],
                    y=top_products[prod_col],
                    orientation='h',
                    marker=dict(
                        color=top_products[amt_col],
                        colorscale=[[0, colors['categorical'][0]], [1, colors['categorical'][4]]],
                        cornerradius=4
                    ),
                    text=top_products[amt_col].apply(lambda x: f"PKR {x:,.0f}"),
                    textposition='auto',
                    textfont=dict(size=10, color='white')
                )])
                fig.update_layout(**create_chart_layout("", dark_mode, 300))
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            elif cat_cols:
                selected = st.selectbox("Select dimension", cat_cols, key="top_dim")
                counts = df[selected].astype(str).value_counts().head(8)
                fig = go.Figure(data=[go.Bar(
                    x=counts.values,
                    y=counts.index,
                    orientation='h',
                    marker=dict(color=colors['categorical'][0], cornerradius=4)
                )])
                fig.update_layout(**create_chart_layout("", dark_mode, 300))
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Branch Performance
        with chart_col4:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<div class="chart-title">🏢 Branch Analysis</div>', unsafe_allow_html=True)
            
            if branch_col and amt_col:
                branch_agg = df.groupby(branch_col).agg({
                    amt_col: 'sum',
                }).reset_index()
                branch_agg['count'] = df.groupby(branch_col).size().values
                
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=branch_agg[branch_col],
                    y=branch_agg[amt_col],
                    name='Revenue',
                    marker=dict(color=colors['categorical'][0], cornerradius=4),
                    text=branch_agg[amt_col].apply(lambda x: f"PKR {x:,.0f}"),
                    textposition='outside',
                    textfont=dict(size=10)
                ))
                fig.update_layout(**create_chart_layout("", dark_mode, 300))
                fig.update_layout(showlegend=False, bargap=0.3)
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            elif num_cols and len(num_cols) >= 2:
                x_col = st.selectbox("X-axis", num_cols, key="scatter_x")
                y_col = st.selectbox("Y-axis", [c for c in num_cols if c != x_col], key="scatter_y")
                fig = px.scatter(df, x=x_col, y=y_col, color_discrete_sequence=[colors['categorical'][0]])
                fig.update_layout(**create_chart_layout("", dark_mode, 300))
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            else:
                st.info("Add branch data to see branch analysis")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Data Table Section
        st.markdown(render_section_header("📋", "Data Preview"), unsafe_allow_html=True)
        st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
        st.dataframe(
            df.head(50),
            use_container_width=True,
            hide_index=True,
            height=400
        )
        st.markdown('</div>', unsafe_allow_html=True)


# ============================================================
# Data Ingestion Page
# ============================================================
elif "Data Ingestion" in page or "Data Import" in page:
    st.markdown("""
    <div class="dashboard-header">
        <h1>📥 Data Ingestion</h1>
        <p>Import data from multiple sources</p>
    </div>
    """, unsafe_allow_html=True)
    
    source_tabs = st.tabs(["📤 Upload File", "🔗 Google Sheets", "🗄️ Database"])
    
    # Upload File Tab
    with source_tabs[0]:
        st.markdown(render_section_header("📤", "Upload File"), unsafe_allow_html=True)
        
        up = st.file_uploader("Drop your CSV or Excel file here", type=["csv", "xlsx"], key="file_upload")
        
        if up:
            try:
                if up.size > MAX_UPLOAD_MB * 1024 * 1024:
                    st.error(f"⚠️ File too large (max {MAX_UPLOAD_MB} MB)")
                    st.stop()
                
                with st.spinner("Processing file..."):
                    df = load_csv_cached(up) if up.name.endswith(".csv") else load_excel_cached(up)
                    load_replace_to_duckdb(df, "etl.uploaded_data")
                    set_active_df(df, f"upload:{up.name}")
                
                st.success(f"✅ Successfully loaded {len(df):,} rows from {up.name}")
                
                st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
                st.dataframe(df.head(20), use_container_width=True, hide_index=True)
                st.markdown('</div>', unsafe_allow_html=True)
                
            except Exception as e:
                st.error(f"❌ Upload failed – {e}")
    
    # Google Sheets Tab
    with source_tabs[1]:
        st.markdown(render_section_header("🔗", "Google Sheets"), unsafe_allow_html=True)
        
        url = st.text_input("Paste Google Sheet URL (ensure 'Anyone with link' can view)", key="gsheet_url")
        
        if url and st.button("Load from Google Sheets", key="load_gsheet"):
            try:
                with st.spinner("Fetching data from Google Sheets..."):
                    if "spreadsheets/d/" in url:
                        sid = url.split("/d/")[1].split("/")[0]
                        url = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv"
                    df = pd.read_csv(url)
                    load_replace_to_duckdb(df, "etl.uploaded_data")
                    set_active_df(df, "google_sheets")
                
                st.success(f"✅ Loaded {len(df):,} rows from Google Sheets")
                
                st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
                st.dataframe(df.head(20), use_container_width=True, hide_index=True)
                st.markdown('</div>', unsafe_allow_html=True)
                
            except Exception as e:
                st.error(f"❌ Failed to load – {e}")
    
    # Database Tab
    with source_tabs[2]:
        st.markdown(render_section_header("🗄️", "Database Connection"), unsafe_allow_html=True)
        
        db_type = st.selectbox("Database Type", ["MySQL", "PostgreSQL", "Snowflake"], key="db_type")
        
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input("Host / Account", key="db_host")
            user = st.text_input("Username", key="db_user")
            database = st.text_input("Database", key="db_name")
        with col2:
            password = st.text_input("Password", type="password", key="db_pass")
            schema = st.text_input("Schema (optional)", key="db_schema")
            if db_type == "Snowflake":
                warehouse = st.text_input("Warehouse", key="db_warehouse")
            else:
                warehouse = None
        
        if "db_connected" not in st.session_state:
            st.session_state["db_connected"] = False
        if "db_tables" not in st.session_state:
            st.session_state["db_tables"] = []
        
        if st.button("🔌 Connect", key="connect_db"):
            try:
                with st.spinner("Connecting..."):
                    if db_type == "MySQL":
                        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
                        tables = pd.read_sql("SHOW TABLES", engine)
                        st.session_state["db_tables"] = tables.iloc[:, 0].astype(str).tolist()
                        st.session_state["active_con"] = engine
                    elif db_type == "PostgreSQL":
                        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
                        tables = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema='public'", engine)
                        st.session_state["db_tables"] = tables["table_name"].astype(str).tolist()
                        st.session_state["active_con"] = engine
                    elif db_type == "Snowflake":
                        import snowflake.connector
                        con = snowflake.connector.connect(
                            user=user, password=password, account=host,
                            warehouse=warehouse or None, database=database or None, schema=schema or None
                        )
                        tables = pd.read_sql("SHOW TABLES", con)
                        name_col = [c for c in tables.columns if c.lower() == "name"][0]
                        st.session_state["db_tables"] = tables[name_col].astype(str).tolist()
                        st.session_state["active_con"] = con
                    
                    st.session_state["db_connected"] = True
                    st.session_state["db_type"] = db_type
                    st.session_state["db_database"] = database
                    st.session_state["db_schema"] = schema
                
                st.success(f"✅ Connected to {db_type}")
            except Exception as e:
                st.error(f"❌ Connection failed – {e}")
        
        if st.session_state.get("db_connected") and st.session_state.get("db_tables"):
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(render_section_header("📋", "Select Table"), unsafe_allow_html=True)
            
            selected_table = st.selectbox("Available tables", st.session_state["db_tables"], key="select_table")
            
            if st.button("📥 Load Table", key="load_table"):
                try:
                    with st.spinner("Loading table..."):
                        con = st.session_state["active_con"]
                        db_type = st.session_state["db_type"]
                        database = st.session_state["db_database"]
                        schema = st.session_state["db_schema"]
                        
                        if db_type == "Snowflake":
                            query = f'SELECT * FROM "{database}"."{schema}"."{selected_table}" LIMIT 100'
                        elif db_type == "PostgreSQL":
                            query = f'SELECT * FROM public."{selected_table}" LIMIT 100'
                        else:
                            query = f"SELECT * FROM `{selected_table}` LIMIT 100"
                        
                        df = pd.read_sql(query, con)
                        load_replace_to_duckdb(df, f"etl.{db_type.lower()}_{selected_table}")
                        set_active_df(df, f"db:{db_type}:{selected_table}")
                    
                    st.success(f"✅ Loaded {len(df):,} rows from {selected_table}")
                    
                    st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
                    st.dataframe(df.head(20), use_container_width=True, hide_index=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                except Exception as e:
                    st.error(f"❌ Load failed – {e}")


# ============================================================
# Analytics Page
# ============================================================
elif "Analytics" in page or page == "⊞ Analytics":
    st.markdown("""
    <div class="dashboard-header">
        <h1>📈 Advanced Analytics</h1>
        <p>Deep dive into your data with interactive visualizations</p>
    </div>
    """, unsafe_allow_html=True)
    
    df = st.session_state.get("df")
    
    if df is None:
        st.markdown(render_empty_state(
            "📊",
            "No Data Available",
            "Load data first to access analytics features."
        ), unsafe_allow_html=True)
    else:
        num_cols = df.select_dtypes(include=np.number).columns.tolist()
        cat_cols = df.select_dtypes(exclude=np.number).columns.tolist()
        date_col = next((c for c in df.columns if "date" in c.lower()), None)
        amt_col = next((c for c in df.columns if "amount" in c.lower() or "total" in c.lower()), None)
        
        # Filter Panel
        st.markdown(render_section_header("🎛️", "Filters"), unsafe_allow_html=True)
        
        filter_cols = st.columns(4)
        filtered_df = df.copy()
        
        with filter_cols[0]:
            if cat_cols:
                filter_cat = st.selectbox("Filter by", ["None"] + cat_cols, key="filter_cat")
                if filter_cat != "None":
                    values = df[filter_cat].unique().tolist()
                    selected_values = st.multiselect("Values", values, default=values[:5] if len(values) > 5 else values)
                    if selected_values:
                        filtered_df = filtered_df[filtered_df[filter_cat].isin(selected_values)]
        
        with filter_cols[1]:
            if date_col:
                filtered_df[date_col] = pd.to_datetime(filtered_df[date_col], errors="coerce")
                min_date = filtered_df[date_col].min()
                max_date = filtered_df[date_col].max()
                if pd.notna(min_date) and pd.notna(max_date):
                    date_range = st.date_input("Date Range", value=(min_date, max_date), key="date_filter")
                    if len(date_range) == 2:
                        filtered_df = filtered_df[
                            (filtered_df[date_col] >= pd.Timestamp(date_range[0])) & 
                            (filtered_df[date_col] <= pd.Timestamp(date_range[1]))
                        ]
        
        st.caption(f"Showing {len(filtered_df):,} of {len(df):,} records")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Chart Builder
        st.markdown(render_section_header("📊", "Chart Builder"), unsafe_allow_html=True)
        
        chart_type = st.selectbox("Chart Type", ["Bar Chart", "Line Chart", "Scatter Plot", "Pie Chart", "Histogram", "Box Plot", "Heatmap"], key="chart_type")
        
        builder_cols = st.columns(3)
        
        with builder_cols[0]:
            x_axis = st.selectbox("X-Axis", df.columns.tolist(), key="x_axis")
        with builder_cols[1]:
            y_axis = st.selectbox("Y-Axis", ["Count"] + num_cols, key="y_axis")
        with builder_cols[2]:
            color_by = st.selectbox("Color By", ["None"] + cat_cols, key="color_by")
        
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        
        try:
            color_col = None if color_by == "None" else color_by
            
            if chart_type == "Bar Chart":
                if y_axis == "Count":
                    agg_df = filtered_df.groupby(x_axis).size().reset_index(name='count')
                    fig = px.bar(agg_df, x=x_axis, y='count', color=color_col if color_col and color_col in agg_df.columns else None,
                                color_discrete_sequence=colors['categorical'])
                else:
                    agg_df = filtered_df.groupby(x_axis)[y_axis].sum().reset_index()
                    fig = px.bar(agg_df, x=x_axis, y=y_axis, color_discrete_sequence=colors['categorical'])
                    
            elif chart_type == "Line Chart":
                if y_axis == "Count":
                    agg_df = filtered_df.groupby(x_axis).size().reset_index(name='count')
                    fig = px.line(agg_df, x=x_axis, y='count', color_discrete_sequence=colors['categorical'], markers=True)
                else:
                    agg_df = filtered_df.groupby(x_axis)[y_axis].sum().reset_index()
                    fig = px.line(agg_df, x=x_axis, y=y_axis, color_discrete_sequence=colors['categorical'], markers=True)
                    
            elif chart_type == "Scatter Plot":
                y_col = y_axis if y_axis != "Count" else num_cols[0] if num_cols else x_axis
                fig = px.scatter(filtered_df, x=x_axis, y=y_col, color=color_col,
                               color_discrete_sequence=colors['categorical'])
                               
            elif chart_type == "Pie Chart":
                if y_axis == "Count":
                    agg_df = filtered_df[x_axis].value_counts().reset_index()
                    agg_df.columns = [x_axis, 'count']
                    fig = px.pie(agg_df, names=x_axis, values='count', color_discrete_sequence=colors['categorical'], hole=0.4)
                else:
                    agg_df = filtered_df.groupby(x_axis)[y_axis].sum().reset_index()
                    fig = px.pie(agg_df, names=x_axis, values=y_axis, color_discrete_sequence=colors['categorical'], hole=0.4)
                    
            elif chart_type == "Histogram":
                fig = px.histogram(filtered_df, x=x_axis, color=color_col,
                                 color_discrete_sequence=colors['categorical'], nbins=30)
                                 
            elif chart_type == "Box Plot":
                fig = px.box(filtered_df, x=color_col if color_col else x_axis, y=x_axis if y_axis == "Count" else y_axis,
                           color_discrete_sequence=colors['categorical'])
                           
            elif chart_type == "Heatmap":
                if len(num_cols) >= 2:
                    corr_df = filtered_df[num_cols].corr()
                    fig = px.imshow(corr_df, color_continuous_scale='RdBu_r', aspect='auto')
                else:
                    fig = go.Figure()
                    fig.add_annotation(text="Need 2+ numeric columns for heatmap", showarrow=False)
            
            fig.update_layout(**create_chart_layout("", dark_mode, 400))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True})
            
        except Exception as e:
            st.error(f"Chart error: {e}")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Trend Analysis
        if date_col and amt_col:
            st.markdown(render_section_header("📅", "Time Series Analysis"), unsafe_allow_html=True)
            
            trend_cols = st.columns([2, 1])
            
            with trend_cols[0]:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.markdown('<div class="chart-title">Weekly Trend</div>', unsafe_allow_html=True)
                
                trend_df = filtered_df.copy()
                trend_df[date_col] = pd.to_datetime(trend_df[date_col], errors="coerce")
                weekly = trend_df.groupby(pd.Grouper(key=date_col, freq="W"))[amt_col].agg(['sum', 'count']).reset_index()
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=weekly[date_col], y=weekly['sum'],
                    name='Revenue',
                    mode='lines+markers',
                    line=dict(color=colors['categorical'][0], width=2),
                    fill='tozeroy',
                    fillcolor=f"rgba(59, 130, 246, 0.1)"
                ))
                fig.update_layout(**create_chart_layout("", dark_mode, 350))
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            with trend_cols[1]:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.markdown('<div class="chart-title">Summary Stats</div>', unsafe_allow_html=True)
                
                stats = {
                    "Total": f"PKR {filtered_df[amt_col].sum():,.0f}",
                    "Average": f"PKR {filtered_df[amt_col].mean():,.0f}",
                    "Median": f"PKR {filtered_df[amt_col].median():,.0f}",
                    "Max": f"PKR {filtered_df[amt_col].max():,.0f}",
                    "Min": f"PKR {filtered_df[amt_col].min():,.0f}",
                    "Std Dev": f"PKR {filtered_df[amt_col].std():,.0f}"
                }
                
                for label, value in stats.items():
                    st.markdown(f"""
                    <div style="display:flex;justify-content:space-between;padding:0.5rem 0;border-bottom:1px solid var(--border);">
                        <span style="color:var(--text-secondary);font-size:0.85rem;">{label}</span>
                        <span style="color:var(--text-primary);font-weight:600;font-size:0.85rem;">{value}</span>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown('</div>', unsafe_allow_html=True)


# ============================================================
# Business Summary Page
# ============================================================
elif "Business Summary" in page or "Business" in page:
    st.markdown("""
    <div class="dashboard-header">
        <h1>💼 Business Summary</h1>
        <p>Executive overview of key business metrics</p>
    </div>
    """, unsafe_allow_html=True)
    
    df = st.session_state.get("df")
    
    if df is None:
        st.markdown(render_empty_state(
            "💼",
            "No Business Data",
            "Load your sales or transaction data to see business insights."
        ), unsafe_allow_html=True)
    else:
        amt_col = next((c for c in df.columns if "amount" in c.lower() or "total" in c.lower()), None)
        date_col = next((c for c in df.columns if "date" in c.lower()), None)
        prod_col = next((c for c in df.columns if "product" in c.lower()), None)
        customer_col = next((c for c in df.columns if "customer" in c.lower()), None)
        staff_col = next((c for c in df.columns if "staff" in c.lower()), None)
        
        if amt_col:
            amounts = pd.to_numeric(df[amt_col], errors="coerce").fillna(0)
            
            # Executive KPIs
            st.markdown(render_section_header("📊", "Executive KPIs"), unsafe_allow_html=True)
            
            kpi_row = st.columns(5)
            
            with kpi_row[0]:
                st.markdown(render_kpi_card("💰", "Total Revenue", f"PKR {amounts.sum():,.0f}", None, "positive", "teal"), unsafe_allow_html=True)
            
            with kpi_row[1]:
                st.markdown(render_kpi_card("🧾", "Avg Transaction", f"PKR {amounts.mean():,.0f}", None, "positive", "emerald"), unsafe_allow_html=True)
            
            with kpi_row[2]:
                st.markdown(render_kpi_card("📦", "Transactions", f"{len(df):,}", None, "positive", "blue"), unsafe_allow_html=True)
            
            with kpi_row[3]:
                if customer_col:
                    st.markdown(render_kpi_card("👥", "Customers", f"{df[customer_col].nunique():,}", None, "positive", "violet"), unsafe_allow_html=True)
                else:
                    st.markdown(render_kpi_card("📈", "Max Sale", f"PKR {amounts.max():,.0f}", None, "positive", "violet"), unsafe_allow_html=True)
            
            with kpi_row[4]:
                if prod_col:
                    st.markdown(render_kpi_card("🏷️", "Products", f"{df[prod_col].nunique():,}", None, "positive", "amber"), unsafe_allow_html=True)
                else:
                    st.markdown(render_kpi_card("📉", "Min Sale", f"PKR {amounts.min():,.0f}", None, "positive", "rose"), unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Performance Charts
            perf_cols = st.columns(2)
            
            with perf_cols[0]:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.markdown('<div class="chart-title">🏆 Top Products by Revenue</div>', unsafe_allow_html=True)
                
                if prod_col:
                    top_prods = df.groupby(prod_col)[amt_col].sum().reset_index()
                    top_prods = top_prods.sort_values(by=amt_col, ascending=False).head(10)
                    
                    fig = go.Figure(data=[go.Bar(
                        x=top_prods[prod_col],
                        y=top_prods[amt_col],
                        marker=dict(
                            color=top_prods[amt_col],
                            colorscale=[[0, colors['categorical'][1]], [1, colors['categorical'][0]]],
                            cornerradius=6
                        ),
                        text=top_prods[amt_col].apply(lambda x: f"PKR {x:,.0f}"),
                        textposition='outside',
                        textfont=dict(size=10)
                    )])
                    fig.update_layout(**create_chart_layout("", dark_mode, 350))
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
                else:
                    st.info("No product column found")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            with perf_cols[1]:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.markdown('<div class="chart-title">👤 Staff Performance</div>', unsafe_allow_html=True)
                
                if staff_col:
                    staff_perf = df.groupby(staff_col)[amt_col].sum().reset_index()
                    staff_perf = staff_perf.sort_values(by=amt_col, ascending=False)
                    
                    fig = go.Figure(data=[go.Bar(
                        x=staff_perf[staff_col],
                        y=staff_perf[amt_col],
                        marker=dict(
                            color=colors['categorical'][:len(staff_perf)],
                            cornerradius=6
                        ),
                        text=staff_perf[amt_col].apply(lambda x: f"PKR {x:,.0f}"),
                        textposition='outside',
                        textfont=dict(size=10)
                    )])
                    fig.update_layout(**create_chart_layout("", dark_mode, 350))
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
                else:
                    st.info("No staff column found")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Revenue Gauge
            if amounts.sum() > 0:
                st.markdown(render_section_header("🎯", "Revenue Gauge"), unsafe_allow_html=True)
                
                gauge_cols = st.columns([2, 1])
                
                with gauge_cols[0]:
                    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                    
                    total = amounts.sum()
                    target = total * 1.2  # 20% above as target
                    
                    fig = go.Figure(go.Indicator(
                        mode="gauge+number+delta",
                        value=total,
                        number={'prefix': "PKR ", 'font': {'size': 32, 'color': colors['text']}},
                        delta={'reference': target * 0.8, 'valueformat': '.0f', 'prefix': 'PKR '},
                        gauge={
                            'axis': {'range': [0, target], 'tickwidth': 1, 'tickcolor': colors['text']},
                            'bar': {'color': colors['categorical'][0]},
                            'bgcolor': colors['grid'],
                            'borderwidth': 0,
                            'steps': [
                                {'range': [0, target * 0.5], 'color': 'rgba(239, 68, 68, 0.2)'},
                                {'range': [target * 0.5, target * 0.8], 'color': 'rgba(245, 158, 11, 0.2)'},
                                {'range': [target * 0.8, target], 'color': 'rgba(34, 197, 94, 0.2)'}
                            ],
                            'threshold': {
                                'line': {'color': colors['categorical'][1], 'width': 4},
                                'thickness': 0.75,
                                'value': target * 0.8
                            }
                        }
                    ))
                    fig.update_layout(**create_chart_layout("", dark_mode, 300))
                    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
                    
                    st.markdown('</div>', unsafe_allow_html=True)
                
                with gauge_cols[1]:
                    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                    st.markdown('<div class="chart-title">Performance Breakdown</div>', unsafe_allow_html=True)
                    
                    perf_data = {
                        "Target": f"PKR {target:,.0f}",
                        "Achieved": f"PKR {total:,.0f}",
                        "Progress": f"{(total/target)*100:.1f}%",
                        "Gap": f"PKR {max(0, target - total):,.0f}"
                    }
                    
                    for label, value in perf_data.items():
                        st.markdown(f"""
                        <div style="display:flex;justify-content:space-between;padding:0.75rem 0;border-bottom:1px solid var(--border);">
                            <span style="color:var(--text-secondary);">{label}</span>
                            <span style="color:var(--text-primary);font-weight:600;">{value}</span>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.warning("⚠️ No numeric amount column found. Please ensure your data has an 'amount' or 'total' column.")


# ============================================================
# Quick Load Page
# ============================================================
elif "Quick Load" in page or page == "⚡ Quick Load":
    st.markdown("""
    <div class="dashboard-header">
        <h1>⚡ Quick Load</h1>
        <p>Instantly load sample datasets to explore the dashboard</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown(render_section_header("📁", "Sample Datasets"), unsafe_allow_html=True)
    
    sample_cards = st.columns(3)
    
    with sample_cards[0]:
        st.markdown("""
        <div class="kpi-card">
            <div class="kpi-icon teal">💰</div>
            <div class="kpi-label">SALES DATA</div>
            <div class="kpi-value" style="font-size:1.2rem;">Sales Sample</div>
            <p style="color:var(--text-muted);font-size:0.85rem;margin:0.5rem 0;">Product sales with customer and staff info</p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("Load Sales Data", key="load_sales", use_container_width=True):
            with st.spinner("Loading..."):
                df = pd.read_csv("data/samples/sales_sample.csv")
                load_replace_to_duckdb(df, "etl.sales_sample_staging")
                set_active_df(df, "sample:sales")
            st.success("✅ Sales sample loaded!")
            st.rerun()
    
    with sample_cards[1]:
        st.markdown("""
        <div class="kpi-card">
            <div class="kpi-icon emerald">📅</div>
            <div class="kpi-label">APPOINTMENTS</div>
            <div class="kpi-value" style="font-size:1.2rem;">Appointments</div>
            <p style="color:var(--text-muted);font-size:0.85rem;margin:0.5rem 0;">Booking and scheduling data</p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("Load Appointments", key="load_appt", use_container_width=True):
            try:
                with st.spinner("Loading..."):
                    df = pd.read_csv("data/samples/appointments_sample.csv")
                    load_replace_to_duckdb(df, "etl.appointments_staging")
                    set_active_df(df, "sample:appointments")
                st.success("✅ Appointments sample loaded!")
                st.rerun()
            except:
                st.error("Sample file not found")
    
    with sample_cards[2]:
        st.markdown("""
        <div class="kpi-card">
            <div class="kpi-icon violet">💸</div>
            <div class="kpi-label">EXPENSES</div>
            <div class="kpi-value" style="font-size:1.2rem;">Expenses</div>
            <p style="color:var(--text-muted);font-size:0.85rem;margin:0.5rem 0;">Business expense tracking</p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("Load Expenses", key="load_exp", use_container_width=True):
            try:
                with st.spinner("Loading..."):
                    df = pd.read_csv("data/samples/expenses_sample.csv")
                    load_replace_to_duckdb(df, "etl.expenses_staging")
                    set_active_df(df, "sample:expenses")
                st.success("✅ Expenses sample loaded!")
                st.rerun()
            except:
                st.error("Sample file not found")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Current data preview
    df = st.session_state.get("df")
    if df is not None:
        st.markdown(render_section_header("📋", "Current Data Preview"), unsafe_allow_html=True)
        st.caption(f"Source: {st.session_state.get('df_label', 'Unknown')} • {len(df):,} rows • {len(df.columns)} columns")
        
        st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
        st.dataframe(df.head(10), use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)
