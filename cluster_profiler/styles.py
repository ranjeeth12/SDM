"""Shared CSS styles for the Streamlit app."""

import streamlit as st

CUSTOM_CSS = """
<style>
/* ── Global typography ─────────────────────────────────────────────────── */
h1 {
    color: #1A1A2E;
    font-weight: 700;
    padding-bottom: 0.3rem;
    border-bottom: 3px solid #4A90D9;
    margin-bottom: 1.5rem;
}

h2, [data-testid="stHeadingWithActionElements"] h2 {
    color: #2C3E50;
    font-weight: 600;
    margin-top: 1.5rem;
}

h3 {
    color: #34495E;
    font-weight: 600;
}

/* ── Metric cards ──────────────────────────────────────────────────────── */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #F0F4F8 0%, #E8EEF4 100%);
    border: 1px solid #D5DFE9;
    border-radius: 10px;
    padding: 1rem 1.2rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}

[data-testid="stMetric"] label {
    color: #5A6C7D;
    font-size: 0.85rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.03em;
}

[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #1A1A2E;
    font-size: 1.8rem;
    font-weight: 700;
}

/* ── Containers / cards ────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid #D5DFE9;
    border-radius: 8px;
    margin-bottom: 0.5rem;
}

/* ── Tabs ──────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0.5rem;
}

.stTabs [data-baseweb="tab"] {
    border-radius: 6px 6px 0 0;
    padding: 0.6rem 1.2rem;
    font-weight: 500;
}

/* ── Dataframes ────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    border: 1px solid #D5DFE9;
    border-radius: 8px;
    overflow: hidden;
}

/* ── Buttons ───────────────────────────────────────────────────────────── */
.stButton > button[kind="primary"] {
    border-radius: 6px;
    font-weight: 600;
    padding: 0.5rem 1.5rem;
    transition: all 0.2s ease;
}

.stButton > button[kind="primary"]:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 8px rgba(74, 144, 217, 0.3);
}

/* ── Sidebar ───────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #F8FAFC 0%, #F0F4F8 100%);
}

section[data-testid="stSidebar"] .stMarkdown hr {
    border-color: #D5DFE9;
}

/* ── Info / warning boxes ──────────────────────────────────────────────── */
[data-testid="stAlert"] {
    border-radius: 8px;
}

/* ── Code blocks ───────────────────────────────────────────────────────── */
[data-testid="stCode"] {
    border-radius: 8px;
}

/* ── Download buttons ──────────────────────────────────────────────────── */
.stDownloadButton > button {
    border-radius: 6px;
    font-weight: 500;
}
</style>
"""


def inject_css():
    """Inject custom CSS into the Streamlit app."""
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
