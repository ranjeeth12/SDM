"""Shared CSS styles for the Streamlit app — Ascendion branded.

Color palette (from Ascendion deck):
  Primary dark:   #1B4332  (dark forest green)
  Primary mid:    #2D6A4F  (forest green)
  Accent mint:    #2DC784  (mint green)
  Accent light:   #D8F3DC  (light mint)
  Surface:        #F7FAF8  (near-white green tint)
  Text primary:   #1A1A2E
  Text secondary: #5A6C7D
"""

import streamlit as st

CUSTOM_CSS = """
<style>
/* ── Font & base ──────────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    -webkit-font-smoothing: antialiased;
}

/* Prevent all text wrapping issues in dataframes and tables */
[data-testid="stDataFrame"] td,
[data-testid="stDataFrame"] th {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 300px;
    font-size: 0.82rem;
}

/* ── Page title ────────────────────────────────────────────────────────── */
h1 {
    color: #1B4332 !important;
    font-weight: 700 !important;
    font-size: 1.75rem !important;
    padding-bottom: 0.4rem;
    border-bottom: 3px solid #2DC784;
    margin-bottom: 1.2rem;
    letter-spacing: -0.01em;
}

/* ── Section headers ───────────────────────────────────────────────────── */
h2, [data-testid="stHeadingWithActionElements"] h2 {
    color: #2D6A4F !important;
    font-weight: 600 !important;
    font-size: 1.25rem !important;
    margin-top: 1.2rem;
    letter-spacing: -0.01em;
}

h3 {
    color: #1B4332 !important;
    font-weight: 600 !important;
    font-size: 1.05rem !important;
}

/* ── Metric cards ──────────────────────────────────────────────────────── */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #F7FAF8 0%, #D8F3DC 100%);
    border: 1px solid #B7E4C7;
    border-radius: 10px;
    padding: 0.9rem 1.1rem;
    box-shadow: 0 1px 3px rgba(27,67,50,0.06);
}

[data-testid="stMetric"] label {
    color: #2D6A4F;
    font-size: 0.72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #1B4332;
    font-size: 1.6rem;
    font-weight: 700;
}

/* ── Sidebar ───────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1B4332 0%, #2D6A4F 100%);
}

section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] .stMarkdown p {
    color: #D8F3DC !important;
}

section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color: #FFFFFF !important;
    border-bottom-color: #2DC784 !important;
}

section[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.15);
}

section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stMultiSelect label,
section[data-testid="stSidebar"] .stNumberInput label {
    color: #B7E4C7 !important;
    font-size: 0.8rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

/* ── Navigation ────────────────────────────────────────────────────────── */
[data-testid="stSidebarNav"] {
    padding-top: 1rem;
}

[data-testid="stSidebarNav"] a {
    color: #D8F3DC !important;
    font-weight: 500;
    font-size: 0.88rem;
    border-radius: 6px;
    padding: 0.4rem 0.8rem;
}

[data-testid="stSidebarNav"] a:hover {
    background: rgba(45,199,132,0.15);
}

[data-testid="stSidebarNav"] a[aria-selected="true"] {
    background: rgba(45,199,132,0.25);
    color: #FFFFFF !important;
    font-weight: 600;
}

/* ── Tabs ──────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 2px solid #D8F3DC;
}

.stTabs [data-baseweb="tab"] {
    border-radius: 6px 6px 0 0;
    padding: 0.5rem 1rem;
    font-weight: 500;
    font-size: 0.85rem;
    color: #5A6C7D;
    white-space: nowrap;
    border: none;
    background: transparent;
}

.stTabs [data-baseweb="tab"]:hover {
    color: #2D6A4F;
    background: #F7FAF8;
}

.stTabs [aria-selected="true"] {
    color: #1B4332 !important;
    font-weight: 600;
    border-bottom: 3px solid #2DC784 !important;
    background: transparent;
}

/* ── Dataframes ────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    border: 1px solid #D8F3DC;
    border-radius: 8px;
    overflow: hidden;
}

[data-testid="stDataFrame"] [data-testid="glideDataEditor"] {
    font-size: 0.82rem;
}

/* ── Expanders ─────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid #D8F3DC;
    border-radius: 8px;
    margin-bottom: 0.5rem;
    background: #FFFFFF;
}

[data-testid="stExpander"] summary {
    font-weight: 500;
    color: #2D6A4F;
    font-size: 0.9rem;
}

/* ── Buttons ───────────────────────────────────────────────────────────── */
.stButton > button {
    border-radius: 6px;
    font-weight: 500;
    font-size: 0.82rem;
    padding: 0.45rem 1.2rem;
    transition: all 0.15s ease;
    border: 1px solid #B7E4C7;
    white-space: nowrap;
}

.stButton > button[kind="primary"] {
    background: #2D6A4F;
    color: #FFFFFF;
    border-color: #2D6A4F;
    font-weight: 600;
}

.stButton > button[kind="primary"]:hover {
    background: #1B4332;
    border-color: #1B4332;
    transform: translateY(-1px);
    box-shadow: 0 4px 8px rgba(27,67,50,0.2);
}

.stButton > button[kind="secondary"]:hover,
.stButton > button:hover {
    border-color: #2DC784;
    color: #1B4332;
    background: #F7FAF8;
}

/* ── Download buttons ──────────────────────────────────────────────────── */
.stDownloadButton > button {
    border-radius: 6px;
    font-weight: 500;
    font-size: 0.82rem;
    border-color: #2DC784;
    color: #1B4332;
    white-space: nowrap;
}

.stDownloadButton > button:hover {
    background: #D8F3DC;
    border-color: #2D6A4F;
}

/* ── Form inputs ───────────────────────────────────────────────────────── */
.stTextInput input,
.stNumberInput input,
.stSelectbox > div > div,
.stMultiSelect > div > div {
    border-radius: 6px;
    font-size: 0.85rem;
    border-color: #D8F3DC;
}

.stTextInput input:focus,
.stNumberInput input:focus {
    border-color: #2DC784;
    box-shadow: 0 0 0 2px rgba(45,199,132,0.15);
}

.stTextInput label,
.stNumberInput label,
.stSelectbox label,
.stMultiSelect label,
.stTextArea label {
    font-size: 0.78rem;
    font-weight: 500;
    color: #2D6A4F;
    text-transform: uppercase;
    letter-spacing: 0.03em;
}

/* ── Radio buttons ─────────────────────────────────────────────────────── */
.stRadio > div {
    gap: 0.5rem;
}

.stRadio [role="radiogroup"] label {
    font-size: 0.85rem;
    font-weight: 400;
    text-transform: none;
    letter-spacing: normal;
}

/* ── Alerts ────────────────────────────────────────────────────────────── */
[data-testid="stAlert"] {
    border-radius: 8px;
    font-size: 0.85rem;
}

/* ── Code blocks ───────────────────────────────────────────────────────── */
[data-testid="stCode"] {
    border-radius: 6px;
    font-size: 0.82rem;
}

/* ── Captions ──────────────────────────────────────────────────────────── */
.stCaption, [data-testid="stCaptionContainer"] {
    font-size: 0.78rem;
    color: #5A6C7D;
}

/* ── Dividers ──────────────────────────────────────────────────────────── */
hr {
    border-color: #D8F3DC;
    margin: 1rem 0;
}

/* ── Success / info messages ───────────────────────────────────────────── */
.stSuccess {
    background: #D8F3DC;
    border-color: #2DC784;
    color: #1B4332;
}

/* ── Checkbox ──────────────────────────────────────────────────────────── */
.stCheckbox label {
    font-size: 0.85rem;
    font-weight: 400;
    text-transform: none;
    letter-spacing: normal;
}

/* ── Progress bar ──────────────────────────────────────────────────────── */
.stProgress > div > div {
    background-color: #2DC784;
}

/* ── Breadcrumb ────────────────────────────────────────────────────────── */
.breadcrumb-text a {
    color: #2D6A4F;
    text-decoration: none;
}

.breadcrumb-text a:hover {
    color: #2DC784;
    text-decoration: underline;
}

/* ── Hide Streamlit branding ───────────────────────────────────────────── */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""


def inject_css():
    """Inject custom CSS into the Streamlit app."""
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
