"""Minimal, professional CSS for the SDM platform.

Design principles:
  - Clean white surfaces, subtle borders
  - System font stack — no external font imports
  - Accent color used sparingly (buttons, active states only)
  - No colored sidebar backgrounds
  - No hidden Streamlit elements
"""

import streamlit as st

CUSTOM_CSS = """
<style>
/* ── Dataframe fix — prevent wrapping ──────────────────────────────────── */
[data-testid="stDataFrame"] td,
[data-testid="stDataFrame"] th {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 280px;
}

/* ── Metric cards ──────────────────────────────────────────────────────── */
[data-testid="stMetric"] {
    background: #F9FAFB;
    border: 1px solid #E5E7EB;
    border-radius: 8px;
    padding: 0.8rem 1rem;
}

[data-testid="stMetric"] label {
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: #6B7280;
}

[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-size: 1.5rem;
    font-weight: 600;
    color: #111827;
}

/* ── Tabs ──────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab"] {
    font-size: 0.875rem;
    font-weight: 500;
    white-space: nowrap;
    padding: 0.5rem 1rem;
}

/* ── Buttons — clean, compact ──────────────────────────────────────────── */
.stButton > button {
    font-size: 0.825rem;
    font-weight: 500;
    padding: 0.4rem 1rem;
    border-radius: 6px;
    white-space: nowrap;
}

.stDownloadButton > button {
    font-size: 0.825rem;
    font-weight: 500;
    border-radius: 6px;
    white-space: nowrap;
}

/* ── Form labels — subtle, consistent ──────────────────────────────────── */
.stTextInput label,
.stNumberInput label,
.stSelectbox label,
.stMultiSelect label,
.stTextArea label {
    font-size: 0.8rem;
    font-weight: 500;
    color: #374151;
}

/* ── Expanders ─────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid #E5E7EB;
    border-radius: 8px;
}

/* ── Alerts ────────────────────────────────────────────────────────────── */
[data-testid="stAlert"] {
    border-radius: 6px;
    font-size: 0.85rem;
}

/* ── Code blocks ───────────────────────────────────────────────────────── */
[data-testid="stCode"] {
    font-size: 0.825rem;
    border-radius: 6px;
}
</style>
"""


def inject_css():
    """Inject custom CSS into the Streamlit app."""
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
