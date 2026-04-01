"""Entry point: multi-page Streamlit app with explicit navigation."""

import sys
from pathlib import Path

from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(Path(__file__).resolve().parent / ".env")

import streamlit as st

from cluster_profiler.styles import inject_css

st.set_page_config(page_title="Pattern Discovery", layout="wide")
inject_css()

landing = st.Page("pages/top_50.py", title="Pattern Discovery", default=True)
profiler = st.Page("pages/1_profiler.py", title="Pattern Profiler")

nav = st.navigation([landing, profiler])
nav.run()
