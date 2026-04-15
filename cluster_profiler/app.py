"""Entry point: multi-page Streamlit app with explicit navigation."""
from dotenv import load_dotenv
load_dotenv("cluster_profiler/.env")

import sys
from pathlib import Path

from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(Path(__file__).resolve().parent / ".env")

import streamlit as st

from cluster_profiler.styles import inject_css
from cluster_profiler.db import bootstrap

st.set_page_config(page_title="SDM Platform", layout="wide")
inject_css()

# Initialize database on first run
bootstrap()

landing = st.Page("pages/top_50.py", title="System Discovered Patterns", default=True)
profiler = st.Page("pages/1_profiler.py", title="Pattern Profiler")
explorer = st.Page("pages/2_dataset_explorer.py", title="Dataset Explorer")
keyword = st.Page("pages/3_keyword_search.py", title="Keyword Search")
gen_config = st.Page("pages/4_generation_config.py", title="Generation Config")

nav = st.navigation([landing, profiler, explorer, keyword, gen_config])
nav.run()
