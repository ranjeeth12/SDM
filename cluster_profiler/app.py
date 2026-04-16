"""Entry point: multi-page Streamlit app with explicit navigation."""

import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st

from cluster_profiler.styles import inject_css
from cluster_profiler.db import bootstrap
from cluster_profiler.paginator import cleanup_temp_files

st.set_page_config(page_title="SDM Platform", layout="wide")
inject_css()

# Clean temp files from previous sessions
if "_temp_cleaned" not in st.session_state:
    cleanup_temp_files()
    st.session_state["_temp_cleaned"] = True

bootstrap()

landing = st.Page("pages/top_50.py", title="System Discovered Patterns", default=True)
profiler = st.Page("pages/1_profiler.py", title="Pattern Profiler")
saved = st.Page("pages/5_saved_patterns.py", title="Saved Patterns")
explorer = st.Page("pages/2_dataset_explorer.py", title="Dataset Explorer")
keyword = st.Page("pages/3_keyword_search.py", title="Keyword Search")
gen_config = st.Page("pages/4_generation_config.py", title="Generation Config")

nav = st.navigation([landing, profiler, saved, explorer, keyword, gen_config])
nav.run()
