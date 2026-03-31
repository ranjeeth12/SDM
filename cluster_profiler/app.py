"""Landing page: discover and display the top 50 K-means patterns across all hierarchy combos."""

import sys
from pathlib import Path

from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(Path(__file__).resolve().parent / ".env")

import pandas as pd
import streamlit as st

from cluster_profiler.config import (
    DEFAULT_DATA_PATH,
    DEFAULT_LABELS_PATH,
    DEFAULT_REFERENCE_DATE,
)
from cluster_profiler.data_loader import load_data
from cluster_profiler.discovery import discover_top_patterns

st.set_page_config(page_title="Pattern Discovery", layout="wide")


@st.cache_data
def cached_load_data():
    return load_data(DEFAULT_DATA_PATH, DEFAULT_LABELS_PATH, DEFAULT_REFERENCE_DATE)


@st.cache_data
def cached_discover(data_hash: str, df, labels_df, top_n=50):
    """Run batch discovery, cached by data hash."""
    placeholder = st.empty()
    progress_bar = st.progress(0, text="Discovering patterns...")

    def progress_callback(i, total, combo):
        pct = (i + 1) / total
        label = combo.get("grgr_name", "")
        if combo.get("sgsg_name") != "All":
            label += f" / {combo['sgsg_name']}"
        progress_bar.progress(pct, text=f"Processing {i+1}/{total}: {label}")

    results = discover_top_patterns(df, labels_df, top_n=top_n, progress_callback=progress_callback)
    progress_bar.empty()
    placeholder.empty()
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

st.title("Pattern Discovery")
st.markdown(
    "Discover the **top 50 patterns** across all valid hierarchy "
    "combinations (Group, Subgroup, Plan Category, Line of Business) at every depth, "
    "ranked by member count."
)

df, labels_df = cached_load_data()

if st.button("Compute Top Patterns", type="primary"):
    st.session_state["run_discovery"] = True

if not st.session_state.get("run_discovery"):
    st.info("Click **Compute Top Patterns** to scan all hierarchy combinations and find the largest patterns.")
    st.stop()

# Use a hash of the dataframe shape + columns as cache key
data_hash = f"{len(df)}_{len(labels_df)}_{hash(tuple(df.columns))}"
patterns = cached_discover(data_hash, df, labels_df, top_n=50)

if not patterns:
    st.warning("No patterns found.")
    st.stop()

# ── Handle pending navigation (from button click on previous rerun) ──────────

if "navigate_to_pattern" in st.session_state:
    combo = st.session_state.pop("navigate_to_pattern")
    st.session_state["preselect_grgr_ck"] = combo["grgr_ck"]
    st.session_state["preselect_sgsg_ck"] = combo["sgsg_ck"]
    st.session_state["preselect_cspd_cat"] = combo["cspd_cat"]
    st.session_state["preselect_lobd_id"] = combo["lobd_id"]
    st.session_state["auto_run"] = True
    st.switch_page("pages/1_profiler.py")

# ── Build display table ───────────────────────────────────────────────────────

st.subheader(f"Top {len(patterns)} Patterns by Member Count")

# Build a dataframe for display
table_data = []
for rank, pattern in enumerate(patterns, 1):
    table_data.append({
        "Rank": rank,
        "Group": pattern["grgr_name"],
        "Subgroup": pattern["sgsg_name"],
        "Plan Category": pattern["cspd_cat_desc"],
        "Line of Business": pattern["plds_desc"],
        "Pattern #": pattern["cluster_id"],
        "Members": pattern["size"],
        "Patterns in Combo": pattern["n_patterns"],
    })

display_df = pd.DataFrame(table_data)
st.dataframe(
    display_df,
    width="stretch",
    hide_index=True,
    column_config={
        "Rank": st.column_config.NumberColumn("Rank", width="small"),
        "Group": st.column_config.TextColumn("Group", width="medium"),
        "Subgroup": st.column_config.TextColumn("Subgroup", width="medium"),
        "Plan Category": st.column_config.TextColumn("Plan Category", width="medium"),
        "Line of Business": st.column_config.TextColumn("Line of Business", width="medium"),
        "Pattern #": st.column_config.NumberColumn("Pattern #", width="small"),
        "Members": st.column_config.NumberColumn("Members", format="%d", width="small"),
        "Patterns in Combo": st.column_config.NumberColumn("Patterns in Combo", width="small"),
    },
)

st.markdown("---")
st.subheader("View a Pattern")

pattern_choice = st.selectbox(
    "Select a pattern to view in the profiler",
    options=range(len(patterns)),
    format_func=lambda i: (
        f"#{i+1} — {patterns[i]['grgr_name']} / {patterns[i]['sgsg_name']} / "
        f"{patterns[i]['cspd_cat_desc']} / {patterns[i]['plds_desc']} — "
        f"Pattern {patterns[i]['cluster_id']} ({patterns[i]['size']:,} members)"
    ),
)

if st.button("View in Profiler", type="primary"):
    st.session_state["navigate_to_pattern"] = patterns[pattern_choice]["combo"]
    st.rerun()
