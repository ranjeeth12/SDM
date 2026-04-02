"""Landing page: discover and display the top 50 K-means patterns across all hierarchy combos."""

import pandas as pd
import streamlit as st

from cluster_profiler.config import (
    DEFAULT_DATA_PATH,
    DEFAULT_LABELS_PATH,
    DEFAULT_REFERENCE_DATE,
)
from cluster_profiler.data_loader import load_data
from cluster_profiler.discovery import discover_top_patterns


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
st.caption(
    "Discover the top 50 patterns across all valid hierarchy "
    "combinations (Group, Subgroup, Plan Category, Line of Business) at every depth, "
    "ranked by member count or silhouette score."
)

df, labels_df = cached_load_data()

# ── Dataset overview metrics ──────────────────────────────────────────────────

n_members = df["MEME_CK"].nunique()
n_groups = df["GRGR_CK"].nunique()
n_subgroups = df["SGSG_CK"].nunique()
n_lobs = df["LOBD_ID"].nunique()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Members", f"{n_members:,}")
m2.metric("Groups", n_groups)
m3.metric("Subgroups", n_subgroups)
m4.metric("Lines of Business", n_lobs)

st.markdown("")  # spacer

rank_by = st.radio(
    "Rank patterns by",
    ["Member Count", "Silhouette Score"],
    horizontal=True,
)

top_n = st.number_input(
    "Number of top patterns to return",
    min_value=1, max_value=500, value=50, step=10,
)

if st.button("Compute Top Patterns", type="primary"):
    st.session_state["run_discovery"] = True

if not st.session_state.get("run_discovery"):
    st.info("Click **Compute Top Patterns** to scan all hierarchy combinations and find the top patterns.")
    st.stop()

# Use a hash of the dataframe shape + columns as cache key
data_hash = f"{len(df)}_{len(labels_df)}_{hash(tuple(df.columns))}"
patterns = cached_discover(data_hash, df, labels_df, top_n=top_n)

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

# ── Sort patterns by selected ranking ────────────────────────────────────────

if rank_by == "Silhouette Score":
    sorted_patterns = sorted(patterns, key=lambda p: p.get("silhouette", 0), reverse=True)[:top_n]
else:
    sorted_patterns = sorted(patterns, key=lambda p: p["size"], reverse=True)[:top_n]

# ── Discovery summary metrics ────────────────────────────────────────────────

largest = max(p["size"] for p in sorted_patterns)
smallest = min(p["size"] for p in sorted_patterns)
n_unique_combos = len({
    (tuple(p["combo"].get("grgr_ck") or []),
     tuple(p["combo"].get("sgsg_ck") or []),
     tuple(p["combo"].get("cspd_cat") or []),
     tuple(p["combo"].get("lobd_id") or []))
    for p in sorted_patterns
})

st.divider()

s1, s2, s3, s4 = st.columns(4)
s1.metric("Patterns Found", len(sorted_patterns))
s2.metric("Largest Pattern", f"{largest:,}")
s3.metric(f"Smallest (in Top {top_n})", f"{smallest:,}")
s4.metric("Unique Combos", n_unique_combos)

# ── Results table ─────────────────────────────────────────────────────────────

sort_label = "Silhouette Score" if rank_by == "Silhouette Score" else "Member Count"
st.subheader(f"Top Patterns by {sort_label}")
st.caption("Click a row to view it in the Pattern Profiler.")

total_population = df["MEME_CK"].nunique()

table_data = []
for rank, pattern in enumerate(sorted_patterns, 1):
    row = {
        "Rank": str(rank),
        "Group": pattern["grgr_name"],
        "Subgroup": pattern["sgsg_name"],
        "Plan Category": pattern["cspd_cat_desc"],
        "Line of Business": pattern["plds_desc"],
        "Pattern": str(pattern["cluster_id"]),
        "Members": f"{pattern['size']:,}",
        "% of Pop": f"{pattern['size'] / total_population * 100:.2f}%",
        "Silhouette": f"{pattern.get('silhouette', 0):.4f}",
    }
    table_data.append(row)

display_df = pd.DataFrame(table_data)
event = st.dataframe(
    display_df,
    width="stretch",
    hide_index=True,
    height=500,
    on_select="rerun",
    selection_mode="single-row",
)

# Handle row click → navigate to profiler
selected_rows = event.selection.rows
if selected_rows:
    idx = selected_rows[0]
    selected = sorted_patterns[idx]
    st.session_state["navigate_to_pattern"] = selected["combo"]
    st.session_state["preselect_k"] = selected["n_patterns"]
    st.session_state["preselect_cluster_id"] = selected["cluster_id"]
    st.rerun()
