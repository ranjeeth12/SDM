"""Landing page: discover and display the top patterns across all hierarchy combos.

Flow:
  1. Check DB for persisted patterns → display instantly if found
  2. First-ever run or explicit refresh → run discovery, persist to DB
  3. Subsequent visits → instant load from DB
"""

import pandas as pd
import streamlit as st

from cluster_profiler.config import (
    MEMBER_DENORM_PATH,
    MEMBER_LABELS_PATH,
    DEFAULT_REFERENCE_DATE,
)
from cluster_profiler.data_loader import load_data
from cluster_profiler.discovery import discover_top_patterns
from cluster_profiler import db


@st.cache_data
def cached_load_data():
    return load_data(MEMBER_DENORM_PATH, MEMBER_LABELS_PATH, DEFAULT_REFERENCE_DATE)


def load_patterns_from_db(top_n=50, rank_by="Member Count"):
    """Load persisted patterns from DB, sorted and limited."""
    all_patterns = db.get_all_patterns()
    if not all_patterns:
        return []

    if rank_by == "Silhouette Score":
        all_patterns.sort(key=lambda p: p.get("silhouette") or 0, reverse=True)
    else:
        all_patterns.sort(key=lambda p: p["member_count"], reverse=True)

    return all_patterns[:top_n]


def run_fresh_discovery(df, labels_df, top_n=50):
    """Run full discovery with progress bar, persist to DB."""
    progress_bar = st.progress(0, text="Discovering patterns...")

    def progress_callback(i, total, combo):
        pct = (i + 1) / total
        label = combo.get("grgr_name", "")
        if combo.get("sgsg_name") != "All":
            label += f" / {combo['sgsg_name']}"
        progress_bar.progress(pct, text=f"Processing {i+1}/{total}: {label}")

    results = discover_top_patterns(df, labels_df, top_n=top_n, progress_callback=progress_callback)
    progress_bar.empty()
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

db.bootstrap()

st.title("Pattern Discovery")
st.caption(
    "Discover the top patterns across all valid hierarchy "
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

# ── Load from DB or run discovery ────────────────────────────────────────────

db_patterns = load_patterns_from_db(top_n=top_n, rank_by=rank_by)
has_persisted = len(db_patterns) > 0

if has_persisted:
    # Patterns exist in DB — show them instantly
    col_info, col_refresh = st.columns([3, 1])
    col_info.success(f"Loaded {len(db_patterns)} patterns from repository (instant).")
    refresh_clicked = col_refresh.button("Refresh Patterns", help="Re-run discovery from source data")

    if refresh_clicked:
        with st.spinner("Re-analyzing all hierarchy combinations..."):
            run_fresh_discovery(df, labels_df, top_n=top_n)
        db_patterns = load_patterns_from_db(top_n=top_n, rank_by=rank_by)
        st.success(f"Refreshed. {len(db_patterns)} patterns updated.")
        st.rerun()

    patterns = db_patterns
    from_db = True
else:
    # No patterns in DB — first-time setup
    st.info("No patterns found in repository. Click below to run initial discovery (one-time setup).")
    if st.button("Run Initial Discovery", type="primary"):
        with st.spinner("Analyzing all hierarchy combinations (first-time setup)..."):
            run_fresh_discovery(df, labels_df, top_n=top_n)
        db_patterns = load_patterns_from_db(top_n=top_n, rank_by=rank_by)
        if db_patterns:
            st.success(f"Discovery complete. {len(db_patterns)} patterns persisted to repository.")
            st.rerun()
        else:
            st.warning("No patterns found.")
            st.stop()
    else:
        st.stop()
    patterns = db_patterns
    from_db = True

if not patterns:
    st.warning("No patterns found.")
    st.stop()

# ── Handle pending navigation (from button click on previous rerun) ──────────

if "navigate_to_pattern" in st.session_state:
    combo = st.session_state.pop("navigate_to_pattern")
    st.session_state["preselect_grgr_ck"] = combo.get("grgr_ck")
    st.session_state["preselect_sgsg_ck"] = combo.get("sgsg_ck")
    st.session_state["preselect_cspd_cat"] = combo.get("cspd_cat")
    st.session_state["preselect_lobd_id"] = combo.get("lobd_id")
    st.session_state["auto_run"] = True
    st.switch_page("pages/1_profiler.py")

# ── Discovery summary metrics ────────────────────────────────────────────────

sizes = [p["member_count"] for p in patterns]
largest = max(sizes)
smallest = min(sizes)

st.divider()

s1, s2, s3, s4 = st.columns(4)
s1.metric("Patterns Found", len(patterns))
s2.metric("Largest Pattern", f"{largest:,}")
s3.metric(f"Smallest (in Top {top_n})", f"{smallest:,}")
total_db = len(db.get_all_patterns())
s4.metric("Total in Repository", total_db)

# ── Results table ─────────────────────────────────────────────────────────────

sort_label = "Silhouette Score" if rank_by == "Silhouette Score" else "Member Count"
st.subheader(f"Top Patterns by {sort_label}")
st.caption("Click a row to view it in the Pattern Profiler.")

total_population = n_members

table_data = []
for rank, pattern in enumerate(patterns, 1):
    member_count = pattern["member_count"]
    tags = db.get_tags(pattern["id"])
    tag_str = ", ".join(t["tag"] for t in tags[:5])
    row = {
        "Rank": str(rank),
        "Pattern Name": pattern.get("contextual_name", ""),
        "Group": pattern.get("grgr_name", ""),
        "Plan Category": pattern.get("cspd_cat_desc", "") or "All",
        "Line of Business": pattern.get("plds_desc", "") or "All",
        "Members": f"{member_count:,}",
        "% of Pop": f"{member_count / total_population * 100:.2f}%",
        "Silhouette": f"{pattern.get('silhouette', 0):.4f}",
        "Tags": tag_str,
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
    pattern = patterns[idx]

    import json
    combo = {
        "grgr_ck": json.loads(pattern["grgr_ck"]) if pattern.get("grgr_ck") else None,
        "sgsg_ck": json.loads(pattern["sgsg_ck"]) if pattern.get("sgsg_ck") else None,
        "cspd_cat": json.loads(pattern["cspd_cat"]) if pattern.get("cspd_cat") else None,
        "lobd_id": json.loads(pattern["lobd_id"]) if pattern.get("lobd_id") else None,
    }
    st.session_state["navigate_to_pattern"] = combo
    st.session_state["preselect_k"] = None
    st.session_state["preselect_cluster_id"] = pattern.get("cluster_id", 0)
    st.rerun()
