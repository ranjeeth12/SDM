"""Dataset Explorer — reverse navigation and cross-pattern dataset analysis.

Flow:
  1. User selects members (by group/subgroup, IDs, or random sample)
  2. System finds all patterns those members belong to
  3. User checks patterns of interest
  4. Clicks "Show datasets" to see members that satisfy ALL checked patterns
  5. Comparison metrics shown alongside the intersection data
"""

import numpy as np
import pandas as pd
import streamlit as st

from cluster_profiler.config import (
    MEMBER_DENORM_PATH, MEMBER_LABELS_PATH, DEFAULT_REFERENCE_DATE,
)
from cluster_profiler.data_loader import load_data, apply_filters
from cluster_profiler.clustering import discover_clusters
from cluster_profiler.profiler import profile_all_clusters
from cluster_profiler.dataset_explorer import (
    find_patterns_for_members,
    compare_patterns,
)
from cluster_profiler.naming import build_contextual_name


@st.cache_data
def cached_load_data():
    return load_data(MEMBER_DENORM_PATH, MEMBER_LABELS_PATH, DEFAULT_REFERENCE_DATE)


df, labels_df = cached_load_data()

st.title("Dataset Explorer")
st.caption(
    "Select members, discover their pattern associations, "
    "then check patterns to see the datasets that satisfy them."
)

# ── Step 1: Member Selection ─────────────────────────────────────────────────

st.subheader("1. Select members")

method = st.radio(
    "Selection method",
    ["Group / Subgroup filter", "Member IDs", "Random sample"],
    horizontal=True,
    label_visibility="collapsed",
)

meme_cks = []

if method == "Group / Subgroup filter":
    col1, col2, col3 = st.columns([2, 2, 1])
    groups = sorted(df["GRGR_NAME"].unique())
    selected_group = col1.selectbox("Group", [""] + list(groups), key="de_group")

    if selected_group:
        group_df = df[df["GRGR_NAME"] == selected_group]
        subgroups = sorted(group_df["SGSG_NAME"].unique())
        selected_subgroup = col2.selectbox("Subgroup", ["All"] + list(subgroups), key="de_sg")

        if selected_subgroup != "All":
            subset = group_df[group_df["SGSG_NAME"] == selected_subgroup]
        else:
            subset = group_df

        meme_cks = subset["MEME_CK"].unique().tolist()
        col3.metric("Members", f"{len(meme_cks):,}")

elif method == "Member IDs":
    ids_input = st.text_area(
        "Paste MEME_CK values (one per line or comma-separated)",
        height=80, key="de_ids",
    )
    if ids_input:
        raw = ids_input.replace(",", "\n").split("\n")
        meme_cks = [int(x.strip()) for x in raw if x.strip().isdigit()]
        st.caption(f"Parsed {len(meme_cks)} member IDs")

elif method == "Random sample":
    sample_col1, sample_col2 = st.columns([3, 1])
    sample_size = sample_col1.slider("Sample size", 10, 500, 50, key="de_sample_size")
    if sample_col2.button("Draw sample"):
        meme_cks = df["MEME_CK"].drop_duplicates().sample(sample_size).tolist()
        st.session_state["de_sampled"] = meme_cks
    meme_cks = st.session_state.get("de_sampled", [])
    if meme_cks:
        st.caption(f"Random sample of {len(meme_cks)} members")

# ── Step 2: Find Patterns ────────────────────────────────────────────────────

if not meme_cks:
    st.stop()

st.divider()

if "de_results" not in st.session_state or st.button("Find patterns", type="primary"):
    with st.spinner("Analyzing pattern associations..."):
        results = find_patterns_for_members(df, labels_df, meme_cks, max_combos=40)
    st.session_state["de_results"] = results

results = st.session_state.get("de_results", [])

if not results:
    st.warning("No pattern associations found for the selected members.")
    st.stop()

# ── Step 3: Pattern List with Checkboxes ─────────────────────────────────────

st.subheader(f"2. Pattern associations ({len(results)} found)")
st.caption("Check patterns to see datasets that satisfy them.")

# Build pattern table with names
table_data = []
for i, r in enumerate(results[:30]):
    name = build_contextual_name(
        r["grgr_name"], r["sgsg_name"],
        r["cspd_cat_desc"], r["plds_desc"],
        r["cluster_id"], r.get("profile"),
    )
    table_data.append({
        "_idx": i,
        "Pattern": name,
        "Your members": r["member_count"],
        "Total in pattern": r["total_in_cluster"],
        "% coverage": f"{r['pct_in_cluster'] * 100:.1f}%",
        "Group": r["grgr_name"],
        "Plan": r["cspd_cat_desc"],
        "LOB": r["plds_desc"],
    })

display_df = pd.DataFrame(table_data)
show_cols = ["Pattern", "Your members", "Total in pattern", "% coverage", "Group", "Plan", "LOB"]

event = st.dataframe(
    display_df[show_cols],
    hide_index=True,
    width="stretch",
    height=min(350, len(table_data) * 38 + 40),
    on_select="rerun",
    selection_mode="multi-row",
)

selected_indices = event.selection.rows
if not selected_indices:
    st.info("Select one or more patterns above, then click **Show datasets** below.")
    st.stop()

selected_patterns = [results[table_data[i]["_idx"]] for i in selected_indices]
selected_names = [table_data[i]["Pattern"] for i in selected_indices]

st.markdown(f"**Selected:** {', '.join(selected_names)}")

# ── Step 4: Show Datasets ────────────────────────────────────────────────────

if st.button("Show datasets", type="primary"):
    st.divider()
    st.subheader("3. Datasets satisfying selected patterns")

    if len(selected_patterns) == 1:
        # Single pattern — show its members directly
        r = selected_patterns[0]
        combo = r["combo"]

        try:
            subset_members, subset_labels, family_data, filters_used = apply_filters(
                df, labels_df,
                grgr_ck=combo.get("grgr_ck"),
                sgsg_ck=combo.get("sgsg_ck"),
                cspd_cat=combo.get("cspd_cat"),
                lobd_id=combo.get("lobd_id"),
            )
            assignments, _ = discover_clusters(
                subset_members, subset_labels,
                k=None, use_labels=False,
                filters_used=filters_used,
            )
            mask = np.array(assignments) == r["cluster_id"]
            pattern_members = subset_members.iloc[mask]

            # Filter to the user's original member selection
            pattern_members = pattern_members[pattern_members["MEME_CK"].isin(meme_cks)]

            m1, m2 = st.columns(2)
            m1.metric("Members matching", f"{len(pattern_members):,}")
            m2.metric("Pattern", selected_names[0])

            st.dataframe(pattern_members, hide_index=True, width="stretch", height=400)

        except ValueError as e:
            st.error(str(e))

    else:
        # Multiple patterns — compute intersection
        member_sets = []
        all_profiles = []

        for r in selected_patterns:
            combo = r["combo"]
            try:
                subset_members, subset_labels, family_data, filters_used = apply_filters(
                    df, labels_df,
                    grgr_ck=combo.get("grgr_ck"),
                    sgsg_ck=combo.get("sgsg_ck"),
                    cspd_cat=combo.get("cspd_cat"),
                    lobd_id=combo.get("lobd_id"),
                )
                assignments, _ = discover_clusters(
                    subset_members, subset_labels,
                    k=None, use_labels=False,
                    filters_used=filters_used,
                )
                mask = np.array(assignments) == r["cluster_id"]
                cluster_meme_cks = set(subset_members.iloc[mask]["MEME_CK"].tolist())
                member_sets.append(cluster_meme_cks)

                if r.get("profile"):
                    all_profiles.append(r["profile"])

            except ValueError:
                member_sets.append(set())

        # Intersection
        intersection = member_sets[0]
        union_set = member_sets[0]
        for s in member_sets[1:]:
            intersection = intersection & s
            union_set = union_set | s

        jaccard = len(intersection) / len(union_set) if union_set else 0

        # Metrics
        cols = st.columns(len(selected_patterns) + 2)
        for i, name in enumerate(selected_names):
            cols[i].metric(f"Pattern {i+1}", f"{len(member_sets[i]):,}")
        cols[-2].metric("Common members", f"{len(intersection):,}")
        cols[-1].metric("Jaccard similarity", f"{jaccard:.4f}")

        # Show intersection data
        if intersection:
            common_df = df[df["MEME_CK"].isin(intersection)].drop_duplicates(subset=["MEME_CK"])
            st.markdown(f"**{len(common_df):,} members** satisfy all {len(selected_patterns)} selected patterns:")
            st.dataframe(common_df, hide_index=True, width="stretch", height=400)
        else:
            st.info("No members found in the intersection of all selected patterns.")

        # Comparison table if we have profiles
        if len(all_profiles) >= 2:
            st.markdown("")
            st.markdown("**Pattern comparison**")
            comp_df = compare_patterns(all_profiles[:len(selected_patterns)])
            st.dataframe(comp_df, hide_index=True, width="stretch")
