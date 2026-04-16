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

from cluster_profiler.config import DEFAULT_REFERENCE_DATE
from cluster_profiler.data_loader import load_data, apply_filters
from cluster_profiler.profiler import profile_all_clusters
from cluster_profiler.dataset_explorer import (
    find_patterns_for_members,
    compare_patterns,
)
from cluster_profiler.naming import build_contextual_name


@st.cache_data
def cached_load_data():
    return load_data()


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
    col1, col2 = st.columns(2)
    groups = sorted(df["GRGR_NAME"].unique())
    selected_group = col1.selectbox("Group", [""] + list(groups), key="de_group")

    subset = df
    if selected_group:
        subset = df[df["GRGR_NAME"] == selected_group]
        subgroups = sorted(subset["SGSG_NAME"].unique())
        selected_subgroup = col2.selectbox("Subgroup", ["All"] + list(subgroups), key="de_sg")

        if selected_subgroup != "All":
            subset = subset[subset["SGSG_NAME"] == selected_subgroup]

        # Plan Category (depth 3)
        col3, col4 = st.columns(2)
        plan_cats = sorted(subset["CSPD_CAT_DESC"].dropna().unique())
        selected_plan = col3.selectbox("Plan category", ["All"] + list(plan_cats), key="de_plan")

        if selected_plan != "All":
            subset = subset[subset["CSPD_CAT_DESC"] == selected_plan]

        # Line of Business (depth 4)
        lobs = sorted(subset["PLDS_DESC"].dropna().unique())
        selected_lob = col4.selectbox("Line of business", ["All"] + list(lobs), key="de_lob")

        if selected_lob != "All":
            subset = subset[subset["PLDS_DESC"] == selected_lob]

        meme_cks = subset["MEME_CK"].unique().tolist()
        st.caption(f"Selected {len(meme_cks):,} members")

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

# Build pattern table with names and tags
from cluster_profiler.tagging import generate_tags

table_data = []
for i, r in enumerate(results[:30]):
    name = build_contextual_name(
        r["grgr_name"], r["sgsg_name"],
        r["cspd_cat_desc"], r["plds_desc"],
        r["cluster_id"], r.get("profile"),
    )

    # Generate tags for this pattern
    profile = r.get("profile", {})
    tags = generate_tags(
        grgr_name=r["grgr_name"],
        sgsg_name=r["sgsg_name"],
        cspd_cat_desc=r["cspd_cat_desc"],
        plds_desc=r["plds_desc"],
        profile=profile,
        silhouette=0,
        pct_of_pop=r["pct_in_cluster"],
    )
    tag_str = ", ".join(tags[:5])

    # Key distinguishing features
    age_mean = profile.get("continuous", {}).get("_age", {}).get("mean", 0)
    spouse_rate = profile.get("family", {}).get("spouse_rate", 0)
    distinguishing = []
    if age_mean > 0:
        distinguishing.append(f"age={age_mean:.0f}")
    if spouse_rate > 0.5:
        distinguishing.append(f"spouse={spouse_rate*100:.0f}%")

    table_data.append({
        "_idx": i,
        "Pattern": name,
        "Your members": r["member_count"],
        "Total in pattern": r["total_in_cluster"],
        "% coverage": f"{r['pct_in_cluster'] * 100:.1f}%",
        "Group": r["grgr_name"],
        "Plan": r["cspd_cat_desc"],
        "LOB": r["plds_desc"],
        "Tags": tag_str,
    })

display_df = pd.DataFrame(table_data)
show_cols = ["Pattern", "Your members", "Total in pattern", "% coverage", "Group", "Plan", "LOB", "Tags"]

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
    st.subheader("3. Datasets for selected patterns")

    if len(selected_patterns) == 1:
        # Single pattern — show members matching the hierarchy filters
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

            # Show searched members that fall within this pattern's hierarchy
            searched_in_pattern = subset_members[subset_members["MEME_CK"].isin(meme_cks)]

            m1, m2, m3 = st.columns(3)
            m1.metric("Your members in this pattern", f"{len(searched_in_pattern):,}")
            m2.metric("Total in pattern hierarchy", f"{len(subset_members):,}")
            m3.metric("Pattern", selected_names[0])

            if not searched_in_pattern.empty:
                st.markdown("**Your searched members matching this pattern:**")
                st.dataframe(searched_in_pattern, hide_index=True, width="stretch", height=300)
            else:
                st.info("Your searched members don't fall within this pattern's hierarchy filters.")

            with st.expander(f"All {len(subset_members):,} members in this pattern's hierarchy"):
                st.dataframe(subset_members, hide_index=True, width="stretch", height=400)

        except ValueError as e:
            st.error(str(e))

    else:
        # Multiple patterns — show union of all hierarchy-filtered members
        member_sets = []
        all_dfs = []
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
                pattern_meme_cks = set(subset_members["MEME_CK"].tolist())
                member_sets.append(pattern_meme_cks)
                all_dfs.append(subset_members)

                if r.get("profile"):
                    all_profiles.append(r["profile"])

            except ValueError:
                member_sets.append(set())

        # Union and intersection
        union_set = set()
        intersection = member_sets[0].copy() if member_sets else set()
        for s in member_sets:
            union_set = union_set | s
            intersection = intersection & s

        # Members from the user's search that appear in ANY selected pattern
        searched_in_union = union_set & set(meme_cks)

        jaccard = len(intersection) / len(union_set) if union_set else 0

        # Metrics
        cols = st.columns(len(selected_patterns) + 3)
        for i, name in enumerate(selected_names):
            cols[i].metric(f"Pattern {i+1}", f"{len(member_sets[i]):,}")
        cols[-3].metric("Union (any pattern)", f"{len(union_set):,}")
        cols[-2].metric("Overlap (all patterns)", f"{len(intersection):,}")
        cols[-1].metric("Jaccard similarity", f"{jaccard:.4f}")

        # Show your searched members across selected patterns
        if searched_in_union:
            searched_df = df[df["MEME_CK"].isin(searched_in_union)].drop_duplicates(subset=["MEME_CK"])
            st.markdown(f"**{len(searched_df):,} of your searched members** found across the selected patterns:")
            st.dataframe(searched_df, hide_index=True, width="stretch", height=300)

        # Show full union
        union_df = df[df["MEME_CK"].isin(union_set)].drop_duplicates(subset=["MEME_CK"])
        with st.expander(f"All {len(union_df):,} members across selected patterns (union)"):
            st.dataframe(union_df, hide_index=True, width="stretch", height=400)

        # Show intersection if any
        if intersection:
            common_df = df[df["MEME_CK"].isin(intersection)].drop_duplicates(subset=["MEME_CK"])
            with st.expander(f"{len(common_df):,} members in ALL selected patterns (overlap)"):
                st.dataframe(common_df, hide_index=True, width="stretch", height=400)

        # Comparison table if we have profiles
        if len(all_profiles) >= 2:
            st.markdown("")
            st.markdown("**Pattern comparison**")
            comp_df = compare_patterns(all_profiles[:len(selected_patterns)])
            st.dataframe(comp_df, hide_index=True, width="stretch")
