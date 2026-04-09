"""Dataset Explorer — reverse navigation from datasets to patterns.

Supports:
  - Select members → see all patterns they belong to
  - Select multiple patterns → see common/overlapping members
  - Pattern comparison table
"""

import numpy as np
import pandas as pd
import streamlit as st

from cluster_profiler.config import (
    DEFAULT_DATA_PATH, DEFAULT_LABELS_PATH, DEFAULT_REFERENCE_DATE,
)
from cluster_profiler.data_loader import load_data
from cluster_profiler.dataset_explorer import (
    find_patterns_for_members,
    find_common_members,
    compare_patterns,
)
from cluster_profiler.naming import build_contextual_name


@st.cache_data
def cached_load_data():
    return load_data(DEFAULT_DATA_PATH, DEFAULT_LABELS_PATH, DEFAULT_REFERENCE_DATE)


df, labels_df = cached_load_data()

st.title("Dataset Explorer")

tab_reverse, tab_cross = st.tabs([
    "Dataset → Patterns",
    "Cross-Pattern Analysis",
])

# ── Tab 1: Reverse Navigation ────────────────────────────────────────────────

with tab_reverse:
    st.caption(
        "Select a set of members and discover which patterns they belong to. "
        "This helps understand the pattern composition of any data subset."
    )

    # Member selection methods
    method = st.radio(
        "How would you like to select members?",
        ["By Group/Subgroup Filter", "By Member IDs", "Random Sample"],
        horizontal=True,
    )

    meme_cks = []

    if method == "By Group/Subgroup Filter":
        groups = sorted(df["GRGR_NAME"].unique())
        selected_group = st.selectbox("Group", [""] + list(groups))

        if selected_group:
            group_df = df[df["GRGR_NAME"] == selected_group]
            subgroups = sorted(group_df["SGSG_NAME"].unique())
            selected_subgroup = st.selectbox("Subgroup (optional)", ["All"] + list(subgroups))

            if selected_subgroup != "All":
                subset = group_df[group_df["SGSG_NAME"] == selected_subgroup]
            else:
                subset = group_df

            meme_cks = subset["MEME_CK"].unique().tolist()
            st.info(f"Selected {len(meme_cks)} members from {selected_group}"
                     + (f" / {selected_subgroup}" if selected_subgroup != "All" else ""))

    elif method == "By Member IDs":
        ids_input = st.text_area(
            "Paste MEME_CK values (one per line or comma-separated)",
            height=100,
        )
        if ids_input:
            raw = ids_input.replace(",", "\n").split("\n")
            meme_cks = [int(x.strip()) for x in raw if x.strip().isdigit()]
            st.info(f"Parsed {len(meme_cks)} member IDs")

    elif method == "Random Sample":
        sample_size = st.slider("Sample size", 10, 500, 50)
        if st.button("Draw Sample"):
            meme_cks = df["MEME_CK"].drop_duplicates().sample(sample_size).tolist()
            st.session_state["sampled_meme_cks"] = meme_cks
        meme_cks = st.session_state.get("sampled_meme_cks", [])
        if meme_cks:
            st.info(f"Random sample of {len(meme_cks)} members")

    if meme_cks and st.button("Find Patterns", type="primary"):
        with st.spinner("Analyzing patterns..."):
            results = find_patterns_for_members(df, labels_df, meme_cks)

        if not results:
            st.warning("No pattern matches found for the selected members.")
        else:
            st.success(f"Found {len(results)} pattern associations")

            table_data = []
            for r in results[:30]:
                name = build_contextual_name(
                    r["grgr_name"], r["sgsg_name"],
                    r["cspd_cat_desc"], r["plds_desc"],
                    r["cluster_id"],
                    r.get("profile"),
                )
                table_data.append({
                    "Pattern": name,
                    "Your Members": r["member_count"],
                    "Total in Pattern": r["total_in_cluster"],
                    "% of Pattern": f"{r['pct_in_cluster'] * 100:.1f}%",
                    "Group": r["grgr_name"],
                    "Subgroup": r["sgsg_name"],
                    "Plan Cat": r["cspd_cat_desc"],
                    "LOB": r["plds_desc"],
                })

            st.dataframe(
                pd.DataFrame(table_data),
                hide_index=True, width="stretch", height=400,
            )


# ── Tab 2: Cross-Pattern Analysis ────────────────────────────────────────────

with tab_cross:
    st.caption(
        "Select two or more patterns to see overlapping members and compare "
        "their demographic profiles side by side."
    )

    st.markdown("**Select patterns by hierarchy:**")

    # Pattern A
    st.markdown("**Pattern A**")
    col_a1, col_a2 = st.columns(2)
    groups = sorted(df["GRGR_NAME"].unique())
    group_a = col_a1.selectbox("Group A", groups, key="xp_group_a")
    group_a_df = df[df["GRGR_NAME"] == group_a]
    subgroups_a = sorted(group_a_df["SGSG_NAME"].unique())
    subgroup_a = col_a2.selectbox("Subgroup A", ["All"] + list(subgroups_a), key="xp_sg_a")

    # Pattern B
    st.markdown("**Pattern B**")
    col_b1, col_b2 = st.columns(2)
    group_b = col_b1.selectbox("Group B", groups, key="xp_group_b")
    group_b_df = df[df["GRGR_NAME"] == group_b]
    subgroups_b = sorted(group_b_df["SGSG_NAME"].unique())
    subgroup_b = col_b2.selectbox("Subgroup B", ["All"] + list(subgroups_b), key="xp_sg_b")

    if st.button("Compare Patterns", type="primary"):
        # Build pattern specs
        def _build_spec(group_name, subgroup_name, full_df):
            grgr_ck = full_df[full_df["GRGR_NAME"] == group_name]["GRGR_CK"].iloc[0]
            combo = {"grgr_ck": [int(grgr_ck)], "sgsg_ck": None, "cspd_cat": None, "lobd_id": None}
            if subgroup_name != "All":
                subset = full_df[(full_df["GRGR_NAME"] == group_name) & (full_df["SGSG_NAME"] == subgroup_name)]
                if not subset.empty:
                    combo["sgsg_ck"] = [int(subset["SGSG_CK"].iloc[0])]
            return {"combo": combo, "cluster_id": 0}

        spec_a = _build_spec(group_a, subgroup_a, df)
        spec_b = _build_spec(group_b, subgroup_b, df)

        with st.spinner("Computing overlap..."):
            try:
                result = find_common_members(df, labels_df, [spec_a, spec_b])

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Pattern A Members", f"{len(result['per_pattern'][0]):,}")
                m2.metric("Pattern B Members", f"{len(result['per_pattern'][1]):,}")
                m3.metric("Common Members", f"{result['overlap_count']:,}")
                m4.metric("Jaccard Similarity", f"{result['jaccard']:.4f}")

                if result["overlap_count"] > 0:
                    st.subheader("Common Members")
                    st.dataframe(
                        result["common_members"].head(50),
                        hide_index=True, width="stretch", height=300,
                    )
                else:
                    st.info("No overlapping members between these patterns.")

            except ValueError as e:
                st.error(str(e))
