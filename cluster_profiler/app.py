"""Streamlit UI for the Pattern Profiler."""

import json
import sys
from pathlib import Path

# Ensure the project root is on sys.path so absolute imports work
# when Streamlit runs this file directly.
from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(Path(__file__).resolve().parent / ".env")

import anthropic
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.decomposition import PCA
import streamlit as st

from cluster_profiler.config import (
    CATEGORICAL_FEATURES,
    CONTINUOUS_FEATURES,
    DEFAULT_DATA_PATH,
    DEFAULT_LABELS_PATH,
    DEFAULT_REFERENCE_DATE,
    DESCRIPTION_COLUMNS,
)
from cluster_profiler.clustering import build_features, discover_clusters
from cluster_profiler.data_loader import apply_filters, load_data
from cluster_profiler.formatters import format_json
from cluster_profiler.profiler import build_subset_summary, profile_all_clusters

st.set_page_config(page_title="Pattern Profiler", layout="wide")


@st.cache_data
def cached_load_data():
    return load_data(DEFAULT_DATA_PATH, DEFAULT_LABELS_PATH, DEFAULT_REFERENCE_DATE)


def format_option(key, name):
    return f"{name} ({key})"


def parse_option_key(option_str):
    """Extract the key from a formatted option like 'Name (key)'."""
    start = option_str.rfind("(")
    end = option_str.rfind(")")
    if start != -1 and end != -1:
        return option_str[start + 1 : end]
    return option_str


@st.cache_data
def generate_pattern_summary(profile_json: str, pattern_id: int, total_members: int) -> str:
    """Use Claude to generate a plain-language summary of a pattern profile."""
    client = anthropic.Anthropic()
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{
            "role": "user",
            "content": (
                f"You are summarizing a member pattern from a health plan population analysis. "
                f"This is pattern {pattern_id} out of a population of {total_members} members. "
                f"Write a concise 2-3 sentence plain-language summary describing who these members are "
                f"based on their demographics, family structure, and categorical attributes. "
                f"Focus on what makes this group distinctive. Do not use bullet points or headers.\n\n"
                f"Profile data:\n{profile_json}"
            ),
        }],
    )
    return message.content[0].text


# ── Data Loading ──────────────────────────────────────────────────────────────

df, labels_df = cached_load_data()

# ── Sidebar: Cascading Filters ────────────────────────────────────────────────

st.sidebar.header("Hierarchy Filters")

# 1. Group
group_pairs = (
    df[["GRGR_CK", "GRGR_NAME"]]
    .drop_duplicates()
    .sort_values("GRGR_NAME")
)
group_options = [
    format_option(row.GRGR_CK, row.GRGR_NAME)
    for row in group_pairs.itertuples()
]
selected_group_labels = st.sidebar.multiselect("Group", group_options)
selected_grgr_cks = [int(parse_option_key(g)) for g in selected_group_labels]

# 2. Subgroup — filtered by selected groups
filtered = df[df["GRGR_CK"].isin(selected_grgr_cks)] if selected_grgr_cks else df
subgroup_pairs = (
    filtered[["SGSG_CK", "SGSG_NAME"]]
    .drop_duplicates()
    .sort_values("SGSG_NAME")
)
subgroup_options = [
    format_option(row.SGSG_CK, row.SGSG_NAME)
    for row in subgroup_pairs.itertuples()
]
selected_subgroup_labels = st.sidebar.multiselect("Subgroup", subgroup_options)
selected_sgsg_cks = [int(parse_option_key(s)) for s in selected_subgroup_labels]

# 3. Plan Category — filtered by group + subgroup
if selected_sgsg_cks:
    filtered = filtered[filtered["SGSG_CK"].isin(selected_sgsg_cks)]
cat_pairs = (
    filtered[["CSPD_CAT", "CSPD_CAT_DESC"]]
    .drop_duplicates()
    .sort_values("CSPD_CAT_DESC")
)
cat_options = [
    format_option(row.CSPD_CAT, row.CSPD_CAT_DESC)
    for row in cat_pairs.itertuples()
]
selected_cat_labels = st.sidebar.multiselect("Plan Category", cat_options)
selected_cspd_cats = [parse_option_key(c) for c in selected_cat_labels]

# 4. Line of Business — filtered by group + subgroup + plan category
if selected_cspd_cats:
    filtered = filtered[filtered["CSPD_CAT"].isin(selected_cspd_cats)]
lob_pairs = (
    filtered[["LOBD_ID", "PLDS_DESC"]]
    .drop_duplicates()
    .sort_values("PLDS_DESC")
)
lob_options = [
    format_option(row.LOBD_ID, row.PLDS_DESC)
    for row in lob_pairs.itertuples()
]
selected_lob_labels = st.sidebar.multiselect("Line of Business", lob_options)
selected_lobd_ids = [parse_option_key(l) for l in selected_lob_labels]

# Pattern detection options
st.sidebar.markdown("---")
st.sidebar.header("Pattern Detection Options")
k_override = st.sidebar.number_input(
    "K override (0 = auto)", min_value=0, max_value=20, value=0, step=1
)
run = st.sidebar.button("Run Profiler", type="primary")

# ── Main Area ─────────────────────────────────────────────────────────────────

st.title("Pattern Profiler")

if not run:
    st.info("Configure filters in the sidebar and click **Run Profiler** to start.")
    st.stop()

# Build filter arguments
grgr_ck = selected_grgr_cks or None
sgsg_ck = selected_sgsg_cks or None
cspd_cat = selected_cspd_cats or None
lobd_id = selected_lobd_ids or None
k = k_override if k_override > 0 else None

# Run the pipeline
try:
    subset_members, subset_labels, family_data, filters_used = apply_filters(
        df, labels_df, grgr_ck=grgr_ck, sgsg_ck=sgsg_ck,
        cspd_cat=cspd_cat, lobd_id=lobd_id,
    )
except ValueError as exc:
    st.error(f"No members match the selected filters: {exc}")
    st.stop()

assignments, metrics = discover_clusters(
    subset_members, subset_labels, k=k, use_labels=False,
    filters_used=filters_used,
)
summary = build_subset_summary(subset_members, filters_used)
profiles = profile_all_clusters(subset_members, family_data, assignments)

# ── Header Metrics ────────────────────────────────────────────────────────────

pct_of_total = summary["total_members"] / len(df) * 100
col1, col2, col3 = st.columns(3)
col1.metric("Percent of Members", f"{pct_of_total:.1f}%")
col2.metric("Patterns", metrics["n_clusters"])
col3.metric("Member Count", summary["total_members"])


# ── Per-Pattern Profiles ──────────────────────────────────────────────────────

if len(profiles) <= 8:
    tab_labels = [f"Pattern {p['cluster_id']}" for p in profiles]
    tabs = st.tabs(tab_labels)
    containers = tabs
else:
    containers = [
        st.expander(f"Pattern {p['cluster_id']}", expanded=False)
        for p in profiles
    ]

for container, profile in zip(containers, profiles):
    with container:
        # Size info
        st.markdown(
            f"**Size:** {profile['size']} members "
            f"({profile['pct_of_subset'] * 100:.1f}% of subset)"
        )

        # LLM summary
        try:
            profile_for_llm = {
                k: profile[k] for k in
                ("cluster_id", "size", "pct_of_subset", "continuous",
                 "categorical", "family", "descriptions")
                if k in profile
            }
            llm_summary = generate_pattern_summary(
                json.dumps(profile_for_llm, default=str),
                profile["cluster_id"],
                summary["total_members"],
            )
            st.subheader("Pattern Summary")
            st.info(llm_summary)
        except Exception as exc:
            st.warning(f"Could not generate AI summary: {exc}")

        # Demographics table
        if profile["continuous"]:
            demo_rows = []
            for col_name, stats in profile["continuous"].items():
                label = "Age (years)" if col_name == "_age" else "Tenure (months)"
                demo_rows.append({
                    "Feature": label,
                    "Mean": f"{stats['mean']:.1f}",
                    "Std": f"{stats['std']:.1f}",
                    "Median": f"{stats['median']:.1f}",
                    "Min": f"{stats['min']:.1f}",
                    "Max": f"{stats['max']:.1f}",
                })
            st.subheader("Demographics")
            st.dataframe(pd.DataFrame(demo_rows), use_container_width=True, hide_index=True)

        # Categorical distributions — horizontal bar charts
        if profile["categorical"]:
            for col_name, cat_data in profile["categorical"].items():
                pct = cat_data["pct"]
                if pct:
                    fig = px.bar(
                        x=list(pct.values()),
                        y=list(pct.keys()),
                        orientation="h",
                        labels={"x": "Percentage (%)", "y": col_name},
                        title=col_name,
                    )
                    fig.update_layout(
                        height=max(200, len(pct) * 40),
                        margin=dict(l=0, r=0, t=30, b=0),
                        showlegend=False,
                    )
                    st.plotly_chart(fig, use_container_width=True,
                                     key=f"cat_{profile['cluster_id']}_{col_name}")

        # Family stats
        if profile.get("family"):
            st.subheader("Family Statistics")
            fc1, fc2 = st.columns(2)
            fc1.metric("Avg Dependents", f"{profile['family']['avg_dependents']:.2f}")
            fc2.metric("Spouse Rate", f"{profile['family']['spouse_rate']:.1%}")

        # Hierarchy descriptions
        if profile.get("descriptions"):
            st.subheader("Hierarchy Descriptions")
            for desc_col, values in profile["descriptions"].items():
                if values:
                    st.markdown(f"**{desc_col}:** {', '.join(str(v) for v in values)}")

        # Raw data viewer
        cid = profile["cluster_id"]
        mask = np.array(assignments) == cid
        raw_df = subset_members.iloc[mask]
        with st.expander(f"View Raw Data ({len(raw_df)} rows)"):
            st.dataframe(raw_df, use_container_width=True, height=400)

# ── Visualizations (Plotly) ───────────────────────────────────────────────────

st.header("Pattern Visualizations")

X, _scaler = build_features(subset_members)
unique_clusters = sorted(set(assignments))
n_clusters = len(unique_clusters)
cluster_labels = [f"Pattern {c}" for c in assignments]

fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=("PCA Scatter", "Age Distribution", "Tenure Distribution",
                    f"{CATEGORICAL_FEATURES[0]} by Pattern"),
)

colors = px.colors.qualitative.T10

# 1. PCA scatter
if X.shape[1] >= 2:
    pca = PCA(n_components=2)
    coords = pca.fit_transform(X)
    for i, cid in enumerate(unique_clusters):
        mask = np.array(assignments) == cid
        fig.add_trace(
            go.Scatter(
                x=coords[mask, 0], y=coords[mask, 1],
                mode="markers",
                marker=dict(color=colors[i % len(colors)], size=5, opacity=0.6),
                name=f"Pattern {cid}",
                legendgroup=f"c{cid}",
            ),
            row=1, col=1,
        )
    fig.update_xaxes(
        title_text=f"PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)",
        row=1, col=1,
    )
    fig.update_yaxes(
        title_text=f"PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)",
        row=1, col=1,
    )

# 2. Age histogram
for i, cid in enumerate(unique_clusters):
    mask = np.array(assignments) == cid
    vals = subset_members.iloc[mask]["_age"].dropna()
    if len(vals) > 0:
        fig.add_trace(
            go.Histogram(
                x=vals, nbinsx=20, opacity=0.5,
                marker_color=colors[i % len(colors)],
                name=f"Pattern {cid}",
                legendgroup=f"c{cid}",
                showlegend=False,
            ),
            row=1, col=2,
        )
fig.update_xaxes(title_text="Age (years)", row=1, col=2)
fig.update_yaxes(title_text="Count", row=1, col=2)
fig.update_layout(barmode="overlay")

# 3. Tenure histogram
for i, cid in enumerate(unique_clusters):
    mask = np.array(assignments) == cid
    vals = subset_members.iloc[mask]["_tenure"].dropna()
    if len(vals) > 0:
        fig.add_trace(
            go.Histogram(
                x=vals, nbinsx=20, opacity=0.5,
                marker_color=colors[i % len(colors)],
                name=f"Pattern {cid}",
                legendgroup=f"c{cid}",
                showlegend=False,
            ),
            row=2, col=1,
        )
fig.update_xaxes(title_text="Tenure (months)", row=2, col=1)
fig.update_yaxes(title_text="Count", row=2, col=1)

# 4. Categorical bar chart
cat_col = CATEGORICAL_FEATURES[0]
if cat_col in subset_members.columns:
    categories = sorted(subset_members[cat_col].dropna().unique())
    for i, cid in enumerate(unique_clusters):
        mask = np.array(assignments) == cid
        subset = subset_members.iloc[mask]
        counts = subset[cat_col].value_counts()
        heights = [counts.get(c, 0) for c in categories]
        fig.add_trace(
            go.Bar(
                x=[str(c) for c in categories], y=heights,
                marker_color=colors[i % len(colors)],
                name=f"Pattern {cid}",
                legendgroup=f"c{cid}",
                showlegend=False,
            ),
            row=2, col=2,
        )
fig.update_xaxes(title_text=cat_col, row=2, col=2)
fig.update_yaxes(title_text="Count", row=2, col=2)

fig.update_layout(height=700, title_text="Pattern Profiles")
st.plotly_chart(fig, use_container_width=True, key="main_viz")

# ── JSON Download ─────────────────────────────────────────────────────────────

json_output = format_json(summary, profiles, metrics)
st.download_button(
    label="Download JSON Report",
    data=json.dumps(json_output, indent=2, default=str),
    file_name="pattern_profile.json",
    mime="application/json",
)
