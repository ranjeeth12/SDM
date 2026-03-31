"""Streamlit UI for the Pattern Profiler."""

import json
import sys
from pathlib import Path

# Ensure the project root is on sys.path so absolute imports work
# when Streamlit runs this file directly.
from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

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


def build_inclusion_rules(profile: dict) -> list[str]:
    """Derive human-readable inclusion rules from a pattern profile."""
    rules = []

    # Continuous feature rules: mean ± 1 std range
    for col_name, stats in profile.get("continuous", {}).items():
        label = "Age" if col_name == "_age" else "Tenure (months)"
        lo = max(0, stats["mean"] - stats["std"])
        hi = stats["mean"] + stats["std"]
        rules.append(f"{lo:.0f} ≤ {label} ≤ {hi:.0f}")

    # Categorical rules: majority category (>50%) or top two if none dominant
    for col_name, cat_data in profile.get("categorical", {}).items():
        pct = cat_data.get("pct", {})
        if not pct:
            continue
        sorted_cats = sorted(pct.items(), key=lambda x: x[1], reverse=True)
        top_val, top_pct = sorted_cats[0]
        if top_pct > 0.50:
            rules.append(f"{col_name} = {top_val} ({top_pct * 100:.0f}%)")
        else:
            top_two = sorted_cats[:2]
            parts = " or ".join(f"{v} ({p * 100:.0f}%)" for v, p in top_two)
            rules.append(f"{col_name} IN [{parts}]")

    # Family rules
    family = profile.get("family", {})
    if family:
        if family.get("spouse_rate", 0) > 0.50:
            rules.append(f"Spouse present ({family['spouse_rate'] * 100:.0f}%)")
        elif family.get("spouse_rate", 0) < 0.20:
            rules.append(f"Spouse unlikely ({family['spouse_rate'] * 100:.0f}%)")
        avg_dep = family.get("avg_dependents", 0)
        if avg_dep >= 1.0:
            rules.append(f"Avg dependents ≥ {avg_dep:.1f}")
        elif avg_dep < 0.3:
            rules.append(f"Few/no dependents (avg {avg_dep:.1f})")

    return rules


def build_save_rule(
    profile: dict,
    filters_used: dict,
    selected_group_labels: list[str],
    selected_subgroup_labels: list[str],
    selected_cat_labels: list[str],
    selected_lob_labels: list[str],
) -> str:
    """Build a composite AND-joined rule combining hierarchy filters and demographic rules."""
    clauses = []

    # Hierarchy conditions from sidebar selections
    if filters_used.get("grgr_ck"):
        keys = ", ".join(str(k) for k in filters_used["grgr_ck"])
        clauses.append(f"GRGR_CK IN ({keys})")
    if filters_used.get("sgsg_ck"):
        keys = ", ".join(str(k) for k in filters_used["sgsg_ck"])
        clauses.append(f"SGSG_CK IN ({keys})")
    if filters_used.get("cspd_cat"):
        keys = ", ".join(f"'{k}'" for k in filters_used["cspd_cat"])
        clauses.append(f"CSPD_CAT IN ({keys})")
    if filters_used.get("lobd_id"):
        keys = ", ".join(f"'{k}'" for k in filters_used["lobd_id"])
        clauses.append(f"LOBD_ID IN ({keys})")

    # Demographic / categorical inclusion rules
    clauses.extend(build_inclusion_rules(profile))

    return " AND\n".join(clauses)


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

# ── Check for pre-selections from landing page ──────────────────────────────

preselect_grgr = st.session_state.pop("preselect_grgr_ck", None)
preselect_sgsg = st.session_state.pop("preselect_sgsg_ck", None)
preselect_cspd = st.session_state.pop("preselect_cspd_cat", None)
preselect_lobd = st.session_state.pop("preselect_lobd_id", None)
auto_run = st.session_state.pop("auto_run", False)

# ── Sidebar: Cascading Filters ────────────────────────────────────────────────

st.sidebar.header("Hierarchy Filters")

if st.sidebar.button("Back to Overview"):
    st.switch_page("app.py")

# Helper: build options list from pairs dataframe
def _build_options(pairs_df, key_col, name_col):
    return [format_option(row[key_col], row[name_col]) for _, row in pairs_df.iterrows()]

# Helper: find option strings matching preselected keys
def _match_preselect(options, preselect_values, cast=str):
    if preselect_values is None:
        return []
    preselect_set = {cast(v) for v in preselect_values}
    return [opt for opt in options if cast(parse_option_key(opt)) in preselect_set]

# 1. Group
group_pairs = (
    df[["GRGR_CK", "GRGR_NAME"]]
    .drop_duplicates()
    .sort_values("GRGR_NAME")
)
group_options = _build_options(group_pairs, "GRGR_CK", "GRGR_NAME")

# Pre-set widget state before rendering so Streamlit uses it
if preselect_grgr is not None and "wk_group" not in st.session_state:
    st.session_state["wk_group"] = _match_preselect(group_options, preselect_grgr, cast=int)

selected_group_labels = st.sidebar.multiselect("Group", group_options, key="wk_group")
selected_grgr_cks = [int(parse_option_key(g)) for g in selected_group_labels]

# 2. Subgroup — filtered by selected groups
filtered = df[df["GRGR_CK"].isin(selected_grgr_cks)] if selected_grgr_cks else df
subgroup_pairs = (
    filtered[["SGSG_CK", "SGSG_NAME"]]
    .drop_duplicates()
    .sort_values("SGSG_NAME")
)
subgroup_options = _build_options(subgroup_pairs, "SGSG_CK", "SGSG_NAME")

if preselect_sgsg is not None and "wk_subgroup" not in st.session_state:
    st.session_state["wk_subgroup"] = _match_preselect(subgroup_options, preselect_sgsg, cast=int)

selected_subgroup_labels = st.sidebar.multiselect("Subgroup", subgroup_options, key="wk_subgroup")
selected_sgsg_cks = [int(parse_option_key(s)) for s in selected_subgroup_labels]

# 3. Plan Category — filtered by group + subgroup
if selected_sgsg_cks:
    filtered = filtered[filtered["SGSG_CK"].isin(selected_sgsg_cks)]
cat_pairs = (
    filtered[["CSPD_CAT", "CSPD_CAT_DESC"]]
    .drop_duplicates()
    .sort_values("CSPD_CAT_DESC")
)
cat_options = _build_options(cat_pairs, "CSPD_CAT", "CSPD_CAT_DESC")

if preselect_cspd is not None and "wk_cat" not in st.session_state:
    st.session_state["wk_cat"] = _match_preselect(cat_options, preselect_cspd, cast=str)

selected_cat_labels = st.sidebar.multiselect("Plan Category", cat_options, key="wk_cat")
selected_cspd_cats = [parse_option_key(c) for c in selected_cat_labels]

# 4. Line of Business — filtered by group + subgroup + plan category
if selected_cspd_cats:
    filtered = filtered[filtered["CSPD_CAT"].isin(selected_cspd_cats)]
lob_pairs = (
    filtered[["LOBD_ID", "PLDS_DESC"]]
    .drop_duplicates()
    .sort_values("PLDS_DESC")
)
lob_options = _build_options(lob_pairs, "LOBD_ID", "PLDS_DESC")

if preselect_lobd is not None and "wk_lob" not in st.session_state:
    st.session_state["wk_lob"] = _match_preselect(lob_options, preselect_lobd, cast=str)

selected_lob_labels = st.sidebar.multiselect("Line of Business", lob_options, key="wk_lob")
selected_lobd_ids = [parse_option_key(l) for l in selected_lob_labels]

# Pattern detection options
st.sidebar.markdown("---")
st.sidebar.header("Pattern Detection Options")
k_override = st.sidebar.number_input(
    "K override (0 = auto)", min_value=0, max_value=20, value=0, step=1
)
run = st.sidebar.button("Run Profiler", type="primary") or auto_run

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
            st.dataframe(pd.DataFrame(demo_rows), width="stretch", hide_index=True)

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
                    st.plotly_chart(fig, width="stretch",
                                     key=f"cat_{profile['cluster_id']}_{col_name}")

        # Family stats
        if profile.get("family"):
            st.subheader("Family Statistics")
            fc1, fc2 = st.columns(2)
            fc1.metric("Avg Dependents", f"{profile['family']['avg_dependents']:.2f}")
            fc2.metric("Spouse Rate", f"{profile['family']['spouse_rate']:.1%}")

        # Composite pattern rule
        save_rule = build_save_rule(
            profile, filters_used,
            selected_group_labels, selected_subgroup_labels,
            selected_cat_labels, selected_lob_labels,
        )
        if save_rule:
            st.subheader("Pattern Rule")
            st.code(save_rule, language=None)
            rule_file_content = (
                f"Pattern {profile['cluster_id']} Rule\n"
                f"Members: {profile['size']}\n"
                f"{'=' * 40}\n\n"
                f"{save_rule}\n"
            )
            st.download_button(
                label="Save Rule",
                data=rule_file_content,
                file_name=f"pattern_{profile['cluster_id']}_rule.txt",
                mime="text/plain",
                key=f"save_rule_{profile['cluster_id']}",
            )

        # Raw data viewer
        cid = profile["cluster_id"]
        mask = np.array(assignments) == cid
        raw_df = subset_members.iloc[mask]
        with st.expander(f"View Raw Data ({len(raw_df)} rows)"):
            st.dataframe(raw_df, width="stretch", height=400)

# ── Visualizations (Plotly) ───────────────────────────────────────────────────

st.header("Pattern Visualizations")

X, _scaler = build_features(subset_members)
unique_clusters = sorted(set(assignments))
n_clusters = len(unique_clusters)
cluster_labels = [f"Pattern {c}" for c in assignments]

fig = make_subplots(
    rows=1, cols=3,
    subplot_titles=("PCA Scatter", "Age Distribution", "Tenure Distribution"),
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
            row=1, col=3,
        )
fig.update_xaxes(title_text="Tenure (months)", row=1, col=3)
fig.update_yaxes(title_text="Count", row=1, col=3)

fig.update_layout(height=450, title_text="Pattern Profiles")
st.plotly_chart(fig, width="stretch", key="main_viz")

# ── JSON Download ─────────────────────────────────────────────────────────────

json_output = format_json(summary, profiles, metrics)
st.download_button(
    label="Download JSON Report",
    data=json.dumps(json_output, indent=2, default=str),
    file_name="pattern_profile.json",
    mime="application/json",
)
