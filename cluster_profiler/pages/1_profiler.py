"""Streamlit UI for the Pattern Profiler."""

import json
from pathlib import Path

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
    MEMBER_DENORM_PATH,
    MEMBER_LABELS_PATH,
    DEFAULT_REFERENCE_DATE,
    DESCRIPTION_COLUMNS,
)
from cluster_profiler.clustering import build_features, discover_clusters
from cluster_profiler.data_loader import apply_filters, load_data
from cluster_profiler.formatters import format_json
from cluster_profiler.profiler import build_subset_summary, profile_all_clusters
from cluster_profiler.synthetic import generate_synthetic_subscribers
from cluster_profiler.synthetic_claims import generate_synthetic_claims, DenormNotAvailableError
from cluster_profiler.synthetic_enrollment import generate_synthetic_enrollments
from cluster_profiler.edi_formatter import enrollment_to_edi
from cluster_profiler.naming import build_contextual_name


@st.cache_data
def cached_load_data():
    return load_data(MEMBER_DENORM_PATH, MEMBER_LABELS_PATH, DEFAULT_REFERENCE_DATE)


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

    for col_name, stats in profile.get("continuous", {}).items():
        label = "Age" if col_name == "_age" else "Tenure (months)"
        lo = max(0, stats["mean"] - stats["std"])
        hi = stats["mean"] + stats["std"]
        rules.append(f"{lo:.0f} \u2264 {label} \u2264 {hi:.0f}")

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

    family = profile.get("family", {})
    if family:
        if family.get("spouse_rate", 0) > 0.50:
            rules.append(f"Spouse present ({family['spouse_rate'] * 100:.0f}%)")
        elif family.get("spouse_rate", 0) < 0.20:
            rules.append(f"Spouse unlikely ({family['spouse_rate'] * 100:.0f}%)")
        avg_dep = family.get("avg_dependents", 0)
        if avg_dep >= 1.0:
            rules.append(f"Avg dependents \u2265 {avg_dep:.1f}")
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

    clauses.extend(build_inclusion_rules(profile))

    return " AND\n".join(clauses)


def _build_local_summary(profile: dict, pattern_id: int, total_members: int) -> str:
    """Build a plain-language summary from profile data without API call."""
    parts = []
    size = profile.get("size", 0)
    pct = profile.get("pct_of_subset", 0) * 100

    # Age and tenure
    age_stats = profile.get("continuous", {}).get("_age", {})
    tenure_stats = profile.get("continuous", {}).get("_tenure", {})
    if age_stats:
        mean_age = age_stats.get("mean", 0)
        parts.append(f"average age {mean_age:.0f}")
    if tenure_stats:
        mean_tenure = tenure_stats.get("mean", 0)
        if mean_tenure >= 60:
            parts.append(f"long-term members ({mean_tenure:.0f} months average tenure)")
        elif mean_tenure < 12:
            parts.append(f"recently enrolled ({mean_tenure:.0f} months average tenure)")
        else:
            parts.append(f"{mean_tenure:.0f} months average tenure")

    # Gender
    sex_pct = profile.get("categorical", {}).get("MEME_SEX", {}).get("pct", {})
    if sex_pct:
        m = sex_pct.get("M", 0) * 100
        f = sex_pct.get("F", 0) * 100
        if abs(m - f) < 10:
            parts.append("nearly equal gender distribution")
        elif m > f:
            parts.append(f"predominantly male ({m:.0f}%)")
        else:
            parts.append(f"predominantly female ({f:.0f}%)")

    # Marital status
    marital_pct = profile.get("categorical", {}).get("MEME_MARITAL_STATUS", {}).get("pct", {})
    if marital_pct:
        married = marital_pct.get("M", 0) * 100
        if married > 60:
            parts.append(f"majority married ({married:.0f}%)")
        elif married < 20:
            parts.append("mostly unmarried")

    # Family
    family = profile.get("family", {})
    avg_dep = family.get("avg_dependents", 0)
    spouse_rate = family.get("spouse_rate", 0)
    if avg_dep >= 1.0:
        parts.append(f"averaging {avg_dep:.1f} dependents")
    elif avg_dep < 0.3:
        parts.append("few or no dependents")
    if spouse_rate > 0.5:
        parts.append(f"{spouse_rate*100:.0f}% spouse coverage rate")

    # Descriptions
    descs = profile.get("descriptions", {})
    locations = descs.get("GRGR_NAME", [])

    location_str = f" in {', '.join(locations)}" if locations else ""
    detail_str = ", ".join(parts) if parts else "mixed demographics"

    return (
        f"This group represents {size:,} members ({pct:.1f}% of the subset){location_str}. "
        f"They are characterized by {detail_str}."
    )


@st.cache_data
def generate_pattern_summary(profile_json: str, pattern_id: int, total_members: int) -> str:
    """Generate a plain-language summary. Uses Claude API if available, local fallback otherwise."""
    import json as _json
    profile = _json.loads(profile_json)

    try:
        client = anthropic.Anthropic()
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": (
                    f"You are summarizing a member pattern from a health plan population analysis. "
                    f"This is pattern {pattern_id} out of a sub-population of {total_members} members. "
                    f"Write a concise 2-3 sentence plain-language summary describing who these members are "
                    f"based on their demographics, family structure, and categorical attributes. "
                    f"Focus on what makes this group distinctive. Do not use bullet points or headers.\n\n"
                    f"Profile data:\n{profile_json}"
                ),
            }],
        )
        return message.content[0].text
    except Exception:
        return _build_local_summary(profile, pattern_id, total_members)


# ── Data Loading ──────────────────────────────────────────────────────────────

df, labels_df = cached_load_data()

# ── Check for pre-selections from landing page ──────────────────────────────

preselect_grgr = st.session_state.pop("preselect_grgr_ck", None)
preselect_sgsg = st.session_state.pop("preselect_sgsg_ck", None)
preselect_cspd = st.session_state.pop("preselect_cspd_cat", None)
preselect_lobd = st.session_state.pop("preselect_lobd_id", None)
preselect_k = st.session_state.pop("preselect_k", None)
preselect_cluster_id = st.session_state.pop("preselect_cluster_id", None)
auto_run = st.session_state.pop("auto_run", False)

# ── Sidebar: Cascading Filters ────────────────────────────────────────────────

st.sidebar.header("Hierarchy Filters")


def _build_options(pairs_df, key_col, name_col):
    return [format_option(row[key_col], row[name_col]) for _, row in pairs_df.iterrows()]


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

if preselect_grgr is not None and "wk_group" not in st.session_state:
    st.session_state["wk_group"] = _match_preselect(group_options, preselect_grgr, cast=int)

selected_group_labels = st.sidebar.multiselect("Group", group_options, key="wk_group")
selected_grgr_cks = [int(parse_option_key(g)) for g in selected_group_labels]

# 2. Subgroup
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

# 3. Plan Category
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

# 4. Line of Business
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
st.sidebar.divider()
st.sidebar.header("Pattern Detection")
k_default = preselect_k if preselect_k is not None else 0
k_override = st.sidebar.number_input(
    "K override (0 = auto)", min_value=0, max_value=20, value=k_default, step=1
)
if k_override == 1:
    st.sidebar.warning("K must be 0 (auto) or at least 2.")
    k_override = 0
if st.sidebar.button("Run Profiler", type="primary", use_container_width=True) or auto_run:
    st.session_state["profiler_run"] = True

run = st.session_state.get("profiler_run", False)

# ── Main Area ─────────────────────────────────────────────────────────────────

st.markdown(
    '<span style="font-size:13px;color:gray;">'
    '<a href="/" target="_self" style="color:inherit;text-decoration:none;">System discovered patterns</a>'
    ' / Pattern profiler</span>',
    unsafe_allow_html=True,
)
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

pct_of_total = summary["total_members"] / df["MEME_CK"].nunique() * 100
col1, col2, col3 = st.columns(3)
col1.metric("Subset of Total", f"{pct_of_total:.1f}%")
col2.metric("Patterns Found", metrics["n_clusters"])
col3.metric("Members in Subset", f"{summary['total_members']:,}")

# ── Per-Pattern Profiles ──────────────────────────────────────────────────────

st.divider()
st.subheader("Pattern Details")

# Reorder profiles so the pre-selected pattern appears first
if preselect_cluster_id is not None:
    profiles = sorted(profiles, key=lambda p: p["cluster_id"] != preselect_cluster_id)

# Build contextual names for each profile
_grgr = summary.get("GRGR_NAME", [""])[0] if summary.get("GRGR_NAME") else ""
_sgsg = summary.get("SGSG_NAME", [""])[0] if summary.get("SGSG_NAME") else ""
_cat = summary.get("CSPD_CAT_DESC", [""])[0] if summary.get("CSPD_CAT_DESC") else ""
_lob = summary.get("PLDS_DESC", [""])[0] if summary.get("PLDS_DESC") else ""

profile_names = {}
for p in profiles:
    profile_names[p["cluster_id"]] = build_contextual_name(
        grgr_name=_grgr, sgsg_name=_sgsg,
        cspd_cat_desc=_cat, plds_desc=_lob,
        cluster_id=p["cluster_id"], profile=p,
    )

if len(profiles) <= 8:
    tab_labels = [profile_names[p["cluster_id"]] for p in profiles]
    tabs = st.tabs(tab_labels)
    containers = tabs
else:
    containers = [
        st.expander(
            profile_names[p["cluster_id"]],
            expanded=(p["cluster_id"] == preselect_cluster_id),
        )
        for p in profiles
    ]

for container, profile in zip(containers, profiles):
    with container:
        cid = profile["cluster_id"]
        mask = np.array(assignments) == cid
        raw_df = subset_members.iloc[mask]
        rule_name = profile_names.get(cid, f"Pattern {cid}")

        # ── Generate data (prominent, at top) ─────────────────────
        gen_col1, gen_col2, gen_col3, gen_col4, gen_col5 = st.columns([1.5, 1.5, 1, 1.5, 1.5])
        gen_type = gen_col1.selectbox(
            "Data type",
            ["Members", "Claims", "Enrollments"],
            key=f"gen_type_{cid}",
            label_visibility="collapsed",
        )
        n_records = gen_col2.number_input(
            "Records",
            min_value=1, max_value=100000, value=100, step=100,
            key=f"n_subs_{cid}",
            label_visibility="collapsed",
        )
        gen_clicked = gen_col3.button("Generate", key=f"gen_{cid}", type="primary")
        raw_clicked = gen_col4.button(f"Raw data ({len(raw_df)})", key=f"raw_{cid}")

        if gen_clicked:
            safe_name = rule_name.replace(" ", "_").replace("/", "_")
            try:
                if gen_type == "Members":
                    result_df = generate_synthetic_subscribers(
                        profile, filters_used, n_records, DEFAULT_REFERENCE_DATE,
                        source_data=raw_df,
                    )
                elif gen_type == "Claims":
                    member_result = generate_synthetic_subscribers(
                        profile, filters_used, max(10, n_records // 5), DEFAULT_REFERENCE_DATE,
                        source_data=raw_df,
                    )
                    result_df = generate_synthetic_claims(
                        profile, filters_used, member_result, n_records, DEFAULT_REFERENCE_DATE,
                    )
                elif gen_type == "Enrollments":
                    member_result = generate_synthetic_subscribers(
                        profile, filters_used, n_records, DEFAULT_REFERENCE_DATE,
                        source_data=raw_df,
                    )
                    result_df = generate_synthetic_enrollments(
                        member_result, filters_used, DEFAULT_REFERENCE_DATE,
                    )

                st.success(f"Generated {len(result_df)} {gen_type.lower()} records.")
                st.dataframe(result_df.head(20), width="stretch", hide_index=True)
                dl_col1, dl_col2 = st.columns(2)
                dl_col1.download_button(
                    f"Download {gen_type} CSV",
                    result_df.to_csv(index=False),
                    file_name=f"synthetic_{gen_type.lower()}_{safe_name}.csv",
                    mime="text/csv", key=f"dl_csv_{cid}_{gen_type}",
                )
                if gen_type == "Enrollments":
                    edi_content = enrollment_to_edi(result_df)
                    dl_col2.download_button(
                        "Download EDI 834", edi_content,
                        file_name=f"synthetic_834_{safe_name}.edi",
                        mime="text/plain", key=f"dl_edi_{cid}",
                    )
                st.caption("Generated data is for export only — not stored in the source repository.")

            except DenormNotAvailableError as e:
                st.error(str(e))
                st.info(
                    "**What's needed:** Provider and Claims denormalized models must be "
                    "delivered to `data/source/` before claims generation is available."
                )

        if raw_clicked:
            st.dataframe(raw_df, width="stretch", height=400)

        st.divider()

        # ── Summary row: size + AI summary ────────────────────────
        size_pct = profile['pct_of_subset'] * 100

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
        except Exception:
            llm_summary = None

        # Size metrics row
        pm1, pm2, pm3, pm4 = st.columns(4)
        pm1.metric("Members", f"{profile['size']:,}")
        pm2.metric("% of Subset", f"{size_pct:.1f}%")
        if profile.get("family"):
            pm3.metric("Avg Dependents", f"{profile['family']['avg_dependents']:.2f}")
            pm4.metric("Spouse Rate", f"{profile['family']['spouse_rate']:.1%}")

        if llm_summary:
            st.subheader("Summary")
            st.info(llm_summary)

        # ── Demographics + Categorical side by side ───────────────────
        left_col, right_col = st.columns(2)

        with left_col:
            if profile["continuous"]:
                st.markdown("**Demographics**")
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
                st.dataframe(
                    pd.DataFrame(demo_rows),
                    width="stretch",
                    hide_index=True,
                )

        with right_col:
            if profile["categorical"]:
                st.markdown("**Categorical Distributions**")
                for col_name, cat_data in profile["categorical"].items():
                    pct = cat_data["pct"]
                    if pct:
                        fig = px.bar(
                            x=list(pct.values()),
                            y=list(pct.keys()),
                            orientation="h",
                            labels={"x": "Proportion", "y": ""},
                            title=col_name,
                        )
                        fig.update_layout(
                            height=max(150, len(pct) * 45),
                            margin=dict(l=0, r=0, t=30, b=0),
                            showlegend=False,
                            xaxis_tickformat=".0%",
                        )
                        st.plotly_chart(
                            fig, width="stretch",
                            key=f"cat_{profile['cluster_id']}_{col_name}",
                        )

        # ── Pattern rule + downloads ──────────────────────────────────
        save_rule = build_save_rule(
            profile, filters_used,
            selected_group_labels, selected_subgroup_labels,
            selected_cat_labels, selected_lob_labels,
        )

        with st.expander("Pattern Rule"):
            if save_rule:
                st.code(save_rule, language=None)

                rule_name = st.text_input(
                    "Rule name",
                    value=profile_names.get(profile['cluster_id'], f"Pattern {profile['cluster_id']}"),
                    key=f"rule_name_{profile['cluster_id']}",
                )

                btn_col1, btn_col2 = st.columns(2)
                save_clicked = btn_col1.button("Save Rule", key=f"save_rule_{profile['cluster_id']}")
                view_rules_clicked = btn_col2.button("View Saved Rules", key=f"view_rules_{profile['cluster_id']}")

                if save_clicked:
                    rules_path = Path("data/pattern_rules.csv")
                    rules_path.parent.mkdir(parents=True, exist_ok=True)
                    new_row = pd.DataFrame([{
                        "name": rule_name,
                        "pattern_id": profile["cluster_id"],
                        "members": profile["size"],
                        "rule": save_rule,
                    }])
                    if rules_path.exists():
                        existing = pd.read_csv(rules_path)
                        combined = pd.concat([existing, new_row], ignore_index=True)
                    else:
                        combined = new_row
                    combined.to_csv(rules_path, index=False)
                    st.success(f"Rule '{rule_name}' saved to {rules_path}")

                if view_rules_clicked:
                    rules_path = Path("data/pattern_rules.csv")
                    if rules_path.exists():
                        rules_df = pd.read_csv(rules_path).astype(str)
                        st.dataframe(rules_df, width="stretch", hide_index=True)
                    else:
                        st.info("No saved rules yet.")


# ── Visualizations ────────────────────────────────────────────────────────────

st.divider()
st.subheader("Pattern Visualizations")

X, _scaler = build_features(subset_members)
unique_clusters = sorted(set(assignments))
colors = px.colors.qualitative.T10

fig = make_subplots(
    rows=1, cols=3,
    subplot_titles=("PCA Scatter", "Age Distribution", "Tenure Distribution"),
    horizontal_spacing=0.08,
)

# PCA scatter
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
                name=profile_names.get(cid, f"P{cid}"),
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

# Age histogram
for i, cid in enumerate(unique_clusters):
    mask = np.array(assignments) == cid
    vals = subset_members.iloc[mask]["_age"].dropna()
    if len(vals) > 0:
        fig.add_trace(
            go.Histogram(
                x=vals, nbinsx=20,
                marker_color=colors[i % len(colors)],
                name=profile_names.get(cid, f"P{cid}"),
                legendgroup=f"c{cid}",
                showlegend=False,
            ),
            row=1, col=2,
        )
fig.update_xaxes(title_text="Age (years)", row=1, col=2)
fig.update_yaxes(title_text="Count", row=1, col=2)

# Tenure histogram
for i, cid in enumerate(unique_clusters):
    mask = np.array(assignments) == cid
    vals = subset_members.iloc[mask]["_tenure"].dropna()
    if len(vals) > 0:
        fig.add_trace(
            go.Histogram(
                x=vals, nbinsx=20,
                marker_color=colors[i % len(colors)],
                name=profile_names.get(cid, f"P{cid}"),
                legendgroup=f"c{cid}",
                showlegend=False,
            ),
            row=1, col=3,
        )
fig.update_xaxes(title_text="Tenure (months)", row=1, col=3)
fig.update_yaxes(title_text="Count", row=1, col=3)

fig.update_layout(
    barmode="stack",
    height=400,
    margin=dict(l=40, r=40, t=40, b=40),
    legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
)
st.plotly_chart(fig, width="stretch", key="main_viz")

# ── JSON Download ─────────────────────────────────────────────────────────────

st.divider()

json_output = format_json(summary, profiles, metrics)
st.download_button(
    label="Download Full JSON Report",
    data=json.dumps(json_output, indent=2, default=str),
    file_name="pattern_profile.json",
    mime="application/json",
)
