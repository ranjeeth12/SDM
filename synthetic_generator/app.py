"""Streamlit UI for the Synthetic Data Generator."""

import os
import sys
from pathlib import Path

_package_dir = str(Path(__file__).resolve().parent)
_project_root = str(Path(__file__).resolve().parent.parent)
if _package_dir not in sys.path:
    sys.path.insert(0, _package_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pandas as pd
import streamlit as st
import yaml

from engine import SyntheticGenerator, generate_auto_config

st.set_page_config(page_title="Synthetic Data Generator", layout="wide")
st.title("Synthetic Data Generator")

# ── Sidebar: Generation Parameters ───────────────────────────────────────────

st.sidebar.header("Generation Mode")
mode = st.sidebar.radio(
    "Configuration source",
    ["Auto-generate", "YAML config file"],
    help="Auto-generate builds a config from real lookup tables. "
         "YAML lets you use a hand-crafted config.",
)

st.sidebar.markdown("---")
st.sidebar.header("Parameters")

seed = st.sidebar.number_input("Random seed", min_value=0, value=42, step=1)
total_subscribers = st.sidebar.number_input(
    "Total subscribers", min_value=10, max_value=10000, value=600, step=50,
)

if mode == "Auto-generate":
    n_groups = st.sidebar.number_input(
        "Number of groups", min_value=1, max_value=20, value=6, step=1,
    )
    n_clusters_per_level = st.sidebar.number_input(
        "Patterns per hierarchy level", min_value=2, max_value=10, value=3, step=1,
    )
else:
    default_config_path = os.path.join(
        os.path.dirname(__file__), "config", "default.yaml"
    )
    config_path = st.sidebar.text_input("Config file path", value=default_config_path)

st.sidebar.markdown("---")
st.sidebar.header("Enrollment Settings")

term_rate = st.sidebar.slider(
    "Termination rate", min_value=0.0, max_value=1.0, value=0.10, step=0.01,
)
reinstate_rate = st.sidebar.slider(
    "Reinstatement rate", min_value=0.0, max_value=1.0, value=0.40, step=0.01,
)

st.sidebar.markdown("---")
st.sidebar.header("Plan Enrollment Weights")
st.sidebar.caption("Probability a subscriber enrolls in each plan type")

weight_medical = st.sidebar.slider(
    "Medical (M)", min_value=0.0, max_value=1.0, value=1.0, step=0.05,
)
weight_dental = st.sidebar.slider(
    "Dental (D)", min_value=0.0, max_value=1.0, value=0.60, step=0.05,
)
weight_case_mgmt = st.sidebar.slider(
    "Case Management (C)", min_value=0.0, max_value=1.0, value=0.30, step=0.05,
)

st.sidebar.markdown("---")
st.sidebar.header("Level Weights")
st.sidebar.caption("Blending weights across hierarchy levels (must sum to 1.0)")

w_group = st.sidebar.slider("Group", min_value=0.0, max_value=1.0, value=0.40, step=0.05)
w_subgroup = st.sidebar.slider("Subgroup", min_value=0.0, max_value=1.0, value=0.20, step=0.05)
w_plan = st.sidebar.slider("Plan", min_value=0.0, max_value=1.0, value=0.20, step=0.05)
w_product = st.sidebar.slider("Product", min_value=0.0, max_value=1.0, value=0.20, step=0.05)

weight_sum = w_group + w_subgroup + w_plan + w_product
if abs(weight_sum - 1.0) > 0.01:
    st.sidebar.warning(f"Level weights sum to {weight_sum:.2f} — should be 1.0")

st.sidebar.markdown("---")
generate_btn = st.sidebar.button("Generate Data", type="primary")

# ── Main Area ─────────────────────────────────────────────────────────────────

if not generate_btn:
    st.info("Configure parameters in the sidebar and click **Generate Data** to start.")
    st.stop()

# Build config
with st.spinner("Building configuration..."):
    if mode == "Auto-generate":
        config = generate_auto_config(
            seed=seed,
            total_subscribers=total_subscribers,
            n_groups=n_groups,
            n_clusters_per_level=n_clusters_per_level,
        )
    else:
        if not os.path.exists(config_path):
            st.error(f"Config file not found: {config_path}")
            st.stop()
        with open(config_path) as f:
            config = yaml.safe_load(f)

    # Apply overrides
    config["seed"] = seed
    config["total_subscribers"] = total_subscribers
    config["level_weights"] = {
        "group": w_group, "subgroup": w_subgroup,
        "plan": w_plan, "product": w_product,
    }
    config["plan_enrollment_weights"] = {
        "M": weight_medical, "D": weight_dental, "C": weight_case_mgmt,
    }
    config.setdefault("enrollment", {})
    config["enrollment"]["term_rate"] = term_rate
    config["enrollment"]["reinstate_rate"] = reinstate_rate

# Generate data
with st.spinner("Generating synthetic data..."):
    try:
        gen = SyntheticGenerator(config)
        data_df, labels_df = gen.generate()
    except Exception as exc:
        st.error(f"Generation failed: {exc}")
        st.stop()

# ── Results ───────────────────────────────────────────────────────────────────

st.header("Generation Results")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Rows", f"{len(data_df):,}")
col2.metric("Unique Members", f"{data_df['MEME_CK'].nunique():,}")
col3.metric("Subscribers", f"{data_df['SBSB_CK'].nunique():,}")
col4.metric("Groups", f"{data_df['GRGR_CK'].nunique():,}")

# Group breakdown
st.subheader("Group Breakdown")
group_summary = (
    data_df.groupby(["GRGR_CK", "GRGR_NAME"])
    .agg(
        rows=("MEME_CK", "count"),
        members=("MEME_CK", "nunique"),
        subscribers=("SBSB_CK", "nunique"),
    )
    .reset_index()
    .sort_values("subscribers", ascending=False)
)
st.dataframe(group_summary, use_container_width=True, hide_index=True)

# Plan type breakdown
st.subheader("Plan Type Distribution")
plan_summary = (
    data_df.groupby(["CSPD_CAT", "CSPD_CAT_DESC"])
    .agg(rows=("MEME_CK", "count"), members=("MEME_CK", "nunique"))
    .reset_index()
    .sort_values("members", ascending=False)
)
st.dataframe(plan_summary, use_container_width=True, hide_index=True)

# Data preview
st.subheader("Data Preview")
with st.expander("View generated data (first 100 rows)"):
    st.dataframe(data_df.head(100), use_container_width=True, height=400)

with st.expander("View labels (first 100 rows)"):
    st.dataframe(labels_df.head(100), use_container_width=True, height=400)

# ── Downloads ─────────────────────────────────────────────────────────────────

st.subheader("Download")
dl1, dl2, dl3 = st.columns(3)

dl1.download_button(
    label="Download Data CSV",
    data=data_df.to_csv(index=False),
    file_name="MEMBER_GROUP_PLAN_FLAT_generated.csv",
    mime="text/csv",
)

dl2.download_button(
    label="Download Labels CSV",
    data=labels_df.to_csv(index=False),
    file_name="MEMBER_GROUP_PLAN_FLAT_generated_labels.csv",
    mime="text/csv",
)

dl3.download_button(
    label="Download Config YAML",
    data=yaml.dump(config, default_flow_style=False, sort_keys=False),
    file_name="generation_config.yaml",
    mime="text/yaml",
)

# ── Save to disk ──────────────────────────────────────────────────────────────

st.subheader("Save to Project")
save_col1, save_col2 = st.columns([3, 1])
default_output = os.path.join(_project_root, "data", "MEMBER_GROUP_PLAN_FLAT_generated.csv")
output_path = save_col1.text_input("Output path", value=default_output)

if save_col2.button("Save to Disk"):
    try:
        labels_path = output_path.replace(".csv", "_labels.csv")
        data_df.to_csv(output_path, index=False)
        labels_df.to_csv(labels_path, index=False)
        st.success(f"Saved {len(data_df):,} rows to `{output_path}` and labels to `{labels_path}`")
    except Exception as exc:
        st.error(f"Failed to save: {exc}")
