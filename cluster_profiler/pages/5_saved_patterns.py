"""Saved Patterns — user-curated patterns ready for data generation.

Patterns saved from the Profiler appear here. Each saved pattern stores:
  - Full hierarchy context (Group, Plan, LOB)
  - Complete demographic profile (age, gender, family structure)
  - Tags for searchability
  - Pattern confidence score

Actions: generate data directly, open in profiler, or remove from saved.
"""

import json

import numpy as np
import pandas as pd
import streamlit as st

from cluster_profiler import db
from cluster_profiler.config import DEFAULT_REFERENCE_DATE
from cluster_profiler.data_loader import load_filtered_members
from cluster_profiler.synthetic import generate_synthetic_subscribers
from cluster_profiler.synthetic_enrollment import generate_synthetic_enrollments
from cluster_profiler.edi_formatter import enrollment_to_edi


st.title("Saved Patterns")
st.caption(
    "Your curated collection of validated patterns. "
    "Save patterns from the Profiler to make them available here for quick data generation."
)

saved = db.get_saved_patterns()

if not saved:
    st.info(
        "No saved patterns yet. Go to the **Pattern Profiler**, explore a hierarchy, "
        "and click **⭐ Save Pattern** on any pattern you want to keep."
    )
    st.stop()

# ── Summary metrics ──────────────────────────────────────────────────────────

m1, m2, m3 = st.columns(3)
m1.metric("Saved Patterns", len(saved))
total_members = sum(p["member_count"] for p in saved)
m2.metric("Total Members Covered", f"{total_members:,}")
unique_groups = len(set(p.get("grgr_name", "") for p in saved if p.get("grgr_name")))
m3.metric("Groups Represented", unique_groups)

st.divider()

# ── Pattern cards ────────────────────────────────────────────────────────────

for i, pattern in enumerate(saved):
    profile = json.loads(pattern["profile_json"]) if pattern.get("profile_json") else {}
    tags = db.get_tags(pattern["id"])

    # Header
    col_name, col_remove = st.columns([5, 1])
    with col_name:
        st.markdown(
            f'<div style="border-left:3px solid #1D9E75;padding-left:12px">'
            f'<span style="font-size:17px;font-weight:500;">{pattern.get("contextual_name", "")}</span>'
            f'<br/><span style="font-size:12px;color:gray;">'
            f'{pattern.get("grgr_name", "")} · {pattern.get("cspd_cat_desc") or "All plans"} · '
            f'{pattern.get("plds_desc") or "All LOB"}'
            f'</span></div>',
            unsafe_allow_html=True,
        )
    with col_remove:
        if st.button("Remove", key=f"unsave_{pattern['id']}", type="secondary"):
            db.mark_pattern_unsaved(pattern["id"])
            st.rerun()

    # Key metrics
    continuous = profile.get("continuous", {})
    age_mean = continuous.get("_age", {}).get("mean", 0)
    tenure_mean = continuous.get("_tenure", {}).get("mean", 0)
    family = profile.get("family", {})
    sex_pct = profile.get("categorical", {}).get("MEME_SEX", {}).get("pct", {})

    pm1, pm2, pm3, pm4, pm5 = st.columns(5)
    pm1.metric("Members", f"{pattern['member_count']:,}")
    pm2.metric("Avg Age", f"{age_mean:.0f}" if age_mean else "—")
    f_pct = sex_pct.get("F", 0) * 100
    m_pct = sex_pct.get("M", 0) * 100
    pm3.metric("F / M", f"{f_pct:.0f} / {m_pct:.0f}" if sex_pct else "—")
    pm4.metric("Avg Dependents", f"{family.get('avg_dependents', 0):.1f}")
    pm5.metric("Confidence", f"{pattern.get('silhouette', 0):.3f}")

    # Tags
    if tags:
        tag_str = " · ".join(f"`{t['tag']}`" for t in tags[:8])
        st.markdown(f"**Tags:** {tag_str}")

    # Actions
    act1, act2, act3, act4 = st.columns(4)

    # Generate members
    if act1.button("Generate Members", key=f"gen_mem_{pattern['id']}", type="primary"):
        st.session_state[f"_gen_active_{pattern['id']}"] = "members"

    # Generate enrollments
    if act2.button("Generate Enrollments", key=f"gen_enr_{pattern['id']}"):
        st.session_state[f"_gen_active_{pattern['id']}"] = "enrollments"

    # Open in profiler
    if act3.button("Open in Profiler", key=f"open_prof_{pattern['id']}"):
        combo = {
            "grgr_ck": json.loads(pattern["grgr_ck"]) if pattern.get("grgr_ck") else None,
            "sgsg_ck": json.loads(pattern["sgsg_ck"]) if pattern.get("sgsg_ck") else None,
            "cspd_cat": json.loads(pattern["cspd_cat"]) if pattern.get("cspd_cat") else None,
            "lobd_id": json.loads(pattern["lobd_id"]) if pattern.get("lobd_id") else None,
        }
        st.session_state["preselect_grgr_ck"] = combo.get("grgr_ck")
        st.session_state["preselect_sgsg_ck"] = combo.get("sgsg_ck")
        st.session_state["preselect_cspd_cat"] = combo.get("cspd_cat")
        st.session_state["preselect_lobd_id"] = combo.get("lobd_id")
        st.session_state["preselect_cluster_id"] = pattern.get("cluster_id", 0)
        st.session_state["auto_run"] = True
        st.switch_page("pages/1_profiler.py")

    # Volume input (shared)
    n_records = act4.number_input(
        "Records", min_value=10, max_value=100000, value=100, step=100,
        key=f"n_rec_{pattern['id']}", label_visibility="collapsed",
    )

    # Handle generation
    gen_active = st.session_state.pop(f"_gen_active_{pattern['id']}", None)
    if gen_active:
        combo = {
            "grgr_ck": json.loads(pattern["grgr_ck"]) if pattern.get("grgr_ck") else None,
            "sgsg_ck": json.loads(pattern["sgsg_ck"]) if pattern.get("sgsg_ck") else None,
            "cspd_cat": json.loads(pattern["cspd_cat"]) if pattern.get("cspd_cat") else None,
            "lobd_id": json.loads(pattern["lobd_id"]) if pattern.get("lobd_id") else None,
        }
        filters = {k: v for k, v in combo.items() if v is not None}

        try:
            subset_m, _, _, f_used = load_filtered_members(**filters)
        except ValueError:
            subset_m = None

        safe_name = pattern.get("contextual_name", "pattern").replace(" ", "_").replace("/", "_")

        if gen_active == "members":
            result = generate_synthetic_subscribers(
                profile, filters, n_records, DEFAULT_REFERENCE_DATE,
                source_data=subset_m,
            )
            st.success(f"Generated {len(result)} member records.")
            st.dataframe(result.head(20), hide_index=True, width="stretch")
            st.download_button(
                "Download CSV", result.to_csv(index=False),
                file_name=f"members_{safe_name}.csv",
                mime="text/csv", key=f"dl_mem_{pattern['id']}",
            )
            db.log_generation([pattern["id"]], "members", len(result))

        elif gen_active == "enrollments":
            members = generate_synthetic_subscribers(
                profile, filters, n_records, DEFAULT_REFERENCE_DATE,
                source_data=subset_m,
            )
            enrollments = generate_synthetic_enrollments(members, filters, DEFAULT_REFERENCE_DATE)
            st.success(f"Generated {len(enrollments)} enrollment records.")
            st.dataframe(enrollments.head(20), hide_index=True, width="stretch")
            dl1, dl2 = st.columns(2)
            dl1.download_button(
                "Download CSV", enrollments.to_csv(index=False),
                file_name=f"enrollments_{safe_name}.csv",
                mime="text/csv", key=f"dl_enr_{pattern['id']}",
            )
            edi = enrollment_to_edi(enrollments)
            dl2.download_button(
                "Download EDI 834", edi,
                file_name=f"834_{safe_name}.edi",
                mime="text/plain", key=f"dl_edi_{pattern['id']}",
            )
            db.log_generation([pattern["id"]], "enrollments", len(enrollments))

        st.caption("Generated data is for export only — not stored in the source repository.")

    st.divider()
