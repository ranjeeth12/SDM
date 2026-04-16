"""Landing page: discover and display the top patterns across all hierarchy combos.

Flow:
  1. Check DB for persisted patterns → display instantly if found
  2. First-ever run or explicit refresh → run discovery, persist to DB
  3. Subsequent visits → instant load from DB
"""

import numpy as np
import pandas as pd
import streamlit as st

from cluster_profiler.config import DEFAULT_REFERENCE_DATE
from cluster_profiler.data_loader import load_data
from cluster_profiler.discovery import discover_top_patterns
from cluster_profiler import db


@st.cache_data
def cached_load_data():
    return load_data()


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

st.title("System Discovered Patterns")
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
        "⭐": "✓" if pattern.get("saved_by") else "",
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

# Handle row click → show inline preview panel
selected_rows = event.selection.rows
if selected_rows:
    idx = selected_rows[0]
    pattern = patterns[idx]

    import json as _json

    combo = {
        "grgr_ck": _json.loads(pattern["grgr_ck"]) if pattern.get("grgr_ck") else None,
        "sgsg_ck": _json.loads(pattern["sgsg_ck"]) if pattern.get("sgsg_ck") else None,
        "cspd_cat": _json.loads(pattern["cspd_cat"]) if pattern.get("cspd_cat") else None,
        "lobd_id": _json.loads(pattern["lobd_id"]) if pattern.get("lobd_id") else None,
    }

    profile = _json.loads(pattern["profile_json"]) if pattern.get("profile_json") else {}
    tags = db.get_tags(pattern["id"])

    # ── Preview Panel ────────────────────────────────────────────────
    st.markdown(
        f'<div style="border-left:3px solid #1D9E75;padding-left:12px;margin-top:4px">'
        f'<span style="font-size:18px;font-weight:500;">{pattern.get("contextual_name", "")}</span>'
        f'<br/><span style="font-size:13px;color:gray;">'
        f'{pattern.get("grgr_name", "")} · {pattern.get("cspd_cat_desc") or "All plans"} · {pattern.get("plds_desc") or "All LOB"}'
        f'</span></div>',
        unsafe_allow_html=True,
    )

    # Key metrics
    continuous = profile.get("continuous", {})
    age_mean = continuous.get("_age", {}).get("mean", 0)
    family = profile.get("family", {})
    sex_pct = profile.get("categorical", {}).get("MEME_SEX", {}).get("pct", {})
    marital_pct = profile.get("categorical", {}).get("MEME_MARITAL_STATUS", {}).get("pct", {})

    pm1, pm2, pm3, pm4, pm5 = st.columns(5)
    pm1.metric("Members", f"{pattern['member_count']:,}")
    pm2.metric("Avg age", f"{age_mean:.1f}" if age_mean else "—")
    f_pct = sex_pct.get("F", 0) * 100
    m_pct = sex_pct.get("M", 0) * 100
    pm3.metric("F / M", f"{f_pct:.0f} / {m_pct:.0f}" if sex_pct else "—")
    pm4.metric("Married", f"{marital_pct.get('M', 0) * 100:.0f}%" if marital_pct else "—")
    pm5.metric("Avg dependents", f"{family.get('avg_dependents', 0):.2f}")

    # Summary
    # Inline summary
    tenure_stats = continuous.get("_tenure", {})
    parts = []
    if age_mean:
        parts.append(f"average age {age_mean:.0f}")
    if tenure_stats.get("mean"):
        parts.append(f"{tenure_stats['mean']:.0f} months tenure")
    if sex_pct:
        if abs(f_pct - m_pct) < 10:
            parts.append("equal gender distribution")
        elif m_pct > f_pct:
            parts.append(f"predominantly male ({m_pct:.0f}%)")
        else:
            parts.append(f"predominantly female ({f_pct:.0f}%)")
    if marital_pct.get("M", 0) > 0.6:
        parts.append(f"majority married ({marital_pct['M']*100:.0f}%)")
    if family.get("avg_dependents", 0) >= 1:
        parts.append(f"{family['avg_dependents']:.1f} dependents on average")
    if parts:
        st.caption(f"{pattern['member_count']:,} members: {', '.join(parts)}.")

    # Tags
    if tags:
        tag_str = " · ".join(f"`{t['tag']}`" for t in tags[:8])
        st.markdown(f"**Tags:** {tag_str}")

    # Action buttons
    btn1, btn2, btn3 = st.columns(3)
    if btn1.button("Open full profiler", key="preview_open_profiler", type="primary"):
        st.session_state["navigate_to_pattern"] = combo
        st.session_state["preselect_k"] = None
        st.session_state["preselect_cluster_id"] = pattern.get("cluster_id", 0)
        st.rerun()

    # Inline quick generate
    if btn2.button("Quick generate members", key="preview_gen_members"):
        from cluster_profiler.synthetic import generate_synthetic_subscribers
        from cluster_profiler.data_loader import apply_filters
        filters = {k: v for k, v in combo.items() if v is not None}
        raw_df = None
        try:
            subset_m, _, _, f_used = apply_filters(df, labels_df, **filters)
            raw_df = subset_m
        except Exception:
            pass
        result = generate_synthetic_subscribers(profile, filters, 100, DEFAULT_REFERENCE_DATE, source_data=raw_df)
        st.success(f"Generated {len(result)} member records.")
        st.dataframe(result.head(20), hide_index=True, width="stretch")
        st.download_button("Download CSV", result.to_csv(index=False),
                           file_name=f"quick_gen_{pattern['contextual_name']}.csv",
                           mime="text/csv", key="preview_dl")
        st.caption("Generated data is for export only — not stored in the source repository.")

    if btn3.button("Quick generate enrollments", key="preview_gen_enroll"):
        from cluster_profiler.synthetic import generate_synthetic_subscribers
        from cluster_profiler.synthetic_enrollment import generate_synthetic_enrollments
        from cluster_profiler.edi_formatter import enrollment_to_edi
        from cluster_profiler.data_loader import apply_filters
        filters = {k: v for k, v in combo.items() if v is not None}
        raw_df = None
        try:
            subset_m, _, _, f_used = apply_filters(df, labels_df, **filters)
            raw_df = subset_m
        except Exception:
            pass
        members = generate_synthetic_subscribers(profile, filters, 100, DEFAULT_REFERENCE_DATE, source_data=raw_df)
        enrollments = generate_synthetic_enrollments(members, filters, DEFAULT_REFERENCE_DATE)
        st.success(f"Generated {len(enrollments)} enrollment records.")
        st.dataframe(enrollments.head(20), hide_index=True, width="stretch")
        dl1, dl2 = st.columns(2)
        dl1.download_button("Download CSV", enrollments.to_csv(index=False),
                           file_name=f"enrollments_{pattern['contextual_name']}.csv",
                           mime="text/csv", key="preview_enroll_csv")
        edi = enrollment_to_edi(enrollments)
        dl2.download_button("Download EDI 834", edi,
                           file_name=f"834_{pattern['contextual_name']}.edi",
                           mime="text/plain", key="preview_enroll_edi")
        st.caption("Generated data is for export only — not stored in the source repository.")
