"""Keyword-driven data access page.

Users type natural language queries like "500 Ohio Medicaid dental claims for adults"
and the system resolves to matching patterns, shows confirmation, and generates data.
"""

import json

import pandas as pd
import streamlit as st

from cluster_profiler import db
from cluster_profiler.keyword_search import parse_query, search, allocate_volume
from cluster_profiler.config import MEMBER_DENORM_PATH, MEMBER_LABELS_PATH, DEFAULT_REFERENCE_DATE
from cluster_profiler.data_loader import load_data
from cluster_profiler.synthetic import generate_synthetic_subscribers
from cluster_profiler.synthetic_claims import generate_synthetic_claims, DenormNotAvailableError
from cluster_profiler.synthetic_enrollment import generate_synthetic_enrollments
from cluster_profiler.edi_formatter import enrollment_to_edi


@st.cache_data
def cached_load_data():
    return load_data(MEMBER_DENORM_PATH, MEMBER_LABELS_PATH, DEFAULT_REFERENCE_DATE)


# ── Init ─────────────────────────────────────────────────────────────────────

db.bootstrap()

st.title("Keyword Data Request")
st.caption(
    "Describe what data you need in plain language. "
    "The system will find matching patterns and generate data automatically."
)

st.markdown("")

# ── Query Input ──────────────────────────────────────────────────────────────

query = st.text_input(
    "What data do you need?",
    placeholder="e.g., 500 Ohio Medicaid dental claims for adults",
    key="keyword_query",
)

examples = [
    "1000 Kentucky Medicare members",
    "500 Ohio Medicaid dental claims for adults",
    "200 enrollments for senior family members",
    "edge-case claims for new members",
    "high-volume medical claims",
]

st.caption("**Examples:** " + " · ".join(f"`{e}`" for e in examples))

if not query:
    st.info("Type a request above to get started.")
    st.stop()

# ── Parse and Search ─────────────────────────────────────────────────────────

results = search(query)
parsed = results["parsed"]
patterns = results["patterns"]

# Show parsed interpretation
st.divider()
st.subheader("Request Interpretation")

col1, col2, col3 = st.columns(3)
col1.metric("Data Type", parsed["data_type"].title())
col2.metric("Volume", f"{parsed['volume']:,}" if parsed["volume"] else "Auto")
col3.metric("Keywords Matched", len(parsed["keywords"]))

if parsed["keywords"]:
    st.markdown(f"**Resolved keywords:** {', '.join(parsed['keywords'])}")
if parsed["raw_terms"] != parsed["keywords"]:
    st.caption(f"Raw terms: {', '.join(parsed['raw_terms'])}")

# ── Results ──────────────────────────────────────────────────────────────────

st.divider()

if not patterns:
    st.warning(
        "No patterns matched your keywords. Try broader terms or check the "
        "tag vocabulary in the Pattern Discovery page."
    )
    st.stop()

st.subheader(f"Found {len(patterns)} Matching Patterns")
st.caption(f"Total members across matches: {results['total_members']:,}")

# Display matching patterns
table_data = []
for p in patterns[:20]:
    tags = db.get_tags(p["id"])
    tag_str = ", ".join(t["tag"] for t in tags[:6])
    table_data.append({
        "Name": p["contextual_name"],
        "Members": f"{p['member_count']:,}",
        "Group": p.get("grgr_name", ""),
        "Plan": p.get("cspd_cat_desc", ""),
        "LOB": p.get("plds_desc", ""),
        "Weight": f"{results['weights'].get(p['id'], 0) * 100:.1f}%",
        "Tags": tag_str,
    })

st.dataframe(pd.DataFrame(table_data), hide_index=True, width="stretch")

# ── Generation ───────────────────────────────────────────────────────────────

st.divider()

volume = parsed["volume"] or st.number_input(
    "How many records?", min_value=10, max_value=100000, value=100, step=100,
)

confirmation = (
    f"**Ready to generate {volume:,} {parsed['data_type']}** "
    f"from {len(patterns)} patterns using weighted distribution."
)
st.markdown(confirmation)

if st.button("Generate Data", type="primary"):
    df, labels_df = cached_load_data()

    allocation = allocate_volume(results["weights"], volume)
    all_outputs = []
    denorm_error = None

    progress = st.progress(0, text="Generating...")

    for i, (pattern_id, count) in enumerate(allocation.items()):
        pattern = next((p for p in patterns if p["id"] == pattern_id), None)
        if not pattern:
            continue

        progress.progress(
            (i + 1) / len(allocation),
            text=f"Generating from {pattern['contextual_name']}... ({count} records)"
        )

        # Reconstruct filters from pattern
        filters = {
            "grgr_ck": json.loads(pattern["grgr_ck"]) if pattern["grgr_ck"] else None,
            "sgsg_ck": json.loads(pattern["sgsg_ck"]) if pattern["sgsg_ck"] else None,
            "cspd_cat": json.loads(pattern["cspd_cat"]) if pattern["cspd_cat"] else None,
            "lobd_id": json.loads(pattern["lobd_id"]) if pattern["lobd_id"] else None,
        }
        # Remove None filters
        filters = {k: v for k, v in filters.items() if v is not None}

        # Load profile
        profile = json.loads(pattern["profile_json"]) if pattern.get("profile_json") else {}

        try:
            if parsed["data_type"] == "members":
                result_df = generate_synthetic_subscribers(
                    profile, filters, count, DEFAULT_REFERENCE_DATE,
                )
            elif parsed["data_type"] == "claims":
                member_result = generate_synthetic_subscribers(
                    profile, filters, max(10, count // 5), DEFAULT_REFERENCE_DATE,
                )
                result_df = generate_synthetic_claims(
                    profile, filters, member_result, count, DEFAULT_REFERENCE_DATE,
                )
            elif parsed["data_type"] == "enrollments":
                member_result = generate_synthetic_subscribers(
                    profile, filters, count, DEFAULT_REFERENCE_DATE,
                )
                result_df = generate_synthetic_enrollments(
                    member_result, filters, DEFAULT_REFERENCE_DATE,
                )
            else:
                result_df = generate_synthetic_subscribers(
                    profile, filters, count, DEFAULT_REFERENCE_DATE,
                )
            all_outputs.append(result_df)

        except DenormNotAvailableError as e:
            denorm_error = str(e)
            break

    progress.empty()

    if denorm_error:
        st.error(denorm_error)
        st.info(
            "**What's needed:** The periodic data scoop process must deliver "
            "Provider and Claims denormalized models to `data/source/` before "
            "claims generation is available. Try requesting **members** or "
            "**enrollments** instead — these work with the Member Denorm alone."
        )
    elif all_outputs:
        combined = pd.concat(all_outputs, ignore_index=True)
        file_name = f"synthetic_{parsed['data_type']}_{len(combined)}.csv"

        # Log the generation event (metadata only, not the data itself)
        db.log_generation(
            pattern_ids=list(allocation.keys()),
            data_type=parsed["data_type"],
            volume=len(combined),
            output_path=f"(download) {file_name}",
            weighting=results["weights"],
        )

        st.success(f"Generated {len(combined):,} {parsed['data_type']} records.")
        st.dataframe(combined.head(20), hide_index=True, width="stretch")

        dl_col1, dl_col2 = st.columns(2)
        dl_col1.download_button(
            "Download CSV",
            combined.to_csv(index=False),
            file_name=file_name,
            mime="text/csv",
        )

        if parsed["data_type"] == "enrollments":
            edi_content = enrollment_to_edi(combined)
            dl_col2.download_button(
                "Download EDI 834",
                edi_content,
                file_name=file_name.replace(".csv", ".edi"),
                mime="text/plain",
            )

        st.caption("Generated data is for export only — it is not stored in the source repository.")
