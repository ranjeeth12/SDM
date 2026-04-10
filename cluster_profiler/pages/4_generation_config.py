"""Generation Configuration — define and manage rules for synthetic data elements.

Users can view, add, edit, and test generation rules for IDs, names, emails,
addresses, and other data elements. Rules are persisted to the DB and used
by the generation engine at runtime.
"""

import pandas as pd
import streamlit as st

from cluster_profiler import db
from cluster_profiler.generation_rules import (
    generate_id, generate_names, generate_email,
    generate_addresses, generate_ssn, get_available_lookups,
    SequenceCounter,
)


db.bootstrap()

st.title("Generation Configuration")
st.caption(
    "Configure how each synthetic data element is generated. "
    "Rules define the method, format, and source for every field."
)

# ── Current Rules ────────────────────────────────────────────────────────────

st.subheader("Data Element Rules")

rules = db.get_generation_rules()

if rules:
    # Group by category
    categories = {}
    for r in rules:
        cat = r.get("field_category", "other")
        categories.setdefault(cat, []).append(r)

    cat_labels = {
        "id": "Identifiers",
        "pii": "Personal Information",
        "contact": "Contact Information",
        "address": "Address Fields",
    }

    for cat, cat_rules in categories.items():
        st.markdown(f"**{cat_labels.get(cat, cat.title())}**")

        table_data = []
        for r in cat_rules:
            table_data.append({
                "Field": r["field_name"],
                "Label": r["field_label"],
                "Method": r["gen_method"],
                "Type": r["data_type"],
                "Length": r["length"] or "",
                "Prefix": r["prefix"] or "",
                "Postfix": r["postfix"] or "",
                "Start": r["start_value"] or "",
                "Domain": r["domain"] or "",
                "Lookup": r["lookup_source"] or "",
                "Active": "Yes" if r["active"] else "No",
            })

        st.dataframe(
            pd.DataFrame(table_data),
            hide_index=True, width="stretch",
        )
else:
    st.info("No generation rules configured yet.")

# ── Add / Edit Rule ──────────────────────────────────────────────────────────

st.divider()
st.subheader("Add or Edit Rule")

with st.form("rule_form"):
    col1, col2 = st.columns(2)

    field_name = col1.text_input(
        "Field Name",
        placeholder="e.g., MEME_CK, EMAIL, PROVIDER_NPI",
        help="Exact column name used in generated output",
    )
    field_label = col2.text_input(
        "Display Label",
        placeholder="e.g., Member Key, Email Address",
    )

    col3, col4 = st.columns(2)
    field_category = col3.selectbox(
        "Category",
        ["id", "pii", "contact", "address", "other"],
    )
    gen_method = col4.selectbox(
        "Generation Method",
        ["sequential", "random", "lookup", "formatted", "derived"],
        help="sequential=counter, random=random value, lookup=from CSV file, formatted=pattern-based, derived=from another field",
    )

    col5, col6 = st.columns(2)
    data_type = col5.selectbox(
        "Data Type",
        ["numeric", "alphanumeric", "text"],
    )
    length = col6.number_input("Length (0=unlimited)", min_value=0, max_value=100, value=10)

    col7, col8, col9 = st.columns(3)
    prefix = col7.text_input("Prefix", placeholder="e.g., SB, CLM, TXN")
    postfix = col8.text_input("Postfix")
    start_value = col9.number_input("Start Value (for sequential)", min_value=0, value=1)

    col10, col11 = st.columns(2)
    domain = col10.text_input(
        "Domain (for email)",
        placeholder="e.g., caresource.com",
    )
    format_pattern = col11.text_input(
        "Format Pattern",
        placeholder="e.g., {first_initial}{last_name}@{domain}",
    )

    available_lookups = get_available_lookups()
    lookup_source = st.selectbox(
        "Lookup Source",
        [""] + available_lookups,
        help="CSV file in data/lookups/ to pick values from",
    )

    submitted = st.form_submit_button("Save Rule", type="primary")

    if submitted and field_name and field_label:
        db.upsert_generation_rule(
            field_name=field_name.upper(),
            field_label=field_label,
            field_category=field_category,
            gen_method=gen_method,
            data_type=data_type,
            length=length,
            prefix=prefix,
            postfix=postfix,
            start_value=start_value,
            domain=domain,
            format_pattern=format_pattern,
            lookup_source=lookup_source,
            updated_by="user",
        )
        st.success(f"Rule saved for {field_name.upper()}")
        st.rerun()
    elif submitted:
        st.error("Field Name and Label are required.")

# ── Delete Rule ──────────────────────────────────────────────────────────────

st.divider()
st.subheader("Delete Rule")

if rules:
    rule_names = [r["field_name"] for r in rules]
    delete_field = st.selectbox("Select rule to delete", [""] + rule_names)
    if delete_field and st.button("Delete", type="secondary"):
        db.delete_generation_rule(delete_field)
        st.success(f"Deleted rule for {delete_field}")
        st.rerun()

# ── Test Generation ──────────────────────────────────────────────────────────

st.divider()
st.subheader("Test Generation")
st.caption("Preview generated values using current rules.")

test_col1, test_col2 = st.columns(2)
test_type = test_col1.selectbox(
    "Test type",
    ["IDs", "Names", "Email", "Addresses", "SSN"],
)
test_count = test_col2.number_input("Count", min_value=1, max_value=50, value=5)

if st.button("Generate Sample", type="primary"):
    SequenceCounter.reset()  # Reset counters for clean test

    if test_type == "IDs":
        st.markdown("**All configured ID fields:**")
        id_rules = [r for r in rules if r["field_category"] == "id"]
        for rule in id_rules:
            values = generate_id(rule, test_count)
            st.text(f"{rule['field_label']} ({rule['field_name']}): {values}")

    elif test_type == "Names":
        names = generate_names(test_count)
        st.dataframe(
            pd.DataFrame(names, columns=["First Name", "Last Name"]),
            hide_index=True,
        )

    elif test_type == "Email":
        names = generate_names(test_count)
        email_rule = db.get_generation_rule("EMAIL")
        domain = email_rule.get("domain", "caresource.com") if email_rule else "caresource.com"
        emails = [generate_email(f, l, domain, i) for i, (f, l) in enumerate(names)]
        st.dataframe(
            pd.DataFrame({
                "Name": [f"{f} {l}" for f, l in names],
                "Email": emails,
            }),
            hide_index=True,
        )

    elif test_type == "Addresses":
        addresses = generate_addresses(test_count)
        st.dataframe(pd.DataFrame(addresses), hide_index=True)

    elif test_type == "SSN":
        ssns = generate_ssn(test_count)
        st.dataframe(pd.DataFrame({"SSN": ssns}), hide_index=True)

# ── Lookup Files ─────────────────────────────────────────────────────────────

st.divider()
st.subheader("Lookup Files")
st.caption("Governed lookup CSVs used for name, address, and zip code generation.")

for lookup in available_lookups:
    with st.expander(f"{lookup}.csv"):
        try:
            from cluster_profiler.generation_rules import _load_lookup
            df = _load_lookup(lookup)
            st.dataframe(df.head(20), hide_index=True, width="stretch")
            st.caption(f"{len(df)} total entries")
        except Exception as e:
            st.error(str(e))
