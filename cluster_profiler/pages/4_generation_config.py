"""Generation Configuration — unified data element grid with rule management.

Single grid showing ALL data elements with color-coded status:
  Reference    → From denorm, not configurable (pass-through)
  Pattern      → From demographic distributions (auto)
  Default      → Always synthetic from lookups (PII — names, addresses)
  Configured   → User has defined a generation rule (override)
  From denorm  → No rule defined — values sampled from pattern source data
"""

import pandas as pd
import streamlit as st

from cluster_profiler import db
from cluster_profiler.generation_rules import (
    generate_id, generate_names, generate_email,
    generate_addresses, get_available_lookups,
    SequenceCounter,
)

# ── Master Element Registry ──────────────────────────────────────────────────

ELEMENT_REGISTRY = {
    # Reference — from denorm, pass-through
    "GRGR_CK":           ("reference", "Group Key"),
    "GRGR_ID":           ("reference", "Group ID"),
    "GRGR_NAME":         ("reference", "Group Name"),
    "GRGR_STATE":        ("reference", "Group State"),
    "GRGR_COUNTY":       ("reference", "Group County"),
    "GRGR_STS":          ("reference", "Group Status"),
    "GRGR_ORIG_EFF_DT":  ("reference", "Group Orig Effective Date"),
    "GRGR_TERM_DT":      ("reference", "Group Term Date"),
    "GRGR_MCTR_TYPE":    ("reference", "Group Type"),
    "SGSG_CK":           ("reference", "Subgroup Key"),
    "SGSG_ID":           ("reference", "Subgroup ID"),
    "SGSG_NAME":         ("reference", "Subgroup Name"),
    "SGSG_STATE":        ("reference", "Subgroup State"),
    "SGSG_STS":          ("reference", "Subgroup Status"),
    "SGSG_ORIG_EFF_DT":  ("reference", "Subgroup Orig Effective Date"),
    "SGSG_TERM_DT":      ("reference", "Subgroup Term Date"),
    "CSCS_ID":           ("reference", "Coverage Set ID"),
    "SBSG_EFF_DT":       ("reference", "Sub-Subgroup Effective Date"),
    "SBSG_TERM_DT":      ("reference", "Sub-Subgroup Term Date"),
    "SBSG_MCTR_TRSN":    ("reference", "Sub-Subgroup Transition"),
    "PAGR_CK":           ("reference", "Provider Agreement Key"),
    "CSPI_ID":           ("reference", "Coverage Set Plan ID"),
    "CSPD_CAT":          ("reference", "Plan Category Code"),
    "CSPI_EFF_DT":       ("reference", "Plan Effective Date"),
    "CSPI_TERM_DT":      ("reference", "Plan Term Date"),
    "PDPD_ID":           ("reference", "Product ID"),
    "CSPI_SEL_IND":      ("reference", "Plan Selection Indicator"),
    "CSPI_HIOS_ID_NVL":  ("reference", "HIOS ID"),
    "CSPD_CAT_DESC":     ("reference", "Plan Category Description"),
    "CSPD_TYPE":         ("reference", "Plan Category Type"),
    "LOBD_ID":           ("reference", "Line of Business ID"),
    "PDPD_RISK_IND":     ("reference", "Product Risk Indicator"),
    "PDPD_MCTR_CCAT":    ("reference", "Product Cost Category"),
    "PLDS_DESC":         ("reference", "Plan Description"),
    "PDDS_DESC":         ("reference", "Product Description"),

    # Pattern-derived
    "MEME_SEX":          ("pattern", "Member Gender"),
    "MEME_BIRTH_DT":     ("pattern", "Member Birth Date"),
    "MEME_MARITAL_STATUS": ("pattern", "Member Marital Status"),
    "MEME_ORIG_EFF_DT":  ("pattern", "Member Orig Effective Date"),
    "MEME_SFX":          ("pattern", "Member Suffix (family position)"),
    "MEME_REL":          ("pattern", "Member Relationship"),

    # Default — always synthetic (PII)
    "MEME_FIRST_NAME":   ("default", "Member First Name"),
    "MEME_LAST_NAME":    ("default", "Member Last Name"),
    "SBSB_FIRST_NAME":   ("default", "Subscriber First Name"),
    "SBSB_LAST_NAME":    ("default", "Subscriber Last Name"),

    # Configurable — user defines rules, no rule = from denorm
    "MEME_CK":           ("configurable", "Member Key"),
    "SBSB_CK":           ("configurable", "Subscriber Key"),
    "SBSB_ID":           ("configurable", "Subscriber ID"),
    "MEME_SSN":          ("configurable", "Member SSN"),
    "MEME_MID_INIT":     ("configurable", "Member Middle Initial"),
    "MEME_MCTR_STS":     ("configurable", "Member Status"),
    "MEME_MEDCD_NO":     ("configurable", "Medicaid Number"),
    "MEME_HICN":         ("configurable", "HICN"),
    "MEME_MCTR_RACE_NVL": ("configurable", "Member Race"),
    "MEME_MCTR_ETHN_NVL": ("configurable", "Member Ethnicity"),
    "SBSB_ORIG_EFF_DT":  ("configurable", "Subscriber Orig Effective Date"),
    "SBSB_MCTR_STS":     ("configurable", "Subscriber Status"),
    "SBSB_EMPLOY_ID":    ("configurable", "Subscriber Employer ID"),
    "EMAIL":             ("configurable", "Email Address"),
}

STATUS_COLORS = {
    "Reference":    "gray",
    "Pattern":      "violet",
    "Default":      "green",
    "Configured":   "blue",
    "From denorm":  "orange",
}


db.bootstrap()

st.title("Generation Configuration")
st.caption(
    "Configure how each data element is generated. "
    "Elements without rules use values from the denormalized source data."
)

# Legend
st.markdown(
    ":gray[**Reference**] From denorm, not configurable · "
    ":violet[**Pattern**] From distributions · "
    ":green[**Default**] Always synthetic (PII) · "
    ":blue[**Configured**] User-defined rule override · "
    ":orange[**From denorm**] No rule — uses source data"
)

st.markdown("")

# ── Load rules ───────────────────────────────────────────────────────────────

rules_list = db.get_generation_rules()
rules_by_field = {r["field_name"]: r for r in rules_list}

# ── Status filter ────────────────────────────────────────────────────────────

filter_options = ["All", "Reference", "Pattern", "Default", "Configurable"]
status_filter = st.radio("Filter", filter_options, horizontal=True, label_visibility="collapsed")

# ── Build grid ───────────────────────────────────────────────────────────────

grid_data = []
for field_name, (category, label) in ELEMENT_REGISTRY.items():
    rule = rules_by_field.get(field_name)

    if category == "reference":
        status = "Reference"
        source = "Pass-through from denorm"
    elif category == "pattern":
        status = "Pattern"
        source = "From pattern distribution"
    elif category == "default":
        status = "Default"
        source = f"Lookup: {rule['lookup_source']}" if rule and rule.get("lookup_source") else "Lookup: names/addresses"
    elif rule:
        status = "Configured"
        parts = []
        parts.append(rule.get("gen_method", ""))
        if rule.get("prefix"):
            parts.append(f"prefix={rule['prefix']}")
        if rule.get("length") and rule["length"] > 0:
            parts.append(f"len={rule['length']}")
        if rule.get("start_value") and rule["start_value"] > 0:
            parts.append(f"start={rule['start_value']}")
        if rule.get("domain"):
            parts.append(f"domain={rule['domain']}")
        source = ", ".join(parts)
    else:
        status = "From denorm"
        source = "No rule — sampled from pattern source data"

    # Apply filter
    if status_filter == "All" or \
       (status_filter == "Configurable" and category == "configurable") or \
       status_filter == status or \
       (status_filter == "Configurable" and status in ("Configured", "From denorm")):

        grid_data.append({
            "Field": field_name,
            "Label": label,
            "Status": status,
            "Source / Rule": source,
            "_category": category,
        })

if not grid_data:
    st.info("No elements match the selected filter.")
    st.stop()

display_df = pd.DataFrame(grid_data)[["Field", "Label", "Status", "Source / Rule"]]

event = st.dataframe(
    display_df,
    hide_index=True,
    width="stretch",
    height=min(400, len(grid_data) * 38 + 40),
    on_select="rerun",
    selection_mode="single-row",
)

# ── Detail panel on row click ────────────────────────────────────────────────

selected_rows = event.selection.rows
if not selected_rows:
    st.info("Click a row to view or configure its generation rule.")
    st.stop()

idx = selected_rows[0]
selected_row = grid_data[idx]
field_name = selected_row["Field"]
category = selected_row["_category"]
label = ELEMENT_REGISTRY[field_name][1]
status = selected_row["Status"]

st.divider()

# Header with status
header_col1, header_col2 = st.columns([3, 1])
header_col1.subheader(field_name)
header_col1.caption(label)

status_color = STATUS_COLORS.get(status, "gray")
header_col2.markdown(f":{status_color}[**{status}**]")

# Reference and pattern — not configurable
if category == "reference":
    st.info("**Reference field** — populated from the denormalized model. Not configurable.")
    st.stop()

if category == "pattern":
    st.info("**Pattern field** — derived from the pattern's demographic distributions. Not configurable.")
    st.stop()

# Default — editable lookup source but not removable
if category == "default":
    st.info("**Default field** — always generated from lookups (PII protection). You can adjust the lookup source but cannot disable generation.")

# ── Rule form ────────────────────────────────────────────────────────────────

existing_rule = rules_by_field.get(field_name)

# Show current state
if existing_rule:
    st.markdown(f"**Current rule:** {existing_rule['gen_method']}"
                + (f", prefix=`{existing_rule['prefix']}`" if existing_rule.get('prefix') else "")
                + (f", length={existing_rule['length']}" if existing_rule.get('length') else "")
                + (f", start={existing_rule['start_value']}" if existing_rule.get('start_value') else "")
                + (f", domain=`{existing_rule['domain']}`" if existing_rule.get('domain') else ""))
elif category == "configurable":
    st.markdown("**No rule defined** — this field currently uses values sampled from the denormalized source data. "
                "Define a rule below to override with synthetic values.")

with st.form(f"rule_form_{field_name}"):
    col1, col2, col3 = st.columns(3)

    gen_method = col1.selectbox(
        "Method",
        ["sequential", "random", "lookup", "formatted"],
        index=["sequential", "random", "lookup", "formatted"].index(
            existing_rule["gen_method"] if existing_rule else ("lookup" if category == "default" else "sequential")
        ),
    )
    data_type = col2.selectbox(
        "Data type",
        ["numeric", "alphanumeric", "text"],
        index=["numeric", "alphanumeric", "text"].index(
            existing_rule["data_type"] if existing_rule else "alphanumeric"
        ),
    )
    length = col3.number_input(
        "Length (0=unlimited)",
        min_value=0, max_value=100,
        value=existing_rule["length"] if existing_rule and existing_rule["length"] else 10,
    )

    col4, col5, col6 = st.columns(3)
    prefix = col4.text_input("Prefix", value=existing_rule["prefix"] if existing_rule and existing_rule["prefix"] else "")
    postfix = col5.text_input("Postfix", value=existing_rule["postfix"] if existing_rule and existing_rule["postfix"] else "")
    start_value = col6.number_input("Start value", min_value=0,
        value=existing_rule["start_value"] if existing_rule and existing_rule["start_value"] else 1)

    col7, col8 = st.columns(2)
    domain = col7.text_input("Domain (for email/formatted)",
        value=existing_rule["domain"] if existing_rule and existing_rule["domain"] else "")

    available_lookups = get_available_lookups()
    current_lookup = existing_rule["lookup_source"] if existing_rule and existing_rule["lookup_source"] else ""
    lookup_options = [""] + available_lookups
    lookup_idx = lookup_options.index(current_lookup) if current_lookup in lookup_options else 0
    lookup_source = col8.selectbox("Lookup source", lookup_options, index=lookup_idx)

    # Buttons
    col_save, col_remove, col_test = st.columns(3)
    save_clicked = col_save.form_submit_button("Save rule", type="primary")

    if category == "configurable":
        remove_clicked = col_remove.form_submit_button("Remove rule (revert to denorm)")
    else:
        remove_clicked = False

    test_clicked = col_test.form_submit_button("Test 5 samples")

    if save_clicked:
        db.upsert_generation_rule(
            field_name=field_name, field_label=label, field_category=category,
            gen_method=gen_method, data_type=data_type, length=length,
            prefix=prefix, postfix=postfix, start_value=start_value,
            domain=domain, format_pattern="", lookup_source=lookup_source,
            updated_by="user",
        )
        st.success(f"Rule saved for {field_name}")
        st.rerun()

    if remove_clicked:
        if existing_rule:
            db.delete_generation_rule(field_name)
            st.success(f"Rule removed for {field_name}. This field will now use values from the denormalized source data.")
            st.rerun()
        else:
            st.info("No rule to remove — already using denorm values.")

    if test_clicked:
        SequenceCounter.reset()
        test_rule = {
            "field_name": field_name, "gen_method": gen_method, "data_type": data_type,
            "length": length, "prefix": prefix, "postfix": postfix,
            "start_value": start_value, "domain": domain, "lookup_source": lookup_source,
        }

        if gen_method == "lookup" and lookup_source in ("first_names", "last_names"):
            samples = generate_names(5)
            st.dataframe(pd.DataFrame(samples, columns=["First", "Last"]), hide_index=True)
        elif gen_method == "lookup" and lookup_source in ("street_names", "zip_city_state"):
            addrs = generate_addresses(5)
            st.dataframe(pd.DataFrame(addrs), hide_index=True)
        elif gen_method == "formatted" and domain:
            names = generate_names(5)
            emails = [generate_email(f, l, domain, i) for i, (f, l) in enumerate(names)]
            st.dataframe(pd.DataFrame({"Name": [f"{f} {l}" for f, l in names], "Email": emails}), hide_index=True)
        else:
            values = generate_id(test_rule, 5)
            st.code("  ".join(values))
