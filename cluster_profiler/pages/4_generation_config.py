"""Generation Configuration — context-aware rule management for all data elements.

Forms adapt based on field type:
  ID fields    → method, length, prefix, start value
  Name fields  → lookup source
  Email fields → domain, local-part method
  Date fields  → method (sequential, constant)
  Status fields → constant value or lookup
  Other        → full options
"""

import pandas as pd
import streamlit as st

from cluster_profiler import db
from cluster_profiler.generation_rules import (
    generate_id, generate_names, generate_email,
    generate_addresses, get_available_lookups,
    SequenceCounter,
)

# ── Element Registry ─────────────────────────────────────────────────────────

ELEMENT_REGISTRY = {
    "GRGR_CK":           ("denorm", "Group Key"),
    "GRGR_ID":           ("denorm", "Group ID"),
    "GRGR_NAME":         ("denorm", "Group Name"),
    "GRGR_STATE":        ("denorm", "Group State"),
    "GRGR_COUNTY":       ("denorm", "Group County"),
    "GRGR_STS":          ("denorm", "Group Status"),
    "GRGR_ORIG_EFF_DT":  ("denorm", "Group Orig Effective Date"),
    "GRGR_TERM_DT":      ("denorm", "Group Term Date"),
    "GRGR_MCTR_TYPE":    ("denorm", "Group Type"),
    "SGSG_CK":           ("denorm", "Subgroup Key"),
    "SGSG_ID":           ("denorm", "Subgroup ID"),
    "SGSG_NAME":         ("denorm", "Subgroup Name"),
    "SGSG_STATE":        ("denorm", "Subgroup State"),
    "SGSG_STS":          ("denorm", "Subgroup Status"),
    "SGSG_ORIG_EFF_DT":  ("denorm", "Subgroup Orig Effective Date"),
    "SGSG_TERM_DT":      ("denorm", "Subgroup Term Date"),
    "CSCS_ID":           ("denorm", "Coverage Set ID"),
    "SBSG_EFF_DT":       ("denorm", "Sub-Subgroup Effective Date"),
    "SBSG_TERM_DT":      ("denorm", "Sub-Subgroup Term Date"),
    "SBSG_MCTR_TRSN":    ("denorm", "Sub-Subgroup Transition"),
    "PAGR_CK":           ("denorm", "Provider Agreement Key"),
    "CSPI_ID":           ("denorm", "Coverage Set Plan ID"),
    "CSPD_CAT":          ("denorm", "Plan Category Code"),
    "CSPI_EFF_DT":       ("denorm", "Plan Effective Date"),
    "CSPI_TERM_DT":      ("denorm", "Plan Term Date"),
    "PDPD_ID":           ("denorm", "Product ID"),
    "CSPI_SEL_IND":      ("denorm", "Plan Selection Indicator"),
    "CSPI_HIOS_ID_NVL":  ("denorm", "HIOS ID"),
    "CSPD_CAT_DESC":     ("denorm", "Plan Category Description"),
    "CSPD_TYPE":         ("denorm", "Plan Category Type"),
    "LOBD_ID":           ("denorm", "Line of Business ID"),
    "PDPD_RISK_IND":     ("denorm", "Product Risk Indicator"),
    "PDPD_MCTR_CCAT":    ("denorm", "Product Cost Category"),
    "PLDS_DESC":         ("denorm", "Plan Description"),
    "PDDS_DESC":         ("denorm", "Product Description"),
    "MEME_SEX":          ("pattern", "Member Gender"),
    "MEME_BIRTH_DT":     ("pattern", "Member Birth Date"),
    "MEME_MARITAL_STATUS": ("pattern", "Member Marital Status"),
    "MEME_ORIG_EFF_DT":  ("pattern", "Member Orig Effective Date"),
    "MEME_SFX":          ("pattern", "Member Suffix"),
    "MEME_REL":          ("pattern", "Member Relationship"),
    "MEME_FIRST_NAME":   ("lookup", "Member First Name"),
    "MEME_LAST_NAME":    ("lookup", "Member Last Name"),
    "SBSB_FIRST_NAME":   ("lookup", "Subscriber First Name"),
    "SBSB_LAST_NAME":    ("lookup", "Subscriber Last Name"),
    "MEME_CK":           ("denorm", "Member Key"),
    "SBSB_CK":           ("denorm", "Subscriber Key"),
    "SBSB_ID":           ("denorm", "Subscriber ID"),
    "MEME_SSN":          ("denorm", "Member SSN"),
    "MEME_MID_INIT":     ("denorm", "Member Middle Initial"),
    "MEME_MCTR_STS":     ("denorm", "Member Status"),
    "MEME_MEDCD_NO":     ("denorm", "Medicaid Number"),
    "MEME_HICN":         ("denorm", "HICN"),
    "MEME_MCTR_RACE_NVL": ("denorm", "Member Race"),
    "MEME_MCTR_ETHN_NVL": ("denorm", "Member Ethnicity"),
    "SBSB_ORIG_EFF_DT":  ("denorm", "Subscriber Orig Effective Date"),
    "SBSB_MCTR_STS":     ("denorm", "Subscriber Status"),
    "SBSB_EMPLOY_ID":    ("denorm", "Subscriber Employer ID"),
    "EMAIL":             ("denorm", "Email Address"),
}


def _detect_field_type(field_name: str, label: str) -> str:
    """Detect the field type for context-aware form rendering."""
    fn = field_name.upper()
    lb = label.lower()
    if "email" in lb or "email" in fn.lower():
        return "email"
    if fn.endswith("_NAME") or "FIRST_NAME" in fn or "LAST_NAME" in fn or "MID_INIT" in fn:
        return "name"
    if fn.endswith("_CK") or fn.endswith("_ID") or fn == "MEME_SSN" or fn == "MEME_MEDCD_NO" or fn == "MEME_HICN":
        return "id"
    if fn.endswith("_DT"):
        return "date"
    if "_STS" in fn or "STATUS" in lb:
        return "status"
    if "_DESC" in fn or "_NAME" in fn:
        return "text"
    return "general"


db.bootstrap()

st.title("Generation Configuration")
st.caption(
    "Every data element is configurable. Without a rule, values come from the "
    "default source. Define a rule to override with synthetic values."
)

st.markdown(
    ":orange[**Denorm**] From source data · "
    ":violet[**Pattern**] From distributions · "
    ":green[**Lookup**] From governed CSV · "
    ":blue[**Rule defined**] User override active"
)

# ── Load rules ───────────────────────────────────────────────────────────────

rules_list = db.get_generation_rules()
rules_by_field = {r["field_name"]: r for r in rules_list}

# ── Filter ───────────────────────────────────────────────────────────────────

status_filter = st.radio("Show", ["All", "Has rule", "No rule"], horizontal=True, label_visibility="collapsed")

# ── Build grid ───────────────────────────────────────────────────────────────

grid_data = []
for field_name, (default_source, label) in ELEMENT_REGISTRY.items():
    rule = rules_by_field.get(field_name)
    has_rule = rule is not None and rule.get("updated_by") != "system"

    if has_rule:
        status = "Rule defined"
        parts = [rule.get("gen_method", "")]
        if rule.get("prefix"): parts.append(f"prefix={rule['prefix']}")
        if rule.get("length") and rule["length"] > 0: parts.append(f"len={rule['length']}")
        if rule.get("domain"): parts.append(f"domain={rule['domain']}")
        source_text = ", ".join(parts)
    else:
        status = f"Default ({default_source})"
        source_text = {"denorm": "From source data", "pattern": "From distribution", "lookup": "From lookup CSV"}.get(default_source, default_source)

    if status_filter == "All" or \
       (status_filter == "Has rule" and has_rule) or \
       (status_filter == "No rule" and not has_rule):
        grid_data.append({
            "Field": field_name,
            "Label": label,
            "Status": status,
            "Source / Rule": source_text,
            "_default": default_source,
        })

if not grid_data:
    st.info("No elements match the selected filter.")
    st.stop()

display_df = pd.DataFrame(grid_data)[["Field", "Label", "Status", "Source / Rule"]]

event = st.dataframe(
    display_df, hide_index=True, width="stretch",
    height=min(450, len(grid_data) * 38 + 40),
    on_select="rerun", selection_mode="single-row",
)

# ── Detail panel ─────────────────────────────────────────────────────────────

selected_rows = event.selection.rows
if not selected_rows:
    st.info("Click any element to configure its generation rule.")
    st.stop()

idx = selected_rows[0]
field_name = grid_data[idx]["Field"]
label = ELEMENT_REGISTRY[field_name][1]
default_source = ELEMENT_REGISTRY[field_name][0]
existing_rule = rules_by_field.get(field_name)
is_user_rule = existing_rule and existing_rule.get("updated_by") != "system"
field_type = _detect_field_type(field_name, label)

st.divider()

hdr1, hdr2 = st.columns([3, 1])
hdr1.subheader(field_name)
hdr1.caption(f"{label} · Type: {field_type}")

if is_user_rule:
    hdr2.markdown(":blue[**Rule defined**]")
else:
    color = {"denorm": "orange", "pattern": "violet", "lookup": "green"}.get(default_source, "gray")
    hdr2.markdown(f":{color}[**Default: {default_source}**]")

if not is_user_rule:
    st.markdown(f"No user rule. Using **{default_source}** values. Configure below to override.")

# ── Context-aware form ───────────────────────────────────────────────────────

with st.form(f"rule_form_{field_name}"):

    if field_type == "email":
        st.markdown("**Email configuration**")
        col1, col2 = st.columns(2)
        domain = col1.text_input("Domain", value=existing_rule["domain"] if is_user_rule and existing_rule.get("domain") else "caresource.com")
        local_method = col2.selectbox("Local part", ["name-based", "random"],
            index=0 if not is_user_rule else (1 if existing_rule.get("format_pattern") == "random" else 0))
        st.caption(f"Preview: jsmith@{domain}")
        gen_method, data_type, length, prefix, postfix, start_value = "formatted", "text", 0, "", "", 0
        format_pattern, lookup_source = local_method, ""

    elif field_type == "name":
        st.markdown("**Name configuration**")
        available_lookups = get_available_lookups()
        name_lookups = [l for l in available_lookups if "name" in l]
        default_lookup = "first_names" if "FIRST" in field_name else "last_names"
        current = existing_rule["lookup_source"] if is_user_rule and existing_rule.get("lookup_source") else default_lookup
        lookup_source = st.selectbox("Lookup source", name_lookups or ["first_names", "last_names"],
            index=0 if current not in (name_lookups or [default_lookup]) else (name_lookups or [default_lookup]).index(current))
        gen_method, data_type, length, prefix, postfix, start_value = "lookup", "text", 0, "", "", 0
        domain, format_pattern = "", ""

    elif field_type == "id":
        st.markdown("**ID configuration**")
        col1, col2, col3 = st.columns(3)
        gen_method = col1.selectbox("Method", ["sequential", "random", "constant"],
            index=["sequential", "random", "constant"].index(existing_rule["gen_method"]) if is_user_rule and existing_rule.get("gen_method") in ["sequential", "random", "constant"] else 0)
        length = col2.number_input("Length", min_value=1, max_value=50,
            value=existing_rule["length"] if is_user_rule and existing_rule.get("length") else 10)
        data_type = col3.selectbox("Data type", ["numeric", "alphanumeric"],
            index=["numeric", "alphanumeric"].index(existing_rule["data_type"]) if is_user_rule and existing_rule.get("data_type") in ["numeric", "alphanumeric"] else 0)
        col4, col5, col6 = st.columns(3)
        prefix = col4.text_input("Prefix", value=existing_rule["prefix"] if is_user_rule and existing_rule.get("prefix") else "")
        postfix = col5.text_input("Postfix", value=existing_rule["postfix"] if is_user_rule and existing_rule.get("postfix") else "")
        start_value = col6.number_input("Start value", min_value=0,
            value=existing_rule["start_value"] if is_user_rule and existing_rule.get("start_value") else 1)
        domain, format_pattern, lookup_source = "", "", ""

    elif field_type == "status":
        st.markdown("**Status / code configuration**")
        gen_method = st.selectbox("Method", ["constant", "lookup"],
            index=["constant", "lookup"].index(existing_rule["gen_method"]) if is_user_rule and existing_rule.get("gen_method") in ["constant", "lookup"] else 0)
        if gen_method == "constant":
            prefix = st.text_input("Value", value=existing_rule["prefix"] if is_user_rule and existing_rule.get("prefix") else "AC")
        else:
            prefix = ""
        available_lookups = get_available_lookups()
        lookup_source = st.selectbox("Lookup source", [""] + available_lookups, index=0) if gen_method == "lookup" else ""
        data_type, length, postfix, start_value = "text", 0, "", 0
        domain, format_pattern = "", ""

    else:
        # General form
        st.markdown("**General configuration**")
        col1, col2, col3 = st.columns(3)
        gen_method = col1.selectbox("Method", ["sequential", "random", "constant", "lookup"],
            index=0)
        data_type = col2.selectbox("Data type", ["text", "numeric", "alphanumeric"], index=0)
        length = col3.number_input("Length (0=unlimited)", min_value=0, max_value=100, value=0)
        col4, col5 = st.columns(2)
        prefix = col4.text_input("Prefix / Value (for constant)", value="")
        start_value = col5.number_input("Start value", min_value=0, value=1)
        postfix, domain, format_pattern, lookup_source = "", "", "", ""

    # ── Buttons ──────────────────────────────────────────────────
    col_save, col_remove, col_test = st.columns(3)
    save_clicked = col_save.form_submit_button("Save rule", type="primary")
    remove_clicked = col_remove.form_submit_button("Remove rule (revert to default)")
    test_clicked = col_test.form_submit_button("Test 5 samples")

    if save_clicked:
        db.upsert_generation_rule(
            field_name=field_name, field_label=label, field_category=default_source,
            gen_method=gen_method, data_type=data_type, length=length,
            prefix=prefix, postfix=postfix, start_value=start_value,
            domain=domain, format_pattern=format_pattern,
            lookup_source=lookup_source, updated_by="user",
        )
        st.success(f"Rule saved for {field_name}")
        st.rerun()

    if remove_clicked:
        if is_user_rule:
            db.delete_generation_rule(field_name)
            st.success(f"Rule removed. {field_name} reverts to default ({default_source}).")
            st.rerun()
        else:
            st.info(f"No user rule to remove — already using default ({default_source}).")

    if test_clicked:
        SequenceCounter.reset()

        if field_type == "email" and domain:
            names = generate_names(5)
            if format_pattern == "random":
                import random as _rnd, string as _str
                samples = [''.join(_rnd.choices(_str.ascii_lowercase, k=8)) + f"@{domain}" for _ in range(5)]
            else:
                samples = [generate_email(f, l, domain, i) for i, (f, l) in enumerate(names)]
            st.code("  ".join(samples))

        elif field_type == "name":
            names = generate_names(5)
            if "FIRST" in field_name:
                st.code("  ".join([n[0] for n in names]))
            elif "LAST" in field_name:
                st.code("  ".join([n[1] for n in names]))
            elif "MID" in field_name:
                st.code("  ".join([n[0][0] for n in names]))
            else:
                st.code("  ".join([f"{n[0]} {n[1]}" for n in names]))

        elif field_type == "id":
            rule = {"field_name": field_name, "gen_method": gen_method, "data_type": data_type,
                    "length": length, "prefix": prefix, "postfix": postfix, "start_value": start_value}
            vals = generate_id(rule, 5)
            st.code("  ".join(vals))

        elif field_type == "status" and gen_method == "constant":
            st.code("  ".join([prefix] * 5))

        else:
            rule = {"field_name": field_name, "gen_method": gen_method, "data_type": data_type,
                    "length": length, "prefix": prefix, "postfix": postfix, "start_value": start_value}
            vals = generate_id(rule, 5)
            st.code("  ".join(vals))
