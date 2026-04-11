"""Generation Configuration — every data element is configurable.

Every element shows its default source (denorm, pattern, lookup) but can be
overridden with a user-defined rule. No element is locked from configuration.

Default source types (what happens without a user rule):
  denorm     → Value sampled from pattern source data
  pattern    → Derived from pattern distributions (age, gender, marital)
  lookup     → From governed lookup CSV (names, addresses)
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
# (field_name): (default_source, label)
# default_source is what happens when NO user rule is defined.
# Every element can be overridden with a user rule.

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


db.bootstrap()

st.title("Generation Configuration")
st.caption(
    "Every data element is configurable. Without a rule, values come from the "
    "default source (denorm, pattern distribution, or lookup). "
    "Define a rule to override any element with synthetic values."
)

# Legend
st.markdown(
    ":orange[**Denorm**] Default: from source data · "
    ":violet[**Pattern**] Default: from distributions · "
    ":green[**Lookup**] Default: from governed CSV · "
    ":blue[**Rule defined**] User override active"
)

st.markdown("")

# ── Load rules ───────────────────────────────────────────────────────────────

rules_list = db.get_generation_rules()
rules_by_field = {r["field_name"]: r for r in rules_list}

# ── Filter ───────────────────────────────────────────────────────────────────

filter_options = ["All", "Has rule", "No rule"]
status_filter = st.radio("Show", filter_options, horizontal=True, label_visibility="collapsed")

# ── Build grid ───────────────────────────────────────────────────────────────

grid_data = []
for field_name, (default_source, label) in ELEMENT_REGISTRY.items():
    rule = rules_by_field.get(field_name)

    if rule:
        status = "Rule defined"
        parts = [rule.get("gen_method", "")]
        if rule.get("prefix"): parts.append(f"prefix={rule['prefix']}")
        if rule.get("length") and rule["length"] > 0: parts.append(f"len={rule['length']}")
        if rule.get("start_value") and rule["start_value"] > 0: parts.append(f"start={rule['start_value']}")
        if rule.get("domain"): parts.append(f"domain={rule['domain']}")
        if rule.get("lookup_source"): parts.append(f"lookup={rule['lookup_source']}")
        source_text = ", ".join(parts)
    else:
        status = f"Default ({default_source})"
        if default_source == "denorm":
            source_text = "Sampled from pattern source data"
        elif default_source == "pattern":
            source_text = "From pattern distribution"
        elif default_source == "lookup":
            source_text = "From governed lookup CSV"
        else:
            source_text = default_source

    # Apply filter
    if status_filter == "All" or \
       (status_filter == "Has rule" and rule) or \
       (status_filter == "No rule" and not rule):
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
    display_df,
    hide_index=True, width="stretch",
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

st.divider()

hdr1, hdr2 = st.columns([3, 1])
hdr1.subheader(field_name)
hdr1.caption(label)

if existing_rule:
    hdr2.markdown(":blue[**Rule defined**]")
else:
    color = {"denorm": "orange", "pattern": "violet", "lookup": "green"}.get(default_source, "gray")
    hdr2.markdown(f":{color}[**Default: {default_source}**]")

if not existing_rule:
    st.markdown(
        f"No rule defined. This field currently uses **{default_source}** values. "
        "Define a rule below to override with synthetic values."
    )

# ── Detect field type for specialized forms ──────────────────────────────────

is_email = "email" in field_name.lower() or "email" in label.lower()
is_name = field_name in ("MEME_FIRST_NAME", "MEME_LAST_NAME", "SBSB_FIRST_NAME", "SBSB_LAST_NAME", "MEME_MID_INIT")

with st.form(f"rule_form_{field_name}"):

    if is_email:
        # ── Email-specific form ──────────────────────────────────────
        st.markdown("**Email configuration**")
        col1, col2 = st.columns(2)
        domain = col1.text_input(
            "Domain",
            value=existing_rule["domain"] if existing_rule and existing_rule.get("domain") else "caresource.com",
            help="e.g., caresource.com",
        )
        local_part_method = col2.selectbox(
            "Local part method",
            ["name-based", "lookup", "random"],
            index=0,
            help="name-based: first initial + last name. lookup: from CSV. random: random string.",
        )
        st.caption(f"Preview: jsmith@{domain}")

        # Store as formatted method
        gen_method = "formatted"
        data_type = "text"
        length = 0
        prefix = ""
        postfix = ""
        start_value = 0
        format_pattern = local_part_method
        lookup_source = ""

    else:
        # ── Standard form ────────────────────────────────────────────
        col1, col2, col3 = st.columns(3)
        gen_method = col1.selectbox(
            "Method",
            ["sequential", "random", "lookup", "constant"],
            index=["sequential", "random", "lookup", "constant"].index(
                existing_rule["gen_method"] if existing_rule and existing_rule["gen_method"] in ["sequential", "random", "lookup", "constant"] else "sequential"
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
        prefix = col4.text_input("Prefix",
            value=existing_rule["prefix"] if existing_rule and existing_rule.get("prefix") else "")
        postfix = col5.text_input("Postfix",
            value=existing_rule["postfix"] if existing_rule and existing_rule.get("postfix") else "")
        start_value = col6.number_input("Start value",
            min_value=0, value=existing_rule["start_value"] if existing_rule and existing_rule.get("start_value") else 1)

        available_lookups = get_available_lookups()
        current_lookup = existing_rule["lookup_source"] if existing_rule and existing_rule.get("lookup_source") else ""
        lookup_options = [""] + available_lookups
        lookup_idx = lookup_options.index(current_lookup) if current_lookup in lookup_options else 0
        lookup_source = st.selectbox("Lookup source", lookup_options, index=lookup_idx)

        domain = ""
        format_pattern = ""

    # ── Buttons ──────────────────────────────────────────────────────
    col_save, col_remove, col_test = st.columns(3)
    save_clicked = col_save.form_submit_button("Save rule", type="primary")
    remove_clicked = col_remove.form_submit_button("Remove rule (revert to default)")
    test_clicked = col_test.form_submit_button("Test 5 samples")

    if save_clicked:
        db.upsert_generation_rule(
            field_name=field_name, field_label=label,
            field_category=default_source,
            gen_method=gen_method, data_type=data_type, length=length,
            prefix=prefix, postfix=postfix, start_value=start_value,
            domain=domain, format_pattern=format_pattern,
            lookup_source=lookup_source, updated_by="user",
        )
        st.success(f"Rule saved for {field_name}")
        st.rerun()

    if remove_clicked:
        if existing_rule:
            db.delete_generation_rule(field_name)
            st.success(f"Rule removed. {field_name} reverts to default ({default_source}).")
            st.rerun()
        else:
            st.info(f"No rule to remove — already using default ({default_source}).")

    if test_clicked:
        SequenceCounter.reset()
        test_rule = {
            "field_name": field_name, "gen_method": gen_method,
            "data_type": data_type, "length": length,
            "prefix": prefix, "postfix": postfix,
            "start_value": start_value, "domain": domain,
            "format_pattern": format_pattern, "lookup_source": lookup_source,
        }

        if is_email and domain:
            names = generate_names(5)
            if format_pattern == "name-based" or format_pattern == "":
                samples = [generate_email(f, l, domain, i) for i, (f, l) in enumerate(names)]
            elif format_pattern == "random":
                import random, string
                samples = [
                    ''.join(random.choices(string.ascii_lowercase, k=8)) + f"@{domain}"
                    for _ in range(5)
                ]
            else:
                samples = [generate_email(f, l, domain, i) for i, (f, l) in enumerate(names)]
            st.code("  ".join(samples))

        elif gen_method == "lookup" and lookup_source:
            if lookup_source == "first_names":
                # Show only first names for FIRST_NAME fields
                names = generate_names(5)
                if "FIRST" in field_name or "first" in label.lower():
                    st.code("  ".join([n[0] for n in names]))
                elif "LAST" in field_name or "last" in label.lower():
                    st.code("  ".join([n[1] for n in names]))
                else:
                    st.code("  ".join([f"{n[0]} {n[1]}" for n in names]))
            elif lookup_source == "last_names":
                names = generate_names(5)
                st.code("  ".join([n[1] for n in names]))
            elif lookup_source in ("street_names", "zip_city_state"):
                addrs = generate_addresses(5)
                if "ZIP" in field_name:
                    st.code("  ".join([a["zip"] for a in addrs]))
                elif "CITY" in field_name:
                    st.code("  ".join([a["city"] for a in addrs]))
                elif "STATE" in field_name:
                    st.code("  ".join([a["state"] for a in addrs]))
                elif "COUNTY" in field_name:
                    st.code("  ".join([a["county"] for a in addrs]))
                elif "STREET" in field_name:
                    st.code("  ".join([a["street_1"] for a in addrs]))
                else:
                    st.dataframe(pd.DataFrame(addrs), hide_index=True)
            else:
                try:
                    from cluster_profiler.generation_rules import _load_lookup
                    lk = _load_lookup(lookup_source)
                    samples = lk.iloc[:5].values.tolist()
                    st.code("  ".join([str(s[0]) if len(s) == 1 else str(s) for s in samples]))
                except Exception as e:
                    st.error(str(e))
        else:
            values = generate_id(test_rule, 5)
            st.code("  ".join(values))
