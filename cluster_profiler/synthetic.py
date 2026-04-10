"""Generate synthetic subscriber/member data matching a pattern profile.

Generation approach
-------------------
- Synthetic PII (from governed lookups + configured rules):
    Names      → shuffled from data/lookups/first_names.csv, last_names.csv
    DOBs       → derived from pattern age distribution
    SSNs       → sequential from invalid range (900-999) per configured rule
    Member IDs → sequential per configured rule (prefix, length, start)
    Emails     → formatted from name + configured domain
    Addresses  → from data/lookups/street_names.csv, zip_city_state.csv

- Reference data (reused from Member Denorm via filters):
    GRGR_CK, SGSG_CK, CSPD_CAT, LOBD_ID, GRGR_NAME, SGSG_NAME,
    CSPD_CAT_DESC, PLDS_DESC, PDDS_DESC — real configuration values.

- Family structure:
    Spouse and dependent records generated based on pattern's spouse_rate
    and avg_dependents statistics.

No AI is used in the generation path. No Faker dependency.
"""

import numpy as np
import pandas as pd

from .generation_rules import (
    generate_id, generate_names, generate_email,
    generate_addresses, generate_ssn, get_all_rules_as_dict,
    SequenceCounter,
)
from . import db


# All columns in the source CSV, in order.
SOURCE_COLUMNS = [
    "MEME_CK", "SBSB_CK", "GRGR_CK", "MEME_SFX", "MEME_REL",
    "MEME_LAST_NAME", "MEME_FIRST_NAME", "MEME_MID_INIT", "MEME_SEX",
    "MEME_BIRTH_DT", "MEME_SSN", "MEME_MCTR_STS", "MEME_ORIG_EFF_DT",
    "MEME_MARITAL_STATUS", "MEME_MEDCD_NO", "MEME_HICN",
    "MEME_MCTR_RACE_NVL", "MEME_MCTR_ETHN_NVL", "SBSB_ID",
    "SBSB_LAST_NAME", "SBSB_FIRST_NAME", "SBSB_ORIG_EFF_DT",
    "SBSB_MCTR_STS", "SBSB_EMPLOY_ID", "SGSG_CK", "SBSG_EFF_DT",
    "SBSG_TERM_DT", "SBSG_MCTR_TRSN", "SGSG_ID", "SGSG_NAME", "CSCS_ID",
    "SGSG_STATE", "SGSG_STS", "SGSG_ORIG_EFF_DT", "SGSG_TERM_DT",
    "GRGR_ID", "GRGR_NAME", "GRGR_STATE", "GRGR_COUNTY", "GRGR_STS",
    "GRGR_ORIG_EFF_DT", "GRGR_TERM_DT", "GRGR_MCTR_TYPE", "PAGR_CK",
    "CSPI_ID", "CSPD_CAT", "CSPI_EFF_DT", "CSPI_TERM_DT", "PDPD_ID",
    "CSPI_SEL_IND", "CSPI_HIOS_ID_NVL", "CSPD_CAT_DESC", "CSPD_TYPE",
    "LOBD_ID", "PDPD_RISK_IND", "PDPD_MCTR_CCAT", "PLDS_DESC", "PDDS_DESC",
]

TERM_DATE_SENTINEL = "9999-12-31"


def _sample_from_dist(pct_dict: dict, n: int, rng: np.random.Generator) -> list:
    """Sample n values from a {value: proportion} distribution."""
    if not pct_dict:
        return [""] * n
    values = list(pct_dict.keys())
    probs = np.array([pct_dict[v] for v in values], dtype=float)
    probs = probs / probs.sum()
    return list(rng.choice(values, size=n, p=probs))


def _sample_continuous(stats: dict, n: int, rng: np.random.Generator) -> np.ndarray:
    """Sample from a normal distribution clamped to [min, max]."""
    vals = rng.normal(stats["mean"], max(stats["std"], 0.1), size=n)
    return np.clip(vals, stats["min"], stats["max"])


def generate_synthetic_subscribers(
    profile: dict,
    filters_used: dict,
    n_subscribers: int,
    reference_date: str,
) -> pd.DataFrame:
    """Generate synthetic member rows matching a pattern profile.

    Uses governed lookups for PII and configured rules for IDs.
    Reference data comes from filters_used (member denorm values).
    """
    rng = np.random.default_rng()
    ref_dt = pd.Timestamp(reference_date)

    # Reset sequential counters for this generation run
    SequenceCounter.reset()

    # Load generation rules
    rules = get_all_rules_as_dict()

    # Profile data
    family = profile.get("family", {})
    spouse_rate = family.get("spouse_rate", 0.0)
    avg_dep = family.get("avg_dependents", 0.0)

    age_stats = profile.get("continuous", {}).get("_age", {})
    tenure_stats = profile.get("continuous", {}).get("_tenure", {})
    cat = profile.get("categorical", {})
    descs = profile.get("descriptions", {})

    # Reference data from member denorm (via filters)
    grgr_ck = _first(filters_used.get("grgr_ck"))
    sgsg_ck = _first(filters_used.get("sgsg_ck"))
    cspd_cat = _first(filters_used.get("cspd_cat"))
    lobd_id = _first(filters_used.get("lobd_id"))

    grgr_name = _first_desc(descs, "GRGR_NAME")
    sgsg_name = _first_desc(descs, "SGSG_NAME")
    cspd_cat_desc = _first_desc(descs, "CSPD_CAT_DESC")
    plds_desc = _first_desc(descs, "PLDS_DESC")
    pdds_desc = _first_desc(descs, "PDDS_DESC")

    # Detect state from group name for address filtering
    state_filter = None
    _state_map = {
        "ohio": "OH", "kentucky": "KY", "indiana": "IN",
        "georgia": "GA", "west virginia": "WV",
    }
    if grgr_name:
        for state_name, abbrev in _state_map.items():
            if state_name in grgr_name.lower():
                state_filter = abbrev
                break

    # Sample subscriber-level demographic attributes from pattern distributions
    sub_sexes = _sample_from_dist(
        cat.get("MEME_SEX", {}).get("pct", {}), n_subscribers, rng,
    )
    sub_marital = _sample_from_dist(
        cat.get("MEME_MARITAL_STATUS", {}).get("pct", {}), n_subscribers, rng,
    )
    sub_ages = _sample_continuous(age_stats, n_subscribers, rng) if age_stats else rng.uniform(25, 65, n_subscribers)
    sub_tenures = _sample_continuous(tenure_stats, n_subscribers, rng) if tenure_stats else rng.uniform(1, 60, n_subscribers)

    # Generate PII from lookups
    sub_names = generate_names(n_subscribers, rng=rng)
    addresses = generate_addresses(n_subscribers, state_filter=state_filter, rng=rng)

    # Generate IDs from rules
    meme_rule = rules.get("MEME_CK", {"field_name": "MEME_CK", "gen_method": "sequential", "start_value": 900000, "prefix": "", "length": 10})
    sbsb_rule = rules.get("SBSB_CK", {"field_name": "SBSB_CK", "gen_method": "sequential", "start_value": 800000, "prefix": "", "length": 10})
    sbsb_id_rule = rules.get("SBSB_ID", {"field_name": "SBSB_ID", "gen_method": "sequential", "start_value": 800000, "prefix": "SB", "length": 10})
    ssn_rule = rules.get("MEME_SSN", {"field_name": "MEME_SSN", "gen_method": "sequential", "start_value": 10000000, "prefix": "9", "length": 9})

    # Email domain
    email_rule = rules.get("EMAIL")
    email_domain = email_rule.get("domain", "caresource.com") if email_rule else "caresource.com"

    rows: list[dict] = []
    email_counter = 0

    for i in range(n_subscribers):
        first_name, last_name = sub_names[i]
        addr = addresses[i]

        birth_dt = ref_dt - pd.DateOffset(years=int(round(sub_ages[i])))
        eff_dt = ref_dt - pd.DateOffset(months=int(round(sub_tenures[i])))

        meme_ck = int(generate_id(meme_rule, 1)[0])
        sbsb_ck_val = int(generate_id(sbsb_rule, 1)[0])
        sbsb_id_val = generate_id(sbsb_id_rule, 1)[0]
        ssn_val = generate_ssn(1, ssn_rule)[0]
        email_val = generate_email(first_name, last_name, email_domain, email_counter)
        email_counter += 1

        base = _base_row(
            grgr_ck, sgsg_ck, cspd_cat, lobd_id,
            grgr_name, sgsg_name, cspd_cat_desc, plds_desc, pdds_desc,
            sbsb_ck_val, sbsb_id_val, eff_dt, last_name, first_name,
            addr,
        )

        # Subscriber row
        sub_row = {
            **base,
            "MEME_CK": meme_ck,
            "MEME_SFX": 1,
            "MEME_REL": "M",
            "MEME_LAST_NAME": last_name,
            "MEME_FIRST_NAME": first_name,
            "MEME_MID_INIT": "",
            "MEME_SEX": sub_sexes[i],
            "MEME_BIRTH_DT": birth_dt.strftime("%Y-%m-%d"),
            "MEME_SSN": ssn_val,
            "MEME_MCTR_STS": "AC",
            "MEME_ORIG_EFF_DT": eff_dt.strftime("%Y-%m-%d"),
            "MEME_MARITAL_STATUS": sub_marital[i],
            "MEME_MEDCD_NO": "",
            "MEME_HICN": "",
            "MEME_MCTR_RACE_NVL": "",
            "MEME_MCTR_ETHN_NVL": "",
        }
        rows.append(sub_row)

        # Spouse
        if rng.random() < spouse_rate:
            sp_meme_ck = int(generate_id(meme_rule, 1)[0])
            sp_ssn = generate_ssn(1, ssn_rule)[0]
            sp_names = generate_names(1, gender="F" if sub_sexes[i] == "M" else "M", rng=rng)
            sp_first = sp_names[0][0]
            sp_sex = "F" if sub_sexes[i] == "M" else "M"
            sp_age = sub_ages[i] + rng.normal(0, 3)
            sp_birth = ref_dt - pd.DateOffset(years=int(round(max(sp_age, 18))))

            sp_row = {
                **base,
                "MEME_CK": sp_meme_ck,
                "MEME_SFX": 2,
                "MEME_REL": "S",
                "MEME_LAST_NAME": last_name,
                "MEME_FIRST_NAME": sp_first,
                "MEME_MID_INIT": "",
                "MEME_SEX": sp_sex,
                "MEME_BIRTH_DT": sp_birth.strftime("%Y-%m-%d"),
                "MEME_SSN": sp_ssn,
                "MEME_MCTR_STS": "AC",
                "MEME_ORIG_EFF_DT": eff_dt.strftime("%Y-%m-%d"),
                "MEME_MARITAL_STATUS": sub_marital[i],
                "MEME_MEDCD_NO": "",
                "MEME_HICN": "",
                "MEME_MCTR_RACE_NVL": "",
                "MEME_MCTR_ETHN_NVL": "",
            }
            rows.append(sp_row)

        # Dependents
        n_deps = rng.poisson(avg_dep)
        sfx = 3
        for d in range(n_deps):
            dep_meme_ck = int(generate_id(meme_rule, 1)[0])
            dep_ssn = generate_ssn(1, ssn_rule)[0]
            dep_names = generate_names(1, rng=rng)
            dep_first = dep_names[0][0]
            dep_age = rng.uniform(0, min(sub_ages[i] - 18, 18)) if sub_ages[i] > 18 else rng.uniform(0, 5)
            dep_birth = ref_dt - pd.DateOffset(years=int(round(max(dep_age, 0))))

            dep_row = {
                **base,
                "MEME_CK": dep_meme_ck,
                "MEME_SFX": sfx,
                "MEME_REL": "D",
                "MEME_LAST_NAME": last_name,
                "MEME_FIRST_NAME": dep_first,
                "MEME_MID_INIT": "",
                "MEME_SEX": rng.choice(["M", "F"]),
                "MEME_BIRTH_DT": dep_birth.strftime("%Y-%m-%d"),
                "MEME_SSN": dep_ssn,
                "MEME_MCTR_STS": "AC",
                "MEME_ORIG_EFF_DT": eff_dt.strftime("%Y-%m-%d"),
                "MEME_MARITAL_STATUS": "",
                "MEME_MEDCD_NO": "",
                "MEME_HICN": "",
                "MEME_MCTR_RACE_NVL": "",
                "MEME_MCTR_ETHN_NVL": "",
            }
            rows.append(dep_row)
            sfx += 1

    result = pd.DataFrame(rows)
    for col in SOURCE_COLUMNS:
        if col not in result.columns:
            result[col] = ""
    return result[SOURCE_COLUMNS]


# ── helpers ──────────────────────────────────────────────────────────────────

def _first(val):
    """Return the first element if val is a list, else val itself."""
    if isinstance(val, list) and val:
        return val[0]
    return val or ""


def _first_desc(descs: dict, key: str) -> str:
    vals = descs.get(key, [])
    return vals[0] if vals else ""


def _base_row(
    grgr_ck, sgsg_ck, cspd_cat, lobd_id,
    grgr_name, sgsg_name, cspd_cat_desc, plds_desc, pdds_desc,
    sbsb_ck, sbsb_id, eff_dt, last_name, first_name,
    address=None,
):
    """Fields shared by all family members within a subscriber unit.

    Reference data (grgr_ck, sgsg_ck, etc.) comes from the member denorm.
    Address comes from governed lookups.
    """
    eff_str = eff_dt.strftime("%Y-%m-%d")
    addr = address or {}

    return {
        "SBSB_CK": sbsb_ck,
        "GRGR_CK": grgr_ck,
        "SBSB_ID": sbsb_id,
        "SBSB_LAST_NAME": last_name,
        "SBSB_FIRST_NAME": first_name,
        "SBSB_ORIG_EFF_DT": eff_str,
        "SBSB_MCTR_STS": "AC",
        "SBSB_EMPLOY_ID": "",
        "SGSG_CK": sgsg_ck,
        "SBSG_EFF_DT": eff_str,
        "SBSG_TERM_DT": TERM_DATE_SENTINEL,
        "SBSG_MCTR_TRSN": "",
        "SGSG_ID": f"SG{sgsg_ck}" if sgsg_ck else "",
        "SGSG_NAME": sgsg_name,
        "CSCS_ID": "",
        "SGSG_STATE": addr.get("state", ""),
        "SGSG_STS": "AC",
        "SGSG_ORIG_EFF_DT": eff_str,
        "SGSG_TERM_DT": TERM_DATE_SENTINEL,
        "GRGR_ID": f"GR{grgr_ck}" if grgr_ck else "",
        "GRGR_NAME": grgr_name,
        "GRGR_STATE": addr.get("state", ""),
        "GRGR_COUNTY": addr.get("county", ""),
        "GRGR_STS": "AC",
        "GRGR_ORIG_EFF_DT": eff_str,
        "GRGR_TERM_DT": TERM_DATE_SENTINEL,
        "GRGR_MCTR_TYPE": "",
        "PAGR_CK": "",
        "CSPI_ID": f"CP{sbsb_ck}" if sbsb_ck else "",
        "CSPD_CAT": cspd_cat,
        "CSPI_EFF_DT": eff_str,
        "CSPI_TERM_DT": TERM_DATE_SENTINEL,
        "PDPD_ID": "",
        "CSPI_SEL_IND": "",
        "CSPI_HIOS_ID_NVL": "",
        "CSPD_CAT_DESC": cspd_cat_desc,
        "CSPD_TYPE": "",
        "LOBD_ID": lobd_id,
        "PDPD_RISK_IND": "",
        "PDPD_MCTR_CCAT": "",
        "PLDS_DESC": plds_desc,
        "PDDS_DESC": pdds_desc,
    }
