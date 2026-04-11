"""Generate synthetic subscriber/member data matching a pattern profile.

Generation philosophy
---------------------
Every field starts from the denormalized source data (pattern's actual records).
User-configured rules OVERRIDE specific fields with synthetic values.
Names and addresses are ALWAYS synthetic (PII protection).

Field resolution order:
  1. Names, addresses → always synthetic from governed lookups (PII)
  2. User-configured rule exists → generate per rule (synthetic override)
  3. No rule → sample from the pattern's actual denorm data (real values)

This means unconfigured fields like MEME_CK, MEME_SSN, MEME_MCTR_STS carry
real values from the denorm unless the user explicitly defines a generation rule.
"""

import numpy as np
import pandas as pd

from .generation_rules import (
    generate_id, generate_names, generate_email,
    generate_addresses, get_all_rules_as_dict,
    SequenceCounter,
)


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

# Fields that are ALWAYS synthetic (PII protection) — never from denorm
ALWAYS_SYNTHETIC = {
    "MEME_FIRST_NAME", "MEME_LAST_NAME", "MEME_MID_INIT",
    "SBSB_FIRST_NAME", "SBSB_LAST_NAME",
}

# Fields derived from pattern distributions (not directly sampled from denorm)
PATTERN_DERIVED = {
    "MEME_SEX", "MEME_BIRTH_DT", "MEME_MARITAL_STATUS",
    "MEME_ORIG_EFF_DT", "MEME_SFX", "MEME_REL",
    "SBSB_ORIG_EFF_DT",
}

TERM_DATE_SENTINEL = "9999-12-31"


def _sample_from_dist(pct_dict: dict, n: int, rng) -> list:
    """Sample n values from a {value: proportion} distribution."""
    if not pct_dict:
        return [""] * n
    values = list(pct_dict.keys())
    probs = np.array([pct_dict[v] for v in values], dtype=float)
    probs = probs / probs.sum()
    return list(rng.choice(values, size=n, p=probs))


def _sample_continuous(stats: dict, n: int, rng) -> np.ndarray:
    """Sample from a normal distribution clamped to [min, max]."""
    vals = rng.normal(stats["mean"], max(stats["std"], 0.1), size=n)
    return np.clip(vals, stats["min"], stats["max"])


def _sample_column_from_data(source_df, col, n, rng):
    """Sample n values from a column in the source data, with replacement."""
    if col not in source_df.columns:
        return [""] * n
    pool = source_df[col].dropna().values
    if len(pool) == 0:
        return [""] * n
    return list(rng.choice(pool, size=n, replace=True))


def generate_synthetic_subscribers(
    profile: dict,
    filters_used: dict,
    n_subscribers: int,
    reference_date: str,
    source_data: pd.DataFrame = None,
) -> pd.DataFrame:
    """Generate synthetic member rows matching a pattern profile.

    Parameters
    ----------
    profile : dict
        Pattern profile with demographics and distributions.
    filters_used : dict
        Hierarchy filters from member denorm.
    n_subscribers : int
        Number of primary subscribers to create.
    reference_date : str
        ISO date for age/tenure derivation.
    source_data : pd.DataFrame, optional
        Actual member records from the pattern (denorm data).
        Used as fallback for fields without user-configured rules.
        If None, unconfigured fields will be empty.
    """
    rng = np.random.default_rng()
    ref_dt = pd.Timestamp(reference_date)

    SequenceCounter.reset()
    rules = get_all_rules_as_dict()

    # Profile data
    family = profile.get("family", {})
    spouse_rate = family.get("spouse_rate", 0.0)
    avg_dep = family.get("avg_dependents", 0.0)

    age_stats = profile.get("continuous", {}).get("_age", {})
    tenure_stats = profile.get("continuous", {}).get("_tenure", {})
    cat = profile.get("categorical", {})

    # Detect state for address filtering
    state_filter = None
    _state_map = {
        "ohio": "OH", "kentucky": "KY", "indiana": "IN",
        "georgia": "GA", "west virginia": "WV",
    }
    if source_data is not None and "GRGR_NAME" in source_data.columns:
        grgr_name = str(source_data["GRGR_NAME"].iloc[0])
        for state_name, abbrev in _state_map.items():
            if state_name in grgr_name.lower():
                state_filter = abbrev
                break

    # Pattern-derived distributions
    sub_sexes = _sample_from_dist(cat.get("MEME_SEX", {}).get("pct", {}), n_subscribers, rng)
    sub_marital = _sample_from_dist(cat.get("MEME_MARITAL_STATUS", {}).get("pct", {}), n_subscribers, rng)
    sub_ages = _sample_continuous(age_stats, n_subscribers, rng) if age_stats else rng.uniform(25, 65, n_subscribers)
    sub_tenures = _sample_continuous(tenure_stats, n_subscribers, rng) if tenure_stats else rng.uniform(1, 60, n_subscribers)

    # Always-synthetic: names and addresses from lookups
    sub_names = generate_names(n_subscribers, rng=rng)
    addresses = generate_addresses(n_subscribers, state_filter=state_filter, rng=rng)

    # Pre-sample denorm values for all non-PII, non-pattern, non-rule fields
    # These are the fallback values when no rule is configured
    denorm_samples = {}
    if source_data is not None and not source_data.empty:
        for col in SOURCE_COLUMNS:
            if col in ALWAYS_SYNTHETIC or col in PATTERN_DERIVED:
                continue
            if col in rules:
                continue  # User has a rule, will generate
            denorm_samples[col] = _sample_column_from_data(source_data, col, n_subscribers, rng)

    rows: list[dict] = []

    for i in range(n_subscribers):
        first_name, last_name = sub_names[i]
        addr = addresses[i]
        birth_dt = ref_dt - pd.DateOffset(years=int(round(sub_ages[i])))
        eff_dt = ref_dt - pd.DateOffset(months=int(round(sub_tenures[i])))

        # Build row: start with denorm fallbacks, then overlay
        row = {}

        # 1. Denorm fallback for all non-PII, non-pattern fields
        for col in SOURCE_COLUMNS:
            if col in denorm_samples:
                row[col] = denorm_samples[col][i]
            else:
                row[col] = ""

        # 2. User-configured rule overrides
        for field_name, rule in rules.items():
            if field_name in ALWAYS_SYNTHETIC:
                continue  # Handled separately
            if field_name in PATTERN_DERIVED:
                continue  # Handled separately
            if field_name in SOURCE_COLUMNS:
                if rule.get("gen_method") == "formatted" and rule.get("domain"):
                    row[field_name] = generate_email(first_name, last_name, rule["domain"], i)
                else:
                    row[field_name] = generate_id(rule, 1)[0]

        # 3. Always-synthetic PII
        row["MEME_FIRST_NAME"] = first_name
        row["MEME_LAST_NAME"] = last_name
        row["MEME_MID_INIT"] = ""
        row["SBSB_FIRST_NAME"] = first_name
        row["SBSB_LAST_NAME"] = last_name

        # 4. Pattern-derived demographics
        row["MEME_SEX"] = sub_sexes[i]
        row["MEME_BIRTH_DT"] = birth_dt.strftime("%Y-%m-%d")
        row["MEME_MARITAL_STATUS"] = sub_marital[i]
        row["MEME_ORIG_EFF_DT"] = eff_dt.strftime("%Y-%m-%d")
        row["SBSB_ORIG_EFF_DT"] = eff_dt.strftime("%Y-%m-%d")
        row["MEME_SFX"] = 1
        row["MEME_REL"] = "M"

        # 5. Address from lookups (state-filtered)
        row["SGSG_STATE"] = addr.get("state", row.get("SGSG_STATE", ""))
        row["GRGR_STATE"] = addr.get("state", row.get("GRGR_STATE", ""))
        row["GRGR_COUNTY"] = addr.get("county", row.get("GRGR_COUNTY", ""))

        rows.append(row)

        # ── Spouse ───────────────────────────────────────────────────────
        if rng.random() < spouse_rate:
            sp_names = generate_names(1, gender="F" if sub_sexes[i] == "M" else "M", rng=rng)
            sp_first = sp_names[0][0]
            sp_sex = "F" if sub_sexes[i] == "M" else "M"
            sp_age = sub_ages[i] + rng.normal(0, 3)
            sp_birth = ref_dt - pd.DateOffset(years=int(round(max(sp_age, 18))))

            sp_row = dict(row)  # Copy subscriber's row (inherits denorm values)
            sp_row["MEME_FIRST_NAME"] = sp_first
            sp_row["MEME_SEX"] = sp_sex
            sp_row["MEME_BIRTH_DT"] = sp_birth.strftime("%Y-%m-%d")
            sp_row["MEME_SFX"] = 2
            sp_row["MEME_REL"] = "S"
            sp_row["MEME_MARITAL_STATUS"] = sub_marital[i]

            # Override IDs if rules exist (new unique values for spouse)
            for field_name, rule in rules.items():
                if field_name in ALWAYS_SYNTHETIC or field_name in PATTERN_DERIVED:
                    continue
                if field_name in SOURCE_COLUMNS and field_name in ("MEME_CK", "MEME_SSN"):
                    sp_row[field_name] = generate_id(rule, 1)[0]

            rows.append(sp_row)

        # ── Dependents ───────────────────────────────────────────────────
        n_deps = rng.poisson(avg_dep)
        sfx = 3
        for d in range(n_deps):
            dep_names = generate_names(1, rng=rng)
            dep_first = dep_names[0][0]
            dep_age = rng.uniform(0, min(sub_ages[i] - 18, 18)) if sub_ages[i] > 18 else rng.uniform(0, 5)
            dep_birth = ref_dt - pd.DateOffset(years=int(round(max(dep_age, 0))))

            dep_row = dict(row)  # Copy subscriber's row
            dep_row["MEME_FIRST_NAME"] = dep_first
            dep_row["MEME_SEX"] = rng.choice(["M", "F"])
            dep_row["MEME_BIRTH_DT"] = dep_birth.strftime("%Y-%m-%d")
            dep_row["MEME_SFX"] = sfx
            dep_row["MEME_REL"] = "D"
            dep_row["MEME_MARITAL_STATUS"] = ""

            # Override IDs if rules exist
            for field_name, rule in rules.items():
                if field_name in ALWAYS_SYNTHETIC or field_name in PATTERN_DERIVED:
                    continue
                if field_name in SOURCE_COLUMNS and field_name in ("MEME_CK", "MEME_SSN"):
                    dep_row[field_name] = generate_id(rule, 1)[0]

            rows.append(dep_row)
            sfx += 1

    result = pd.DataFrame(rows)
    for col in SOURCE_COLUMNS:
        if col not in result.columns:
            result[col] = ""
    return result[SOURCE_COLUMNS]
