"""Generate synthetic subscriber data matching a pattern profile."""

import random

import numpy as np
import pandas as pd

try:
    from faker import Faker

    _faker = Faker()
except ImportError:
    _faker = None

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
    probs = probs / probs.sum()  # normalise in case of rounding
    return list(rng.choice(values, size=n, p=probs))


def _sample_continuous(stats: dict, n: int, rng: np.random.Generator) -> np.ndarray:
    """Sample from a normal distribution clamped to [min, max]."""
    vals = rng.normal(stats["mean"], max(stats["std"], 0.1), size=n)
    return np.clip(vals, stats["min"], stats["max"])


def _fake_name():
    if _faker:
        return _faker.last_name(), _faker.first_name()
    return "Synth", "Member"


def generate_synthetic_subscribers(
    profile: dict,
    filters_used: dict,
    n_subscribers: int,
    reference_date: str,
) -> pd.DataFrame:
    """Generate synthetic rows matching *profile*.

    Parameters
    ----------
    profile : dict
        A single pattern profile as returned by ``profile_cluster``.
    filters_used : dict
        The hierarchy filters active when profiling (grgr_ck, sgsg_ck, …).
    n_subscribers : int
        Number of primary subscribers (MEME_REL='M') to create.
    reference_date : str
        ISO date string used to derive birth/eff dates from age/tenure.

    Returns
    -------
    pd.DataFrame with one row per member (subscribers + spouses + dependents),
    columns matching ``SOURCE_COLUMNS``.
    """
    rng = np.random.default_rng()
    ref_dt = pd.Timestamp(reference_date)

    family = profile.get("family", {})
    spouse_rate = family.get("spouse_rate", 0.0)
    avg_dep = family.get("avg_dependents", 0.0)

    age_stats = profile.get("continuous", {}).get("_age", {})
    tenure_stats = profile.get("continuous", {}).get("_tenure", {})
    cat = profile.get("categorical", {})
    descs = profile.get("descriptions", {})

    # Pre-pick filter values (take first if list)
    grgr_ck = _first(filters_used.get("grgr_ck"))
    sgsg_ck = _first(filters_used.get("sgsg_ck"))
    cspd_cat = _first(filters_used.get("cspd_cat"))
    lobd_id = _first(filters_used.get("lobd_id"))

    # Description look-ups — pick first available value
    grgr_name = _first_desc(descs, "GRGR_NAME")
    sgsg_name = _first_desc(descs, "SGSG_NAME")
    cspd_cat_desc = _first_desc(descs, "CSPD_CAT_DESC")
    plds_desc = _first_desc(descs, "PLDS_DESC")
    pdds_desc = _first_desc(descs, "PDDS_DESC")

    # Sample subscriber-level attributes
    sub_sexes = _sample_from_dist(
        cat.get("MEME_SEX", {}).get("pct", {}), n_subscribers, rng,
    )
    sub_marital = _sample_from_dist(
        cat.get("MEME_MARITAL_STATUS", {}).get("pct", {}), n_subscribers, rng,
    )
    sub_ages = _sample_continuous(age_stats, n_subscribers, rng) if age_stats else rng.uniform(25, 65, n_subscribers)
    sub_tenures = _sample_continuous(tenure_stats, n_subscribers, rng) if tenure_stats else rng.uniform(1, 60, n_subscribers)

    rows: list[dict] = []
    meme_ck = 900_000
    sbsb_ck = 800_000

    for i in range(n_subscribers):
        last, first = _fake_name()
        birth_dt = ref_dt - pd.DateOffset(years=int(round(sub_ages[i])))
        eff_dt = ref_dt - pd.DateOffset(months=int(round(sub_tenures[i])))

        base = _base_row(
            grgr_ck, sgsg_ck, cspd_cat, lobd_id,
            grgr_name, sgsg_name, cspd_cat_desc, plds_desc, pdds_desc,
            sbsb_ck, eff_dt, last, first,
        )

        # Subscriber row
        sub_row = {
            **base,
            "MEME_CK": meme_ck,
            "MEME_SFX": 1,
            "MEME_REL": "M",
            "MEME_LAST_NAME": last,
            "MEME_FIRST_NAME": first,
            "MEME_MID_INIT": "",
            "MEME_SEX": sub_sexes[i],
            "MEME_BIRTH_DT": birth_dt.strftime("%Y-%m-%d"),
            "MEME_SSN": f"S{meme_ck}",
            "MEME_MCTR_STS": "AC",
            "MEME_ORIG_EFF_DT": eff_dt.strftime("%Y-%m-%d"),
            "MEME_MARITAL_STATUS": sub_marital[i],
            "MEME_MEDCD_NO": "",
            "MEME_HICN": "",
            "MEME_MCTR_RACE_NVL": "",
            "MEME_MCTR_ETHN_NVL": "",
        }
        rows.append(sub_row)
        meme_ck += 1

        # Spouse
        if rng.random() < spouse_rate:
            sp_last, sp_first = _fake_name()
            sp_sex = "F" if sub_sexes[i] == "M" else "M"
            sp_age = sub_ages[i] + rng.normal(0, 3)
            sp_birth = ref_dt - pd.DateOffset(years=int(round(max(sp_age, 18))))
            sp_row = {
                **base,
                "MEME_CK": meme_ck,
                "MEME_SFX": 2,
                "MEME_REL": "S",
                "MEME_LAST_NAME": last,
                "MEME_FIRST_NAME": sp_first,
                "MEME_MID_INIT": "",
                "MEME_SEX": sp_sex,
                "MEME_BIRTH_DT": sp_birth.strftime("%Y-%m-%d"),
                "MEME_SSN": f"S{meme_ck}",
                "MEME_MCTR_STS": "AC",
                "MEME_ORIG_EFF_DT": eff_dt.strftime("%Y-%m-%d"),
                "MEME_MARITAL_STATUS": sub_marital[i],
                "MEME_MEDCD_NO": "",
                "MEME_HICN": "",
                "MEME_MCTR_RACE_NVL": "",
                "MEME_MCTR_ETHN_NVL": "",
            }
            rows.append(sp_row)
            meme_ck += 1

        # Dependents
        n_deps = rng.poisson(avg_dep)
        sfx = 3
        for _ in range(n_deps):
            dep_first = _fake_name()[1]
            dep_age = rng.uniform(0, min(sub_ages[i] - 18, 18)) if sub_ages[i] > 18 else rng.uniform(0, 5)
            dep_birth = ref_dt - pd.DateOffset(years=int(round(max(dep_age, 0))))
            dep_row = {
                **base,
                "MEME_CK": meme_ck,
                "MEME_SFX": sfx,
                "MEME_REL": "D",
                "MEME_LAST_NAME": last,
                "MEME_FIRST_NAME": dep_first,
                "MEME_MID_INIT": "",
                "MEME_SEX": rng.choice(["M", "F"]),
                "MEME_BIRTH_DT": dep_birth.strftime("%Y-%m-%d"),
                "MEME_SSN": f"S{meme_ck}",
                "MEME_MCTR_STS": "AC",
                "MEME_ORIG_EFF_DT": eff_dt.strftime("%Y-%m-%d"),
                "MEME_MARITAL_STATUS": "",
                "MEME_MEDCD_NO": "",
                "MEME_HICN": "",
                "MEME_MCTR_RACE_NVL": "",
                "MEME_MCTR_ETHN_NVL": "",
            }
            rows.append(dep_row)
            meme_ck += 1
            sfx += 1

        sbsb_ck += 1

    result = pd.DataFrame(rows)
    # Ensure all source columns are present and ordered
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
    sbsb_ck, eff_dt, last, first,
):
    """Fields shared by all family members within a subscriber unit."""
    eff_str = eff_dt.strftime("%Y-%m-%d")
    return {
        "SBSB_CK": sbsb_ck,
        "GRGR_CK": grgr_ck,
        "SBSB_ID": f"SB{sbsb_ck}",
        "SBSB_LAST_NAME": last,
        "SBSB_FIRST_NAME": first,
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
        "SGSG_STATE": "",
        "SGSG_STS": "AC",
        "SGSG_ORIG_EFF_DT": eff_str,
        "SGSG_TERM_DT": TERM_DATE_SENTINEL,
        "GRGR_ID": f"GR{grgr_ck}" if grgr_ck else "",
        "GRGR_NAME": grgr_name,
        "GRGR_STATE": "",
        "GRGR_COUNTY": "",
        "GRGR_STS": "AC",
        "GRGR_ORIG_EFF_DT": eff_str,
        "GRGR_TERM_DT": TERM_DATE_SENTINEL,
        "GRGR_MCTR_TYPE": "",
        "PAGR_CK": "",
        "CSPI_ID": f"CP{sbsb_ck}",
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
