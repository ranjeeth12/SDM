"""Generate synthetic claims data matching a pattern profile.

Produces claim-line records with realistic ICD/CPT codes, provider
assignments, and adjudication outcomes based on pattern demographics
and probabilistic correlations.
"""

import numpy as np
import pandas as pd

try:
    from faker import Faker
    _faker = Faker()
except ImportError:
    _faker = None


# ── ICD-10 / CPT reference distributions by age bucket ───────────────────────

_ICD_BY_AGE = {
    "pediatric": [
        ("J06.9", 0.25),  # Upper respiratory infection
        ("J20.9", 0.15),  # Acute bronchitis
        ("H66.90", 0.12), # Otitis media
        ("R50.9", 0.10),  # Fever
        ("Z00.129", 0.20),# Well-child visit
        ("J45.20", 0.08), # Mild persistent asthma
        ("K21.0", 0.10),  # GERD
    ],
    "adult": [
        ("Z00.00", 0.15), # General exam
        ("E11.9", 0.12),  # Type 2 diabetes
        ("I10", 0.15),    # Essential hypertension
        ("M54.5", 0.10),  # Low back pain
        ("J06.9", 0.08),  # Upper respiratory infection
        ("F41.1", 0.08),  # Generalized anxiety
        ("E78.5", 0.07),  # Hyperlipidemia
        ("K21.0", 0.05),  # GERD
        ("M25.50", 0.05), # Joint pain
        ("G43.909", 0.05),# Migraine
        ("Z23", 0.10),    # Immunization
    ],
    "senior": [
        ("I10", 0.18),    # Hypertension
        ("E11.9", 0.15),  # Type 2 diabetes
        ("E78.5", 0.10),  # Hyperlipidemia
        ("M17.11", 0.08), # Knee osteoarthritis
        ("I25.10", 0.07), # Coronary artery disease
        ("J44.1", 0.07),  # COPD with exacerbation
        ("N18.3", 0.05),  # CKD stage 3
        ("G30.9", 0.04),  # Alzheimer's
        ("I48.91", 0.05), # Atrial fibrillation
        ("Z00.00", 0.11), # General exam
        ("M81.0", 0.05),  # Osteoporosis
        ("F32.9", 0.05),  # Depression
    ],
}

_CPT_BY_ICD = {
    "Z00.00": [("99395", 0.5), ("99396", 0.5)],
    "Z00.129": [("99391", 0.3), ("99392", 0.3), ("99393", 0.4)],
    "Z23": [("90471", 0.5), ("90472", 0.5)],
    "I10": [("99213", 0.4), ("99214", 0.4), ("80053", 0.2)],
    "E11.9": [("99214", 0.3), ("83036", 0.4), ("99213", 0.3)],
    "E78.5": [("80061", 0.5), ("99213", 0.3), ("99214", 0.2)],
    "M54.5": [("99213", 0.3), ("72148", 0.3), ("97110", 0.4)],
    "J06.9": [("99213", 0.6), ("87880", 0.4)],
    "F41.1": [("99214", 0.4), ("90837", 0.6)],
}

_DENTAL_CODES = [
    ("D0120", 0.20),  # Periodic oral eval
    ("D0150", 0.10),  # Comprehensive oral eval
    ("D1110", 0.25),  # Prophylaxis adult
    ("D1120", 0.10),  # Prophylaxis child
    ("D0274", 0.10),  # Bitewing X-rays
    ("D2391", 0.08),  # Resin filling
    ("D2750", 0.05),  # Crown
    ("D7140", 0.05),  # Extraction
    ("D4341", 0.04),  # Periodontal scaling
    ("D0220", 0.03),  # Periapical X-ray
]

_CLAIM_TYPES = {
    "medical": ["I", "O", "P"],  # Inpatient, Outpatient, Professional
    "dental": ["D"],
    "vision": ["V"],
}

CLAIM_COLUMNS = [
    "CLAIM_ID", "CLAIM_LINE", "MEME_CK", "SBSB_CK",
    "GRGR_CK", "SGSG_CK", "CSPD_CAT", "LOBD_ID",
    "CLAIM_TYPE", "SERVICE_DT", "PAID_DT",
    "ICD_PRIMARY", "CPT_CODE", "PROVIDER_ID", "PROVIDER_NPI",
    "BILLED_AMT", "ALLOWED_AMT", "PAID_AMT",
    "COPAY_AMT", "COINSURANCE_AMT", "DEDUCTIBLE_AMT",
    "CLAIM_STATUS", "ADJUDICATION",
]


def _get_age_bucket(mean_age: float) -> str:
    if mean_age < 18:
        return "pediatric"
    if mean_age < 65:
        return "adult"
    return "senior"


def _sample_weighted(items: list[tuple], n: int, rng) -> list:
    """Sample from a list of (value, weight) tuples."""
    values = [v for v, _ in items]
    weights = np.array([w for _, w in items], dtype=float)
    weights = weights / weights.sum()
    return list(rng.choice(values, size=n, p=weights))


def generate_synthetic_claims(
    profile: dict,
    filters_used: dict,
    member_df: pd.DataFrame,
    n_claims: int,
    reference_date: str = "2025-01-01",
) -> pd.DataFrame:
    """Generate synthetic claim lines for members in a pattern.

    Parameters
    ----------
    profile : dict
        Pattern profile from profile_cluster().
    filters_used : dict
        Active hierarchy filters.
    member_df : pd.DataFrame
        Member records (from pattern or generated) to assign claims to.
    n_claims : int
        Number of claim lines to generate.
    reference_date : str
        ISO date for service date generation window.

    Returns
    -------
    pd.DataFrame with CLAIM_COLUMNS.
    """
    rng = np.random.default_rng()
    ref_dt = pd.Timestamp(reference_date)

    # Determine claim domain from plan category
    cspd_cat = filters_used.get("cspd_cat", [None])
    if isinstance(cspd_cat, list):
        cspd_cat = cspd_cat[0] if cspd_cat else None

    cat_lower = (cspd_cat or "M").lower()
    if cat_lower == "d":
        domain = "dental"
    elif cat_lower == "v":
        domain = "vision"
    else:
        domain = "medical"

    # Age bucket for ICD selection
    age_stats = profile.get("continuous", {}).get("_age", {})
    age_bucket = _get_age_bucket(age_stats.get("mean", 40))

    # Available members to assign claims to
    if member_df is not None and not member_df.empty:
        member_pool = member_df[["MEME_CK", "SBSB_CK"]].values.tolist()
    else:
        member_pool = [[900000 + i, 800000 + i] for i in range(100)]

    rows = []
    claim_id_start = 700000

    for i in range(n_claims):
        # Pick a random member
        meme_ck, sbsb_ck = member_pool[rng.integers(0, len(member_pool))]

        # Service date: random within 12 months before reference
        days_back = int(rng.integers(1, 365))
        service_dt = ref_dt - pd.DateOffset(days=days_back)
        paid_dt = service_dt + pd.DateOffset(days=int(rng.integers(14, 60)))

        # ICD / CPT selection
        if domain == "dental":
            code_items = _DENTAL_CODES
            icd = ""
            cpt = _sample_weighted(code_items, 1, rng)[0]
            claim_type = "D"
        else:
            icd_items = _ICD_BY_AGE.get(age_bucket, _ICD_BY_AGE["adult"])
            icd = _sample_weighted(icd_items, 1, rng)[0]
            cpt_items = _CPT_BY_ICD.get(icd, [("99213", 1.0)])
            cpt = _sample_weighted(cpt_items, 1, rng)[0]
            claim_types = _CLAIM_TYPES.get(domain, ["P"])
            claim_type = rng.choice(claim_types)

        # Amounts
        billed = round(float(rng.uniform(50, 2000)), 2)
        allowed = round(billed * float(rng.uniform(0.5, 0.9)), 2)
        copay = round(float(rng.choice([0, 20, 30, 40, 50])), 2)
        coinsurance = round(allowed * float(rng.uniform(0, 0.2)), 2)
        deductible = round(float(rng.choice([0, 0, 0, 100, 250, 500])), 2)
        paid = round(max(0, allowed - copay - coinsurance - deductible), 2)

        # Provider
        provider_npi = f"{rng.integers(1000000000, 9999999999)}"
        provider_id = f"PRV{rng.integers(10000, 99999)}"

        # Adjudication
        adj_roll = rng.random()
        if adj_roll < 0.85:
            adjudication = "APPROVED"
            claim_status = "PAID"
        elif adj_roll < 0.95:
            adjudication = "DENIED"
            claim_status = "DENIED"
            paid = 0
        else:
            adjudication = "PENDING"
            claim_status = "IN_REVIEW"

        rows.append({
            "CLAIM_ID": f"CLM{claim_id_start + i}",
            "CLAIM_LINE": 1,
            "MEME_CK": meme_ck,
            "SBSB_CK": sbsb_ck,
            "GRGR_CK": filters_used.get("grgr_ck", [""])[0] if isinstance(filters_used.get("grgr_ck"), list) else "",
            "SGSG_CK": filters_used.get("sgsg_ck", [""])[0] if isinstance(filters_used.get("sgsg_ck"), list) else "",
            "CSPD_CAT": cspd_cat or "",
            "LOBD_ID": filters_used.get("lobd_id", [""])[0] if isinstance(filters_used.get("lobd_id"), list) else "",
            "CLAIM_TYPE": claim_type,
            "SERVICE_DT": service_dt.strftime("%Y-%m-%d"),
            "PAID_DT": paid_dt.strftime("%Y-%m-%d"),
            "ICD_PRIMARY": icd,
            "CPT_CODE": cpt,
            "PROVIDER_ID": provider_id,
            "PROVIDER_NPI": provider_npi,
            "BILLED_AMT": billed,
            "ALLOWED_AMT": allowed,
            "PAID_AMT": paid,
            "COPAY_AMT": copay,
            "COINSURANCE_AMT": coinsurance,
            "DEDUCTIBLE_AMT": deductible,
            "CLAIM_STATUS": claim_status,
            "ADJUDICATION": adjudication,
        })

    return pd.DataFrame(rows, columns=CLAIM_COLUMNS)
