"""Generate synthetic claims data using denormalized reference models.

Principles
----------
- Provider NPIs, IDs, specialties → from Provider Denorm (required)
- ICD/CPT code distributions      → from Claims Denorm (required)
- Adjudication patterns, amounts  → from Claims Denorm (required)
- Group/Plan/Benefit keys         → from Member Denorm (via pattern filters)
- Synthetic (generated) fields    → Claim IDs, service dates, paid dates only

If Provider or Claims denorms are not available, generation is not possible
and the caller is informed. We do not fabricate reference data.
"""

import numpy as np
import pandas as pd

from .data_loader import load_provider_denorm, load_claims_denorm


CLAIM_COLUMNS = [
    "CLAIM_ID", "CLAIM_LINE", "MEME_CK", "SBSB_CK",
    "GRGR_CK", "SGSG_CK", "CSPD_CAT", "LOBD_ID",
    "CLAIM_TYPE", "SERVICE_DT", "PAID_DT",
    "ICD_PRIMARY", "CPT_CODE",
    "PROVIDER_NPI", "PROVIDER_ID", "PROVIDER_SPECIALTY",
    "BILLED_AMT", "ALLOWED_AMT", "PAID_AMT",
    "COPAY_AMT", "COINSURANCE_AMT", "DEDUCTIBLE_AMT",
    "CLAIM_STATUS", "ADJUDICATION",
]


class DenormNotAvailableError(Exception):
    """Raised when a required denormalized model is not available."""
    pass


def check_denorm_availability():
    """Check which denorms are available and return status."""
    provider_df = load_provider_denorm()
    claims_df = load_claims_denorm()
    return {
        "provider_available": provider_df is not None,
        "claims_available": claims_df is not None,
        "provider_df": provider_df,
        "claims_df": claims_df,
    }


def _extract_provider_pool(provider_df, filters_used):
    """Extract relevant providers from provider denorm based on hierarchy filters.

    Filters by group/subgroup/network if those columns exist in the denorm.
    Returns a DataFrame of provider rows to sample from.
    """
    pool = provider_df.copy()

    # Filter by group if available and filter is set
    if "GRGR_CK" in pool.columns and filters_used.get("grgr_ck"):
        pool = pool[pool["GRGR_CK"].isin(filters_used["grgr_ck"])]

    # If filtering emptied the pool, fall back to full provider set
    if pool.empty:
        pool = provider_df.copy()

    return pool


def _extract_code_distributions(claims_df, filters_used, cspd_cat=None):
    """Extract ICD/CPT code frequency distributions from claims denorm.

    Returns dict with:
        icd_dist: list of (code, proportion) tuples
        cpt_dist: list of (code, proportion) tuples
        amount_stats: dict with mean/std for billed, allowed, paid
        adjudication_dist: dict of status → proportion
    """
    pool = claims_df.copy()

    # Filter by plan category if available
    if cspd_cat and "CSPD_CAT" in pool.columns:
        filtered = pool[pool["CSPD_CAT"] == cspd_cat]
        if not filtered.empty:
            pool = filtered

    # Filter by group if available
    if "GRGR_CK" in pool.columns and filters_used.get("grgr_ck"):
        filtered = pool[pool["GRGR_CK"].isin(filters_used["grgr_ck"])]
        if not filtered.empty:
            pool = filtered

    result = {}

    # ICD distribution
    if "ICD_PRIMARY" in pool.columns:
        icd_counts = pool["ICD_PRIMARY"].dropna().value_counts(normalize=True)
        result["icd_dist"] = list(zip(icd_counts.index, icd_counts.values))
    else:
        result["icd_dist"] = []

    # CPT distribution
    if "CPT_CODE" in pool.columns:
        cpt_counts = pool["CPT_CODE"].dropna().value_counts(normalize=True)
        result["cpt_dist"] = list(zip(cpt_counts.index, cpt_counts.values))
    else:
        result["cpt_dist"] = []

    # Amount statistics
    amount_cols = {
        "billed": "BILLED_AMT", "allowed": "ALLOWED_AMT",
        "paid": "PAID_AMT", "copay": "COPAY_AMT",
        "coinsurance": "COINSURANCE_AMT", "deductible": "DEDUCTIBLE_AMT",
    }
    amount_stats = {}
    for key, col in amount_cols.items():
        if col in pool.columns:
            vals = pd.to_numeric(pool[col], errors="coerce").dropna()
            if len(vals) > 0:
                amount_stats[key] = {
                    "mean": float(vals.mean()),
                    "std": float(vals.std()),
                    "min": float(vals.min()),
                    "max": float(vals.max()),
                }
    result["amount_stats"] = amount_stats

    # Adjudication distribution
    if "ADJUDICATION" in pool.columns:
        adj_counts = pool["ADJUDICATION"].dropna().value_counts(normalize=True)
        result["adjudication_dist"] = dict(zip(adj_counts.index, adj_counts.values))
    elif "CLAIM_STATUS" in pool.columns:
        adj_counts = pool["CLAIM_STATUS"].dropna().value_counts(normalize=True)
        result["adjudication_dist"] = dict(zip(adj_counts.index, adj_counts.values))
    else:
        result["adjudication_dist"] = {}

    return result


def _sample_from_dist(dist, n, rng):
    """Sample n values from a list of (value, proportion) tuples."""
    if not dist:
        return [""] * n
    values = [v for v, _ in dist]
    weights = np.array([w for _, w in dist], dtype=float)
    weights = weights / weights.sum()
    return list(rng.choice(values, size=n, p=weights))


def _sample_amount(stats, n, rng):
    """Sample amounts from a normal distribution based on denorm statistics."""
    if not stats:
        return np.zeros(n)
    vals = rng.normal(stats["mean"], max(stats.get("std", 1), 0.01), size=n)
    return np.clip(vals, max(stats.get("min", 0), 0), stats.get("max", 10000)).round(2)


def generate_synthetic_claims(
    profile: dict,
    filters_used: dict,
    member_df: pd.DataFrame,
    n_claims: int,
    reference_date: str = "2025-01-01",
    provider_df: pd.DataFrame = None,
    claims_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """Generate synthetic claim lines for members in a pattern.

    Parameters
    ----------
    profile : dict
        Pattern profile from profile_cluster().
    filters_used : dict
        Active hierarchy filters (from member denorm).
    member_df : pd.DataFrame
        Member records to assign claims to (from member denorm or generated).
    n_claims : int
        Number of claim lines to generate.
    reference_date : str
        ISO date for service date generation window.
    provider_df : pd.DataFrame, optional
        Provider denorm. If None, attempts to load from config path.
    claims_df : pd.DataFrame, optional
        Claims denorm. If None, attempts to load from config path.

    Returns
    -------
    pd.DataFrame with CLAIM_COLUMNS.

    Raises
    ------
    DenormNotAvailableError
        If required denorms (provider, claims) are not available.
    """
    # Load denorms if not provided
    if provider_df is None:
        provider_df = load_provider_denorm()
    if claims_df is None:
        claims_df = load_claims_denorm()

    # Check availability
    missing = []
    if provider_df is None:
        missing.append("Provider Denorm")
    if claims_df is None:
        missing.append("Claims Denorm")

    if missing:
        raise DenormNotAvailableError(
            f"Cannot generate claims: {', '.join(missing)} not available. "
            f"Claims generation requires provider and claims denormalized models "
            f"to supply NPIs, ICD/CPT distributions, and adjudication patterns. "
            f"These are populated by the periodic data scoop process."
        )

    rng = np.random.default_rng()
    ref_dt = pd.Timestamp(reference_date)

    # Get plan category from filters
    cspd_cat = None
    if filters_used.get("cspd_cat"):
        cspd_cat = filters_used["cspd_cat"][0] if isinstance(filters_used["cspd_cat"], list) else filters_used["cspd_cat"]

    # Extract reference data from denorms
    provider_pool = _extract_provider_pool(provider_df, filters_used)
    code_dists = _extract_code_distributions(claims_df, filters_used, cspd_cat)

    # Member pool to assign claims to
    if member_df is not None and not member_df.empty:
        member_pool = member_df[["MEME_CK", "SBSB_CK"]].values.tolist()
    else:
        raise ValueError("Member data is required to generate claims.")

    rows = []
    claim_id_start = 700000

    for i in range(n_claims):
        # Pick a random member
        meme_ck, sbsb_ck = member_pool[rng.integers(0, len(member_pool))]

        # Service date: random within 12 months before reference
        days_back = int(rng.integers(1, 365))
        service_dt = ref_dt - pd.DateOffset(days=days_back)
        paid_dt = service_dt + pd.DateOffset(days=int(rng.integers(14, 60)))

        # Provider: sample from provider denorm
        prov_row = provider_pool.iloc[rng.integers(0, len(provider_pool))]
        provider_npi = str(prov_row.get("NPI", prov_row.get("PROVIDER_NPI", "")))
        provider_id = str(prov_row.get("PROVIDER_ID", prov_row.get("PRPR_ID", "")))
        provider_specialty = str(prov_row.get("SPECIALTY", prov_row.get("PRPR_SPECIALTY", "")))

        # ICD/CPT: sample from claims denorm distributions
        icd = _sample_from_dist(code_dists["icd_dist"], 1, rng)[0] if code_dists["icd_dist"] else ""
        cpt = _sample_from_dist(code_dists["cpt_dist"], 1, rng)[0] if code_dists["cpt_dist"] else ""

        # Claim type from claims denorm or derive from plan category
        if cspd_cat:
            cat_lower = str(cspd_cat).lower()
            if cat_lower == "d":
                claim_type = "D"
            elif cat_lower == "v":
                claim_type = "V"
            else:
                claim_type = rng.choice(["I", "O", "P"])
        else:
            claim_type = "P"

        # Amounts: from claims denorm distributions
        amt_stats = code_dists.get("amount_stats", {})
        billed = _sample_amount(amt_stats.get("billed"), 1, rng)[0]
        allowed = _sample_amount(amt_stats.get("allowed"), 1, rng)[0]
        paid = _sample_amount(amt_stats.get("paid"), 1, rng)[0]
        copay = _sample_amount(amt_stats.get("copay"), 1, rng)[0]
        coinsurance = _sample_amount(amt_stats.get("coinsurance"), 1, rng)[0]
        deductible = _sample_amount(amt_stats.get("deductible"), 1, rng)[0]

        # Adjudication: from claims denorm distribution
        adj_dist = code_dists.get("adjudication_dist", {})
        if adj_dist:
            adj_items = list(adj_dist.items())
            adjudication = _sample_from_dist(adj_items, 1, rng)[0]
            claim_status = adjudication
        else:
            adjudication = ""
            claim_status = ""

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
            "PROVIDER_NPI": provider_npi,
            "PROVIDER_ID": provider_id,
            "PROVIDER_SPECIALTY": provider_specialty,
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
