"""Generate synthetic enrollment data in EDI 834-compatible format.

Produces enrollment transaction records from a member dataset,
suitable for downstream EDI file generation or database loading.
"""

import numpy as np
import pandas as pd


ENROLLMENT_COLUMNS = [
    "TRANSACTION_ID", "TRANSACTION_TYPE", "TRANSACTION_DT",
    "MEME_CK", "SBSB_CK", "SBSB_ID",
    "MEME_LAST_NAME", "MEME_FIRST_NAME", "MEME_MID_INIT",
    "MEME_SSN", "MEME_BIRTH_DT", "MEME_SEX",
    "MEME_REL", "MEME_MARITAL_STATUS",
    "GRGR_CK", "GRGR_ID", "GRGR_NAME",
    "SGSG_CK", "SGSG_ID", "SGSG_NAME",
    "CSPD_CAT", "CSPD_CAT_DESC", "LOBD_ID", "PLDS_DESC",
    "COVERAGE_EFF_DT", "COVERAGE_TERM_DT",
    "MAINTENANCE_TYPE", "MAINTENANCE_REASON",
    "INS_LINE_CD", "BENEFIT_STATUS",
]


def generate_synthetic_enrollments(
    member_df: pd.DataFrame,
    filters_used: dict,
    reference_date: str = "2025-01-01",
    transaction_type: str = "021",
) -> pd.DataFrame:
    """Generate enrollment transactions from a member DataFrame.

    Parameters
    ----------
    member_df : pd.DataFrame
        Member records with standard FACETS columns.
    filters_used : dict
        Active hierarchy filters.
    reference_date : str
        Reference date for transaction dating.
    transaction_type : str
        834 transaction type code (021=addition, 001=change, 024=termination).

    Returns
    -------
    pd.DataFrame with ENROLLMENT_COLUMNS.
    """
    rng = np.random.default_rng()
    ref_dt = pd.Timestamp(reference_date)

    # Maintenance type mapping
    maint_map = {
        "021": ("021", "AI", "Addition"),   # New enrollment
        "001": ("001", "AO", "Change"),     # Audit or compare
        "024": ("024", "XN", "Termination"),# Cancellation
    }
    maint_type, maint_reason, _ = maint_map.get(
        transaction_type, ("021", "AI", "Addition")
    )

    rows = []
    txn_id_start = 500000

    for idx, member in member_df.iterrows():
        # Coverage dates
        eff_dt = pd.to_datetime(member.get("MEME_ORIG_EFF_DT", reference_date))
        term_dt_raw = member.get("SBSG_TERM_DT", "9999-12-31")
        if pd.isna(term_dt_raw) or str(term_dt_raw).startswith("9999"):
            term_dt = "9999-12-31"
        else:
            term_dt = str(term_dt_raw)[:10]

        # Transaction date
        txn_days = int(rng.integers(0, 30))
        txn_dt = ref_dt - pd.DateOffset(days=txn_days)

        rows.append({
            "TRANSACTION_ID": f"TXN{txn_id_start + len(rows)}",
            "TRANSACTION_TYPE": transaction_type,
            "TRANSACTION_DT": txn_dt.strftime("%Y-%m-%d"),
            "MEME_CK": member.get("MEME_CK", ""),
            "SBSB_CK": member.get("SBSB_CK", ""),
            "SBSB_ID": member.get("SBSB_ID", ""),
            "MEME_LAST_NAME": member.get("MEME_LAST_NAME", ""),
            "MEME_FIRST_NAME": member.get("MEME_FIRST_NAME", ""),
            "MEME_MID_INIT": member.get("MEME_MID_INIT", ""),
            "MEME_SSN": member.get("MEME_SSN", ""),
            "MEME_BIRTH_DT": str(member.get("MEME_BIRTH_DT", ""))[:10],
            "MEME_SEX": member.get("MEME_SEX", ""),
            "MEME_REL": member.get("MEME_REL", ""),
            "MEME_MARITAL_STATUS": member.get("MEME_MARITAL_STATUS", ""),
            "GRGR_CK": member.get("GRGR_CK", ""),
            "GRGR_ID": member.get("GRGR_ID", ""),
            "GRGR_NAME": member.get("GRGR_NAME", ""),
            "SGSG_CK": member.get("SGSG_CK", ""),
            "SGSG_ID": member.get("SGSG_ID", ""),
            "SGSG_NAME": member.get("SGSG_NAME", ""),
            "CSPD_CAT": member.get("CSPD_CAT", ""),
            "CSPD_CAT_DESC": member.get("CSPD_CAT_DESC", ""),
            "LOBD_ID": member.get("LOBD_ID", ""),
            "PLDS_DESC": member.get("PLDS_DESC", ""),
            "COVERAGE_EFF_DT": str(eff_dt)[:10],
            "COVERAGE_TERM_DT": term_dt,
            "MAINTENANCE_TYPE": maint_type,
            "MAINTENANCE_REASON": maint_reason,
            "INS_LINE_CD": "HLT",
            "BENEFIT_STATUS": "A" if term_dt == "9999-12-31" else "T",
        })

    return pd.DataFrame(rows, columns=ENROLLMENT_COLUMNS)
