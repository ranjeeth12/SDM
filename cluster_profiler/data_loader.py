"""Load denormalized models and apply hierarchy filters.

Three denorm models:
  Member   → demographics, group/plan/benefit hierarchy
  Provider → NPIs, specialties, networks (optional, loaded when available)
  Claims   → ICD/CPT correlations, adjudication (optional, loaded when available)
"""

import pandas as pd
from pathlib import Path

from .config import (
    FILTER_COLUMNS,
    MEMBER_DENORM_PATH,
    MEMBER_LABELS_PATH,
    PROVIDER_DENORM_PATH,
    CLAIMS_DENORM_PATH,
    DEFAULT_REFERENCE_DATE,
)


def load_member_denorm(data_path=None, labels_path=None, reference_date=None):
    """Read member denorm and labels, derive _age and _tenure features."""
    data_path = data_path or MEMBER_DENORM_PATH
    labels_path = labels_path or MEMBER_LABELS_PATH
    reference_date = reference_date or DEFAULT_REFERENCE_DATE

    df = pd.read_csv(data_path)
    labels_df = pd.read_csv(labels_path)
    ref = pd.Timestamp(reference_date)

    df['_age'] = (ref - pd.to_datetime(df['MEME_BIRTH_DT'])).dt.days / 365.25
    df['_tenure'] = (ref - pd.to_datetime(df['MEME_ORIG_EFF_DT'])).dt.days / 30.44

    return df, labels_df


def load_provider_denorm(path=None):
    """Load provider denorm if available. Returns DataFrame or None."""
    path = path or PROVIDER_DENORM_PATH
    if path is None or not Path(path).exists():
        return None
    return pd.read_csv(path)


def load_claims_denorm(path=None):
    """Load claims denorm if available. Returns DataFrame or None."""
    path = path or CLAIMS_DENORM_PATH
    if path is None or not Path(path).exists():
        return None
    return pd.read_csv(path)


# Backward-compatible alias
def load_data(data_path=None, labels_path=None, reference_date=None):
    """Alias for load_member_denorm (backward compatibility)."""
    return load_member_denorm(data_path, labels_path, reference_date)


def apply_filters(df, labels_df, grgr_ck=None, sgsg_ck=None, cspd_cat=None, lobd_id=None):
    """AND-combine hierarchy filters on both data and labels.

    Each parameter is None (skip) or a list of values to include.
    Returns (subset_members, subset_labels, family_data, filters_used).
    subset_members is deduplicated to one row per MEME_CK.
    family_data retains all rows for family structure analysis.
    """
    filters = {
        'grgr_ck': grgr_ck,
        'sgsg_ck': sgsg_ck,
        'cspd_cat': cspd_cat,
        'lobd_id': lobd_id,
    }

    mask = pd.Series(True, index=df.index)
    label_mask = pd.Series(True, index=labels_df.index)

    filters_used = {}
    for key, values in filters.items():
        if values is not None:
            col = FILTER_COLUMNS[key]
            mask &= df[col].isin(values)
            if col in labels_df.columns:
                label_mask &= labels_df[col].isin(values)
            filters_used[key] = values

    filtered = df[mask].copy()
    filtered_labels = labels_df[label_mask].copy()

    if filtered.empty:
        raise ValueError(
            f"No members match the applied filters: {filters_used}"
        )

    # Keep full filtered data for family analysis before dedup
    family_data = filtered.copy()

    # Deduplicate to one row per member
    members = filtered.drop_duplicates(subset=['MEME_CK']).copy()

    return members, filtered_labels, family_data, filters_used
