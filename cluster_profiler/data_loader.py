"""Load CSVs, derive features, and apply hierarchy filters."""

import pandas as pd

from .config import FILTER_COLUMNS


def load_data(data_path, labels_path, reference_date='2025-01-01'):
    """Read member and label CSVs, derive _age and _tenure features."""
    df = pd.read_csv(data_path)
    labels_df = pd.read_csv(labels_path)
    ref = pd.Timestamp(reference_date)

    df['_age'] = (ref - pd.to_datetime(df['MEME_BIRTH_DT'])).dt.days / 365.25
    df['_tenure'] = (ref - pd.to_datetime(df['MEME_ORIG_EFF_DT'])).dt.days / 30.44

    return df, labels_df


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
