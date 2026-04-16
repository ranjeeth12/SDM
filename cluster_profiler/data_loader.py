"""Load data from SQL Server and apply hierarchy filters.

SQL Server only - no CSV fallback.
"""

import pandas as pd
import pyodbc
import streamlit as st

from .config import (
    FILTER_COLUMNS,
    DEFAULT_REFERENCE_DATE,
    get_connection_string,
    SQL_SCHEMA,
)


@st.cache_data(ttl=3600)
def load_member_denorm(reference_date=None):
    """Read member denorm from SQL Server, derive _age and _tenure. Cached 1 hour."""
    reference_date = reference_date or DEFAULT_REFERENCE_DATE
    ref = pd.Timestamp(reference_date)

    conn = pyodbc.connect(get_connection_string())
    df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.member_denorm", conn)
    conn.close()

    # Labels
    try:
        conn = pyodbc.connect(get_connection_string())
        labels_df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.member_labels", conn)
        conn.close()
    except Exception:
        labels_df = df[['MEME_CK', 'GRGR_CK', 'SGSG_CK', 'CSPD_CAT', 'LOBD_ID']].copy()

    # Derive age and tenure
    if 'MEME_BIRTH_DT' in df.columns:
        df['_age'] = (ref - pd.to_datetime(df['MEME_BIRTH_DT'], errors='coerce')).dt.days / 365.25
    else:
        df['_age'] = 0

    if 'MEME_ORIG_EFF_DT' in df.columns:
        df['_tenure'] = (ref - pd.to_datetime(df['MEME_ORIG_EFF_DT'], errors='coerce')).dt.days / 30.44
    else:
        df['_tenure'] = 0

    return df, labels_df


def load_data(data_path=None, labels_path=None, reference_date=None):
    """Backward-compatible alias."""
    return load_member_denorm(reference_date)


def load_provider_denorm(path=None):
    """Load provider denorm from SQL Server. Returns DataFrame or None."""
    try:
        conn = pyodbc.connect(get_connection_string())
        df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.provider_denorm", conn)
        conn.close()
        return df
    except Exception:
        return None


def load_claims_denorm(path=None):
    """Load claims denorm from SQL Server. Returns DataFrame or None."""
    try:
        conn = pyodbc.connect(get_connection_string())
        df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.claims_denorm", conn)
        conn.close()
        return df
    except Exception:
        return None


def apply_filters(df, labels_df, grgr_ck=None, sgsg_ck=None, cspd_cat=None, lobd_id=None):
    """AND-combine hierarchy filters on both data and labels."""
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

    family_data = filtered.copy()
    members = filtered.drop_duplicates(subset=['MEME_CK']).copy()

    return members, filtered_labels, family_data, filters_used
