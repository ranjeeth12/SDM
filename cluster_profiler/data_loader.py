"""Load data from SQL Server and apply hierarchy filters.

Primary source: sdm.member_denorm table in SQL Server.
Fallback: CSV files if SQL Server is unavailable.
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
    get_connection_string,
    SQL_SCHEMA,
)


def _try_sql_connection():
    """Test if SQL Server is reachable."""
    try:
        import pyodbc
        conn = pyodbc.connect(get_connection_string(), timeout=5)
        conn.close()
        return True
    except Exception:
        return False


def load_member_denorm(data_path=None, labels_path=None, reference_date=None):
    """Read member denorm from SQL Server, derive _age and _tenure.

    Falls back to CSV if SQL Server is unavailable.
    """
    reference_date = reference_date or DEFAULT_REFERENCE_DATE
    ref = pd.Timestamp(reference_date)

    # Try SQL Server first
    if _try_sql_connection():
        import pyodbc
        conn = pyodbc.connect(get_connection_string())
        df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.member_denorm", conn)
        conn.close()

        # Labels: generate from denorm (clustering labels derived from member data)
        # If a labels table exists in SQL Server, read it; otherwise build from denorm
        try:
            conn = pyodbc.connect(get_connection_string())
            labels_df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.member_labels", conn)
            conn.close()
        except Exception:
            # Build labels from denorm data
            labels_df = df[['MEME_CK', 'GRGR_CK', 'SGSG_CK', 'CSPD_CAT', 'LOBD_ID']].copy()

    else:
        # Fallback to CSV
        data_path = data_path or MEMBER_DENORM_PATH
        labels_path = labels_path or MEMBER_LABELS_PATH

        if data_path and Path(data_path).exists():
            df = pd.read_csv(data_path)
            labels_df = pd.read_csv(labels_path) if labels_path and Path(labels_path).exists() else \
                         df[['MEME_CK', 'GRGR_CK', 'SGSG_CK', 'CSPD_CAT', 'LOBD_ID']].copy()
        else:
            raise FileNotFoundError(
                "SQL Server unavailable and no CSV fallback found. "
                "Set SDM_SQL_SERVER env var or place CSV in data/source/."
            )

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


def load_provider_denorm(path=None):
    """Load provider denorm. SQL Server first, then CSV fallback."""
    if _try_sql_connection():
        try:
            import pyodbc
            conn = pyodbc.connect(get_connection_string())
            df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.provider_denorm", conn)
            conn.close()
            return df
        except Exception:
            pass

    path = path or PROVIDER_DENORM_PATH
    if path is not None and Path(path).exists():
        return pd.read_csv(path)
    return None


def load_claims_denorm(path=None):
    """Load claims denorm. SQL Server first, then CSV fallback."""
    if _try_sql_connection():
        try:
            import pyodbc
            conn = pyodbc.connect(get_connection_string())
            df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.claims_denorm", conn)
            conn.close()
            return df
        except Exception:
            pass

    path = path or CLAIMS_DENORM_PATH
    if path is not None and Path(path).exists():
        return pd.read_csv(path)
    return None


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

    family_data = filtered.copy()
    members = filtered.drop_duplicates(subset=['MEME_CK']).copy()

    return members, filtered_labels, family_data, filters_used
