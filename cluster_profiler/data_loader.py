"""Load data from SQL Server with filter pushdown.

SQL Server only. No full table loads — queries are filtered at the SQL level.
Memory usage: only the rows you need, when you need them.
"""

import pandas as pd
import pyodbc

from .config import (
    FILTER_COLUMNS,
    DEFAULT_REFERENCE_DATE,
    get_connection_string,
    SQL_SCHEMA,
)

_TABLE = f"{SQL_SCHEMA}.member_denorm"


def _conn():
    return pyodbc.connect(get_connection_string())


# ── Overview metrics (lightweight, no DataFrame) ─────────────────────────────

def get_overview_metrics():
    """Return dict of aggregate counts. No DataFrame created."""
    conn = _conn()
    row = pd.read_sql(f"""
        SELECT
            COUNT(DISTINCT MEME_CK)  AS n_members,
            COUNT(DISTINCT GRGR_CK)  AS n_groups,
            COUNT(DISTINCT SGSG_CK)  AS n_subgroups,
            COUNT(DISTINCT LOBD_ID)  AS n_lobs
        FROM {_TABLE}
    """, conn).iloc[0]
    conn.close()
    return row.to_dict()


# ── Cascading filter options (small queries, no full table) ──────────────────

def get_groups():
    """Return DataFrame of distinct GRGR_CK + GRGR_NAME."""
    conn = _conn()
    df = pd.read_sql(f"""
        SELECT DISTINCT GRGR_CK, GRGR_NAME
        FROM {_TABLE}
        ORDER BY GRGR_NAME
    """, conn)
    conn.close()
    return df


def get_subgroups(grgr_cks=None):
    """Return distinct SGSG_CK + SGSG_NAME, filtered by group."""
    conn = _conn()
    if grgr_cks:
        ids = ",".join(str(int(x)) for x in grgr_cks)
        df = pd.read_sql(f"""
            SELECT DISTINCT SGSG_CK, SGSG_NAME
            FROM {_TABLE}
            WHERE GRGR_CK IN ({ids})
            ORDER BY SGSG_NAME
        """, conn)
    else:
        df = pd.read_sql(f"""
            SELECT DISTINCT SGSG_CK, SGSG_NAME
            FROM {_TABLE}
            ORDER BY SGSG_NAME
        """, conn)
    conn.close()
    return df


def get_plan_categories(grgr_cks=None, sgsg_cks=None):
    """Return distinct CSPD_CAT + CSPD_CAT_DESC, filtered by group/subgroup."""
    conn = _conn()
    where_parts = []
    if grgr_cks:
        ids = ",".join(str(int(x)) for x in grgr_cks)
        where_parts.append(f"GRGR_CK IN ({ids})")
    if sgsg_cks:
        ids = ",".join(str(int(x)) for x in sgsg_cks)
        where_parts.append(f"SGSG_CK IN ({ids})")

    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    df = pd.read_sql(f"""
        SELECT DISTINCT CSPD_CAT, CSPD_CAT_DESC
        FROM {_TABLE}
        {where}
        ORDER BY CSPD_CAT_DESC
    """, conn)
    conn.close()
    return df


def get_lobs(grgr_cks=None, sgsg_cks=None, cspd_cats=None):
    """Return distinct LOBD_ID + PLDS_DESC, filtered by hierarchy."""
    conn = _conn()
    where_parts = []
    if grgr_cks:
        ids = ",".join(str(int(x)) for x in grgr_cks)
        where_parts.append(f"GRGR_CK IN ({ids})")
    if sgsg_cks:
        ids = ",".join(str(int(x)) for x in sgsg_cks)
        where_parts.append(f"SGSG_CK IN ({ids})")
    if cspd_cats:
        vals = ",".join(f"'{str(x)}'" for x in cspd_cats)
        where_parts.append(f"CSPD_CAT IN ({vals})")

    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    df = pd.read_sql(f"""
        SELECT DISTINCT LOBD_ID, PLDS_DESC
        FROM {_TABLE}
        {where}
        ORDER BY PLDS_DESC
    """, conn)
    conn.close()
    return df


# ── Filtered member loading (only matching rows) ─────────────────────────────

def _build_where(grgr_ck=None, sgsg_ck=None, cspd_cat=None, lobd_id=None):
    """Build WHERE clause with inlined values (no parameter markers)."""
    parts = []
    if grgr_ck:
        ids = ",".join(str(int(x)) for x in grgr_ck)
        parts.append(f"GRGR_CK IN ({ids})")
    if sgsg_ck:
        ids = ",".join(str(int(x)) for x in sgsg_ck)
        parts.append(f"SGSG_CK IN ({ids})")
    if cspd_cat:
        vals = ",".join(f"'{str(x)}'" for x in cspd_cat)
        parts.append(f"CSPD_CAT IN ({vals})")
    if lobd_id:
        vals = ",".join(f"'{str(x)}'" for x in lobd_id)
        parts.append(f"LOBD_ID IN ({vals})")
    where = f"WHERE {' AND '.join(parts)}" if parts else ""
    return where


def load_filtered_members(grgr_ck=None, sgsg_ck=None, cspd_cat=None, lobd_id=None,
                           reference_date=None):
    """Load only members matching the given filters from SQL Server.

    Returns (members_df, labels_df, family_data, filters_used).
    Derives _age and _tenure. Deduplicates members by MEME_CK.
    """
    reference_date = reference_date or DEFAULT_REFERENCE_DATE
    ref = pd.Timestamp(reference_date)

    where = _build_where(grgr_ck, sgsg_ck, cspd_cat, lobd_id)

    conn = _conn()
    df = pd.read_sql(f"SELECT * FROM {_TABLE} {where}", conn)
    conn.close()

    if df.empty:
        filters_used = {k: v for k, v in [
            ("grgr_ck", grgr_ck), ("sgsg_ck", sgsg_ck),
            ("cspd_cat", cspd_cat), ("lobd_id", lobd_id),
        ] if v is not None}
        raise ValueError(f"No members match the applied filters: {filters_used}")

    # Derive age and tenure
    if 'MEME_BIRTH_DT' in df.columns:
        df['_age'] = (ref - pd.to_datetime(df['MEME_BIRTH_DT'], errors='coerce')).dt.days / 365.25
    else:
        df['_age'] = 0

    if 'MEME_ORIG_EFF_DT' in df.columns:
        df['_tenure'] = (ref - pd.to_datetime(df['MEME_ORIG_EFF_DT'], errors='coerce')).dt.days / 30.44
    else:
        df['_tenure'] = 0

    # Build filters_used
    filters_used = {}
    if grgr_ck:
        filters_used['grgr_ck'] = grgr_ck
    if sgsg_ck:
        filters_used['sgsg_ck'] = sgsg_ck
    if cspd_cat:
        filters_used['cspd_cat'] = cspd_cat
    if lobd_id:
        filters_used['lobd_id'] = lobd_id

    # Build labels
    label_cols = ['MEME_CK', 'GRGR_CK', 'SGSG_CK', 'CSPD_CAT', 'LOBD_ID']
    labels_df = df[[c for c in label_cols if c in df.columns]].copy()

    family_data = df.copy()
    members = df.drop_duplicates(subset=['MEME_CK']).copy()

    return members, labels_df, family_data, filters_used


def load_members_by_ids(meme_cks, reference_date=None):
    """Load specific members by MEME_CK list."""
    if not meme_cks:
        return pd.DataFrame()

    reference_date = reference_date or DEFAULT_REFERENCE_DATE
    ref = pd.Timestamp(reference_date)

    # Inline IDs directly (cast to int for safety) — avoids pyodbc param count issues
    safe_ids = ",".join(str(int(x)) for x in meme_cks)
    conn = _conn()
    df = pd.read_sql(f"SELECT * FROM {_TABLE} WHERE MEME_CK IN ({safe_ids})", conn)
    conn.close()

    if 'MEME_BIRTH_DT' in df.columns:
        df['_age'] = (ref - pd.to_datetime(df['MEME_BIRTH_DT'], errors='coerce')).dt.days / 365.25
    if 'MEME_ORIG_EFF_DT' in df.columns:
        df['_tenure'] = (ref - pd.to_datetime(df['MEME_ORIG_EFF_DT'], errors='coerce')).dt.days / 30.44

    return df


# ── Backward-compatible functions ────────────────────────────────────────────

def apply_filters(df, labels_df, grgr_ck=None, sgsg_ck=None, cspd_cat=None, lobd_id=None):
    """Apply filters to an already-loaded DataFrame. For backward compat."""
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
        raise ValueError(f"No members match the applied filters: {filters_used}")

    family_data = filtered.copy()
    members = filtered.drop_duplicates(subset=['MEME_CK']).copy()

    return members, filtered_labels, family_data, filters_used


def load_data(data_path=None, labels_path=None, reference_date=None):
    """DEPRECATED — loads full table. Use load_filtered_members instead."""
    return load_filtered_members(reference_date=reference_date)


def load_member_denorm(reference_date=None):
    """DEPRECATED — loads full table. Use load_filtered_members instead."""
    return load_filtered_members(reference_date=reference_date)


def load_provider_denorm(path=None):
    """Load provider denorm from SQL Server. Returns DataFrame or None."""
    try:
        conn = _conn()
        df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.provider_denorm", conn)
        conn.close()
        return df
    except Exception:
        return None


def load_claims_denorm(path=None):
    """Load claims denorm from SQL Server. Returns DataFrame or None."""
    try:
        conn = _conn()
        df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.claims_denorm", conn)
        conn.close()
        return df
    except Exception:
        return None
