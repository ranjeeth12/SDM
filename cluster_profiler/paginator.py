"""Paginated data loading with temp file caching.

Fetches data from SQL Server in batches using OFFSET/FETCH.
Each batch is cached as a pickle file on disk for the session.
Temp files are cleaned on session start.

Usage:
    from cluster_profiler.paginator import paginated_view

    # In a Streamlit page:
    paginated_view(
        query_id="profiler_raw_123",
        where="WHERE GRGR_CK IN (1,2,3)",
        params=[1, 2, 3],
        page_size=100,
        key="raw_data_viewer",
    )
"""

import hashlib
import os
import pickle
import shutil
import time
from pathlib import Path

import pandas as pd
import pyodbc
import streamlit as st

from .config import get_connection_string, SQL_SCHEMA, DEFAULT_REFERENCE_DATE

_TABLE = f"{SQL_SCHEMA}.member_denorm"
_TEMP_DIR = Path("data/temp")
_PAGE_SIZE_DEFAULT = 100


def _conn():
    return pyodbc.connect(get_connection_string())


# ── Session temp directory management ────────────────────────────────────────

def _session_dir() -> Path:
    """Get or create a temp directory for the current Streamlit session."""
    # Use session ID if available, otherwise a timestamp-based fallback
    sid = "default"
    if hasattr(st, "session_state"):
        if "_paginator_session_id" not in st.session_state:
            st.session_state["_paginator_session_id"] = hashlib.md5(
                str(time.time()).encode()
            ).hexdigest()[:12]
        sid = st.session_state["_paginator_session_id"]

    session_path = _TEMP_DIR / sid
    session_path.mkdir(parents=True, exist_ok=True)
    return session_path


def cleanup_temp_files():
    """Remove ALL temp directories. Call on session start."""
    if _TEMP_DIR.exists():
        for d in _TEMP_DIR.iterdir():
            if d.is_dir():
                shutil.rmtree(d, ignore_errors=True)
    _TEMP_DIR.mkdir(parents=True, exist_ok=True)


def cleanup_session_files():
    """Remove temp files for the current session only."""
    sdir = _session_dir()
    if sdir.exists():
        shutil.rmtree(sdir, ignore_errors=True)
    sdir.mkdir(parents=True, exist_ok=True)


# ── Batch fetching with cache ────────────────────────────────────────────────

def _query_hash(where: str, params: list) -> str:
    """Build a hash key for a query + params combination."""
    raw = f"{where}|{params}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def get_total_count(where: str = "", params: list = None) -> int:
    """Get total row count for a query. Cached in temp file."""
    qhash = _query_hash(where, params or [])
    count_file = _session_dir() / f"{qhash}_count.txt"

    if count_file.exists():
        return int(count_file.read_text())

    conn = _conn()
    result = pd.read_sql(
        f"SELECT COUNT(*) AS cnt FROM {_TABLE} {where}",
        conn, params=params or None,
    )
    conn.close()
    total = int(result.iloc[0]["cnt"])

    count_file.write_text(str(total))
    return total


def fetch_batch(where: str = "", params: list = None,
                page: int = 0, page_size: int = _PAGE_SIZE_DEFAULT,
                order_by: str = "MEME_CK") -> pd.DataFrame:
    """Fetch a single page of results. Cached as pickle on disk."""
    qhash = _query_hash(where, params or [])
    batch_file = _session_dir() / f"{qhash}_p{page}_s{page_size}.pkl"

    # Return from cache if available
    if batch_file.exists():
        with open(batch_file, "rb") as f:
            return pickle.load(f)

    # Fetch from SQL
    offset = page * page_size
    conn = _conn()
    df = pd.read_sql(f"""
        SELECT * FROM {_TABLE}
        {where}
        ORDER BY {order_by}
        OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY
    """, conn, params=params or None)
    conn.close()

    # Derive _age and _tenure
    ref = pd.Timestamp(DEFAULT_REFERENCE_DATE)
    if 'MEME_BIRTH_DT' in df.columns:
        df['_age'] = (ref - pd.to_datetime(df['MEME_BIRTH_DT'], errors='coerce')).dt.days / 365.25
    if 'MEME_ORIG_EFF_DT' in df.columns:
        df['_tenure'] = (ref - pd.to_datetime(df['MEME_ORIG_EFF_DT'], errors='coerce')).dt.days / 30.44

    # Cache to disk
    with open(batch_file, "wb") as f:
        pickle.dump(df, f, protocol=pickle.HIGHEST_PROTOCOL)

    return df


def fetch_batch_by_ids(meme_cks: list, page: int = 0,
                        page_size: int = _PAGE_SIZE_DEFAULT) -> pd.DataFrame:
    """Fetch a page of specific member IDs. Cached."""
    if not meme_cks:
        return pd.DataFrame()

    safe_ids = ",".join(str(int(x)) for x in meme_cks)
    where = f"WHERE MEME_CK IN ({safe_ids})"
    return fetch_batch(where=where, params=[],
                       page=page, page_size=page_size)


def get_count_by_ids(meme_cks: list) -> int:
    """Get count for specific member IDs."""
    if not meme_cks:
        return 0
    safe_ids = ",".join(str(int(x)) for x in meme_cks)
    where = f"WHERE MEME_CK IN ({safe_ids})"
    return get_total_count(where=where, params=[])


# ── Streamlit paginated display component ────────────────────────────────────

def paginated_view(where: str = "", params: list = None,
                   page_size: int = _PAGE_SIZE_DEFAULT,
                   key: str = "paginator",
                   title: str = None,
                   height: int = 400):
    """Render a paginated dataframe with prev/next controls.

    Parameters
    ----------
    where : str
        SQL WHERE clause (e.g., "WHERE GRGR_CK = ?")
    params : list
        Parameters for the WHERE clause
    page_size : int
        Rows per page (default 100)
    key : str
        Unique Streamlit key for this paginator instance
    title : str
        Optional title shown above the data
    height : int
        DataFrame display height in pixels
    """
    params = params or []

    # Get total count
    total = get_total_count(where, params)
    if total == 0:
        st.info("No data found.")
        return

    total_pages = max(1, (total + page_size - 1) // page_size)

    # Page state
    page_key = f"_page_{key}"
    if page_key not in st.session_state:
        st.session_state[page_key] = 0

    current_page = st.session_state[page_key]

    # Header
    if title:
        st.markdown(f"**{title}** — {total:,} rows")
    else:
        st.caption(f"{total:,} rows total")

    # Navigation controls
    nav1, nav2, nav3, nav4, nav5 = st.columns([1, 1, 2, 1, 1])

    if nav1.button("⏮", key=f"{key}_first", disabled=current_page == 0):
        st.session_state[page_key] = 0
        st.rerun()

    if nav2.button("◀ Prev", key=f"{key}_prev", disabled=current_page == 0):
        st.session_state[page_key] = max(0, current_page - 1)
        st.rerun()

    nav3.markdown(
        f"<div style='text-align:center;padding-top:8px;color:gray;'>"
        f"Page {current_page + 1} of {total_pages} &nbsp;·&nbsp; "
        f"Rows {current_page * page_size + 1}–{min((current_page + 1) * page_size, total)} of {total:,}"
        f"</div>",
        unsafe_allow_html=True,
    )

    if nav4.button("Next ▶", key=f"{key}_next", disabled=current_page >= total_pages - 1):
        st.session_state[page_key] = min(total_pages - 1, current_page + 1)
        st.rerun()

    if nav5.button("⏭", key=f"{key}_last", disabled=current_page >= total_pages - 1):
        st.session_state[page_key] = total_pages - 1
        st.rerun()

    # Fetch and display current page
    batch = fetch_batch(where, params, page=current_page, page_size=page_size)
    st.dataframe(batch, hide_index=True, width="stretch", height=height)

    return batch


def paginated_view_by_ids(meme_cks: list, page_size: int = _PAGE_SIZE_DEFAULT,
                           key: str = "id_paginator", title: str = None,
                           height: int = 400):
    """Render a paginated view for specific member IDs."""
    if not meme_cks:
        st.info("No members to display.")
        return

    safe_ids = ",".join(str(int(x)) for x in meme_cks)
    where = f"WHERE MEME_CK IN ({safe_ids})"
    return paginated_view(
        where=where, params=[],
        page_size=page_size, key=key, title=title, height=height,
    )


def paginated_df(df: pd.DataFrame, page_size: int = _PAGE_SIZE_DEFAULT,
                  key: str = "df_paginator", title: str = None,
                  height: int = 400):
    """Paginate an already-loaded DataFrame (in-memory, no SQL).

    Use this for DataFrames that are already in memory from clustering
    or generation — avoids re-fetching from SQL.
    """
    if df is None or df.empty:
        st.info("No data to display.")
        return

    total = len(df)
    total_pages = max(1, (total + page_size - 1) // page_size)

    page_key = f"_page_{key}"
    if page_key not in st.session_state:
        st.session_state[page_key] = 0

    current_page = st.session_state[page_key]

    if title:
        st.markdown(f"**{title}** — {total:,} rows")
    else:
        st.caption(f"{total:,} rows")

    if total_pages > 1:
        nav1, nav2, nav3, nav4, nav5 = st.columns([1, 1, 2, 1, 1])

        if nav1.button("⏮", key=f"{key}_first", disabled=current_page == 0):
            st.session_state[page_key] = 0
            st.rerun()

        if nav2.button("◀ Prev", key=f"{key}_prev", disabled=current_page == 0):
            st.session_state[page_key] = max(0, current_page - 1)
            st.rerun()

        nav3.markdown(
            f"<div style='text-align:center;padding-top:8px;color:gray;'>"
            f"Page {current_page + 1} of {total_pages}"
            f"</div>",
            unsafe_allow_html=True,
        )

        if nav4.button("Next ▶", key=f"{key}_next", disabled=current_page >= total_pages - 1):
            st.session_state[page_key] = min(total_pages - 1, current_page + 1)
            st.rerun()

        if nav5.button("⏭", key=f"{key}_last", disabled=current_page >= total_pages - 1):
            st.session_state[page_key] = total_pages - 1
            st.rerun()

    start = current_page * page_size
    end = min(start + page_size, total)
    st.dataframe(df.iloc[start:end], hide_index=True, width="stretch", height=height)
