"""Tests for data_loader module."""

import pandas as pd
import pytest

from cluster_profiler.data_loader import apply_filters


def _make_data():
    """Create a small test DataFrame mimicking generated member data."""
    df = pd.DataFrame({
        'MEME_CK': [1, 1, 2, 3, 4, 5],
        'SBSB_CK': [100, 100, 200, 300, 400, 500],
        'GRGR_CK': [1, 1, 1, 2, 2, 3],
        'SGSG_CK': [10, 10, 10, 20, 20, 30],
        'CSPD_CAT': ['M', 'D', 'M', 'M', 'D', 'M'],
        'LOBD_ID': ['L1', 'L2', 'L1', 'L1', 'L2', 'L3'],
        'MEME_SEX': ['M', 'M', 'F', 'M', 'F', 'F'],
        'MEME_MARITAL_STATUS': ['S', 'S', 'M', 'S', 'M', 'S'],
        'MEME_REL': ['M', 'M', 'M', 'M', 'M', 'M'],
        '_age': [30, 30, 45, 25, 55, 40],
        '_tenure': [12, 12, 24, 6, 36, 18],
    })
    labels = pd.DataFrame({
        'MEME_CK': [1, 2, 3, 4, 5],
        'GRGR_CK': [1, 1, 2, 2, 3],
        'SGSG_CK': [10, 10, 20, 20, 30],
        'CSPD_CAT': ['M', 'M', 'M', 'D', 'M'],
        'LOBD_ID': ['L1', 'L1', 'L1', 'L2', 'L3'],
        'group_cluster_idx': [0, 0, 1, 1, 2],
    })
    return df, labels


def test_filter_by_grgr_ck():
    df, labels = _make_data()
    members, _, _, filters_used = apply_filters(df, labels, grgr_ck=[1])
    assert set(members['GRGR_CK']) == {1}
    assert 'grgr_ck' in filters_used


def test_deduplication():
    df, labels = _make_data()
    # MEME_CK=1 has two rows; after dedup should have one
    members, _, _, _ = apply_filters(df, labels, grgr_ck=[1])
    assert members['MEME_CK'].is_unique


def test_empty_subset_raises():
    df, labels = _make_data()
    with pytest.raises(ValueError, match='No members match'):
        apply_filters(df, labels, grgr_ck=[999])


def test_combined_filters():
    df, labels = _make_data()
    members, _, _, filters_used = apply_filters(df, labels, grgr_ck=[1], cspd_cat=['M'])
    # GRGR_CK=1 and CSPD_CAT='M': MEME_CK 1 and 2
    assert set(members['MEME_CK']) == {1, 2}
    assert filters_used == {'grgr_ck': [1], 'cspd_cat': ['M']}


def test_family_data_not_deduplicated():
    df, labels = _make_data()
    _, _, family_data, _ = apply_filters(df, labels, grgr_ck=[1])
    # Family data should retain both rows for MEME_CK=1
    assert len(family_data[family_data['MEME_CK'] == 1]) == 2


def test_no_filters():
    df, labels = _make_data()
    members, _, _, filters_used = apply_filters(df, labels)
    assert len(members) == 5  # 5 unique MEME_CKs
    assert filters_used == {}
