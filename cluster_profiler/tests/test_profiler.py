"""Tests for profiler module."""

import numpy as np
import pandas as pd
import pytest

from cluster_profiler.profiler import (
    build_subset_summary,
    profile_all_clusters,
    profile_cluster,
)


def _make_data():
    """Create test data with known demographics and family structure."""
    members = pd.DataFrame({
        'MEME_CK': [1, 2, 3, 4, 5, 6],
        'SBSB_CK': [100, 200, 300, 400, 500, 600],
        'GRGR_CK': [1, 1, 1, 1, 1, 1],
        'SGSG_CK': [10, 10, 10, 10, 10, 10],
        'CSPD_CAT': ['M', 'M', 'M', 'M', 'M', 'M'],
        'MEME_SEX': ['M', 'M', 'F', 'F', 'M', 'F'],
        'MEME_MARITAL_STATUS': ['S', 'M', 'S', 'M', 'S', 'M'],
        'MEME_REL': ['M', 'M', 'M', 'M', 'M', 'M'],
        '_age': [30.0, 40.0, 50.0, 60.0, 35.0, 45.0],
        '_tenure': [12.0, 24.0, 36.0, 48.0, 18.0, 30.0],
        'GRGR_NAME': ['Group A'] * 6,
        'SGSG_NAME': ['Sub 1'] * 6,
        'CSPD_CAT_DESC': ['Medical'] * 6,
        'PLDS_DESC': ['Plan X'] * 6,
        'PDDS_DESC': ['Product Y'] * 6,
    })
    # Family data: subscriber 100 has a spouse and 2 dependents
    family = pd.DataFrame({
        'MEME_CK': [1, 7, 8, 9, 2, 3, 4, 5, 6],
        'SBSB_CK': [100, 100, 100, 100, 200, 300, 400, 500, 600],
        'MEME_REL': ['M', 'S', 'D', 'D', 'M', 'M', 'M', 'M', 'M'],
        'GRGR_CK': [1] * 9,
        'MEME_SEX': ['M', 'F', 'M', 'F', 'M', 'F', 'F', 'M', 'F'],
    })
    return members, family


def test_profile_dict_completeness():
    members, family = _make_data()
    mask = np.array([True] * 3 + [False] * 3)
    profile = profile_cluster(members, family, mask, cluster_id=0, total_n=6)

    assert profile['cluster_id'] == 0
    assert profile['size'] == 3
    assert 'pct_of_subset' in profile
    assert '_age' in profile['continuous']
    assert '_tenure' in profile['continuous']
    assert 'MEME_SEX' in profile['categorical']
    assert 'MEME_MARITAL_STATUS' in profile['categorical']
    assert 'avg_dependents' in profile['family']
    assert 'spouse_rate' in profile['family']
    assert 'GRGR_NAME' in profile['descriptions']


def test_continuous_stats_consistency():
    members, family = _make_data()
    mask = np.array([True] * 6)
    profile = profile_cluster(members, family, mask, cluster_id=0, total_n=6)

    age_stats = profile['continuous']['_age']
    assert age_stats['min'] <= age_stats['mean'] <= age_stats['max']
    assert age_stats['min'] <= age_stats['median'] <= age_stats['max']
    assert age_stats['std'] >= 0


def test_pct_sums_to_one():
    members, family = _make_data()
    assignments = np.array([0, 0, 0, 1, 1, 1])
    profiles = profile_all_clusters(members, family, assignments)

    total_pct = sum(p['pct_of_subset'] for p in profiles)
    assert abs(total_pct - 1.0) < 0.01


def test_categorical_pct_sums():
    members, family = _make_data()
    mask = np.array([True] * 6)
    profile = profile_cluster(members, family, mask, cluster_id=0, total_n=6)

    for feat, info in profile['categorical'].items():
        pct_sum = sum(info['pct'].values())
        assert abs(pct_sum - 1.0) < 0.01, f'{feat} pct does not sum to 1'


def test_family_computation():
    members, family = _make_data()
    # Cluster 0 = first 3 members (SBSB_CK: 100, 200, 300)
    # SBSB_CK 100 has 2 dependents and 1 spouse
    mask = np.array([True, True, True, False, False, False])
    profile = profile_cluster(members, family, mask, cluster_id=0, total_n=6)

    fam = profile['family']
    # 2 dependents across 3 subscribers = 0.67
    assert abs(fam['avg_dependents'] - 0.67) < 0.01
    # 1 out of 3 subscribers has a spouse = 0.3333
    assert abs(fam['spouse_rate'] - 0.3333) < 0.01


def test_profile_all_clusters_count():
    members, family = _make_data()
    assignments = np.array([0, 0, 1, 1, 2, 2])
    profiles = profile_all_clusters(members, family, assignments)
    assert len(profiles) == 3
    assert [p['cluster_id'] for p in profiles] == [0, 1, 2]


def test_build_subset_summary():
    members, _ = _make_data()
    summary = build_subset_summary(members, {'grgr_ck': [1]})
    assert summary['total_members'] == 6
    assert summary['filters'] == {'grgr_ck': [1]}
    assert 'GRGR_NAME' in summary
