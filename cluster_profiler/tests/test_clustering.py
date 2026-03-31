"""Tests for clustering module."""

import numpy as np
import pandas as pd
import pytest

from cluster_profiler.clustering import (
    build_features,
    discover_clusters,
    find_optimal_k,
    select_label_column,
)


def _make_members(n=50):
    """Generate a synthetic member DataFrame for testing."""
    rng = np.random.RandomState(42)
    return pd.DataFrame({
        'MEME_CK': range(n),
        'SBSB_CK': range(n),
        'GRGR_CK': rng.choice([1, 2], n),
        'SGSG_CK': rng.choice([10, 20], n),
        'CSPD_CAT': rng.choice(['M', 'D'], n),
        'LOBD_ID': rng.choice(['L1', 'L2'], n),
        'MEME_SEX': rng.choice(['M', 'F'], n),
        'MEME_MARITAL_STATUS': rng.choice(['S', 'M', 'D'], n),
        'MEME_REL': ['M'] * n,
        '_age': rng.normal(40, 15, n).clip(18, 90),
        '_tenure': rng.normal(24, 12, n).clip(1, 120),
    })


def _make_labels(members):
    return pd.DataFrame({
        'MEME_CK': members['MEME_CK'],
        'GRGR_CK': members['GRGR_CK'],
        'group_cluster_idx': np.where(members['_age'] > 40, 0, 1),
        'subgroup_cluster_idx': np.where(members['_age'] > 40, 0, 1),
        'plan_cluster_idx': np.where(members['_age'] > 40, 0, 1),
        'product_cluster_idx': np.where(members['_age'] > 40, 0, 1),
    })


def test_build_features_shape():
    members = _make_members()
    X, scaler = build_features(members)
    # 2 continuous + one-hot categoricals (SEX has 2 vals, MARITAL has 3 vals = 5 cat cols)
    assert X.shape[0] == len(members)
    assert X.shape[1] >= 4  # at least 2 continuous + 2 categorical dummies


def test_build_features_continuous_weighted():
    members = _make_members()
    X, scaler = build_features(members)
    # First two columns should be scaled & weighted (mean ~0, larger range than [-1,1])
    cont_part = X[:, :2]
    assert abs(cont_part.mean()) < 1.0  # roughly centered


def test_find_optimal_k_returns_valid():
    members = _make_members(100)
    X, _ = build_features(members)
    k = find_optimal_k(X, k_range=range(2, 6))
    assert 2 <= k <= 5


def test_select_label_column_hierarchy():
    assert select_label_column({'lobd_id': ['L1']}) == 'product'
    assert select_label_column({'cspd_cat': ['M']}) == 'plan'
    assert select_label_column({'sgsg_ck': [10]}) == 'subgroup'
    assert select_label_column({'grgr_ck': [1]}) == 'group'
    assert select_label_column({}) == 'group'


def test_select_label_most_specific_wins():
    # lobd_id is most specific even when other filters present
    assert select_label_column({'grgr_ck': [1], 'lobd_id': ['L1']}) == 'product'
    assert select_label_column({'sgsg_ck': [10], 'cspd_cat': ['M']}) == 'plan'


def test_small_subset_single_cluster():
    members = _make_members(5)
    labels = _make_labels(members)
    assignments, metrics = discover_clusters(members, labels)
    assert len(assignments) == 5
    assert np.all(assignments == 0)
    assert metrics['method'] == 'single_cluster'


def test_discover_clusters_kmeans():
    members = _make_members(50)
    labels = _make_labels(members)
    assignments, metrics = discover_clusters(members, labels)
    assert len(assignments) == 50
    assert metrics['method'] == 'kmeans'
    assert 'silhouette' in metrics
    assert len(set(assignments)) >= 2


def test_discover_clusters_with_labels():
    members = _make_members(50)
    labels = _make_labels(members)
    assignments, metrics = discover_clusters(
        members, labels, use_labels=True, filters_used={'grgr_ck': [1]},
    )
    assert metrics['method'] == 'labels'
    assert metrics['level'] == 'group'
    assert len(set(assignments)) == 2


def test_discover_clusters_user_k():
    members = _make_members(50)
    labels = _make_labels(members)
    assignments, metrics = discover_clusters(members, labels, k=3)
    assert metrics['n_clusters'] == 3
    assert metrics['k_selection'] == 'user_specified'
    assert len(set(assignments)) == 3
