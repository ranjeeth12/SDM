"""K-means clustering with auto-k selection and label-based mode."""

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.preprocessing import StandardScaler

from .config import CATEGORICAL_FEATURES, CONTINUOUS_FEATURES, CONTINUOUS_WEIGHT, LABEL_CLUSTER_COLUMNS


def build_features(data, continuous_cols=None):
    """Build feature matrix: standardized continuous (weighted 2x) + one-hot categoricals."""
    if continuous_cols is None:
        continuous_cols = CONTINUOUS_FEATURES

    cont = data[continuous_cols].values
    cats = pd.get_dummies(
        data[CATEGORICAL_FEATURES], dtype=float,
    ).values

    scaler = StandardScaler()
    cont_scaled = scaler.fit_transform(cont) * CONTINUOUS_WEIGHT
    X = np.hstack([cont_scaled, cats])
    return X, scaler


def find_optimal_k(X, k_range=range(2, 11)):
    """Silhouette sweep over k_range, return best k."""
    best_k = 2
    best_score = -1
    for k in k_range:
        if k >= X.shape[0]:
            break
        km = KMeans(n_clusters=k, n_init=10, random_state=42)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels)
        if score > best_score:
            best_score = score
            best_k = k
    return best_k


def select_label_column(filters_used):
    """Pick the most specific label column based on which filters were applied."""
    if 'lobd_id' in filters_used:
        return 'product'
    if 'cspd_cat' in filters_used:
        return 'plan'
    if 'sgsg_ck' in filters_used:
        return 'subgroup'
    return 'group'


def discover_clusters(data, labels_df, k=None, use_labels=False, filters_used=None):
    """Run clustering and return (assignments, metrics).

    If use_labels is True and label data is available, use the true cluster
    labels from the labels CSV. Otherwise, run K-means with auto-k or a
    user-specified k.

    If the subset has fewer than 8 members, treat everything as one cluster.
    """
    n = len(data)
    metrics = {}

    # Guard: too few members for meaningful clustering
    if n < 8:
        assignments = np.zeros(n, dtype=int)
        metrics['method'] = 'single_cluster'
        metrics['reason'] = f'n={n} < 8, too few for clustering'
        return assignments, metrics

    X, scaler = build_features(data)

    if use_labels and labels_df is not None and not labels_df.empty:
        level = select_label_column(filters_used or {})
        col = LABEL_CLUSTER_COLUMNS[level]

        # Join labels to data on MEME_CK
        label_lookup = labels_df.drop_duplicates(subset=['MEME_CK'])[['MEME_CK', col]].copy()
        merged = data[['MEME_CK']].merge(label_lookup, on='MEME_CK', how='left')
        assignments = merged[col].fillna(-1).astype(int).values

        # Re-index assignments to 0..n_clusters-1
        unique_labels = sorted(set(assignments[assignments >= 0]))
        label_map = {old: new for new, old in enumerate(unique_labels)}
        assignments = np.array([label_map.get(a, -1) for a in assignments])

        # Validate with K-means
        n_clusters = len(unique_labels)
        if n_clusters >= 2:
            km = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
            km_labels = km.fit_predict(X)
            ari = adjusted_rand_score(assignments, km_labels)
            sil = silhouette_score(X, assignments)
            metrics['ari'] = round(ari, 4)
            metrics['silhouette'] = round(sil, 4)

        metrics['method'] = 'labels'
        metrics['level'] = level
        metrics['n_clusters'] = n_clusters
        return assignments, metrics

    # K-means mode
    if k is None:
        k = find_optimal_k(X)
        metrics['k_selection'] = 'silhouette'
    else:
        metrics['k_selection'] = 'user_specified'

    km = KMeans(n_clusters=k, n_init=10, random_state=42)
    assignments = km.fit_predict(X)

    sil = silhouette_score(X, assignments)
    metrics['method'] = 'kmeans'
    metrics['n_clusters'] = k
    metrics['silhouette'] = round(sil, 4)

    return assignments, metrics
