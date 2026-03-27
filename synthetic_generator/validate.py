"""Cluster recovery validation for synthetic MEMBER_GROUP_PLAN_FLAT data.

Proves that the embedded cluster structure is recoverable from the raw data
using standard unsupervised techniques (K-means, silhouette analysis).

Validation runs at TWO levels:
  1. Subscriber-level: filters to MEME_REL='M' (the independent unit of clustering).
     Uses age + tenure as continuous features with one-hot categoricals.
  2. Group-level: aggregates subscriber features per GRGR_CK and clusters groups.
"""

import sys
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.decomposition import PCA


def load_and_prepare(data_path, labels_path, reference_date='2025-01-01'):
    """Load data and engineer features for clustering."""
    df = pd.read_csv(data_path)
    labels = pd.read_csv(labels_path)
    ref = pd.Timestamp(reference_date)

    # Derive continuous features
    df['_age'] = (ref - pd.to_datetime(df['MEME_BIRTH_DT'])).dt.days / 365.25
    df['_tenure'] = (ref - pd.to_datetime(df['MEME_ORIG_EFF_DT'])).dt.days / 30.44

    # Deduplicate to one row per member (plan expansion creates duplicates)
    members = df.drop_duplicates(subset=['MEME_CK'])[
        ['MEME_CK', 'SBSB_CK', 'GRGR_CK', '_age', '_tenure',
         'MEME_SEX', 'MEME_REL', 'MEME_MARITAL_STATUS']
    ].copy()

    # Align labels
    member_labels = labels.drop_duplicates(subset=['MEME_CK']).set_index('MEME_CK')
    members = members.set_index('MEME_CK')
    members = members.join(member_labels[['member_cluster_idx', 'member_cluster',
                                           'group_cluster', 'group_cluster_idx']])
    members = members.dropna(subset=['member_cluster_idx'])
    members['member_cluster_idx'] = members['member_cluster_idx'].astype(int)
    members['group_cluster_idx'] = members['group_cluster_idx'].astype(int)

    return members


def build_subscriber_features(subscribers):
    """Build feature matrix for subscriber-level clustering.

    Uses continuous features (age, tenure) where the Gaussian structure lives,
    plus one-hot categoricals (sex, marital status) for additional separation.
    Continuous features are weighted 2x relative to categoricals since they
    carry the primary cluster signal.
    """
    continuous = subscribers[['_age', '_tenure']].values
    categoricals = pd.get_dummies(
        subscribers[['MEME_SEX', 'MEME_MARITAL_STATUS']],
        dtype=float,
    ).values

    scaler = StandardScaler()
    cont_scaled = scaler.fit_transform(continuous)
    # Weight continuous features more heavily (they carry the Gaussian signal)
    cont_weighted = cont_scaled * 2.0
    X = np.hstack([cont_weighted, categoricals])
    return X, scaler


def build_group_features(members):
    """Build feature matrix for group-level clustering.

    Aggregates subscriber demographics per group to create group-level features:
    mean age, mean tenure, age std, pct_male, pct_married, subscriber count.
    """
    subs = members[members['MEME_REL'] == 'M'].copy()
    group_agg = subs.groupby('GRGR_CK').agg(
        mean_age=('_age', 'mean'),
        std_age=('_age', 'std'),
        mean_tenure=('_tenure', 'mean'),
        pct_male=('MEME_SEX', lambda x: (x == 'M').mean()),
        pct_married=('MEME_MARITAL_STATUS', lambda x: (x == 'M').mean()),
        sub_count=('SBSB_CK', 'nunique'),
        group_cluster_idx=('group_cluster_idx', 'first'),
        group_cluster=('group_cluster', 'first'),
    ).reset_index()
    group_agg['std_age'] = group_agg['std_age'].fillna(0)

    feature_cols = ['mean_age', 'std_age', 'mean_tenure', 'pct_male',
                    'pct_married', 'sub_count']
    scaler = StandardScaler()
    X = scaler.fit_transform(group_agg[feature_cols].values)
    return X, group_agg


def sweep_k(X, true_labels, k_range):
    """Run K-means for each k, compute silhouette and ARI."""
    results = []
    for k in k_range:
        if k >= len(X):
            continue
        km = KMeans(n_clusters=k, n_init=20, random_state=42, max_iter=500)
        pred = km.fit_predict(X)
        sil = silhouette_score(X, pred) if k < len(X) else 0
        ari = adjusted_rand_score(true_labels, pred)
        results.append({'k': k, 'silhouette': round(sil, 4), 'ari': round(ari, 4)})
    return pd.DataFrame(results)


def plot_clusters(X, true_labels, pred_labels, output_path, title_prefix=''):
    """Create PCA scatter plot comparing true vs recovered clusters."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    pca = PCA(n_components=2)
    X2d = pca.fit_transform(X)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for ax, labels, title in [
        (axes[0], true_labels, f'{title_prefix}True Clusters'),
        (axes[1], pred_labels, f'{title_prefix}K-Means Recovered'),
    ]:
        scatter = ax.scatter(X2d[:, 0], X2d[:, 1], c=labels, cmap='tab10',
                             alpha=0.5, s=12)
        ax.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%} var)')
        ax.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%} var)')
        ax.set_title(title)
        ax.legend(*scatter.legend_elements(), title='Cluster', loc='best',
                  fontsize='small')

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    return output_path


def _print_sweep(results, n_true):
    print(f"  {'k':>4}  {'Silhouette':>11}  {'ARI':>7}")
    print(f"  {'─' * 4}  {'─' * 11}  {'─' * 7}")
    for _, r in results.iterrows():
        marker = ' <-- true k' if int(r['k']) == n_true else ''
        print(f"  {int(r['k']):>4}  {r['silhouette']:>11.4f}  {r['ari']:>7.4f}{marker}")


def _quality_label(ari):
    if ari > 0.8: return 'EXCELLENT'
    if ari > 0.6: return 'GOOD'
    if ari > 0.4: return 'MODERATE'
    return 'WEAK'


def validate(data_path, labels_path, reference_date='2025-01-01',
             plot_path=None):
    """Full validation pipeline. Returns results dict."""
    members = load_and_prepare(data_path, labels_path, reference_date)

    # ── Level 1: Subscriber-level clustering ─────────────────────
    subscribers = members[members['MEME_REL'] == 'M'].copy()
    X_sub, _ = build_subscriber_features(subscribers)
    true_sub_labels = subscribers['member_cluster_idx'].values
    n_true_sub = len(np.unique(true_sub_labels))

    print(f"\n{'=' * 60}")
    print(f"  LEVEL 1: Subscriber-Level Cluster Recovery")
    print(f"{'=' * 60}")
    print(f"  Subscribers:       {len(subscribers)}")
    print(f"  True clusters:     {n_true_sub}")
    print(f"  Feature dims:      {X_sub.shape[1]}")
    print(f"  Subscribers per cluster:")
    for idx in sorted(np.unique(true_sub_labels)):
        name = subscribers.loc[subscribers['member_cluster_idx'] == idx, 'member_cluster'].iloc[0]
        count = (true_sub_labels == idx).sum()
        print(f"    [{idx}] {name}: {count}")

    k_lo = max(2, n_true_sub - 2)
    k_hi = n_true_sub + 4
    sub_results = sweep_k(X_sub, true_sub_labels, range(k_lo, k_hi + 1))

    print(f"\n  K-Means Sweep (Subscribers):")
    _print_sweep(sub_results, n_true_sub)

    best_row = sub_results.loc[sub_results['silhouette'].idxmax()]
    true_k_ari = sub_results.loc[sub_results['k'] == n_true_sub, 'ari'].values[0]

    print(f"\n  Best k by silhouette:  {int(best_row['k'])}")
    print(f"  ARI at true k={n_true_sub}:      {true_k_ari:.4f}")
    print(f"  Recovery quality:      {_quality_label(true_k_ari)}")

    if plot_path:
        km = KMeans(n_clusters=n_true_sub, n_init=20, random_state=42)
        pred = km.fit_predict(X_sub)
        sub_plot = plot_path.replace('.png', '_subscribers.png')
        plot_clusters(X_sub, true_sub_labels, pred, sub_plot, 'Subscribers: ')
        print(f"  Plot saved: {sub_plot}")

    # ── Level 2: Group-level clustering ──────────────────────────
    X_grp, group_agg = build_group_features(members)
    true_grp_labels = group_agg['group_cluster_idx'].values
    n_true_grp = len(np.unique(true_grp_labels))
    n_groups = len(group_agg)

    print(f"\n{'=' * 60}")
    print(f"  LEVEL 2: Group-Level Cluster Recovery")
    print(f"{'=' * 60}")
    print(f"  Groups:            {n_groups}")
    print(f"  True group clusters: {n_true_grp}")
    print(f"  Groups per cluster:")
    for idx in sorted(np.unique(true_grp_labels)):
        name = group_agg.loc[group_agg['group_cluster_idx'] == idx, 'group_cluster'].iloc[0]
        count = (true_grp_labels == idx).sum()
        print(f"    [{idx}] {name}: {count}")

    if n_groups > n_true_grp + 1:
        grp_k_lo = max(2, n_true_grp - 1)
        grp_k_hi = min(n_groups - 1, n_true_grp + 3)
        grp_results = sweep_k(X_grp, true_grp_labels, range(grp_k_lo, grp_k_hi + 1))
        print(f"\n  K-Means Sweep (Groups):")
        _print_sweep(grp_results, n_true_grp)
        grp_ari = grp_results.loc[grp_results['k'] == n_true_grp, 'ari'].values[0] if n_true_grp in grp_results['k'].values else 0
        print(f"\n  ARI at true k={n_true_grp}:      {grp_ari:.4f}")
        print(f"  Recovery quality:      {_quality_label(grp_ari)}")

        if plot_path:
            km_g = KMeans(n_clusters=n_true_grp, n_init=20, random_state=42)
            pred_g = km_g.fit_predict(X_grp)
            grp_plot = plot_path.replace('.png', '_groups.png')
            plot_clusters(X_grp, true_grp_labels, pred_g, grp_plot, 'Groups: ')
            print(f"  Plot saved: {grp_plot}")
    else:
        print(f"  (Too few groups for K-means sweep)")

    print()
    return sub_results


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Validate cluster recovery')
    parser.add_argument('--data', required=True, help='Path to generated CSV')
    parser.add_argument('--labels', required=True, help='Path to labels CSV')
    parser.add_argument('--reference-date', default='2025-01-01')
    parser.add_argument('--plot', default=None, help='Output path for PCA plot PNG')
    args = parser.parse_args()
    validate(args.data, args.labels, args.reference_date, args.plot)


if __name__ == '__main__':
    main()
