"""Cluster recovery validation for synthetic MEMBER_GROUP_PLAN_FLAT data.

Proves that the embedded per-level cluster structure is recoverable from the raw
data using standard unsupervised techniques (K-means, silhouette analysis).

Validation has two dimensions:
  1. ASSIGNMENT — ARI: did K-means assign points to the correct cluster?
  2. DISTRIBUTION — per-feature: do recovered clusters match the generating
     distributions?  Continuous features are compared with KS tests (two-sample
     Kolmogorov–Smirnov); categorical features with total variation distance.

Validation is HIERARCHICAL — each level is validated within a partition where all
parent-level signals are constant:
  1. Group-level: Cluster within each group → recover group-level clusters.
  2. Subgroup-level: Within each (group, group_cluster) → recover subgroup clusters.
  3. Plan-level: Within each (group, group_cl, sg_cl, CSPD_CAT) → recover plan.
  4. Product-level: Within each (…, plan_cl, LOBD_ID) → recover product.
  5. Composite: Cluster all subscribers globally with group identity as label.
"""

import sys
import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from scipy.stats import ks_2samp
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score, silhouette_score, confusion_matrix
from sklearn.decomposition import PCA


# ── Feature definitions ──────────────────────────────────────────────────

CONTINUOUS_FEATURES = ['_age', '_tenure']
CATEGORICAL_FEATURES = ['MEME_SEX', 'MEME_MARITAL_STATUS']


# ── Data loading ─────────────────────────────────────────────────────────

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
        ['MEME_CK', 'SBSB_CK', 'GRGR_CK', 'SGSG_CK', '_age', '_tenure',
         'MEME_SEX', 'MEME_REL', 'MEME_MARITAL_STATUS', 'CSPD_CAT', 'LOBD_ID']
    ].copy()

    member_labels = labels.drop_duplicates(subset=['MEME_CK'])
    member_labels = member_labels.set_index('MEME_CK')
    plan_labels = labels.copy()

    members = members.set_index('MEME_CK')
    label_cols = [
        'GRGR_CK', 'SGSG_CK',
        'group_cluster', 'group_cluster_idx',
        'subgroup_cluster', 'subgroup_cluster_idx',
        'plan_cluster', 'plan_cluster_idx',
        'product_cluster', 'product_cluster_idx',
    ]
    available_cols = [c for c in label_cols if c in member_labels.columns]
    join_cols = [c for c in available_cols if c not in members.columns]
    members = members.join(member_labels[join_cols])
    members = members.dropna(subset=['group_cluster_idx'])

    for col in members.columns:
        if col.endswith('_idx'):
            members[col] = members[col].astype(int)

    return members, plan_labels


def build_feature_matrix(data, continuous_cols=None):
    """Build feature matrix: standardized continuous (weighted 2x) + one-hot categoricals."""
    if continuous_cols is None:
        continuous_cols = CONTINUOUS_FEATURES

    continuous = data[continuous_cols].values
    categoricals = pd.get_dummies(
        data[CATEGORICAL_FEATURES],
        dtype=float,
    ).values

    scaler = StandardScaler()
    cont_scaled = scaler.fit_transform(continuous)
    cont_weighted = cont_scaled * 2.0
    X = np.hstack([cont_weighted, categoricals])
    return X, scaler


# ── Cluster alignment ────────────────────────────────────────────────────

def _align_predictions(true_labels, pred_labels):
    """Align predicted cluster indices to true labels using Hungarian algorithm.

    Returns pred_aligned array where cluster IDs are remapped to best match
    the true labels.
    """
    unique_true = np.unique(true_labels)
    unique_pred = np.unique(pred_labels)
    cm = confusion_matrix(true_labels, pred_labels,
                          labels=np.union1d(unique_true, unique_pred))
    # Hungarian: maximize overlap (minimize -cm)
    n = max(cm.shape)
    padded = np.zeros((n, n), dtype=cm.dtype)
    padded[:cm.shape[0], :cm.shape[1]] = cm
    row_ind, col_ind = linear_sum_assignment(-padded)

    all_labels = np.union1d(unique_true, unique_pred)
    mapping = {}
    for r, c in zip(row_ind, col_ind):
        if c < len(all_labels) and r < len(all_labels):
            mapping[all_labels[c]] = all_labels[r]

    pred_aligned = np.array([mapping.get(p, p) for p in pred_labels])
    return pred_aligned


# ── Per-feature distribution comparison ──────────────────────────────────

def _compare_continuous(true_vals, recovered_vals):
    """Compare continuous distributions. Returns dict with stats."""
    ks_stat, ks_p = ks_2samp(true_vals, recovered_vals)
    return {
        'true_mean': float(np.mean(true_vals)),
        'true_std': float(np.std(true_vals)),
        'recov_mean': float(np.mean(recovered_vals)),
        'recov_std': float(np.std(recovered_vals)),
        'mean_err': float(abs(np.mean(true_vals) - np.mean(recovered_vals))),
        'ks_stat': float(ks_stat),
        'ks_p': float(ks_p),
    }


def _compare_categorical(true_series, recovered_series):
    """Compare categorical distributions. Returns dict with stats."""
    true_dist = true_series.value_counts(normalize=True).sort_index()
    recov_dist = recovered_series.value_counts(normalize=True).sort_index()
    all_vals = sorted(set(true_dist.index) | set(recov_dist.index))

    true_probs = {v: true_dist.get(v, 0.0) for v in all_vals}
    recov_probs = {v: recov_dist.get(v, 0.0) for v in all_vals}

    # Total variation distance: 0.5 * sum |p - q|
    tvd = 0.5 * sum(abs(true_probs[v] - recov_probs[v]) for v in all_vals)

    return {
        'true_dist': dict(true_probs),
        'recov_dist': dict(recov_probs),
        'tvd': float(tvd),
    }


def compare_cluster_distributions(data, true_labels, pred_aligned,
                                  name_col=None):
    """Per-cluster, per-feature comparison between true and recovered assignments.

    Returns list of per-cluster result dicts.
    """
    cluster_results = []
    for cl_idx in sorted(np.unique(true_labels)):
        true_mask = true_labels == cl_idx
        pred_mask = pred_aligned == cl_idx

        if true_mask.sum() < 2 or pred_mask.sum() < 2:
            continue

        cl_name = None
        if name_col and name_col in data.columns:
            cl_name = data.loc[true_mask, name_col].iloc[0]

        result = {
            'cluster_idx': int(cl_idx),
            'cluster_name': cl_name,
            'true_n': int(true_mask.sum()),
            'recov_n': int(pred_mask.sum()),
            'continuous': {},
            'categorical': {},
        }

        for feat in CONTINUOUS_FEATURES:
            if feat in data.columns:
                result['continuous'][feat] = _compare_continuous(
                    data.loc[true_mask, feat].values,
                    data.loc[pred_mask, feat].values,
                )

        for feat in CATEGORICAL_FEATURES:
            if feat in data.columns:
                result['categorical'][feat] = _compare_categorical(
                    data.loc[true_mask, feat],
                    data.loc[pred_mask, feat],
                )

        cluster_results.append(result)

    return cluster_results


def _print_distribution_report(cluster_results, indent='    '):
    """Print a compact per-cluster distribution comparison report."""
    for cr in cluster_results:
        name = cr['cluster_name'] or f"cluster_{cr['cluster_idx']}"
        print(f"{indent}{name}:  true_n={cr['true_n']}  recov_n={cr['recov_n']}")

        for feat, stats in cr['continuous'].items():
            label = feat.lstrip('_')
            pass_fail = 'PASS' if stats['ks_p'] > 0.05 else 'FAIL'
            print(f"{indent}  {label:>8}: "
                  f"mean {stats['true_mean']:6.1f} -> {stats['recov_mean']:6.1f} "
                  f"(err={stats['mean_err']:.1f})  "
                  f"std {stats['true_std']:5.1f} -> {stats['recov_std']:5.1f}  "
                  f"KS={stats['ks_stat']:.3f} p={stats['ks_p']:.3f} [{pass_fail}]")

        for feat, stats in cr['categorical'].items():
            dist_parts = []
            for v in sorted(stats['true_dist'].keys()):
                t = stats['true_dist'][v]
                r = stats['recov_dist'].get(v, 0)
                dist_parts.append(f"{v}:{t:.0%}->{r:.0%}")
            pass_fail = 'PASS' if stats['tvd'] < 0.10 else 'FAIL'
            print(f"{indent}  {feat:>8}: "
                  f"{' '.join(dist_parts)}  "
                  f"TVD={stats['tvd']:.3f} [{pass_fail}]")


def aggregate_distribution_metrics(all_cluster_results):
    """Aggregate per-feature metrics across all partitions.

    Returns summary dict with mean KS p-values and TVDs per feature.
    """
    cont_metrics = {f: {'ks_stats': [], 'ks_ps': [], 'mean_errs': []}
                    for f in CONTINUOUS_FEATURES}
    cat_metrics = {f: {'tvds': []} for f in CATEGORICAL_FEATURES}

    for cr in all_cluster_results:
        for feat, stats in cr['continuous'].items():
            cont_metrics[feat]['ks_stats'].append(stats['ks_stat'])
            cont_metrics[feat]['ks_ps'].append(stats['ks_p'])
            cont_metrics[feat]['mean_errs'].append(stats['mean_err'])
        for feat, stats in cr['categorical'].items():
            cat_metrics[feat]['tvds'].append(stats['tvd'])

    summary = {}
    for feat, m in cont_metrics.items():
        if m['ks_ps']:
            n_pass = sum(1 for p in m['ks_ps'] if p > 0.05)
            summary[feat] = {
                'type': 'continuous',
                'mean_ks': float(np.mean(m['ks_stats'])),
                'median_ks_p': float(np.median(m['ks_ps'])),
                'mean_err': float(np.mean(m['mean_errs'])),
                'ks_pass_rate': n_pass / len(m['ks_ps']),
                'n_clusters': len(m['ks_ps']),
            }
    for feat, m in cat_metrics.items():
        if m['tvds']:
            n_pass = sum(1 for t in m['tvds'] if t < 0.10)
            summary[feat] = {
                'type': 'categorical',
                'mean_tvd': float(np.mean(m['tvds'])),
                'median_tvd': float(np.median(m['tvds'])),
                'tvd_pass_rate': n_pass / len(m['tvds']),
                'n_clusters': len(m['tvds']),
            }
    return summary


# ── Core K-means + distribution validation ───────────────────────────────

def _validate_partition(data, label_col, name_col=None, min_samples=8):
    """Run K-means on a partition, return (ari, pred_aligned, cluster_results) or None."""
    true_labels = data[label_col].values
    n_true = len(np.unique(true_labels))

    if n_true < 2 or len(data) < max(min_samples, n_true + 2):
        return None

    X, _ = build_feature_matrix(data)
    km = KMeans(n_clusters=n_true, n_init=20, random_state=42, max_iter=500)
    pred = km.fit_predict(X)
    ari = adjusted_rand_score(true_labels, pred)
    pred_aligned = _align_predictions(true_labels, pred)
    cluster_results = compare_cluster_distributions(
        data, true_labels, pred_aligned, name_col=name_col)

    return ari, pred_aligned, cluster_results


# ── Legacy helpers (kept for backward compat) ────────────────────────────

def sweep_k(X, true_labels, k_range):
    """Run K-means for each k, compute silhouette and ARI."""
    results = []
    for k in k_range:
        if k >= len(X) or k < 2:
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


def _quality_label(ari):
    if ari > 0.8: return 'EXCELLENT'
    if ari > 0.6: return 'GOOD'
    if ari > 0.4: return 'MODERATE'
    return 'WEAK'


def _weighted_mean(pairs):
    """Compute size-weighted mean from list of (value, size) tuples."""
    total_weight = sum(s for _, s in pairs)
    if total_weight == 0:
        return 0.0
    return sum(a * s for a, s in pairs) / total_weight


# Keep old name for backward compat
_weighted_mean_ari = _weighted_mean


# ── Main validation pipeline ─────────────────────────────────────────────

def validate(data_path, labels_path, reference_date='2025-01-01',
             plot_path=None):
    """Full hierarchical validation pipeline.

    Returns dict with per-level results including ARI and distribution metrics.
    """
    members, plan_labels = load_and_prepare(data_path, labels_path, reference_date)
    subscribers = members[members['MEME_REL'] == 'M'].copy()

    results = {}
    all_dist_results = []  # collect across all levels for global summary

    # ── Level 1: Group-level (within each group) ───────────────
    print(f"\n{'=' * 70}")
    print(f"  LEVEL 1: Group-Level Cluster Recovery (Within Groups)")
    print(f"{'=' * 70}")
    if 'group_cluster_idx' in subscribers.columns:
        grp_aris = []
        level_dist_results = []
        for grgr_ck in sorted(subscribers['GRGR_CK'].unique()):
            grp_subs = subscribers[subscribers['GRGR_CK'] == grgr_ck].copy()
            result = _validate_partition(
                grp_subs, 'group_cluster_idx', name_col='group_cluster')
            if result is None:
                continue
            ari, pred_aligned, cluster_results = result
            n_unique = len(grp_subs['group_cluster_idx'].unique())
            grp_aris.append(ari)
            level_dist_results.extend(cluster_results)

            print(f"\n  Group {grgr_ck}: ARI = {ari:.4f} ({_quality_label(ari)}) "
                  f"[{len(grp_subs)} subs, {n_unique} cl]")
            _print_distribution_report(cluster_results)

        if grp_aris:
            mean_ari = np.mean(grp_aris)
            results['group'] = {'ari': mean_ari}
            all_dist_results.extend(level_dist_results)
            print(f"\n  Mean group ARI: {mean_ari:.4f} ({_quality_label(mean_ari)}) "
                  f"[{len(grp_aris)} groups]")
        else:
            print(f"  (No groups with multiple group clusters)")

    # ── Level 2: Subgroup-level ──────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  LEVEL 2: Subgroup-Level Recovery")
    print(f"  (within each group x group_cluster partition)")
    print(f"{'=' * 70}")
    if 'subgroup_cluster_idx' in subscribers.columns:
        aris = []
        level_dist_results = []
        for keys, grp in subscribers.groupby(['GRGR_CK', 'group_cluster_idx']):
            result = _validate_partition(
                grp, 'subgroup_cluster_idx', name_col='subgroup_cluster',
                min_samples=10)
            if result is None:
                continue
            ari, pred_aligned, cluster_results = result
            aris.append((ari, len(grp)))
            level_dist_results.extend(cluster_results)

            key_str = '/'.join(str(k) for k in keys)
            n_unique = len(grp['subgroup_cluster_idx'].unique())
            print(f"\n  {key_str}: ARI = {ari:.4f} ({_quality_label(ari)}) "
                  f"[{len(grp)} subs, {n_unique} cl]")
            _print_distribution_report(cluster_results)

        if aris:
            mean_ari = _weighted_mean(aris)
            results['subgroup'] = {'ari': mean_ari}
            all_dist_results.extend(level_dist_results)
            print(f"\n  Weighted mean subgroup ARI: {mean_ari:.4f} "
                  f"({_quality_label(mean_ari)}) [{len(aris)} partitions]")
        else:
            print(f"  (Insufficient data for subgroup validation)")

    # ── Level 3: Plan-level ──────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  LEVEL 3: Plan-Level Recovery")
    print(f"  (within group x group_cl x sg_cl x plan_type)")
    print(f"{'=' * 70}")
    if 'plan_cluster_idx' in plan_labels.columns:
        plan_subs = plan_labels[plan_labels['meme_rel'] == 'M'].copy()
        sub_features = subscribers[['GRGR_CK', 'SGSG_CK', '_age', '_tenure',
                                     'MEME_SEX', 'MEME_MARITAL_STATUS',
                                     'group_cluster_idx',
                                     'subgroup_cluster_idx']].copy()
        merged = plan_subs.set_index('MEME_CK').join(
            sub_features, how='inner', rsuffix='_feat')
        merged = merged.dropna(subset=['plan_cluster_idx'])
        merged['plan_cluster_idx'] = merged['plan_cluster_idx'].astype(int)

        aris = []
        level_dist_results = []
        partition_cols = ['GRGR_CK', 'group_cluster_idx',
                          'subgroup_cluster_idx', 'CSPD_CAT']
        for keys, grp in merged.groupby(partition_cols):
            result = _validate_partition(
                grp, 'plan_cluster_idx', name_col='plan_cluster',
                min_samples=15)
            if result is None:
                continue
            ari, pred_aligned, cluster_results = result
            aris.append((ari, len(grp)))
            level_dist_results.extend(cluster_results)

            key_str = '/'.join(str(k) for k in keys)
            n_unique = len(grp['plan_cluster_idx'].unique())
            print(f"  {key_str}: ARI = {ari:.4f} ({_quality_label(ari)}) "
                  f"[{len(grp)} subs, {n_unique} cl]")

        if aris:
            mean_ari = _weighted_mean(aris)
            results['plan'] = {'ari': mean_ari}
            all_dist_results.extend(level_dist_results)
            print(f"\n  Weighted mean plan ARI: {mean_ari:.4f} "
                  f"({_quality_label(mean_ari)}) [{len(aris)} partitions]")
        else:
            print(f"  (Insufficient data for plan-level validation)")

    # ── Level 4: Product-level ───────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  LEVEL 4: Product-Level Recovery")
    print(f"  (within group x group_cl x sg_cl x plan_cl x LOBD_ID)")
    print(f"{'=' * 70}")
    if 'product_cluster_idx' in plan_labels.columns:
        plan_subs = plan_labels[plan_labels['meme_rel'] == 'M'].copy()
        sub_features = subscribers[['GRGR_CK', 'SGSG_CK', '_age', '_tenure',
                                     'MEME_SEX', 'MEME_MARITAL_STATUS',
                                     'group_cluster_idx',
                                     'subgroup_cluster_idx']].copy()
        merged = plan_subs.set_index('MEME_CK').join(
            sub_features, how='inner', rsuffix='_feat')
        merged = merged.dropna(subset=['product_cluster_idx'])
        merged['product_cluster_idx'] = merged['product_cluster_idx'].astype(int)
        merged['plan_cluster_idx'] = merged['plan_cluster_idx'].astype(int)

        aris = []
        level_dist_results = []
        partition_cols = ['GRGR_CK', 'group_cluster_idx',
                          'subgroup_cluster_idx', 'plan_cluster_idx', 'LOBD_ID']
        for keys, grp in merged.groupby(partition_cols):
            result = _validate_partition(
                grp, 'product_cluster_idx', name_col='product_cluster',
                min_samples=8)
            if result is None:
                continue
            ari, pred_aligned, cluster_results = result
            aris.append((ari, len(grp)))
            level_dist_results.extend(cluster_results)

            key_str = '/'.join(str(k) for k in keys)
            n_unique = len(grp['product_cluster_idx'].unique())
            print(f"  {key_str}: ARI = {ari:.4f} ({_quality_label(ari)}) "
                  f"[{len(grp)} subs, {n_unique} cl]")

        if aris:
            mean_ari = _weighted_mean(aris)
            results['product'] = {'ari': mean_ari}
            all_dist_results.extend(level_dist_results)
            print(f"\n  Weighted mean product ARI: {mean_ari:.4f} "
                  f"({_quality_label(mean_ari)}) [{len(aris)} partitions]")
        else:
            print(f"  (Insufficient data for product-level validation)")

    # ── Level 5: Composite (global by group identity) ──────────
    print(f"\n{'=' * 70}")
    print(f"  LEVEL 5: Composite (Global — recover group identity)")
    print(f"{'=' * 70}")
    n_groups = subscribers['GRGR_CK'].nunique()
    if n_groups >= 2:
        result = _validate_partition(subscribers, 'GRGR_CK')
        if result is not None:
            ari, pred_aligned, _ = result
            results['composite'] = {'ari': ari}
            print(f"  Groups: {n_groups}")
            print(f"  ARI = {ari:.4f} ({_quality_label(ari)})")

            if plot_path:
                X, _ = build_feature_matrix(subscribers)
                km = KMeans(n_clusters=n_groups, n_init=20, random_state=42)
                pred = km.fit_predict(X)
                pf = plot_path.replace('.png', '_composite.png')
                plot_clusters(X, subscribers['GRGR_CK'].values, pred, pf,
                              'Composite: ')
                print(f"  Plot saved: {pf}")

    # ── Distribution Summary ──────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  DISTRIBUTION RECOVERY (all levels, all partitions)")
    print(f"{'=' * 70}")
    if all_dist_results:
        dist_summary = aggregate_distribution_metrics(all_dist_results)
        for feat, stats in sorted(dist_summary.items()):
            if stats['type'] == 'continuous':
                label = feat.lstrip('_')
                pass_rate = stats['ks_pass_rate']
                grade = ('EXCELLENT' if pass_rate > 0.90 else
                         'GOOD' if pass_rate > 0.75 else
                         'MODERATE' if pass_rate > 0.50 else 'WEAK')
                print(f"  {label:>18}:  KS pass rate = {pass_rate:.0%} "
                      f"({stats['n_clusters']} clusters)  "
                      f"mean |err| = {stats['mean_err']:.1f}  "
                      f"median KS p = {stats['median_ks_p']:.3f}  [{grade}]")
            else:
                pass_rate = stats['tvd_pass_rate']
                grade = ('EXCELLENT' if pass_rate > 0.90 else
                         'GOOD' if pass_rate > 0.75 else
                         'MODERATE' if pass_rate > 0.50 else 'WEAK')
                print(f"  {feat:>18}:  TVD pass rate = {pass_rate:.0%} "
                      f"({stats['n_clusters']} clusters)  "
                      f"mean TVD = {stats['mean_tvd']:.3f}  "
                      f"median TVD = {stats['median_tvd']:.3f}  [{grade}]")

        results['distribution'] = dist_summary

    # ── Assignment Summary ────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  ASSIGNMENT RECOVERY (ARI)")
    print(f"{'=' * 70}")
    for level in ['group', 'subgroup', 'plan', 'product', 'composite']:
        if level in results and 'ari' in results[level]:
            ari = results[level]['ari']
            print(f"  {level:>12}:  ARI = {ari:.4f}  ({_quality_label(ari)})")
        else:
            print(f"  {level:>12}:  (skipped)")
    print()

    return results


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
