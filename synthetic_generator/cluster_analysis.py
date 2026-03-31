"""Per-level cluster analysis for a specific group/subgroup/plan/product combination.

Demonstrates that the embedded cluster structure is recoverable at each hierarchy
level by performing K-means clustering within the appropriate partition and comparing
against ground-truth labels.

Usage:
    python3 -m synthetic_generator.cluster_analysis
    python3 -m synthetic_generator.cluster_analysis --group 1002 --plan-type M
"""

import argparse
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import (
    adjusted_rand_score, silhouette_score, confusion_matrix,
)
from sklearn.decomposition import PCA
from scipy.optimize import linear_sum_assignment


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_data(data_path, labels_path, reference_date='2025-01-01'):
    """Load and join generated data with labels, deriving continuous features."""
    df = pd.read_csv(data_path)
    labels = pd.read_csv(labels_path)
    ref = pd.Timestamp(reference_date)

    # Derive continuous features from the raw data (as a real analyst would)
    df['_age'] = (ref - pd.to_datetime(df['MEME_BIRTH_DT'])).dt.days / 365.25
    df['_tenure'] = (ref - pd.to_datetime(df['MEME_ORIG_EFF_DT'])).dt.days / 30.44

    # Deduplicate main data to one row per member
    members = df.drop_duplicates(subset=['MEME_CK'])[
        ['MEME_CK', 'SBSB_CK', 'GRGR_CK', 'SGSG_CK',
         '_age', '_tenure', 'MEME_SEX', 'MEME_REL', 'MEME_MARITAL_STATUS']
    ].copy()

    # Labels have one row per member per plan — keep that grain
    return members, labels


def build_features(data, continuous_cols=('_age', '_tenure')):
    """Standardized continuous (weighted 2x) + one-hot categoricals."""
    cont = data[list(continuous_cols)].values
    cats = pd.get_dummies(
        data[['MEME_SEX', 'MEME_MARITAL_STATUS']], dtype=float,
    ).values
    scaler = StandardScaler()
    cont_scaled = scaler.fit_transform(cont) * 2.0
    X = np.hstack([cont_scaled, cats])
    return X, scaler


def cluster_and_evaluate(X, true_labels, level_name, k=None):
    """Run K-means, evaluate ARI/silhouette, return predictions."""
    unique_labels = np.unique(true_labels)
    n_true = len(unique_labels)
    k = k or n_true

    if k < 2 or len(X) < k + 2:
        print(f"  {level_name}: Skipped (insufficient data: {len(X)} samples, {k} clusters)")
        return None, None

    km = KMeans(n_clusters=k, n_init=20, random_state=42, max_iter=500)
    pred = km.fit_predict(X)
    ari = adjusted_rand_score(true_labels, pred)
    sil = silhouette_score(X, pred) if k < len(X) else 0.0

    quality = 'EXCELLENT' if ari > 0.8 else 'GOOD' if ari > 0.6 else 'MODERATE' if ari > 0.4 else 'WEAK'
    print(f"  {level_name}:")
    print(f"    Samples: {len(X)}, True k: {n_true}, Used k: {k}")
    print(f"    ARI: {ari:.4f} ({quality})")
    print(f"    Silhouette: {sil:.4f}")

    return pred, {'ari': ari, 'silhouette': sil, 'k': k, 'n': len(X)}


def aligned_confusion_matrix(true_labels, pred_labels):
    """Confusion matrix with predicted clusters aligned to best-matching true clusters."""
    labels_true_u = np.unique(true_labels)
    labels_pred_u = np.unique(pred_labels)
    cm = confusion_matrix(true_labels, pred_labels, labels=labels_true_u)
    # Hungarian algorithm to align columns to rows
    cost = -cm
    row_ind, col_ind = linear_sum_assignment(cost[:, :len(labels_pred_u)])
    return cm[:, col_ind], labels_true_u, labels_pred_u[col_ind]


# ── Plotting ─────────────────────────────────────────────────────────────────

def plot_level_analysis(X, true_labels, pred_labels, true_names, level_name,
                        ax_true, ax_pred, pca=None):
    """PCA scatter: true clusters vs K-means recovered."""
    if pca is None:
        pca = PCA(n_components=2)
        X2d = pca.fit_transform(X)
    else:
        X2d = pca.transform(X)

    for ax, labels, title in [
        (ax_true, true_labels, f'{level_name} — True'),
        (ax_pred, pred_labels, f'{level_name} — K-Means Recovered'),
    ]:
        unique = np.unique(labels)
        cmap = plt.colormaps.get_cmap('tab10').resampled(max(len(unique), 2))
        for i, u in enumerate(unique):
            mask = labels == u
            label = true_names.get(u, f'Cluster {u}') if labels is true_labels else f'Pred {u}'
            ax.scatter(X2d[mask, 0], X2d[mask, 1], c=[cmap(i)], alpha=0.5,
                       s=18, label=label, edgecolors='none')
        ax.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%})')
        ax.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%})')
        ax.set_title(title, fontsize=10)
        ax.legend(fontsize=7, loc='best', markerscale=1.5)

    return pca


def plot_feature_distributions(data, label_col, label_names, axes):
    """Histograms of age and tenure by cluster."""
    for ax, feat, feat_name in [(axes[0], '_age', 'Age'), (axes[1], '_tenure', 'Tenure (months)')]:
        for idx in sorted(data[label_col].unique()):
            subset = data[data[label_col] == idx][feat]
            name = label_names.get(idx, f'Cluster {idx}')
            ax.hist(subset, bins=20, alpha=0.5, label=f'{name} (n={len(subset)})')
        ax.set_xlabel(feat_name)
        ax.set_ylabel('Count')
        ax.legend(fontsize=7)
        ax.set_title(f'{feat_name} by Cluster', fontsize=10)


# ── Main analysis ────────────────────────────────────────────────────────────

def run_analysis(data_path, labels_path, grgr_ck, plan_type, lobd_id,
                 output_path='data/cluster_analysis.png'):
    members, labels_df = load_data(data_path, labels_path)

    # Filter to subscribers only
    subscribers = members[members['MEME_REL'] == 'M'].copy()

    # Join cluster labels onto subscriber features
    sub_labels = labels_df[labels_df['meme_rel'] == 'M'].copy()

    # For plan/product-level labels, filter to the chosen plan type and product
    plan_sub = sub_labels[
        (sub_labels['CSPD_CAT'] == plan_type) & (sub_labels['LOBD_ID'] == lobd_id)
    ].drop_duplicates(subset=['MEME_CK'])

    # Merge features with labels
    merged = subscribers.set_index('MEME_CK').join(
        plan_sub.set_index('MEME_CK')[
            ['group_cluster', 'group_cluster_idx',
             'subgroup_cluster', 'subgroup_cluster_idx',
             'plan_cluster', 'plan_cluster_idx',
             'product_cluster', 'product_cluster_idx']
        ],
        how='inner',
    )

    # Filter to the chosen group
    group_data = merged[merged['GRGR_CK'] == grgr_ck].copy()
    sgsg_ck = group_data['SGSG_CK'].mode().iloc[0]  # pick the dominant subgroup

    n_subs = len(group_data)
    print(f"\n{'=' * 70}")
    print(f"  CLUSTER ANALYSIS")
    print(f"  Group: {grgr_ck}  |  Subgroup: {sgsg_ck}  |  "
          f"Plan: {plan_type}  |  Product: {lobd_id}")
    print(f"  Subscribers in scope: {n_subs}")
    print(f"{'=' * 70}")

    for col in ['group_cluster_idx', 'subgroup_cluster_idx',
                'plan_cluster_idx', 'product_cluster_idx']:
        group_data[col] = group_data[col].astype(int)

    # Build name mappings for pretty labels
    name_maps = {}
    for level in ['group', 'subgroup', 'plan', 'product']:
        idx_col = f'{level}_cluster_idx'
        name_col = f'{level}_cluster'
        name_maps[level] = dict(zip(group_data[idx_col], group_data[name_col]))

    results = {}
    plot_data = []  # (level_name, X, true_labels, pred_labels, name_map, partition_data)

    # ── Level 1: Group-level (within this group) ──────────────────────
    print(f"\n--- Level 1: Group-Level (within group {grgr_ck}) ---")
    X, _ = build_features(group_data)
    true = group_data['group_cluster_idx'].values
    pred, metrics = cluster_and_evaluate(X, true, 'Group-level')
    if pred is not None:
        results['group'] = metrics
        plot_data.append(('Group', X, true, pred, name_maps['group'], group_data))

    # ── Level 2: Subgroup-level (within group × group_cluster) ────────
    print(f"\n--- Level 2: Subgroup-Level (within group {grgr_ck} × group_cluster) ---")
    sg_aris = []
    best_sg_partition = None
    for gc_idx in sorted(group_data['group_cluster_idx'].unique()):
        partition = group_data[group_data['group_cluster_idx'] == gc_idx]
        gc_name = name_maps['group'].get(gc_idx, f'gc_{gc_idx}')
        print(f"\n  Partition: group_cluster={gc_name} ({len(partition)} subs)")
        X_sg, _ = build_features(partition)
        true_sg = partition['subgroup_cluster_idx'].values
        pred_sg, metrics_sg = cluster_and_evaluate(X_sg, true_sg, f'  Subgroup (gc={gc_name})')
        if metrics_sg:
            sg_aris.append((metrics_sg['ari'], len(partition)))
            if best_sg_partition is None or metrics_sg['ari'] > best_sg_partition[0]:
                best_sg_partition = (metrics_sg['ari'], 'Subgroup', X_sg, true_sg,
                                     pred_sg, name_maps['subgroup'], partition)
    if sg_aris:
        wmean = sum(a * s for a, s in sg_aris) / sum(s for _, s in sg_aris)
        results['subgroup'] = {'ari': wmean, 'n_partitions': len(sg_aris)}
        print(f"\n  Weighted mean subgroup ARI: {wmean:.4f}")
        if best_sg_partition:
            plot_data.append(best_sg_partition[1:])

    # ── Level 3: Plan-level (within group × gc × sgc × plan_type) ────
    print(f"\n--- Level 3: Plan-Level (within group {grgr_ck} × gc × sgc, plan={plan_type}) ---")
    plan_aris = []
    best_plan_partition = None
    for (gc_idx, sgc_idx), partition in group_data.groupby(
            ['group_cluster_idx', 'subgroup_cluster_idx']):
        gc_name = name_maps['group'].get(gc_idx, f'gc_{gc_idx}')
        sgc_name = name_maps['subgroup'].get(sgc_idx, f'sgc_{sgc_idx}')
        true_plan = partition['plan_cluster_idx'].values
        n_unique = len(np.unique(true_plan))
        if n_unique < 2 or len(partition) < 8:
            continue
        print(f"\n  Partition: gc={gc_name}, sgc={sgc_name} ({len(partition)} subs)")
        X_pl, _ = build_features(partition)
        pred_pl, metrics_pl = cluster_and_evaluate(
            X_pl, true_plan, f'  Plan (gc={gc_name}, sgc={sgc_name})')
        if metrics_pl:
            plan_aris.append((metrics_pl['ari'], len(partition)))
            if best_plan_partition is None or metrics_pl['ari'] > best_plan_partition[0]:
                best_plan_partition = (metrics_pl['ari'], 'Plan', X_pl, true_plan,
                                       pred_pl, name_maps['plan'], partition)
    if plan_aris:
        wmean = sum(a * s for a, s in plan_aris) / sum(s for _, s in plan_aris)
        results['plan'] = {'ari': wmean, 'n_partitions': len(plan_aris)}
        print(f"\n  Weighted mean plan ARI: {wmean:.4f}")
        if best_plan_partition:
            plot_data.append(best_plan_partition[1:])

    # ── Level 4: Product-level (within group × gc × sgc × plc × LOB) ─
    print(f"\n--- Level 4: Product-Level (within gc × sgc × plc, product={lobd_id}) ---")
    prod_aris = []
    best_prod_partition = None
    for (gc_idx, sgc_idx, plc_idx), partition in group_data.groupby(
            ['group_cluster_idx', 'subgroup_cluster_idx', 'plan_cluster_idx']):
        true_prod = partition['product_cluster_idx'].values
        n_unique = len(np.unique(true_prod))
        if n_unique < 2 or len(partition) < 8:
            continue
        gc_name = name_maps['group'].get(gc_idx, f'gc_{gc_idx}')
        sgc_name = name_maps['subgroup'].get(sgc_idx, f'sgc_{sgc_idx}')
        plc_name = name_maps['plan'].get(plc_idx, f'plc_{plc_idx}')
        print(f"\n  Partition: gc={gc_name}, sgc={sgc_name}, plc={plc_name} ({len(partition)} subs)")
        X_pr, _ = build_features(partition)
        pred_pr, metrics_pr = cluster_and_evaluate(
            X_pr, true_prod, f'  Product (gc={gc_name}, sgc={sgc_name}, plc={plc_name})')
        if metrics_pr:
            prod_aris.append((metrics_pr['ari'], len(partition)))
            if best_prod_partition is None or metrics_pr['ari'] > best_prod_partition[0]:
                best_prod_partition = (metrics_pr['ari'], 'Product', X_pr, true_prod,
                                       pred_pr, name_maps['product'], partition)
    if prod_aris:
        wmean = sum(a * s for a, s in prod_aris) / sum(s for _, s in prod_aris)
        results['product'] = {'ari': wmean, 'n_partitions': len(prod_aris)}
        print(f"\n  Weighted mean product ARI: {wmean:.4f}")
        if best_prod_partition:
            plot_data.append(best_prod_partition[1:])

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY — Group {grgr_ck}, Plan {plan_type}/{lobd_id}")
    print(f"{'=' * 70}")
    for level in ['group', 'subgroup', 'plan', 'product']:
        if level in results:
            r = results[level]
            ari = r['ari']
            quality = 'EXCELLENT' if ari > 0.8 else 'GOOD' if ari > 0.6 else 'MODERATE' if ari > 0.4 else 'WEAK'
            extra = f"  [{r.get('n_partitions', 1)} partitions]" if 'n_partitions' in r else ''
            print(f"  {level:>10}:  ARI = {ari:.4f}  ({quality}){extra}")
        else:
            print(f"  {level:>10}:  (skipped — insufficient data)")

    # ── Generate plots ────────────────────────────────────────────────
    n_levels = len(plot_data)
    if n_levels == 0:
        print("\n  No levels had enough data to plot.")
        return results

    fig, axes = plt.subplots(n_levels, 4, figsize=(22, 5 * n_levels))
    if n_levels == 1:
        axes = axes[np.newaxis, :]

    for row, (level_name, X, true_labels, pred_labels, name_map, partition_data) in enumerate(plot_data):
        pca = plot_level_analysis(
            X, true_labels, pred_labels, name_map, level_name,
            axes[row, 0], axes[row, 1],
        )
        plot_feature_distributions(
            partition_data, f'{level_name.lower()}_cluster_idx', name_map,
            [axes[row, 2], axes[row, 3]],
        )

        # Print confusion matrix for this level
        if pred_labels is not None:
            cm, true_u, pred_u = aligned_confusion_matrix(true_labels, pred_labels)
            print(f"\n  {level_name} confusion matrix (true rows × predicted cols, aligned):")
            header = ''.join(f'{f"Pred {p}":>10}' for p in pred_u)
            print(f"  {'':>20}{header}")
            for i, t in enumerate(true_u):
                tname = name_map.get(t, f'Cluster {t}')
                vals = ''.join(f'{cm[i, j]:>10}' for j in range(cm.shape[1]))
                print(f"  {tname:>20}{vals}")

    fig.suptitle(
        f'Cluster Analysis — Group {grgr_ck}, Plan {plan_type}/{lobd_id}  '
        f'({n_subs} subscribers)',
        fontsize=14, fontweight='bold', y=1.01,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n  Plot saved: {output_path}")

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Per-level cluster analysis for a specific combination')
    parser.add_argument('--data', default='data/MEMBER_GROUP_PLAN_FLAT_generated.csv')
    parser.add_argument('--labels', default='data/MEMBER_GROUP_PLAN_FLAT_generated_labels.csv')
    parser.add_argument('--group', type=int, default=None,
                        help='GRGR_CK to analyze (default: random)')
    parser.add_argument('--plan-type', default=None,
                        help='Plan type: M, D, or V (default: M)')
    parser.add_argument('--output', default='data/cluster_analysis.png')
    args = parser.parse_args()

    # Pick defaults / random
    labels = pd.read_csv(args.labels)
    subs = labels[labels['meme_rel'] == 'M']

    if args.group is None:
        grgr_ck = int(np.random.choice(subs['GRGR_CK'].unique()))
        print(f"Randomly selected group: {grgr_ck}")
    else:
        grgr_ck = args.group

    plan_type = args.plan_type or 'M'
    # Determine LOBD_ID from the labels data for the chosen group + plan type
    group_labels = subs[(subs['GRGR_CK'] == grgr_ck) & (subs['CSPD_CAT'] == plan_type)]
    if len(group_labels) > 0 and 'LOBD_ID' in group_labels.columns:
        lobd_id = group_labels['LOBD_ID'].mode().iloc[0]
    else:
        lobd_id = plan_type  # fallback

    run_analysis(args.data, args.labels, grgr_ck, plan_type, lobd_id, args.output)


if __name__ == '__main__':
    main()
