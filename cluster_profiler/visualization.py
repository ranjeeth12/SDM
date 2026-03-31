"""PCA scatter and demographic distribution plots."""

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

from .config import CATEGORICAL_FEATURES


def plot_profiles(X, assignments, data, output_path):
    """Generate a 2x2 figure with cluster visualizations.

    Panels:
      1. PCA scatter colored by cluster
      2. Age histogram by cluster
      3. Tenure histogram by cluster
      4. Categorical bar charts by cluster
    """
    unique_clusters = sorted(set(assignments))
    n_clusters = len(unique_clusters)
    colors = plt.cm.tab10(np.linspace(0, 1, max(n_clusters, 1)))

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # 1. PCA scatter
    ax = axes[0, 0]
    if X.shape[1] >= 2:
        pca = PCA(n_components=2)
        coords = pca.fit_transform(X)
        for i, cid in enumerate(unique_clusters):
            mask = np.array(assignments) == cid
            ax.scatter(
                coords[mask, 0], coords[mask, 1],
                c=[colors[i]], label=f'Cluster {cid}', alpha=0.6, s=20,
            )
        ax.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)')
        ax.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)')
    ax.set_title('PCA Scatter')
    ax.legend(fontsize=8)

    # 2. Age histogram
    ax = axes[0, 1]
    for i, cid in enumerate(unique_clusters):
        mask = np.array(assignments) == cid
        vals = data.iloc[mask]['_age'].dropna()
        if len(vals) > 0:
            ax.hist(vals, bins=20, alpha=0.5, label=f'Cluster {cid}', color=colors[i])
    ax.set_title('Age Distribution')
    ax.set_xlabel('Age (years)')
    ax.set_ylabel('Count')
    ax.legend(fontsize=8)

    # 3. Tenure histogram
    ax = axes[1, 0]
    for i, cid in enumerate(unique_clusters):
        mask = np.array(assignments) == cid
        vals = data.iloc[mask]['_tenure'].dropna()
        if len(vals) > 0:
            ax.hist(vals, bins=20, alpha=0.5, label=f'Cluster {cid}', color=colors[i])
    ax.set_title('Tenure Distribution')
    ax.set_xlabel('Tenure (months)')
    ax.set_ylabel('Count')
    ax.legend(fontsize=8)

    # 4. Categorical bar chart (first categorical feature)
    ax = axes[1, 1]
    cat_col = CATEGORICAL_FEATURES[0]  # MEME_SEX
    if cat_col in data.columns:
        categories = sorted(data[cat_col].dropna().unique())
        x_pos = np.arange(len(categories))
        width = 0.8 / max(n_clusters, 1)
        for i, cid in enumerate(unique_clusters):
            mask = np.array(assignments) == cid
            subset = data.iloc[mask]
            counts = subset[cat_col].value_counts()
            heights = [counts.get(c, 0) for c in categories]
            ax.bar(x_pos + i * width, heights, width, label=f'Cluster {cid}', color=colors[i])
        ax.set_xticks(x_pos + width * (n_clusters - 1) / 2)
        ax.set_xticklabels(categories)
    ax.set_title(f'{cat_col} by Cluster')
    ax.set_ylabel('Count')
    ax.legend(fontsize=8)

    fig.suptitle('Cluster Profiles', fontsize=14, fontweight='bold')
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f'Plot saved to {output_path}')
