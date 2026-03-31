"""Per-cluster profile computation."""

import numpy as np

from .config import CATEGORICAL_FEATURES, CONTINUOUS_FEATURES, DESCRIPTION_COLUMNS


def profile_cluster(data, family_data, mask, cluster_id, total_n):
    """Compute demographic profile for a single cluster.

    Args:
        data: Deduplicated member DataFrame.
        family_data: Full (non-deduplicated) DataFrame for family analysis.
        mask: Boolean array selecting members in this cluster.
        cluster_id: Integer cluster label.
        total_n: Total members in subset (for pct calculation).
    """
    subset = data[mask]
    size = len(subset)

    profile = {
        'cluster_id': int(cluster_id),
        'size': size,
        'pct_of_subset': round(size / total_n, 4) if total_n > 0 else 0.0,
    }

    # Continuous feature stats
    continuous = {}
    for col in CONTINUOUS_FEATURES:
        vals = subset[col].dropna()
        if len(vals) > 0:
            continuous[col] = {
                'mean': round(float(vals.mean()), 2),
                'std': round(float(vals.std()), 2),
                'min': round(float(vals.min()), 2),
                'max': round(float(vals.max()), 2),
                'median': round(float(vals.median()), 2),
            }
        else:
            continuous[col] = {'mean': 0, 'std': 0, 'min': 0, 'max': 0, 'median': 0}
    profile['continuous'] = continuous

    # Categorical distributions
    categorical = {}
    for col in CATEGORICAL_FEATURES:
        counts = subset[col].value_counts().to_dict()
        total = sum(counts.values())
        pct = {k: round(v / total, 4) for k, v in counts.items()} if total > 0 else {}
        categorical[col] = {'counts': counts, 'pct': pct}
    profile['categorical'] = categorical

    # Family structure
    cluster_sbsb_cks = subset['SBSB_CK'].unique()
    family_subset = family_data[family_data['SBSB_CK'].isin(cluster_sbsb_cks)]
    family = _compute_family_stats(family_subset, cluster_sbsb_cks)
    profile['family'] = family

    # Description columns
    descriptions = {}
    for col in DESCRIPTION_COLUMNS:
        if col in subset.columns:
            descriptions[col] = sorted(subset[col].dropna().unique().tolist())
        else:
            descriptions[col] = []
    profile['descriptions'] = descriptions

    return profile


def _compute_family_stats(family_subset, subscriber_cks):
    """Compute average dependents and spouse rate from family data."""
    n_subscribers = len(subscriber_cks)
    if n_subscribers == 0:
        return {'avg_dependents': 0.0, 'spouse_rate': 0.0}

    if 'MEME_REL' not in family_subset.columns:
        return {'avg_dependents': 0.0, 'spouse_rate': 0.0}

    # Count dependents (MEME_REL='D') per subscriber
    dep_counts = (
        family_subset[family_subset['MEME_REL'] == 'D']
        .groupby('SBSB_CK')['MEME_CK']
        .nunique()
    )
    avg_dependents = float(dep_counts.sum()) / n_subscribers

    # Spouse rate: fraction of subscribers with at least one MEME_REL='S'
    has_spouse = (
        family_subset[family_subset['MEME_REL'] == 'S']['SBSB_CK'].nunique()
    )
    spouse_rate = has_spouse / n_subscribers

    return {
        'avg_dependents': round(avg_dependents, 2),
        'spouse_rate': round(spouse_rate, 4),
    }


def profile_all_clusters(data, family_data, assignments):
    """Compute profiles for all clusters."""
    unique_clusters = sorted(set(assignments))
    total_n = len(data)

    profiles = []
    for cluster_id in unique_clusters:
        mask = np.array(assignments) == cluster_id
        profile = profile_cluster(data, family_data, mask, cluster_id, total_n)
        profiles.append(profile)

    return profiles


def build_subset_summary(data, filters_used):
    """Build metadata summary for the filtered subset."""
    summary = {
        'total_members': len(data),
        'filters': filters_used,
    }

    # Add hierarchy descriptions from the data
    for col in DESCRIPTION_COLUMNS:
        if col in data.columns:
            summary[col] = sorted(data[col].dropna().unique().tolist())

    return summary
