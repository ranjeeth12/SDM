"""Console text and JSON output formatters."""

import json


def format_console_report(summary, profiles, metrics):
    """Format a human-readable console report."""
    lines = []
    lines.append('=' * 70)
    lines.append('CLUSTER PROFILER REPORT')
    lines.append('=' * 70)

    # Subset summary
    lines.append(f"\nSubset size: {summary['total_members']} members")
    if summary['filters']:
        lines.append('Filters:')
        for key, values in summary['filters'].items():
            lines.append(f'  {key}: {values}')
    else:
        lines.append('Filters: none (full dataset)')

    # Hierarchy descriptions
    for col in ['GRGR_NAME', 'SGSG_NAME', 'CSPD_CAT_DESC', 'PLDS_DESC', 'PDDS_DESC']:
        if col in summary and summary[col]:
            lines.append(f'{col}: {", ".join(str(v) for v in summary[col])}')

    # Clustering metrics
    lines.append(f"\nClustering method: {metrics.get('method', 'unknown')}")
    if 'n_clusters' in metrics:
        lines.append(f"Number of clusters: {metrics['n_clusters']}")
    if 'silhouette' in metrics:
        lines.append(f"Silhouette score: {metrics['silhouette']}")
    if 'ari' in metrics:
        lines.append(f"Adjusted Rand Index: {metrics['ari']}")
    if 'level' in metrics:
        lines.append(f"Label level: {metrics['level']}")
    if 'k_selection' in metrics:
        lines.append(f"K selection: {metrics['k_selection']}")

    # Per-cluster profiles
    for p in profiles:
        lines.append('')
        lines.append('-' * 50)
        lines.append(f"Cluster {p['cluster_id']}  |  n={p['size']}  |  {p['pct_of_subset']*100:.1f}% of subset")
        lines.append('-' * 50)

        # Continuous features
        lines.append('\n  Demographics:')
        for feat, stats in p['continuous'].items():
            label = 'Age (years)' if feat == '_age' else 'Tenure (months)'
            lines.append(
                f"    {label:20s}  mean={stats['mean']:6.1f}  std={stats['std']:5.1f}"
                f"  median={stats['median']:6.1f}  [{stats['min']:.1f} – {stats['max']:.1f}]"
            )

        # Categorical features
        lines.append('\n  Categorical distributions:')
        for feat, info in p['categorical'].items():
            lines.append(f'    {feat}:')
            for val, pct in sorted(info['pct'].items()):
                count = info['counts'][val]
                lines.append(f'      {str(val):20s}  {pct*100:5.1f}%  (n={count})')

        # Family
        fam = p['family']
        lines.append(f"\n  Family structure:")
        lines.append(f"    Avg dependents:  {fam['avg_dependents']:.2f}")
        lines.append(f"    Spouse rate:     {fam['spouse_rate']*100:.1f}%")

        # Descriptions
        if any(p['descriptions'].values()):
            lines.append('\n  Hierarchy:')
            for col, vals in p['descriptions'].items():
                if vals:
                    lines.append(f'    {col}: {", ".join(str(v) for v in vals)}')

    lines.append('')
    lines.append('=' * 70)
    return '\n'.join(lines)


def format_json(summary, profiles, metrics):
    """Return a JSON-serializable dict."""
    return {
        'summary': summary,
        'metrics': metrics,
        'clusters': profiles,
    }


def write_json(output, path):
    """Write JSON output to a file."""
    with open(path, 'w') as f:
        json.dump(output, f, indent=2, default=str)
