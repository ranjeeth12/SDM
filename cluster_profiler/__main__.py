"""CLI entry point for the cluster profiler."""

import argparse
import os
import sys

# Support both `python -m cluster_profiler` (relative imports work) and
# `python cluster_profiler` (no parent package, so use absolute imports).
if __package__:
    from .config import DEFAULT_DATA_PATH, DEFAULT_LABELS_PATH, DEFAULT_REFERENCE_DATE
    from .data_loader import apply_filters, load_data
    from .clustering import build_features, discover_clusters
    from .profiler import build_subset_summary, profile_all_clusters
    from .formatters import format_console_report, format_json, write_json
else:
    # When run as a path, add the repo root to sys.path so absolute imports work.
    _repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from cluster_profiler.config import DEFAULT_DATA_PATH, DEFAULT_LABELS_PATH, DEFAULT_REFERENCE_DATE
    from cluster_profiler.data_loader import apply_filters, load_data
    from cluster_profiler.clustering import build_features, discover_clusters
    from cluster_profiler.profiler import build_subset_summary, profile_all_clusters
    from cluster_profiler.formatters import format_console_report, format_json, write_json


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description='Profile member clusters by hierarchy level.',
    )
    parser.add_argument(
        '--data', type=str, default=DEFAULT_DATA_PATH,
        help='Path to generated member CSV',
    )
    parser.add_argument(
        '--labels', type=str, default=DEFAULT_LABELS_PATH,
        help='Path to labels CSV',
    )
    parser.add_argument(
        '--grgr-ck', type=int, nargs='+', default=None,
        help='Group key(s) to filter on',
    )
    parser.add_argument(
        '--sgsg-ck', type=int, nargs='+', default=None,
        help='Subgroup key(s) to filter on',
    )
    parser.add_argument(
        '--cspd-cat', type=str, nargs='+', choices=['M', 'D', 'C'], default=None,
        help='Plan type(s): M=Medical, D=Dental, C=???',
    )
    parser.add_argument(
        '--lobd-id', type=str, nargs='+', default=None,
        help='Product/Line of business ID(s)',
    )
    parser.add_argument(
        '--k', type=int, default=None,
        help='Force number of clusters (default: auto-select via silhouette)',
    )
    parser.add_argument(
        '--use-labels', action='store_true', default=False,
        help='Use true cluster labels from the labels CSV',
    )
    parser.add_argument(
        '--output-json', type=str, default=None,
        help='Path to write JSON output',
    )
    parser.add_argument(
        '--output-plot', type=str, default=None,
        help='Path to write PNG visualization',
    )
    parser.add_argument(
        '--reference-date', type=str, default=DEFAULT_REFERENCE_DATE,
        help='Reference date for age/tenure calculation (YYYY-MM-DD)',
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    # Load
    print('Loading data...')
    df, labels_df = load_data(args.data, args.labels, args.reference_date)

    # Filter
    print('Applying filters...')
    try:
        members, filtered_labels, family_data, filters_used = apply_filters(
            df, labels_df,
            grgr_ck=args.grgr_ck,
            sgsg_ck=args.sgsg_ck,
            cspd_cat=args.cspd_cat,
            lobd_id=args.lobd_id,
        )
    except ValueError as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1

    print(f'Subset: {len(members)} unique members')

    # Cluster
    print('Discovering clusters...')
    assignments, metrics = discover_clusters(
        members, filtered_labels,
        k=args.k, use_labels=args.use_labels, filters_used=filters_used,
    )

    # Profile
    print('Profiling clusters...')
    summary = build_subset_summary(members, filters_used)
    profiles = profile_all_clusters(members, family_data, assignments)

    # Output: console
    report = format_console_report(summary, profiles, metrics)
    print(report)

    # Output: JSON
    if args.output_json:
        output = format_json(summary, profiles, metrics)
        write_json(output, args.output_json)
        print(f'JSON written to {args.output_json}')

    # Output: plot
    if args.output_plot:
        from cluster_profiler.visualization import plot_profiles
        X, _ = build_features(members)
        plot_profiles(X, assignments, members, args.output_plot)

    return 0


if __name__ == '__main__':
    sys.exit(main())
