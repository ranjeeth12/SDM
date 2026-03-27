"""CLI entry point: python -m synthetic_generator [generate|validate]"""

import argparse
import os
import sys
import yaml

# Ensure package directory is on path for sibling imports
sys.path.insert(0, os.path.dirname(__file__))

from engine import SyntheticGenerator, generate_auto_config
from validate import validate


def cmd_generate(args):
    if args.auto:
        print(f"Auto-generating cluster config (seed={args.seed}, "
              f"subscribers={args.subscribers})")
        config = generate_auto_config(
            seed=args.seed,
            total_subscribers=args.subscribers or 600,
            n_member_clusters=args.auto_member_clusters,
            n_group_clusters=args.auto_group_clusters,
        )
    else:
        config_path = args.config
        if not os.path.exists(config_path):
            print(f"Config not found: {config_path}", file=sys.stderr)
            sys.exit(1)
        with open(config_path) as f:
            config = yaml.safe_load(f)
        print(f"Loaded config from {config_path}")

    if args.seed is not None:
        config['seed'] = args.seed
    if args.subscribers is not None:
        config['total_subscribers'] = args.subscribers

    gen = SyntheticGenerator(config)
    df, labels_df = gen.generate()

    output = args.output
    labels_output = output.replace('.csv', '_labels.csv')

    df.to_csv(output, index=False)
    labels_df.to_csv(labels_output, index=False)

    print(f"\nGeneration complete:")
    print(f"  Output rows:     {len(df)}")
    print(f"  Unique members:  {df['MEME_CK'].nunique()}")
    print(f"  Unique subscribers: {df['SBSB_CK'].nunique()}")
    print(f"  Groups:          {df['GRGR_CK'].nunique()}")
    print(f"  Data file:       {output}")
    print(f"  Labels file:     {labels_output}")

    if args.validate:
        plot_path = output.replace('.csv', '_clusters.png')
        print("\nRunning cluster recovery validation...")
        validate(output, labels_output, config.get('reference_date', '2025-01-01'),
                 plot_path=plot_path)


def cmd_validate(args):
    validate(args.data, args.labels, args.reference_date, plot=args.plot)


def main():
    parser = argparse.ArgumentParser(
        prog='synthetic_generator',
        description='Generate synthetic MEMBER_GROUP_PLAN_FLAT data with '
                    'embedded cluster structure.',
    )
    sub = parser.add_subparsers(dest='command')

    # ── generate ─────────────────────────────────────────────────
    gen_p = sub.add_parser('generate', help='Generate synthetic data')
    gen_p.add_argument('--config', default=os.path.join(
        os.path.dirname(__file__), 'config', 'default.yaml'),
        help='Path to YAML config (ignored if --auto)')
    gen_p.add_argument('--auto', action='store_true',
        help='Auto-select clusters and centroids instead of using config')
    gen_p.add_argument('--auto-member-clusters', type=int, default=5,
        help='Number of member clusters in auto mode (default: 5)')
    gen_p.add_argument('--auto-group-clusters', type=int, default=3,
        help='Number of group clusters in auto mode (default: 3)')
    gen_p.add_argument('--seed', type=int, default=42)
    gen_p.add_argument('--subscribers', type=int, default=None,
        help='Total subscribers to generate (overrides config)')
    gen_p.add_argument('--output', default=os.path.join(
        os.path.dirname(__file__), '..', 'data', 'MEMBER_GROUP_PLAN_FLAT_generated.csv'),
        help='Output CSV path')
    gen_p.add_argument('--validate', action='store_true',
        help='Run cluster recovery validation after generation')

    # ── validate ─────────────────────────────────────────────────
    val_p = sub.add_parser('validate', help='Validate cluster recovery')
    val_p.add_argument('--data', required=True, help='Path to generated CSV')
    val_p.add_argument('--labels', required=True, help='Path to labels CSV')
    val_p.add_argument('--reference-date', default='2025-01-01')
    val_p.add_argument('--plot', default=None, help='Output path for PCA scatter PNG')

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == 'generate':
        cmd_generate(args)
    elif args.command == 'validate':
        cmd_validate(args)


if __name__ == '__main__':
    main()
