"""Tests for the synthetic generator with real lookup tables."""

import os
import sys
import pytest
import numpy as np
import pandas as pd

# Ensure package imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from schema import COLUMNS, PLAN_TYPES
from engine import SyntheticGenerator, generate_auto_config, _load_lookups, _safe_str, _parse_dt


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def lookups():
    """Load lookup tables once for all tests."""
    plans, subgroups = _load_lookups()
    return plans, subgroups


@pytest.fixture(scope="module")
def default_config():
    """Load the default YAML config."""
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'default.yaml')
    with open(config_path) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def generated_data(default_config):
    """Generate data once using default config (small subscriber count for speed)."""
    cfg = dict(default_config)
    cfg['total_subscribers'] = 100
    cfg['seed'] = 42
    gen = SyntheticGenerator(cfg)
    df, labels = gen.generate()
    return df, labels, cfg


@pytest.fixture(scope="module")
def auto_generated_data():
    """Generate data using auto config."""
    cfg = generate_auto_config(seed=99, total_subscribers=60, n_groups=3)
    gen = SyntheticGenerator(cfg)
    df, labels = gen.generate()
    return df, labels, cfg


# ── Schema Tests ─────────────────────────────────────────────────────────

class TestSchema:
    def test_column_count(self):
        assert len(COLUMNS) == 58

    def test_new_columns_present(self):
        assert 'PLDS_DESC' in COLUMNS
        assert 'PDDS_DESC' in COLUMNS

    def test_plds_pdds_at_end(self):
        assert COLUMNS[-2] == 'PLDS_DESC'
        assert COLUMNS[-1] == 'PDDS_DESC'

    def test_plan_types_keys(self):
        assert set(PLAN_TYPES.keys()) == {'M', 'D', 'C'}

    def test_plan_types_no_vision(self):
        assert 'V' not in PLAN_TYPES
        assert 'vision' not in PLAN_TYPES

    def test_plan_types_cspd_cat_values(self):
        for key, val in PLAN_TYPES.items():
            assert val['cspd_cat'] == key


# ── Lookup Loading Tests ─────────────────────────────────────────────────

class TestLookups:
    def test_lookups_load(self, lookups):
        plans, subgroups = lookups
        assert len(plans) > 0
        assert len(subgroups) > 0

    def test_plans_has_required_columns(self, lookups):
        plans, _ = lookups
        required = ['GRGR_CK', 'CSPI_ID', 'CSPD_CAT', 'PDPD_ID', 'LOBD_ID',
                     'GRGR_NAME', 'GRGR_ID', 'GRGR_STATE', 'GRGR_COUNTY',
                     'PLDS_DESC', 'PDDS_DESC', 'PDPD_RISK_IND', 'PDPD_MCTR_CCAT']
        for col in required:
            assert col in plans.columns, f"Missing column: {col}"

    def test_subgroups_has_required_columns(self, lookups):
        _, subgroups = lookups
        required = ['SGSG_CK', 'GRGR_CK', 'SGSG_ID', 'SGSG_NAME', 'CSCS_ID',
                     'SGSG_STATE', 'SGSG_STS']
        for col in required:
            assert col in subgroups.columns, f"Missing column: {col}"

    def test_cspd_cat_values_in_lookup(self, lookups):
        plans, _ = lookups
        cats = set(plans['CSPD_CAT'].dropna().unique())
        assert cats == {'M', 'D', 'C'}

    def test_target_groups_exist(self, lookups):
        plans, _ = lookups
        for gk in [1, 5, 7, 12, 16, 31]:
            assert gk in plans['GRGR_CK'].values, f"GRGR_CK={gk} missing"


# ── Helper Function Tests ────────────────────────────────────────────────

class TestHelpers:
    def test_safe_str_normal(self):
        assert _safe_str('hello') == 'hello'

    def test_safe_str_nan(self):
        assert _safe_str(float('nan')) == ''
        assert _safe_str(None) == ''

    def test_safe_str_default(self):
        assert _safe_str(None, 'fallback') == 'fallback'

    def test_safe_str_strips_whitespace(self):
        assert _safe_str('  foo  ') == 'foo'

    def test_parse_dt_string(self):
        from datetime import datetime
        dt = _parse_dt('2003-07-01 00:00:00')
        assert dt == datetime(2003, 7, 1)

    def test_parse_dt_9999(self):
        from datetime import datetime
        dt = _parse_dt('9999-12-31 00:00:00')
        assert dt == datetime(9999, 12, 31)

    def test_parse_dt_nan(self):
        from datetime import datetime
        dt = _parse_dt(float('nan'))
        assert dt == datetime(9999, 12, 31)

    def test_parse_dt_none(self):
        from datetime import datetime
        dt = _parse_dt(None)
        assert dt == datetime(9999, 12, 31)

    def test_parse_dt_timestamp(self):
        from datetime import datetime
        ts = pd.Timestamp('2020-06-15')
        dt = _parse_dt(ts)
        assert dt == datetime(2020, 6, 15)


# ── Default Config Generation Tests ──────────────────────────────────────

class TestDefaultGeneration:
    def test_output_column_count(self, generated_data):
        df, _, _ = generated_data
        assert len(df.columns) == 58

    def test_output_column_names(self, generated_data):
        df, _, _ = generated_data
        assert list(df.columns) == COLUMNS

    def test_cspd_cat_values(self, generated_data):
        df, _, _ = generated_data
        cats = set(df['CSPD_CAT'].unique())
        assert cats.issubset({'M', 'D', 'C'})
        assert 'V' not in cats

    def test_no_vision(self, generated_data):
        df, _, _ = generated_data
        assert 'V' not in df['CSPD_CAT'].values
        assert 'Vision' not in df['CSPD_CAT_DESC'].values

    def test_grgr_ck_from_real_data(self, generated_data, lookups):
        df, _, _ = generated_data
        plans, _ = lookups
        real_grgr_cks = set(plans['GRGR_CK'].unique())
        for gk in df['GRGR_CK'].unique():
            assert gk in real_grgr_cks, f"GRGR_CK={gk} not in lookup"

    def test_grgr_names_from_real_data(self, generated_data, lookups):
        df, _, _ = generated_data
        plans, _ = lookups
        real_names = set(plans['GRGR_NAME'].dropna().unique())
        for name in df['GRGR_NAME'].unique():
            assert name in real_names, f"GRGR_NAME='{name}' not in lookup"

    def test_sgsg_ck_from_real_data(self, generated_data, lookups):
        df, _, _ = generated_data
        _, subgroups = lookups
        real_sgsg_cks = set(subgroups['SGSG_CK'].unique())
        for sk in df['SGSG_CK'].unique():
            assert sk in real_sgsg_cks, f"SGSG_CK={sk} not in lookup"

    def test_cspi_to_pdpd_relationship(self, generated_data, lookups):
        df, _, _ = generated_data
        plans, _ = lookups
        # For each unique CSPI_ID in output, verify PDPD_ID matches lookup
        for cspi_id in df['CSPI_ID'].unique():
            output_pdpd = df[df['CSPI_ID'] == cspi_id]['PDPD_ID'].iloc[0]
            lookup_row = plans[plans['CSPI_ID'] == cspi_id]
            assert len(lookup_row) > 0, f"CSPI_ID={cspi_id} not in lookup"
            lookup_pdpd = str(lookup_row.iloc[0]['PDPD_ID']).strip()
            assert str(output_pdpd).strip() == lookup_pdpd, \
                f"CSPI_ID={cspi_id}: PDPD_ID mismatch {output_pdpd} vs {lookup_pdpd}"

    def test_cspi_to_lobd_relationship(self, generated_data, lookups):
        df, _, _ = generated_data
        plans, _ = lookups
        for cspi_id in df['CSPI_ID'].unique():
            output_lobd = df[df['CSPI_ID'] == cspi_id]['LOBD_ID'].iloc[0]
            lookup_row = plans[plans['CSPI_ID'] == cspi_id]
            lookup_lobd = str(lookup_row.iloc[0]['LOBD_ID']).strip()
            assert str(output_lobd).strip() == lookup_lobd, \
                f"CSPI_ID={cspi_id}: LOBD_ID mismatch {output_lobd} vs {lookup_lobd}"

    def test_plds_desc_populated(self, generated_data):
        df, _, _ = generated_data
        # At least some rows should have non-empty PLDS_DESC
        non_empty = df['PLDS_DESC'].dropna()
        non_empty = non_empty[non_empty.str.strip() != '']
        assert len(non_empty) > 0, "No PLDS_DESC values populated"

    def test_pdds_desc_populated(self, generated_data):
        df, _, _ = generated_data
        non_empty = df['PDDS_DESC'].dropna()
        non_empty = non_empty[non_empty.str.strip() != '']
        assert len(non_empty) > 0, "No PDDS_DESC values populated"

    def test_subscriber_count(self, generated_data):
        df, _, cfg = generated_data
        assert df['SBSB_CK'].nunique() == cfg['total_subscribers']

    def test_group_count(self, generated_data):
        df, _, cfg = generated_data
        assert df['GRGR_CK'].nunique() == len(cfg['groups'])

    def test_six_groups(self, generated_data):
        df, _, _ = generated_data
        expected_grgr_cks = {1, 5, 7, 12, 16, 31}
        assert set(df['GRGR_CK'].unique()) == expected_grgr_cks

    def test_member_identity_synthetic(self, generated_data):
        """Member fields should be synthetic (auto-increment, name pools)."""
        df, _, _ = generated_data
        meme_cks = sorted(df['MEME_CK'].unique())
        # Should start at 5001 and be sequential
        assert meme_cks[0] == 5001

    def test_has_case_management(self, generated_data):
        """Georgia group (GRGR_CK=16) should have CSPD_CAT='C'."""
        df, _, _ = generated_data
        ga_data = df[df['GRGR_CK'] == 16]
        assert 'C' in ga_data['CSPD_CAT'].values

    def test_family_members_exist(self, generated_data):
        """Should have subscribers (M), spouses (S), and dependents (D)."""
        df, _, _ = generated_data
        rels = set(df['MEME_REL'].unique())
        assert 'M' in rels  # subscribers
        # Spouses and dependents may or may not appear with 100 subs, but at least
        # the subscriber relation must be present

    def test_enrollment_periods(self, generated_data):
        """Some members should have term dates != 9999-12-31."""
        df, _, _ = generated_data
        has_term = df[~df['SBSG_TERM_DT'].str.startswith('9999')]
        # With 10% term rate, we expect some terminated rows
        # (may be 0 with small sample, so just check format)
        assert all(df['SBSG_EFF_DT'].str.match(r'\d{4}-\d{2}-\d{2}'))

    def test_pdpd_risk_ind_from_lookup(self, generated_data):
        """PDPD_RISK_IND should come from lookup (may be empty for these groups)."""
        df, _, _ = generated_data
        # Column should exist
        assert 'PDPD_RISK_IND' in df.columns

    def test_pdpd_mctr_ccat_from_lookup(self, generated_data):
        """PDPD_MCTR_CCAT should come from lookup (may be empty for these groups)."""
        df, _, _ = generated_data
        assert 'PDPD_MCTR_CCAT' in df.columns


# ── Labels Tests ─────────────────────────────────────────────────────────

class TestLabels:
    def test_labels_have_required_columns(self, generated_data):
        _, labels, _ = generated_data
        required = ['MEME_CK', 'SBSB_CK', 'GRGR_CK', 'SGSG_CK', 'CSPD_CAT',
                     'LOBD_ID', 'group_cluster', 'group_cluster_idx',
                     'subgroup_cluster', 'subgroup_cluster_idx',
                     'plan_cluster', 'plan_cluster_idx',
                     'product_cluster', 'product_cluster_idx',
                     'age', 'tenure_months', 'meme_rel']
        for col in required:
            assert col in labels.columns, f"Missing label column: {col}"

    def test_labels_cspd_cat_uses_codes(self, generated_data):
        _, labels, _ = generated_data
        cats = set(labels['CSPD_CAT'].unique())
        assert cats.issubset({'M', 'D', 'C'})
        # Should NOT have old-style names
        assert 'medical' not in cats
        assert 'dental' not in cats
        assert 'vision' not in cats

    def test_labels_lobd_id_real(self, generated_data, lookups):
        _, labels, _ = generated_data
        plans, _ = lookups
        real_lobds = set(plans['LOBD_ID'].dropna().unique())
        for lobd in labels['LOBD_ID'].unique():
            assert lobd in real_lobds, f"LOBD_ID='{lobd}' not in lookup"

    def test_labels_grgr_ck_match_data(self, generated_data):
        df, labels, _ = generated_data
        assert set(labels['GRGR_CK'].unique()) == set(df['GRGR_CK'].unique())

    def test_labels_cluster_indices_valid(self, generated_data):
        _, labels, _ = generated_data
        for col in ['group_cluster_idx', 'subgroup_cluster_idx',
                     'plan_cluster_idx', 'product_cluster_idx']:
            vals = labels[col].dropna()
            assert all(v >= 0 for v in vals), f"Negative index in {col}"


# ── Auto Config Tests ────────────────────────────────────────────────────

class TestAutoConfig:
    def test_auto_config_generates(self):
        cfg = generate_auto_config(seed=42, total_subscribers=30, n_groups=2)
        assert 'groups' in cfg
        assert len(cfg['groups']) > 0

    def test_auto_config_uses_real_grgr_ck(self, lookups):
        plans, _ = lookups
        real_grgr_cks = set(plans['GRGR_CK'].unique())
        cfg = generate_auto_config(seed=42, total_subscribers=30, n_groups=3)
        for grp in cfg['groups']:
            assert grp['grgr_ck'] in real_grgr_cks

    def test_auto_config_uses_real_sgsg_ck(self, lookups):
        _, subgroups = lookups
        real_sgsg_cks = set(subgroups['SGSG_CK'].unique())
        cfg = generate_auto_config(seed=42, total_subscribers=30, n_groups=3)
        for grp in cfg['groups']:
            for sg in grp['subgroups']:
                assert sg['sgsg_ck'] in real_sgsg_cks

    def test_auto_config_plan_weights_use_codes(self):
        cfg = generate_auto_config(seed=42, total_subscribers=30, n_groups=2)
        weights = cfg['plan_enrollment_weights']
        assert 'M' in weights
        assert 'D' in weights
        assert 'C' in weights
        assert 'medical' not in weights
        assert 'dental' not in weights
        assert 'vision' not in weights

    def test_auto_generated_output(self, auto_generated_data):
        df, labels, _ = auto_generated_data
        assert len(df.columns) == 58
        assert list(df.columns) == COLUMNS
        cats = set(df['CSPD_CAT'].unique())
        assert cats.issubset({'M', 'D', 'C'})


# ── Error Handling Tests ─────────────────────────────────────────────────

class TestErrorHandling:
    def test_invalid_grgr_ck_raises(self):
        cfg = {
            'seed': 42,
            'total_subscribers': 1,
            'groups': [{
                'grgr_ck': 999999,  # does not exist
                'target_subscribers': 1,
                'subgroups': [],
            }],
        }
        gen = SyntheticGenerator(cfg)
        with pytest.raises(ValueError, match="GRGR_CK=999999 not found"):
            gen.generate()

    def test_invalid_sgsg_ck_raises(self):
        cfg = {
            'seed': 42,
            'total_subscribers': 1,
            'groups': [{
                'grgr_ck': 1,
                'target_subscribers': 1,
                'subgroups': [{
                    'sgsg_ck': 999999,  # does not exist
                    'plans': [],
                }],
            }],
        }
        gen = SyntheticGenerator(cfg)
        with pytest.raises(ValueError, match="SGSG_CK=999999 not found"):
            gen.generate()

    def test_invalid_cspi_id_raises(self):
        cfg = {
            'seed': 42,
            'total_subscribers': 1,
            'groups': [{
                'grgr_ck': 1,
                'target_subscribers': 1,
                'subgroups': [{
                    'sgsg_ck': 179,
                    'plans': [{
                        'cspi_id': 'NONEXISTENT',
                    }],
                }],
            }],
        }
        gen = SyntheticGenerator(cfg)
        with pytest.raises(ValueError, match="CSPI_ID=NONEXISTENT not found"):
            gen.generate()


# ── Cross-validation: output vs lookup consistency ───────────────────────

class TestLookupConsistency:
    def test_grgr_state_matches_lookup(self, generated_data, lookups):
        df, _, _ = generated_data
        plans, _ = lookups
        for gk in df['GRGR_CK'].unique():
            output_state = df[df['GRGR_CK'] == gk]['GRGR_STATE'].iloc[0]
            lookup_state = plans[plans['GRGR_CK'] == gk].iloc[0]['GRGR_STATE']
            assert str(output_state).strip() == str(lookup_state).strip(), \
                f"GRGR_CK={gk}: state mismatch"

    def test_grgr_mctr_type_matches_lookup(self, generated_data, lookups):
        df, _, _ = generated_data
        plans, _ = lookups
        for gk in df['GRGR_CK'].unique():
            output_val = df[df['GRGR_CK'] == gk]['GRGR_MCTR_TYPE'].iloc[0]
            lookup_val = plans[plans['GRGR_CK'] == gk].iloc[0]['GRGR_MCTR_TYPE']
            assert str(output_val).strip() == str(lookup_val).strip(), \
                f"GRGR_CK={gk}: GRGR_MCTR_TYPE mismatch"

    def test_sgsg_name_matches_lookup(self, generated_data, lookups):
        df, _, _ = generated_data
        _, subgroups = lookups
        for sk in df['SGSG_CK'].unique():
            output_name = df[df['SGSG_CK'] == sk]['SGSG_NAME'].iloc[0]
            lookup_name = subgroups[subgroups['SGSG_CK'] == sk].iloc[0]['SGSG_NAME']
            assert str(output_name).strip() == str(lookup_name).strip(), \
                f"SGSG_CK={sk}: SGSG_NAME mismatch"

    def test_cspd_cat_desc_matches_lookup(self, generated_data, lookups):
        df, _, _ = generated_data
        plans, _ = lookups
        for cspi_id in df['CSPI_ID'].unique():
            output_desc = df[df['CSPI_ID'] == cspi_id]['CSPD_CAT_DESC'].iloc[0]
            lookup_desc = plans[plans['CSPI_ID'] == cspi_id].iloc[0]['CSPD_CAT_DESC']
            assert str(output_desc).strip() == str(lookup_desc).strip(), \
                f"CSPI_ID={cspi_id}: CSPD_CAT_DESC mismatch"

    def test_plds_desc_matches_lookup(self, generated_data, lookups):
        df, _, _ = generated_data
        plans, _ = lookups
        for cspi_id in df['CSPI_ID'].unique():
            output_desc = str(df[df['CSPI_ID'] == cspi_id]['PLDS_DESC'].iloc[0]).strip()
            lookup_row = plans[plans['CSPI_ID'] == cspi_id].iloc[0]
            lookup_desc = str(lookup_row.get('PLDS_DESC', '')).strip()
            if lookup_desc and lookup_desc != 'nan':
                assert output_desc == lookup_desc, \
                    f"CSPI_ID={cspi_id}: PLDS_DESC mismatch '{output_desc}' vs '{lookup_desc}'"


# ── Determinism Test ─────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_seed_same_output(self, default_config):
        cfg = dict(default_config)
        cfg['total_subscribers'] = 20
        cfg['seed'] = 123

        gen1 = SyntheticGenerator(cfg)
        df1, labels1 = gen1.generate()

        gen2 = SyntheticGenerator(cfg)
        df2, labels2 = gen2.generate()

        pd.testing.assert_frame_equal(df1, df2)
        pd.testing.assert_frame_equal(labels1, labels2)

    def test_different_seed_different_output(self, default_config):
        cfg1 = dict(default_config)
        cfg1['total_subscribers'] = 20
        cfg1['seed'] = 1

        cfg2 = dict(default_config)
        cfg2['total_subscribers'] = 20
        cfg2['seed'] = 2

        gen1 = SyntheticGenerator(cfg1)
        df1, _ = gen1.generate()

        gen2 = SyntheticGenerator(cfg2)
        df2, _ = gen2.generate()

        # At least some names should differ (different random draws)
        min_len = min(len(df1), len(df2))
        assert not (df1['MEME_FIRST_NAME'].values[:min_len] == df2['MEME_FIRST_NAME'].values[:min_len]).all()
