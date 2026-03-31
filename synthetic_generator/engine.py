"""Core generation engine for MEMBER_GROUP_PLAN_FLAT synthetic data.

Uses per-level member clustering: each hierarchy level (group, subgroup, plan,
product) defines its own member clusters. A member's final demographics are a
weighted blend of independent samples from each level's cluster.

Lookup tables from data/lookups_joined/ provide real CareSource group, subgroup,
and plan identity fields. Member demographics remain synthetic.
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from schema import (
    COLUMNS, PLAN_TYPES, LAST_NAMES, MALE_FIRST, FEMALE_FIRST,
    CHILD_MALE, CHILD_FEMALE, MID_INITIALS, RACE_CODES, ETHN_CODES,
)

# Path to lookup parquets (relative to repo root)
_LOOKUPS_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'lookups_joined')


def _load_lookups():
    """Load lookup parquets. Returns (plans_df, subgroups_df)."""
    plans_path = os.path.join(_LOOKUPS_DIR, 'plans.parquet')
    subgroups_path = os.path.join(_LOOKUPS_DIR, 'subgroups.parquet')
    plans = pd.read_parquet(plans_path)
    subgroups = pd.read_parquet(subgroups_path)
    return plans, subgroups


def _parse_dt(val):
    """Parse a date value from parquet (may be string or Timestamp) to datetime."""
    if pd.isna(val):
        return datetime(9999, 12, 31)
    if isinstance(val, str):
        # Handle '9999-12-31 00:00:00' or '2003-07-01' etc.
        return datetime.strptime(val[:10], '%Y-%m-%d')
    if hasattr(val, 'to_pydatetime'):
        return val.to_pydatetime().replace(tzinfo=None)
    return datetime(9999, 12, 31)


def _safe_str(val, default=''):
    """Convert a parquet value to str, handling NaN/None."""
    if pd.isna(val) or val is None:
        return default
    return str(val).strip()


class SyntheticGenerator:
    """Generates synthetic MEMBER_GROUP_PLAN_FLAT data with per-level cluster structure."""

    def __init__(self, config: dict):
        self.config = config
        self.rng = np.random.default_rng(config.get('seed', 42))
        self.ref_date = datetime.strptime(
            config.get('reference_date', '2025-01-01'), '%Y-%m-%d'
        )
        self.level_weights = config.get('level_weights', {
            'group': 0.40, 'subgroup': 0.20, 'plan': 0.20, 'product': 0.20,
        })
        self._meme_ck = 5001
        self._sbsb_ck = 3001
        self._ssn = 900000001

        # Load real lookup tables
        self.plans_lookup, self.subgroups_lookup = _load_lookups()

    def generate(self):
        """Generate the full dataset. Returns (data_df, labels_df)."""
        hierarchy = self._parse_hierarchy()
        enrollment_cfg = self.config.get('enrollment', {})
        plan_enroll_weights = self.config.get('plan_enrollment_weights', {
            'M': 1.0, 'D': 0.60, 'C': 0.30,
        })
        total_subs = self.config.get('total_subscribers', 500)

        # Distribute subscribers across groups proportional to target_subscribers
        group_capacities = np.array(
            [g['target_subscribers'] for g in hierarchy], dtype=float)
        group_probs = group_capacities / group_capacities.sum()

        all_rows = []
        all_labels = []

        for _ in range(total_subs):
            # a) Pick group
            g_idx = self.rng.choice(len(hierarchy), p=group_probs)
            group = hierarchy[g_idx]

            # b) Pick subgroup (equal weight)
            subgroups = group['subgroups']
            sg_idx = self.rng.integers(len(subgroups))
            subgroup = subgroups[sg_idx]

            # c) Pick primary plan (M first if available)
            plans = subgroup['plans']
            primary_plan = None
            for p in plans:
                if p['type'] == 'M':
                    primary_plan = p
                    break
            if primary_plan is None:
                primary_plan = plans[0]

            # d) Get product from primary plan
            primary_product = primary_plan['products'][0]

            # e) Pick a cluster independently at each level
            gc_idx, gc = self._pick_cluster(group['member_clusters'])
            sgc_idx, sgc = self._pick_cluster(subgroup['member_clusters'])
            pc_idx, pc = self._pick_cluster(primary_plan['member_clusters'])
            prc_idx, prc = self._pick_cluster(primary_product['member_clusters'])

            cluster_picks = {
                'group': gc, 'subgroup': sgc, 'plan': pc, 'product': prc,
            }

            # f) Blend continuous attributes
            sub_age = self._blend_continuous(cluster_picks, 'age', 18, 70)
            sub_tenure = self._blend_continuous(cluster_picks, 'tenure_months', 1, 360)

            # g) Blend categorical attributes
            sub_sex = self._blend_categorical(cluster_picks, 'meme_sex')
            sub_marital = self._blend_categorical(cluster_picks, 'meme_marital_status')

            # Generate subscriber identity
            sbsb_ck = self._next_sbsb_ck()
            sub_last = self.rng.choice(LAST_NAMES)
            sub_first = self.rng.choice(MALE_FIRST if sub_sex == 'M' else FEMALE_FIRST)
            sub_mid = self.rng.choice(MID_INITIALS)
            sub_ssn = self._next_ssn()
            sub_meme_ck = self._next_meme_ck()
            sub_birth_dt = self._age_to_date(sub_age)
            sub_eff_dt = self._tenure_to_date(sub_tenure)
            sub_race = self.rng.choice(RACE_CODES)
            sub_ethn = self.rng.choice(ETHN_CODES)

            subscriber_info = {
                'sbsb_ck': sbsb_ck,
                'sbsb_id': f'SUB{sbsb_ck:06d}',
                'sbsb_last_name': sub_last,
                'sbsb_first_name': sub_first,
                'sbsb_orig_eff_dt': sub_eff_dt,
                'sbsb_mctr_sts': 'AC',
                'sbsb_employ_id': f'EMP{sbsb_ck:06d}',
            }

            # h) Generate family from group-level cluster's family config
            family_cfg = gc.get('family', {})
            family = [self._make_member(
                meme_ck=sub_meme_ck, sbsb_ck=sbsb_ck, sfx=1, rel='M',
                last_name=sub_last, first_name=sub_first, mid_init=sub_mid,
                sex=sub_sex, birth_dt=sub_birth_dt, ssn=sub_ssn,
                eff_dt=sub_eff_dt, marital=sub_marital,
                race=sub_race, ethn=sub_ethn,
                age=sub_age, tenure=sub_tenure,
            )]

            sfx = 2
            # Spouse
            if self.rng.random() < family_cfg.get('spouse_prob', 0):
                sp_sex = 'F' if sub_sex == 'M' else 'M'
                sp_age = max(18, sub_age + self.rng.normal(0, 3))
                sp_first = self.rng.choice(
                    MALE_FIRST if sp_sex == 'M' else FEMALE_FIRST)
                family.append(self._make_member(
                    meme_ck=self._next_meme_ck(), sbsb_ck=sbsb_ck, sfx=sfx, rel='S',
                    last_name=sub_last, first_name=sp_first,
                    mid_init=self.rng.choice(MID_INITIALS),
                    sex=sp_sex, birth_dt=self._age_to_date(sp_age),
                    ssn=self._next_ssn(), eff_dt=sub_eff_dt, marital='M',
                    race=sub_race, ethn=sub_ethn,
                    age=sp_age, tenure=sub_tenure,
                ))
                sfx += 1

            # Children
            n_children_cfg = family_cfg.get('children', {'mean': 0, 'std': 0})
            n_children = max(0, int(round(
                self.rng.normal(n_children_cfg['mean'], n_children_cfg['std'])
            )))
            for _ in range(n_children):
                ch_sex = self.rng.choice(['M', 'F'])
                ch_age = max(0, min(sub_age - 18,
                                    self.rng.normal(max(5, sub_age - 28), 4)))
                ch_first = self.rng.choice(
                    CHILD_MALE if ch_sex == 'M' else CHILD_FEMALE)
                family.append(self._make_member(
                    meme_ck=self._next_meme_ck(), sbsb_ck=sbsb_ck, sfx=sfx, rel='D',
                    last_name=sub_last, first_name=ch_first,
                    mid_init=self.rng.choice(MID_INITIALS),
                    sex=ch_sex, birth_dt=self._age_to_date(ch_age),
                    ssn=self._next_ssn(), eff_dt=sub_eff_dt, marital='S',
                    race=sub_race, ethn=sub_ethn,
                    age=ch_age, tenure=sub_tenure,
                ))
                sfx += 1

            # i) Determine additional plan enrollments via plan_enrollment_weights
            active_plans = []
            for plan in plans:
                ptype = plan['type']
                weight = plan_enroll_weights.get(ptype, 0.5)
                if ptype == primary_plan['type'] or self.rng.random() < weight:
                    active_plans.append(plan)
            if not active_plans:
                active_plans = [primary_plan]

            # j) For each active plan, pick plan/product clusters (for labels only)
            plan_cluster_map = {}
            for plan in active_plans:
                product = plan['products'][0]
                if plan is primary_plan:
                    plan_cluster_map[plan['type']] = {
                        'plan_cluster': pc['name'],
                        'plan_cluster_idx': pc_idx,
                        'product_cluster': prc['name'],
                        'product_cluster_idx': prc_idx,
                        'lobd_id': product['lobd_id'],
                        'plan_detail': plan['plan_detail'],
                    }
                else:
                    alt_pc_idx, alt_pc = self._pick_cluster(plan['member_clusters'])
                    alt_prc_idx, alt_prc = self._pick_cluster(
                        product['member_clusters'])
                    plan_cluster_map[plan['type']] = {
                        'plan_cluster': alt_pc['name'],
                        'plan_cluster_idx': alt_pc_idx,
                        'product_cluster': alt_prc['name'],
                        'product_cluster_idx': alt_prc_idx,
                        'lobd_id': product['lobd_id'],
                        'plan_detail': plan['plan_detail'],
                    }

            # k) Generate enrollment periods
            enrollment_periods = self._generate_enrollments(sub_eff_dt, enrollment_cfg)

            # l) Expand: member x enrollment_period x enrolled_plans -> rows
            for member in family:
                for eff_dt, term_dt, trsn in enrollment_periods:
                    for plan in active_plans:
                        ptype = plan['type']
                        pd_detail = plan_cluster_map[ptype]['plan_detail']
                        row = self._build_row(
                            member, subscriber_info, group, subgroup,
                            eff_dt, term_dt, trsn, pd_detail,
                        )
                        all_rows.append(row)

                # m) Record labels with cluster assignment at ALL four levels
                for plan in active_plans:
                    ptype = plan['type']
                    pcm = plan_cluster_map[ptype]
                    all_labels.append({
                        'MEME_CK': member['meme_ck'],
                        'SBSB_CK': sbsb_ck,
                        'GRGR_CK': group['grgr_ck'],
                        'SGSG_CK': subgroup['sgsg_ck'],
                        'CSPD_CAT': ptype,
                        'LOBD_ID': pcm['lobd_id'],
                        'group_cluster': gc['name'],
                        'group_cluster_idx': gc_idx,
                        'subgroup_cluster': sgc['name'],
                        'subgroup_cluster_idx': sgc_idx,
                        'plan_cluster': pcm['plan_cluster'],
                        'plan_cluster_idx': pcm['plan_cluster_idx'],
                        'product_cluster': pcm['product_cluster'],
                        'product_cluster_idx': pcm['product_cluster_idx'],
                        'age': member['age'],
                        'tenure_months': member['tenure'],
                        'meme_rel': member['rel'],
                    })

        df = pd.DataFrame(all_rows, columns=COLUMNS)
        labels_df = pd.DataFrame(all_labels)
        return df, labels_df

    # -- Hierarchy Parsing (uses real lookup tables) --------------------

    def _parse_hierarchy(self):
        """Parse config into hierarchy using real lookup tables for identity fields."""
        groups_cfg = self.config['groups']
        hierarchy = []

        for g_cfg in groups_cfg:
            grgr_ck = g_cfg['grgr_ck']

            # Look up group identity from plans_lookup (one row per group is enough)
            grp_rows = self.plans_lookup[self.plans_lookup['GRGR_CK'] == grgr_ck]
            if len(grp_rows) == 0:
                raise ValueError(f"GRGR_CK={grgr_ck} not found in plans lookup")
            grp_ref = grp_rows.iloc[0]

            group = {
                'grgr_ck': int(grgr_ck),
                'grgr_id': _safe_str(grp_ref['GRGR_ID']),
                'grgr_name': _safe_str(grp_ref['GRGR_NAME']),
                'grgr_state': _safe_str(grp_ref['GRGR_STATE'], 'OH'),
                'grgr_county': _safe_str(grp_ref['GRGR_COUNTY']),
                'grgr_sts': _safe_str(grp_ref['GRGR_STS'], 'AC'),
                'grgr_orig_eff_dt': _parse_dt(grp_ref['GRGR_ORIG_EFF_DT']),
                'grgr_term_dt': _parse_dt(grp_ref['GRGR_TERM_DT']),
                'grgr_mctr_type': _safe_str(grp_ref['GRGR_MCTR_TYPE'], 'MDCD'),
                'pagr_ck': int(grp_ref['PAGR_CK']) if not pd.isna(grp_ref['PAGR_CK']) else 0,
                'target_subscribers': g_cfg.get('target_subscribers', 50),
                'member_clusters': g_cfg.get('member_clusters', []),
                'subgroups': [],
            }

            # Parse subgroups
            for sg_cfg in g_cfg.get('subgroups', []):
                sgsg_ck = sg_cfg['sgsg_ck']

                # Look up subgroup identity from subgroups_lookup
                sg_rows = self.subgroups_lookup[
                    self.subgroups_lookup['SGSG_CK'] == sgsg_ck
                ]
                if len(sg_rows) == 0:
                    raise ValueError(
                        f"SGSG_CK={sgsg_ck} not found in subgroups lookup"
                    )
                sg_ref = sg_rows.iloc[0]

                subgroup = {
                    'sgsg_ck': int(sgsg_ck),
                    'sgsg_id': _safe_str(sg_ref['SGSG_ID']),
                    'sgsg_name': _safe_str(sg_ref['SGSG_NAME']),
                    'cscs_id': _safe_str(sg_ref['CSCS_ID']),
                    'sgsg_state': _safe_str(sg_ref['SGSG_STATE'], 'OH'),
                    'sgsg_sts': _safe_str(sg_ref['SGSG_STS'], 'AC'),
                    'sgsg_orig_eff_dt': _parse_dt(sg_ref['SGSG_ORIG_EFF_DT']),
                    'sgsg_term_dt': _parse_dt(sg_ref['SGSG_TERM_DT']),
                    'member_clusters': sg_cfg.get('member_clusters', []),
                    'plans': [],
                }

                # Parse plans
                for plan_cfg in sg_cfg.get('plans', []):
                    cspi_id = plan_cfg['cspi_id']

                    # Look up plan from plans_lookup
                    plan_rows = self.plans_lookup[
                        (self.plans_lookup['GRGR_CK'] == grgr_ck) &
                        (self.plans_lookup['CSPI_ID'] == cspi_id)
                    ]
                    if len(plan_rows) == 0:
                        raise ValueError(
                            f"CSPI_ID={cspi_id} not found for GRGR_CK={grgr_ck}"
                        )
                    pl_ref = plan_rows.iloc[0]

                    cspd_cat = _safe_str(pl_ref['CSPD_CAT'])
                    lobd_id = _safe_str(pl_ref['LOBD_ID'])

                    plan_detail = {
                        'cspi_id': cspi_id,
                        'cspd_cat': cspd_cat,
                        'cspi_eff_dt': _parse_dt(pl_ref['CSPI_EFF_DT']),
                        'cspi_term_dt': _parse_dt(pl_ref['CSPI_TERM_DT']),
                        'pdpd_id': _safe_str(pl_ref['PDPD_ID']),
                        'cspi_sel_ind': _safe_str(pl_ref['CSPI_SEL_IND'], 'Y'),
                        'cspi_hios_id_nvl': _safe_str(pl_ref.get('CSPI_HIOS_ID_NVL', '')),
                        'cspd_cat_desc': _safe_str(pl_ref['CSPD_CAT_DESC']),
                        'cspd_type': _safe_str(pl_ref['CSPD_TYPE']),
                        'lobd_id': lobd_id,
                        'pdpd_risk_ind': _safe_str(pl_ref['PDPD_RISK_IND'], 'N'),
                        'pdpd_mctr_ccat': _safe_str(pl_ref['PDPD_MCTR_CCAT'], ''),
                        'plds_desc': _safe_str(pl_ref.get('PLDS_DESC', '')),
                        'pdds_desc': _safe_str(pl_ref.get('PDDS_DESC', '')),
                    }

                    products = []
                    for prod_cfg in plan_cfg.get('products', []):
                        products.append({
                            'lobd_id': prod_cfg.get('lobd_id', lobd_id),
                            'member_clusters': prod_cfg.get('member_clusters', []),
                        })
                    if not products:
                        products.append({
                            'lobd_id': lobd_id,
                            'member_clusters': [],
                        })

                    plan = {
                        'type': cspd_cat,  # 'M', 'D', or 'C'
                        'plan_detail': plan_detail,
                        'member_clusters': plan_cfg.get('member_clusters', []),
                        'products': products,
                    }
                    subgroup['plans'].append(plan)

                group['subgroups'].append(subgroup)

            hierarchy.append(group)

        return hierarchy

    # -- Blending ------------------------------------------------------

    def _pick_cluster(self, clusters):
        """Pick a cluster from a list using weights. Returns (index, cluster_dict)."""
        if not clusters:
            # Return a neutral cluster if none defined
            return 0, {
                'name': 'default',
                'weight': 1.0,
                'continuous': {
                    'age': {'mean': 40, 'std': 10},
                    'tenure_months': {'mean': 60, 'std': 20},
                },
                'categorical': {
                    'meme_sex': {'M': 0.50, 'F': 0.50},
                    'meme_marital_status': {'M': 0.50, 'S': 0.50},
                },
            }
        weights = np.array([c.get('weight', 1.0) for c in clusters], dtype=float)
        weights /= weights.sum()
        idx = self.rng.choice(len(clusters), p=weights)
        return idx, clusters[idx]

    def _blend_continuous(self, cluster_picks, attr_name, lo, hi):
        """Weighted Gaussian sample across 4 levels for a continuous attribute."""
        w = self.level_weights
        total = 0.0
        for level in ['group', 'subgroup', 'plan', 'product']:
            cluster = cluster_picks[level]
            params = cluster.get('continuous', {}).get(attr_name, {'mean': 40, 'std': 10})
            sample = self.rng.normal(params['mean'], params['std'])
            total += w[level] * sample
        return float(np.clip(total, lo, hi))

    def _blend_categorical(self, cluster_picks, attr_name):
        """Weighted blend of categorical distributions, then sample once."""
        w = self.level_weights
        merged = {}
        for level in ['group', 'subgroup', 'plan', 'product']:
            cluster = cluster_picks[level]
            dist = cluster.get('categorical', {}).get(attr_name, {})
            for val, prob in dist.items():
                merged[val] = merged.get(val, 0.0) + w[level] * prob
        if not merged:
            return 'M'  # fallback
        values = list(merged.keys())
        probs = np.array(list(merged.values()), dtype=float)
        probs /= probs.sum()
        return self.rng.choice(values, p=probs)

    # -- Enrollment ----------------------------------------------------

    def _generate_enrollments(self, orig_eff_dt, enrollment_cfg):
        """Generate enrollment periods for a subscriber."""
        term_rate = enrollment_cfg.get('term_rate', 0.10)
        reinstate_rate = enrollment_cfg.get('reinstate_rate', 0.40)
        term_reasons = enrollment_cfg.get('term_reasons', ['VTRM'])
        open_end = datetime(9999, 12, 31)

        if self.rng.random() < term_rate:
            gap_days = int(self.rng.uniform(90, 365))
            term_dt = orig_eff_dt + timedelta(days=gap_days)
            trsn = self.rng.choice(term_reasons)
            periods = [(orig_eff_dt, term_dt, trsn)]
            if self.rng.random() < reinstate_rate:
                re_eff = term_dt + timedelta(days=1)
                periods.append((re_eff, open_end, ''))
            return periods
        return [(orig_eff_dt, open_end, '')]

    # -- Row Building --------------------------------------------------

    def _make_member(self, *, meme_ck, sbsb_ck, sfx, rel, last_name, first_name,
                     mid_init, sex, birth_dt, ssn, eff_dt, marital,
                     race, ethn, age, tenure):
        return {
            'meme_ck': meme_ck, 'sbsb_ck': sbsb_ck, 'sfx': sfx, 'rel': rel,
            'last_name': last_name, 'first_name': first_name, 'mid_init': mid_init,
            'sex': sex, 'birth_dt': birth_dt, 'ssn': ssn,
            'eff_dt': eff_dt, 'marital': marital,
            'race': race, 'ethn': ethn,
            'age': age, 'tenure': tenure,
        }

    def _build_row(self, member, sbsb, group, sg, eff_dt, term_dt, trsn, pd_detail):
        fmt = lambda dt: dt.strftime('%Y-%m-%d')
        return [
            member['meme_ck'],                      # MEME_CK
            member['sbsb_ck'],                      # SBSB_CK
            group['grgr_ck'],                       # GRGR_CK
            member['sfx'],                          # MEME_SFX
            member['rel'],                          # MEME_REL
            member['last_name'],                    # MEME_LAST_NAME
            member['first_name'],                   # MEME_FIRST_NAME
            member['mid_init'],                     # MEME_MID_INIT
            member['sex'],                          # MEME_SEX
            fmt(member['birth_dt']),                # MEME_BIRTH_DT
            member['ssn'],                          # MEME_SSN
            'AC',                                   # MEME_MCTR_STS
            fmt(member['eff_dt']),                  # MEME_ORIG_EFF_DT
            member['marital'],                      # MEME_MARITAL_STATUS
            '',                                     # MEME_MEDCD_NO
            '',                                     # MEME_HICN
            member['race'],                         # MEME_MCTR_RACE_NVL
            member['ethn'],                         # MEME_MCTR_ETHN_NVL
            sbsb['sbsb_id'],                        # SBSB_ID
            sbsb['sbsb_last_name'],                 # SBSB_LAST_NAME
            sbsb['sbsb_first_name'],                # SBSB_FIRST_NAME
            fmt(sbsb['sbsb_orig_eff_dt']),          # SBSB_ORIG_EFF_DT
            sbsb['sbsb_mctr_sts'],                  # SBSB_MCTR_STS
            sbsb['sbsb_employ_id'],                 # SBSB_EMPLOY_ID
            sg['sgsg_ck'],                          # SGSG_CK
            fmt(eff_dt),                            # SBSG_EFF_DT
            fmt(term_dt),                           # SBSG_TERM_DT
            trsn,                                   # SBSG_MCTR_TRSN
            sg['sgsg_id'],                          # SGSG_ID
            sg['sgsg_name'],                        # SGSG_NAME
            sg['cscs_id'],                          # CSCS_ID
            sg['sgsg_state'],                       # SGSG_STATE
            sg['sgsg_sts'],                         # SGSG_STS
            fmt(sg['sgsg_orig_eff_dt']),            # SGSG_ORIG_EFF_DT
            fmt(sg['sgsg_term_dt']),                # SGSG_TERM_DT
            group['grgr_id'],                       # GRGR_ID
            group['grgr_name'],                     # GRGR_NAME
            group['grgr_state'],                    # GRGR_STATE
            group['grgr_county'],                   # GRGR_COUNTY
            group['grgr_sts'],                      # GRGR_STS
            fmt(group['grgr_orig_eff_dt']),         # GRGR_ORIG_EFF_DT
            fmt(group['grgr_term_dt']),             # GRGR_TERM_DT
            group['grgr_mctr_type'],                # GRGR_MCTR_TYPE
            group['pagr_ck'],                       # PAGR_CK
            pd_detail['cspi_id'],                   # CSPI_ID
            pd_detail['cspd_cat'],                  # CSPD_CAT
            fmt(pd_detail['cspi_eff_dt']),          # CSPI_EFF_DT
            fmt(pd_detail['cspi_term_dt']),         # CSPI_TERM_DT
            pd_detail['pdpd_id'],                   # PDPD_ID
            pd_detail['cspi_sel_ind'],              # CSPI_SEL_IND
            pd_detail['cspi_hios_id_nvl'],          # CSPI_HIOS_ID_NVL
            pd_detail['cspd_cat_desc'],             # CSPD_CAT_DESC
            pd_detail['cspd_type'],                 # CSPD_TYPE
            pd_detail['lobd_id'],                   # LOBD_ID
            pd_detail['pdpd_risk_ind'],             # PDPD_RISK_IND
            pd_detail['pdpd_mctr_ccat'],            # PDPD_MCTR_CCAT
            pd_detail['plds_desc'],                 # PLDS_DESC
            pd_detail['pdds_desc'],                 # PDDS_DESC
        ]

    # -- Sampling Helpers ----------------------------------------------

    def _sample_gaussian_clipped(self, params, lo, hi):
        val = self.rng.normal(params['mean'], params['std'])
        return float(np.clip(val, lo, hi))

    def _sample_categorical(self, dist):
        values = list(dist.keys())
        probs = np.array(list(dist.values()), dtype=float)
        probs /= probs.sum()
        return self.rng.choice(values, p=probs)

    def _age_to_date(self, age):
        return self.ref_date - timedelta(days=int(age * 365.25))

    def _tenure_to_date(self, months):
        return self.ref_date - timedelta(days=int(months * 30.44))

    def _next_meme_ck(self):
        v = self._meme_ck; self._meme_ck += 1; return v

    def _next_sbsb_ck(self):
        v = self._sbsb_ck; self._sbsb_ck += 1; return v

    def _next_ssn(self):
        v = self._ssn; self._ssn += 1; return f'{v:09d}'


def generate_auto_config(seed=42, total_subscribers=500, n_groups=6,
                         n_clusters_per_level=3):
    """Generate cluster config automatically using real lookup tables.

    Samples real groups from the plans lookup and builds cluster definitions
    with well-separated per-level centroids.
    """
    rng = np.random.default_rng(seed)
    plans_lookup, subgroups_lookup = _load_lookups()

    level_weights = {'group': 0.40, 'subgroup': 0.20, 'plan': 0.20, 'product': 0.20}
    k = n_clusters_per_level

    def _make_clusters(k, age_lo, age_hi, tenure_lo, tenure_hi, prefix,
                       include_family=False):
        """Generate k well-separated clusters in a given age/tenure range."""
        age_centers = np.linspace(age_lo, age_hi, k)
        tenure_centers = np.linspace(tenure_lo, tenure_hi, k)
        if k >= 3:
            tenure_order = list(range(k))
            tenure_order[-1], tenure_order[-2] = tenure_order[-2], tenure_order[-1]
        else:
            tenure_order = list(range(k))

        clusters = []
        sex_base = [
            {'M': 0.72, 'F': 0.28},
            {'M': 0.38, 'F': 0.62},
            {'M': 0.50, 'F': 0.50},
        ]
        marital_base = [
            {'S': 0.85, 'M': 0.15},
            {'M': 0.80, 'S': 0.10, 'D': 0.10},
            {'M': 0.50, 'S': 0.25, 'D': 0.15, 'W': 0.10},
        ]
        family_configs = [
            {'spouse_prob': 0.08, 'children': {'mean': 0.1, 'std': 0.3}},
            {'spouse_prob': 0.75, 'children': {'mean': 2.0, 'std': 0.8}},
            {'spouse_prob': 0.45, 'children': {'mean': 0.7, 'std': 0.6}},
        ]

        for i in range(k):
            age_std = rng.uniform(1.0, 2.0)
            tenure_std = rng.uniform(1.5, 3.0)
            c = {
                'name': f'{prefix}_{i}',
                'weight': round(1.0 / k, 4),
                'continuous': {
                    'age': {'mean': float(np.clip(age_centers[i], 19, 68)),
                            'std': float(age_std)},
                    'tenure_months': {
                        'mean': float(np.clip(tenure_centers[tenure_order[i]], 2, 350)),
                        'std': float(tenure_std)},
                },
                'categorical': {
                    'meme_sex': sex_base[i % len(sex_base)],
                    'meme_marital_status': marital_base[i % len(marital_base)],
                },
            }
            if include_family:
                c['family'] = family_configs[i % len(family_configs)]
            clusters.append(c)

        return clusters

    # -- Build SHARED cluster sets for subgroup/plan/product levels --
    shared_subgroup_clusters = _make_clusters(k, 22, 58, 5, 210, 'sg')
    shared_plan_clusters = {
        'M': _make_clusters(k, 28, 52, 20, 120, 'med'),
        'D': _make_clusters(k, 28, 52, 20, 120, 'den'),
        'C': _make_clusters(k, 28, 52, 20, 120, 'cm'),
    }

    # Collect unique LOBD_IDs from lookup
    all_lobd_ids = plans_lookup['LOBD_ID'].dropna().unique()
    shared_product_clusters = {}
    for lobd_id in all_lobd_ids:
        shared_product_clusters[lobd_id] = _make_clusters(
            k, 35, 45, 50, 90, lobd_id[:6])

    # -- Sample real groups from lookup --
    available_groups = sorted(plans_lookup['GRGR_CK'].unique())
    selected_grgr_cks = list(rng.choice(
        available_groups, size=min(n_groups, len(available_groups)), replace=False))

    groups = []
    for g_i, grgr_ck in enumerate(selected_grgr_cks):
        grgr_ck = int(grgr_ck)
        grp_plans = plans_lookup[plans_lookup['GRGR_CK'] == grgr_ck]
        grp_ref = grp_plans.iloc[0]
        target_subs = int(rng.integers(40, 130))

        # Per-group clusters with shifted centroids
        age_shift = rng.uniform(-8, 8)
        tenure_shift = rng.uniform(-25, 25)
        group_clusters = _make_clusters(
            k, 22 + age_shift, 60 + age_shift,
            5 + tenure_shift, 180 + tenure_shift,
            f'g{g_i}', include_family=True)

        # Get real subgroups for this group
        grp_sgs = subgroups_lookup[
            (subgroups_lookup['GRGR_CK'] == grgr_ck) &
            (subgroups_lookup['SGSG_STS'] == 'AC')
        ]
        if len(grp_sgs) == 0:
            grp_sgs = subgroups_lookup[subgroups_lookup['GRGR_CK'] == grgr_ck]
        n_subgroups = min(2, len(grp_sgs))
        if n_subgroups == 0:
            continue
        selected_sgs = grp_sgs.head(n_subgroups)

        # Get real CSPI_IDs per CSPD_CAT (pick 1-2 per category)
        cat_plans = {}
        for cat in sorted(grp_plans['CSPD_CAT'].unique()):
            cat_rows = grp_plans[grp_plans['CSPD_CAT'] == cat].drop_duplicates(
                subset=['CSPI_ID'])
            # Prefer active plans
            active = cat_rows[cat_rows['CSPI_TERM_DT'].astype(str).str.contains('9999')]
            if len(active) > 0:
                cat_rows = active
            cat_plans[cat] = list(cat_rows['CSPI_ID'].head(2))

        subgroups = []
        for _, sg_row in selected_sgs.iterrows():
            sg_plans = []
            for cat, cspi_ids in cat_plans.items():
                for cspi_id in cspi_ids:
                    pl_row = grp_plans[grp_plans['CSPI_ID'] == cspi_id].iloc[0]
                    lobd_id = _safe_str(pl_row['LOBD_ID'])
                    prod_clusters = shared_product_clusters.get(
                        lobd_id, _make_clusters(k, 35, 45, 50, 90, 'dflt'))
                    sg_plans.append({
                        'cspi_id': cspi_id,
                        'member_clusters': shared_plan_clusters.get(
                            cat, shared_plan_clusters['M']),
                        'products': [{
                            'lobd_id': lobd_id,
                            'member_clusters': prod_clusters,
                        }],
                    })

            subgroups.append({
                'sgsg_ck': int(sg_row['SGSG_CK']),
                'member_clusters': shared_subgroup_clusters,
                'plans': sg_plans,
            })

        groups.append({
            'grgr_ck': grgr_ck,
            'target_subscribers': target_subs,
            'member_clusters': group_clusters,
            'subgroups': subgroups,
        })

    return {
        'seed': seed,
        'reference_date': '2025-01-01',
        'total_subscribers': total_subscribers,
        'level_weights': level_weights,
        'groups': groups,
        'enrollment': {
            'term_rate': 0.10,
            'reinstate_rate': 0.40,
            'term_reasons': ['VTRM', 'NPAY', 'MOVE'],
        },
        'plan_enrollment_weights': {
            'M': 1.0,
            'D': 0.60,
            'C': 0.30,
        },
    }
