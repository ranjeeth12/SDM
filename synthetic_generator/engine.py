"""Core generation engine for MEMBER_GROUP_PLAN_FLAT synthetic data."""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from schema import (
    COLUMNS, PLAN_TYPES, LAST_NAMES, MALE_FIRST, FEMALE_FIRST,
    CHILD_MALE, CHILD_FEMALE, MID_INITIALS, RACE_CODES, ETHN_CODES,
)


class SyntheticGenerator:
    """Generates synthetic MEMBER_GROUP_PLAN_FLAT data with embedded cluster structure."""

    def __init__(self, config: dict):
        self.config = config
        self.rng = np.random.default_rng(config.get('seed', 42))
        self.ref_date = datetime.strptime(
            config.get('reference_date', '2025-01-01'), '%Y-%m-%d'
        )
        self._meme_ck = 5001
        self._sbsb_ck = 3001
        self._grgr_ck = 1001
        self._sgsg_ck = 2001
        self._ssn = 900000001

    def generate(self):
        """Generate the full dataset. Returns (data_df, labels_df)."""
        groups = self._build_groups()
        member_clusters = self.config['member_clusters']
        plan_profiles = self.config['plan_profiles']
        enrollment_cfg = self.config.get('enrollment', {})
        total_subs = self.config.get('total_subscribers', 500)

        # Distribute subscribers across groups proportional to group subscriber_count
        group_capacities = np.array([g['target_subs'] for g in groups], dtype=float)
        group_probs = group_capacities / group_capacities.sum()

        all_rows = []
        all_labels = []

        for _ in range(total_subs):
            # Pick group
            g_idx = self.rng.choice(len(groups), p=group_probs)
            group = groups[g_idx]

            # Pick member cluster using group-specific weights
            weights = np.array(group['member_cluster_weights'])
            cluster_idx = self.rng.choice(len(member_clusters), p=weights)
            cluster = member_clusters[cluster_idx]

            # Generate subscriber
            sbsb_ck = self._next_sbsb_ck()
            sub_age = self._sample_gaussian_clipped(cluster['continuous']['age'], 18, 70)
            sub_tenure = self._sample_gaussian_clipped(cluster['continuous']['tenure_months'], 1, 360)
            sub_sex = self._sample_categorical(cluster['categorical']['meme_sex'])
            sub_marital = self._sample_categorical(cluster['categorical']['meme_marital_status'])
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

            # Build family members list
            family = [self._make_member(
                meme_ck=sub_meme_ck, sbsb_ck=sbsb_ck, sfx=1, rel='M',
                last_name=sub_last, first_name=sub_first, mid_init=sub_mid,
                sex=sub_sex, birth_dt=sub_birth_dt, ssn=sub_ssn,
                eff_dt=sub_eff_dt, marital=sub_marital,
                race=sub_race, ethn=sub_ethn,
                age=sub_age, tenure=sub_tenure,
                cluster_name=cluster['name'], cluster_idx=cluster_idx,
                group_cluster=group['group_cluster_name'],
            )]

            sfx = 2
            family_cfg = cluster.get('family', {})

            # Spouse
            if self.rng.random() < family_cfg.get('spouse_prob', 0):
                sp_sex = 'F' if sub_sex == 'M' else 'M'
                sp_age = max(18, sub_age + self.rng.normal(0, 3))
                sp_first = self.rng.choice(MALE_FIRST if sp_sex == 'M' else FEMALE_FIRST)
                family.append(self._make_member(
                    meme_ck=self._next_meme_ck(), sbsb_ck=sbsb_ck, sfx=sfx, rel='S',
                    last_name=sub_last, first_name=sp_first,
                    mid_init=self.rng.choice(MID_INITIALS),
                    sex=sp_sex, birth_dt=self._age_to_date(sp_age),
                    ssn=self._next_ssn(), eff_dt=sub_eff_dt, marital='M',
                    race=sub_race, ethn=sub_ethn,
                    age=sp_age, tenure=sub_tenure,
                    cluster_name=cluster['name'], cluster_idx=cluster_idx,
                    group_cluster=group['group_cluster_name'],
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
                    cluster_name=cluster['name'], cluster_idx=cluster_idx,
                    group_cluster=group['group_cluster_name'],
                ))
                sfx += 1

            # Determine plans for this family
            plan_pref = cluster.get('plan_preference', 'medical_only')
            preferred_plans = plan_profiles.get(plan_pref, ['medical'])
            available_plans = group['subgroup']['plans']
            active_plans = [p for p in preferred_plans if p in available_plans]
            if not active_plans:
                active_plans = [available_plans[0]]

            # Generate enrollment periods
            enrollment_periods = self._generate_enrollments(sub_eff_dt, enrollment_cfg)

            # Expand: member × enrollment_period × plan
            sg = group['subgroup']
            for member in family:
                for eff_dt, term_dt, trsn in enrollment_periods:
                    for plan_name in active_plans:
                        row = self._build_row(
                            member, subscriber_info, group, sg,
                            eff_dt, term_dt, trsn, plan_name,
                        )
                        all_rows.append(row)

                all_labels.append({
                    'MEME_CK': member['meme_ck'],
                    'SBSB_CK': sbsb_ck,
                    'member_cluster': cluster['name'],
                    'member_cluster_idx': cluster_idx,
                    'group_cluster': group['group_cluster_name'],
                    'group_cluster_idx': group['group_cluster_idx'],
                    'age': member['age'],
                    'tenure_months': member['tenure'],
                    'meme_rel': member['rel'],
                })

        df = pd.DataFrame(all_rows, columns=COLUMNS)
        labels_df = pd.DataFrame(all_labels)
        return df, labels_df

    # ── Group Building ───────────────────────────────────────────────

    def _build_groups(self):
        """Build group records from group_clusters config."""
        group_clusters = self.config['group_clusters']
        prefixes = list(self.config.get('group_name_prefixes', ['Group']))
        self.rng.shuffle(prefixes)
        prefix_idx = 0
        groups = []

        for gc_idx, gc in enumerate(group_clusters):
            count = gc.get('count', 3)
            attrs = gc['attributes']
            templates = gc.get('name_templates', ['{prefix} Corp'])

            for i in range(count):
                grgr_ck = self._next_grgr_ck()
                prefix = prefixes[prefix_idx % len(prefixes)]
                prefix_idx += 1
                template = templates[i % len(templates)]
                grgr_name = template.format(prefix=prefix)

                orig_year = int(round(self._sample_gaussian_clipped(
                    attrs['grgr_orig_year'], 2010, 2024)))
                orig_dt = datetime(orig_year, 1, 1)

                target_subs = max(10, int(round(
                    self.rng.normal(attrs['subscriber_count']['mean'],
                                    attrs['subscriber_count']['std']))))

                # Build member_cluster_weights aligned to member_clusters order
                mc_names = [mc['name'] for mc in self.config['member_clusters']]
                weight_map = gc.get('member_cluster_weights', {})
                mc_weights = np.array([weight_map.get(n, 0.0) for n in mc_names])
                if mc_weights.sum() == 0:
                    mc_weights = np.ones(len(mc_names))
                mc_weights = mc_weights / mc_weights.sum()

                # Build subgroup and plan details
                sg_cfg = gc.get('subgroup', {'plans': ['medical']})
                sgsg_ck = self._next_sgsg_ck()
                plan_details = {}
                for k, plan_name in enumerate(sg_cfg['plans']):
                    pt = PLAN_TYPES[plan_name]
                    cspi_id = f'CSPI{grgr_ck % 100:02d}{k+1:02d}'
                    pdpd_id = f'{pt["lobd_id"]}P{grgr_ck % 100:02d}{k+1:02d}'
                    hios = f'12345{attrs["grgr_state"]}{grgr_ck % 100:03d}{k+1:04d}' if pt['has_hios'] else ''
                    plan_details[plan_name] = {
                        'cspi_id': cspi_id,
                        'cspd_cat': pt['cspd_cat'],
                        'cspi_eff_dt': orig_dt,
                        'cspi_term_dt': datetime(9999, 12, 31),
                        'pdpd_id': pdpd_id,
                        'cspi_sel_ind': 'Y',
                        'cspi_hios_id_nvl': hios,
                        'cspd_cat_desc': pt['cspd_cat_desc'],
                        'cspd_type': pt['cspd_type'],
                        'lobd_id': pt['lobd_id'],
                    }

                groups.append({
                    'grgr_ck': grgr_ck,
                    'grgr_id': f'GRP{grgr_ck:05d}',
                    'grgr_name': grgr_name,
                    'grgr_state': attrs['grgr_state'],
                    'grgr_county': attrs['grgr_county'],
                    'grgr_sts': 'AC',
                    'grgr_orig_eff_dt': orig_dt,
                    'grgr_term_dt': datetime(9999, 12, 31),
                    'grgr_mctr_type': attrs['grgr_mctr_type'],
                    'pagr_ck': 0,
                    'target_subs': target_subs,
                    'member_cluster_weights': mc_weights,
                    'group_cluster_name': gc['name'],
                    'group_cluster_idx': gc_idx,
                    'subgroup': {
                        'sgsg_ck': sgsg_ck,
                        'sgsg_id': f'SG{sgsg_ck % 100:02d}',
                        'sgsg_name': f'{grgr_name} Standard',
                        'cscs_id': f'CS{sgsg_ck % 100:02d}',
                        'sgsg_state': attrs['grgr_state'],
                        'sgsg_sts': 'AC',
                        'sgsg_orig_eff_dt': orig_dt,
                        'sgsg_term_dt': datetime(9999, 12, 31),
                        'plans': sg_cfg['plans'],
                        'plan_details': plan_details,
                    },
                })

        return groups

    # ── Enrollment ───────────────────────────────────────────────────

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

    # ── Row Building ─────────────────────────────────────────────────

    def _make_member(self, *, meme_ck, sbsb_ck, sfx, rel, last_name, first_name,
                     mid_init, sex, birth_dt, ssn, eff_dt, marital,
                     race, ethn, age, tenure, cluster_name, cluster_idx,
                     group_cluster):
        return {
            'meme_ck': meme_ck, 'sbsb_ck': sbsb_ck, 'sfx': sfx, 'rel': rel,
            'last_name': last_name, 'first_name': first_name, 'mid_init': mid_init,
            'sex': sex, 'birth_dt': birth_dt, 'ssn': ssn,
            'eff_dt': eff_dt, 'marital': marital,
            'race': race, 'ethn': ethn,
            'age': age, 'tenure': tenure,
            'cluster_name': cluster_name, 'cluster_idx': cluster_idx,
            'group_cluster': group_cluster,
        }

    def _build_row(self, member, sbsb, group, sg, eff_dt, term_dt, trsn, plan_name):
        pd_detail = sg['plan_details'][plan_name]
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
            'N',                                    # PDPD_RISK_IND
            'COMM',                                 # PDPD_MCTR_CCAT
        ]

    # ── Sampling Helpers ─────────────────────────────────────────────

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

    def _next_grgr_ck(self):
        v = self._grgr_ck; self._grgr_ck += 1; return v

    def _next_sgsg_ck(self):
        v = self._sgsg_ck; self._sgsg_ck += 1; return v

    def _next_ssn(self):
        v = self._ssn; self._ssn += 1; return f'{v:09d}'


def generate_auto_config(seed=42, total_subscribers=500, n_member_clusters=5, n_group_clusters=3):
    """Generate cluster config automatically with well-separated centroids.

    Places member cluster centroids in age x tenure space with guaranteed
    separation > 2*max(std) on at least one continuous dimension.
    """
    rng = np.random.default_rng(seed)

    # Auto-place centroids in age(20-65) x tenure(1-200) space
    age_range = np.linspace(23, 60, n_member_clusters)
    tenure_range = np.linspace(4, 180, n_member_clusters)
    # Shuffle tenure to avoid perfect correlation with age
    tenure_order = list(range(n_member_clusters))
    # Keep some correlation but not perfect
    if n_member_clusters >= 3:
        tenure_order[-1], tenure_order[-2] = tenure_order[-2], tenure_order[-1]

    sex_ratios = [{'M': 0.5 + rng.uniform(-0.08, 0.08),
                   'F': 0.5 + rng.uniform(-0.08, 0.08)}
                  for _ in range(n_member_clusters)]

    marital_options = [
        {'S': 0.85, 'M': 0.15},
        {'M': 0.85, 'S': 0.15},
        {'M': 0.70, 'S': 0.15, 'D': 0.15},
        {'M': 0.60, 'S': 0.15, 'W': 0.10, 'D': 0.15},
        {'S': 0.95, 'M': 0.05},
    ]

    family_configs = [
        {'spouse_prob': 0.10, 'children': {'mean': 0.1, 'std': 0.3}},
        {'spouse_prob': 0.85, 'children': {'mean': 2.0, 'std': 0.8}},
        {'spouse_prob': 0.55, 'children': {'mean': 1.0, 'std': 0.8}},
        {'spouse_prob': 0.45, 'children': {'mean': 0.2, 'std': 0.4}},
        {'spouse_prob': 0.03, 'children': {'mean': 0, 'std': 0}},
    ]

    plan_prefs = ['medical_only', 'full_suite', 'medical_dental',
                  'medical_dental', 'medical_only']

    member_clusters = []
    for i in range(n_member_clusters):
        age_std = rng.uniform(1.5, 4.0)
        tenure_std = rng.uniform(2, max(3, tenure_range[tenure_order[i]] * 0.15))
        member_clusters.append({
            'name': f'cluster_{i}',
            'continuous': {
                'age': {'mean': float(age_range[i]), 'std': float(age_std)},
                'tenure_months': {'mean': float(tenure_range[tenure_order[i]]),
                                  'std': float(tenure_std)},
            },
            'categorical': {
                'meme_sex': sex_ratios[i % len(sex_ratios)],
                'meme_marital_status': marital_options[i % len(marital_options)],
            },
            'family': family_configs[i % len(family_configs)],
            'plan_preference': plan_prefs[i % len(plan_prefs)],
        })

    # Auto group clusters
    states = ['OH', 'IN', 'KY']
    counties = [['Franklin', 'Cuyahoga', 'Hamilton'],
                ['Marion', 'Allen', 'Lake'],
                ['Jefferson', 'Fayette', 'Kenton']]
    plan_sets = [['medical', 'dental', 'vision'], ['medical', 'dental'], ['medical']]

    group_clusters = []
    for j in range(n_group_clusters):
        mc_weights = {}
        base = rng.dirichlet(np.ones(n_member_clusters))
        for i, mc in enumerate(member_clusters):
            mc_weights[mc['name']] = float(base[i])

        group_clusters.append({
            'name': f'group_type_{j}',
            'weight': 1.0 / n_group_clusters,
            'count': max(2, int(round(rng.normal(3, 1)))),
            'attributes': {
                'subscriber_count': {'mean': int(rng.uniform(30, 120)),
                                     'std': int(rng.uniform(5, 20))},
                'grgr_state': states[j % len(states)],
                'grgr_county': rng.choice(counties[j % len(counties)]),
                'grgr_mctr_type': 'COMM',
                'grgr_orig_year': {'mean': int(rng.uniform(2014, 2022)), 'std': 2},
            },
            'name_templates': ['{prefix} Corp', '{prefix} Inc', '{prefix} LLC',
                               '{prefix} Group'],
            'subgroup': {'plans': plan_sets[j % len(plan_sets)]},
            'member_cluster_weights': mc_weights,
        })

    return {
        'seed': seed,
        'reference_date': '2025-01-01',
        'total_subscribers': total_subscribers,
        'group_clusters': group_clusters,
        'member_clusters': member_clusters,
        'plan_profiles': {
            'medical_only': ['medical'],
            'medical_dental': ['medical', 'dental'],
            'full_suite': ['medical', 'dental', 'vision'],
        },
        'enrollment': {
            'term_rate': 0.10,
            'reinstate_rate': 0.40,
            'term_reasons': ['VTRM', 'NPAY', 'MOVE'],
        },
        'group_name_prefixes': [
            'Buckeye', 'Great Lakes', 'Midwest', 'Heritage', 'Summit',
            'Cascade', 'Pinnacle', 'Horizon', 'Keystone', 'Frontier',
            'Liberty', 'Pioneer', 'Crestview', 'Sterling', 'Apex',
        ],
    }
