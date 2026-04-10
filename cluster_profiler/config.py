"""Constants and column definitions for the cluster profiler.

Architecture
------------
This tool reads from three denormalized models, populated by a separate
ETL/data-scoop process from source tables (e.g., FACETS). The tool never
accesses source tables directly.

  Member Denorm   → groups, subgroups, products, plans, benefits, demographics
  Provider Denorm → NPIs, specialties, networks, locations, geographic coverage
  Claims Denorm   → ICD/CPT correlations, adjudication patterns, billed/paid amounts

Generated synthetic data is for export/download only — it is never written
back into the denormalized models or source tables.
"""

# ── Denormalized Model Paths (read-only source of truth) ─────────────────────

MEMBER_DENORM_PATH = 'data/source/MEMBER_GROUP_PLAN_FLAT_generated.csv'
MEMBER_LABELS_PATH = 'data/source/MEMBER_GROUP_PLAN_FLAT_generated_labels.csv'

# Provider and Claims denorms — set to None until data scoop delivers them
PROVIDER_DENORM_PATH = None   # e.g., 'data/source/PROVIDER_DENORM.csv'
CLAIMS_DENORM_PATH = None     # e.g., 'data/source/CLAIMS_DENORM.csv'

DEFAULT_REFERENCE_DATE = '2025-01-01'

# ── Pattern Analysis Features ────────────────────────────────────────────────

CONTINUOUS_FEATURES = ['_age', '_tenure']
CATEGORICAL_FEATURES = ['MEME_SEX', 'MEME_MARITAL_STATUS']

DESCRIPTION_COLUMNS = [
    'GRGR_NAME', 'SGSG_NAME', 'CSPD_CAT_DESC', 'PLDS_DESC', 'PDDS_DESC',
]

FILTER_COLUMNS = {
    'grgr_ck': 'GRGR_CK',
    'sgsg_ck': 'SGSG_CK',
    'cspd_cat': 'CSPD_CAT',
    'lobd_id': 'LOBD_ID',
}

CONTINUOUS_WEIGHT = 2.0

LABEL_CLUSTER_COLUMNS = {
    'group': 'group_cluster_idx',
    'subgroup': 'subgroup_cluster_idx',
    'plan': 'plan_cluster_idx',
    'product': 'product_cluster_idx',
}

# ── Synthetic Data Generation Rules ──────────────────────────────────────────
# What gets generated (synthetic PII):
#   Names, DOBs, SSNs, Member IDs, addresses
#
# What gets reused from denorms (reference/configuration data):
#   NPIs, Provider IDs        → from Provider Denorm
#   ICD/CPT distributions     → from Claims Denorm
#   Group/Subgroup/Plan/Product/Benefit keys → from Member Denorm
#   LOBD_ID, CSCS_ID, CSPD_CAT → from Member Denorm
