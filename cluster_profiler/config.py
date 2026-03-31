"""Constants and column definitions for the cluster profiler."""

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

DEFAULT_DATA_PATH = 'data/MEMBER_GROUP_PLAN_FLAT_generated.csv'
DEFAULT_LABELS_PATH = 'data/MEMBER_GROUP_PLAN_FLAT_generated_labels.csv'
DEFAULT_REFERENCE_DATE = '2025-01-01'

LABEL_CLUSTER_COLUMNS = {
    'group': 'group_cluster_idx',
    'subgroup': 'subgroup_cluster_idx',
    'plan': 'plan_cluster_idx',
    'product': 'product_cluster_idx',
}
