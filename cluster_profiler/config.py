"""Constants and column definitions for the SDM cluster profiler.

SQL Server only — no CSV fallback.
"""

SQL_SERVER   = "ASCUSLAP20522\\SQLEXPRESS2"
SQL_DATABASE = "SDM_Platform"
SQL_SCHEMA   = "sdm"
SQL_DRIVER   = "ODBC Driver 17 for SQL Server"


def get_connection_string():
    """Build pyodbc connection string (Windows Auth)."""
    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )


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
