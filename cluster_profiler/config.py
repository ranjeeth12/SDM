"""Constants and column definitions for the cluster profiler.

Architecture
------------
This tool reads from SQL Server tables populated by a separate
ETL/data-scoop process from source tables (e.g., FACETS).

  sdm.member_denorm   → demographics, group/plan/benefit hierarchy
  sdm.lkp_*           → Governed lookup tables for synthetic generation
  sdm.patterns        → System-discovered pattern metadata
  sdm.generation_rules → Per-field generation configuration
"""

import os

# ── SQL Server Connection ────────────────────────────────────────────────────
# Override via environment variables or .env file

SQL_SERVER   = os.environ.get("SDM_SQL_SERVER", "localhost")
SQL_DATABASE = os.environ.get("SDM_SQL_DATABASE", "SDM_Platform")
SQL_SCHEMA   = os.environ.get("SDM_SQL_SCHEMA", "sdm")
SQL_USERNAME = os.environ.get("SDM_SQL_USERNAME", "")
SQL_PASSWORD = os.environ.get("SDM_SQL_PASSWORD", "")
SQL_DRIVER   = os.environ.get("SDM_SQL_DRIVER", "ODBC Driver 17 for SQL Server")


def get_connection_string():
    """Build pyodbc connection string."""
    if SQL_USERNAME:
        return (
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
    else:
        return (
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )


DEFAULT_REFERENCE_DATE = '2025-01-01'

# ── Legacy CSV paths (fallback if SQL Server unavailable) ────────────────────

MEMBER_DENORM_PATH = 'data/source/MEMBER_GROUP_PLAN_FLAT_generated.csv'
MEMBER_LABELS_PATH = 'data/source/MEMBER_GROUP_PLAN_FLAT_generated_labels.csv'
PROVIDER_DENORM_PATH = None
CLAIMS_DENORM_PATH = None

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
