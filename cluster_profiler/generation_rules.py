"""Rule-based and lookup-driven data generation engine.

SQL Server only. Lookups read from sdm.lkp_* tables.
No AI in the generation path.
"""

from typing import Optional

import numpy as np
import pandas as pd
import pyodbc

from . import db
from .config import get_connection_string, SQL_SCHEMA

# SQL Server lookup table name mapping
_SQL_LOOKUP_MAP = {
    "first_names": "lkp_first_names",
    "last_names": "lkp_last_names",
    "street_names": "lkp_street_names",
    "zip_city_state": "lkp_zip_city_state",
}

_lookup_cache = {}


def _load_lookup(name: str) -> pd.DataFrame:
    """Load a lookup by name from SQL Server. Cached."""
    if name in _lookup_cache:
        return _lookup_cache[name]

    sql_table = _SQL_LOOKUP_MAP.get(name)
    if not sql_table:
        raise ValueError(f"Unknown lookup: '{name}'. Available: {list(_SQL_LOOKUP_MAP.keys())}")

    conn = pyodbc.connect(get_connection_string())
    df = pd.read_sql(f"SELECT * FROM {SQL_SCHEMA}.{sql_table}", conn)
    conn.close()

    drop_cols = [c for c in ("id", "created_at") if c in df.columns]
    df = df.drop(columns=drop_cols)
    _lookup_cache[name] = df
    return df


def clear_lookup_cache():
    _lookup_cache.clear()


def get_available_lookups() -> list[str]:
    """List available lookup names from SQL Server."""
    lookups = set()
    conn = pyodbc.connect(get_connection_string())
    cur = conn.cursor()
    cur.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE 'lkp_%'",
        (SQL_SCHEMA,)
    )
    for row in cur.fetchall():
        short_name = row[0].replace("lkp_", "", 1)
        lookups.add(short_name)
    conn.close()
    return sorted(lookups)


# ── Rule-based generators ────────────────────────────────────────────────────

class SequenceCounter:
    _counters = {}

    @classmethod
    def next(cls, field_name: str, start: int = 1) -> int:
        if field_name not in cls._counters:
            cls._counters[field_name] = start
        val = cls._counters[field_name]
        cls._counters[field_name] += 1
        return val

    @classmethod
    def reset(cls, field_name: str = None):
        if field_name:
            cls._counters.pop(field_name, None)
        else:
            cls._counters.clear()


def generate_id(rule: dict, n: int = 1) -> list[str]:
    """Generate n ID values using the rule configuration."""
    pfx = rule.get("prefix", "") or ""
    pstfx = rule.get("postfix", "") or ""
    length = rule.get("length", 0) or 0
    start = rule.get("start_value", 1) or 1
    method = rule.get("gen_method", "sequential")

    num_width = max(1, length - len(pfx) - len(pstfx)) if length > 0 else 0

    results = []
    for _ in range(n):
        if method == "constant":
            result = pfx
        elif method == "sequential":
            val = SequenceCounter.next(rule["field_name"], start)
            num_str = str(val).zfill(num_width) if length > 0 else str(val)
            result = f"{pfx}{num_str}{pstfx}"
        elif method == "random":
            rng = np.random.default_rng()
            max_val = 10 ** max(1, num_width) - 1
            val = int(rng.integers(1, max(2, max_val)))
            num_str = str(val).zfill(num_width) if length > 0 else str(val)
            result = f"{pfx}{num_str}{pstfx}"
        else:
            val = SequenceCounter.next(rule["field_name"], start)
            num_str = str(val).zfill(num_width) if length > 0 else str(val)
            result = f"{pfx}{num_str}{pstfx}"

        if length > 0 and len(result) > length:
            result = result[:length]

        results.append(result)
    return results


def generate_names(n: int, gender: str = None, rng=None) -> list[tuple[str, str]]:
    """Generate n (first_name, last_name) tuples from lookups."""
    if rng is None:
        rng = np.random.default_rng()

    first_df = _load_lookup("first_names")
    last_df = _load_lookup("last_names")

    if gender and "gender" in first_df.columns:
        filtered = first_df[first_df["gender"] == gender]
        if not filtered.empty:
            first_df = filtered

    first_names = first_df["name"].values
    last_names = last_df["name"].values

    first_picks = rng.choice(first_names, size=n, replace=True)
    last_picks = rng.choice(last_names, size=n, replace=True)

    return list(zip(first_picks, last_picks))


def generate_email(first_name: str, last_name: str, domain: str = None, index: int = 0) -> str:
    """Generate an email address from name components."""
    if domain is None:
        rule = db.get_generation_rule("EMAIL")
        domain = rule.get("domain", "caresource.com") if rule else "caresource.com"

    first_initial = first_name[0].lower() if first_name else "x"
    clean_last = last_name.lower().replace(" ", "").replace("'", "")
    suffix = str(index) if index > 0 else ""
    return f"{first_initial}{clean_last}{suffix}@{domain}"


def generate_addresses(n: int, state_filter: str = None, rng=None) -> list[dict]:
    """Generate n address records from lookups."""
    if rng is None:
        rng = np.random.default_rng()

    street_df = _load_lookup("street_names")
    zip_df = _load_lookup("zip_city_state")

    if state_filter and "state" in zip_df.columns:
        filtered = zip_df[zip_df["state"] == state_filter]
        if not filtered.empty:
            zip_df = filtered

    streets = street_df.values
    zips = zip_df.values

    street2_rule = db.get_generation_rule("STREET_2")
    street2_prob = 0.2

    addresses = []
    for _ in range(n):
        zip_row = zips[rng.integers(0, len(zips))]
        zip_code = str(zip_row[0])
        city = str(zip_row[1])
        state = str(zip_row[2])
        county = str(zip_row[3]) if len(zip_row) > 3 else ""

        street_row = streets[rng.integers(0, len(streets))]
        house_num = int(rng.integers(100, 9999))
        street_1 = f"{house_num} {street_row[0]} {street_row[1]}"

        street_2 = ""
        if rng.random() < street2_prob:
            apt_num = int(rng.integers(1, 999))
            street_2 = f"Apt {apt_num}"

        addresses.append({
            "street_1": street_1, "street_2": street_2,
            "zip": zip_code, "city": city, "state": state, "county": county,
        })

    return addresses


def generate_ssn(n: int, rule: dict = None) -> list[str]:
    """Generate n synthetic SSNs using the invalid 900-999 range."""
    if rule is None:
        rule = db.get_generation_rule("MEME_SSN") or {
            "field_name": "MEME_SSN", "prefix": "9", "start_value": 10000000,
            "gen_method": "sequential", "length": 9,
        }
    return generate_id(rule, n)


def get_all_rules_as_dict() -> dict:
    """Load all generation rules indexed by field_name."""
    rules = db.get_generation_rules()
    return {r["field_name"]: r for r in rules}
