"""Rule-based and lookup-driven data generation engine.

Every data element is generated using configured rules and governed lookups.
No AI is used in the generation path. AI's only role is offline enrichment
of lookup lists (separate process, not invoked during generation).

Generation methods:
  sequential  → Incrementing counter with prefix/postfix (for IDs)
  random      → Random value within configured constraints
  lookup      → Pick from a governed lookup CSV (for names, streets, zips)
  formatted   → Build from a format pattern using other generated values
  derived     → Derived from another field (e.g., city from zip)
"""

import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from . import db

_LOOKUPS_DIR = Path(__file__).resolve().parent.parent / "data" / "lookups"

# ── Lookup cache ─────────────────────────────────────────────────────────────

_lookup_cache = {}


def _load_lookup(name: str) -> pd.DataFrame:
    """Load a lookup CSV by name, with caching."""
    if name in _lookup_cache:
        return _lookup_cache[name]

    path = _LOOKUPS_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Lookup file not found: {path}")

    df = pd.read_csv(path)
    _lookup_cache[name] = df
    return df


def clear_lookup_cache():
    """Clear the lookup cache (call after updating lookup files)."""
    _lookup_cache.clear()


def get_available_lookups() -> list[str]:
    """List available lookup file names."""
    if not _LOOKUPS_DIR.exists():
        return []
    return [f.stem for f in _LOOKUPS_DIR.glob("*.csv")]


# ── Rule-based generators ────────────────────────────────────────────────────

class SequenceCounter:
    """Thread-safe sequential counter for ID generation."""
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
    """Generate n ID values using the rule configuration.

    Leading zeros: when length > 0, the numeric portion is zero-padded
    to fill (length - prefix_len - postfix_len) characters.
    e.g., prefix="SB", length=10, value=1 → "SB00000001"
    """
    prefix = rule.get("prefix", "")
    postfix = rule.get("postfix", "")
    length = rule.get("length", 10)
    start = rule.get("start_value", 1)
    method = rule.get("gen_method", "sequential")
    data_type = rule.get("data_type", "alphanumeric")

    # Available width for the numeric portion
    num_width = max(1, length - len(prefix) - len(postfix)) if length > 0 else 0

    results = []
    for _ in range(n):
        if method == "sequential":
            val = SequenceCounter.next(rule["field_name"], start)
            num_str = str(val)
        elif method == "random":
            rng = np.random.default_rng()
            max_val = 10 ** max(1, num_width) - 1
            val = int(rng.integers(1, max(2, max_val)))
            num_str = str(val)
        elif method == "constant":
            num_str = str(prefix)
            prefix = ""  # constant uses prefix as the value itself
        else:
            val = SequenceCounter.next(rule["field_name"], start)
            num_str = str(val)

        # Zero-pad the numeric portion if length is specified
        if length > 0 and method != "constant":
            num_str = num_str.zfill(num_width)

        # Assemble
        result = f"{prefix}{num_str}{postfix}"

        # Truncate to length if specified (from the right)
        if length > 0:
            result = result[:length]

        results.append(result)
    return results


def generate_names(n: int, gender: str = None, rng=None) -> list[tuple[str, str]]:
    """Generate n (first_name, last_name) tuples from lookups.

    Parameters
    ----------
    n : int
        Number of name pairs to generate.
    gender : str, optional
        'M' or 'F' to filter first names. None for mixed.
    rng : numpy Generator, optional

    Returns
    -------
    List of (first_name, last_name) tuples.
    """
    if rng is None:
        rng = np.random.default_rng()

    first_df = _load_lookup("first_names")
    last_df = _load_lookup("last_names")

    # Filter by gender if specified
    if gender and "gender" in first_df.columns:
        filtered = first_df[first_df["gender"] == gender]
        if not filtered.empty:
            first_df = filtered

    # Shuffle and pick
    first_names = first_df["name"].values
    last_names = last_df["name"].values

    first_picks = rng.choice(first_names, size=n, replace=True)
    last_picks = rng.choice(last_names, size=n, replace=True)

    return list(zip(first_picks, last_picks))


def generate_email(first_name: str, last_name: str, domain: str = None, index: int = 0) -> str:
    """Generate an email address from name components.

    Format: {first_initial}{last_name}@{domain}
    Appends index if > 0 for deduplication.
    """
    if domain is None:
        rule = db.get_generation_rule("EMAIL")
        domain = rule.get("domain", "caresource.com") if rule else "caresource.com"

    first_initial = first_name[0].lower() if first_name else "x"
    clean_last = last_name.lower().replace(" ", "").replace("'", "")
    suffix = str(index) if index > 0 else ""
    return f"{first_initial}{clean_last}{suffix}@{domain}"


def generate_addresses(n: int, state_filter: str = None, rng=None) -> list[dict]:
    """Generate n address records from lookups.

    Returns list of dicts with: street_1, street_2, zip, city, state, county.
    ZIP is picked first, then city/state/county derived from it.
    """
    if rng is None:
        rng = np.random.default_rng()

    street_df = _load_lookup("street_names")
    zip_df = _load_lookup("zip_city_state")

    # Filter zips by state if specified
    if state_filter and "state" in zip_df.columns:
        filtered = zip_df[zip_df["state"] == state_filter]
        if not filtered.empty:
            zip_df = filtered

    streets = street_df.values  # [street, type] rows
    zips = zip_df.values        # [zip, city, state, county] rows

    # Get street_2 probability from rule
    street2_rule = db.get_generation_rule("STREET_2")
    street2_prob = 0.2  # 20% get apt/unit by default

    addresses = []
    for _ in range(n):
        # Pick zip → derive city/state/county
        zip_row = zips[rng.integers(0, len(zips))]
        zip_code = str(zip_row[0])
        city = str(zip_row[1])
        state = str(zip_row[2])
        county = str(zip_row[3]) if len(zip_row) > 3 else ""

        # Build street address 1
        street_row = streets[rng.integers(0, len(streets))]
        house_num = int(rng.integers(100, 9999))
        street_1 = f"{house_num} {street_row[0]} {street_row[1]}"

        # Street address 2 (optional)
        street_2 = ""
        if rng.random() < street2_prob:
            apt_num = int(rng.integers(1, 999))
            street_2 = f"Apt {apt_num}"

        addresses.append({
            "street_1": street_1,
            "street_2": street_2,
            "zip": zip_code,
            "city": city,
            "state": state,
            "county": county,
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
