"""Keyword-driven pattern search.

Resolves user queries like "500 Ohio Medicaid dental claims for adults"
into matching patterns with weights, using the tag vocabulary for
synonym resolution.
"""

import re
from typing import Optional

from . import db


def parse_query(query: str) -> dict:
    """Parse a user query into structured components.

    Returns dict with:
        keywords: list of resolved tags
        volume: int or None
        data_type: 'members' | 'claims' | 'enrollments' | None
        raw_terms: original parsed terms
    """
    text = query.lower().strip()

    # Extract volume (e.g., "500", "10000", "10k", "200K")
    volume = None
    vol_match = re.search(r'\b(\d+)\s*k\b', text)
    if vol_match:
        volume = int(vol_match.group(1)) * 1000
        text = text[:vol_match.start()] + text[vol_match.end():]
    else:
        vol_match = re.search(r'\b(\d{2,})\b', text)
        if vol_match:
            volume = int(vol_match.group(1))
            text = text[:vol_match.start()] + text[vol_match.end():]

    # Extract data type
    data_type = None
    type_patterns = {
        'claims': r'\bclaims?\b',
        'enrollments': r'\benroll(?:ment|ments|ees?)?\b|834',
        'members': r'\bmembers?\b|subscribers?\b',
    }
    for dtype, pattern in type_patterns.items():
        if re.search(pattern, text):
            data_type = dtype
            text = re.sub(pattern, '', text)
            break

    # Remove common stop words
    stop_words = {
        'give', 'me', 'get', 'create', 'generate', 'make', 'need',
        'want', 'for', 'with', 'the', 'a', 'an', 'of', 'in', 'and',
        'some', 'data', 'test', 'please', 'can', 'you', 'i',
    }
    raw_terms = [
        w for w in re.findall(r'[\w\-\+]+', text)
        if w not in stop_words and len(w) > 1
    ]

    # Resolve synonyms
    keywords = db.resolve_synonyms(raw_terms) if raw_terms else []

    return {
        'keywords': keywords,
        'volume': volume,
        'data_type': data_type or 'members',
        'raw_terms': raw_terms,
    }


def search(query: str) -> dict:
    """Search for patterns matching a keyword query.

    Returns dict with:
        patterns: list of matching patterns (from DB)
        parsed: the parsed query structure
        total_members: sum of members across matching patterns
        weights: dict of pattern_id → weight for generation
    """
    parsed = parse_query(query)
    keywords = parsed['keywords']

    if not keywords:
        # No keywords → return all patterns (auto-mix)
        patterns = db.get_all_patterns()
    else:
        patterns = db.search_patterns_by_tags(keywords)

    if not patterns:
        return {
            'patterns': [],
            'parsed': parsed,
            'total_members': 0,
            'weights': {},
        }

    # Compute weights based on member count (proportional)
    total = sum(p['member_count'] for p in patterns)
    weights = {}
    for p in patterns:
        weights[p['id']] = round(p['member_count'] / total, 4) if total > 0 else 0

    return {
        'patterns': patterns,
        'parsed': parsed,
        'total_members': total,
        'weights': weights,
    }


def allocate_volume(weights: dict, total_volume: int) -> dict:
    """Distribute requested volume across patterns by weight.

    Returns dict of pattern_id → count, ensuring sum = total_volume.
    """
    if not weights:
        return {}

    allocation = {}
    remaining = total_volume

    sorted_ids = sorted(weights.keys(), key=lambda k: weights[k], reverse=True)
    for pid in sorted_ids[:-1]:
        count = max(1, round(weights[pid] * total_volume))
        allocation[pid] = count
        remaining -= count

    # Last pattern gets the remainder
    allocation[sorted_ids[-1]] = max(1, remaining)

    return allocation
