"""SQL Server backend for pattern storage, rules, tags, and keyword vocabulary.

Tables (all in sdm schema)
--------------------------
patterns          – Discovered patterns with metadata and contextual names.
pattern_rules     – Saved pattern rules (hierarchy + demographic filters).
pattern_tags      – Keyword tags associated with each pattern.
tag_vocabulary    – Governed tag vocabulary with synonyms and SME status.
generation_log    – Audit log of all data generation requests.
generation_rules  – Per-field generation configuration.
"""

import json
import pyodbc
from contextlib import contextmanager
from datetime import datetime

from .config import get_connection_string, SQL_SCHEMA

_SCHEMA = SQL_SCHEMA


@contextmanager
def get_connection():
    """Yield a pyodbc connection to SQL Server."""
    conn = pyodbc.connect(get_connection_string(), autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _dict_row(cursor):
    """Convert a cursor row to a dict."""
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, cursor.fetchone()))


def _dict_rows(cursor):
    """Convert all cursor rows to list of dicts."""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def init_db():
    """No-op for SQL Server — tables are created by the DDL script."""
    pass


# ── Pattern CRUD ─────────────────────────────────────────────────────────────

def _make_pattern_key(combo, cluster_id):
    """Deterministic key from hierarchy combo + cluster_id."""
    parts = [
        str(combo.get("grgr_ck") or "ALL"),
        str(combo.get("sgsg_ck") or "ALL"),
        str(combo.get("cspd_cat") or "ALL"),
        str(combo.get("lobd_id") or "ALL"),
        str(cluster_id),
    ]
    return "|".join(parts)


def upsert_pattern(combo, cluster_id, contextual_name, member_count,
                    pct_of_pop, silhouette, profile=None, ai_summary=None):
    """Insert or update a discovered pattern."""
    key = _make_pattern_key(combo, cluster_id)
    profile_json = json.dumps(profile, default=str) if profile else None

    with get_connection() as conn:
        cur = conn.cursor()

        # Check if pattern exists
        cur.execute(f"SELECT id FROM {_SCHEMA}.patterns WHERE pattern_key = ?", (key,))
        existing = cur.fetchone()

        if existing:
            cur.execute(f"""
                UPDATE {_SCHEMA}.patterns SET
                    contextual_name = ?,
                    member_count = ?,
                    pct_of_pop = ?,
                    silhouette = ?,
                    profile_json = ?,
                    ai_summary = COALESCE(?, ai_summary),
                    updated_at = GETDATE()
                WHERE pattern_key = ?
            """, (
                contextual_name, member_count, pct_of_pop, silhouette,
                profile_json, ai_summary, key,
            ))
            return existing[0]
        else:
            cur.execute(f"""
                INSERT INTO {_SCHEMA}.patterns
                    (pattern_key, contextual_name, grgr_ck, sgsg_ck, cspd_cat, lobd_id,
                     grgr_name, sgsg_name, cspd_cat_desc, plds_desc,
                     cluster_id, member_count, pct_of_pop, silhouette,
                     profile_json, ai_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                key, contextual_name,
                json.dumps(combo.get("grgr_ck")),
                json.dumps(combo.get("sgsg_ck")),
                json.dumps(combo.get("cspd_cat")),
                json.dumps(combo.get("lobd_id")),
                combo.get("grgr_name", ""),
                combo.get("sgsg_name", ""),
                combo.get("cspd_cat_desc", ""),
                combo.get("plds_desc", ""),
                cluster_id, member_count, pct_of_pop, silhouette,
                profile_json, ai_summary,
            ))
            # Get inserted ID
            cur.execute("SELECT @@IDENTITY")
            row = cur.fetchone()
            return int(row[0])


def get_pattern(pattern_id):
    """Fetch a single pattern by ID."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_SCHEMA}.patterns WHERE id = ?", (pattern_id,))
        if cur.description is None:
            return None
        row = cur.fetchone()
        if row is None:
            return None
        columns = [col[0] for col in cur.description]
        return dict(zip(columns, row))


def get_all_patterns():
    """Fetch all patterns ordered by member_count desc."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_SCHEMA}.patterns ORDER BY member_count DESC")
        return _dict_rows(cur)


def get_patterns_for_members(meme_cks):
    """Fetch all patterns (DB-level convenience — caller uses clustering for real lookup)."""
    return get_all_patterns()


# ── Pattern Rules ────────────────────────────────────────────────────────────

def save_rule(pattern_id, rule_name, rule_text, member_count=None, created_by="system"):
    """Save a pattern rule."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO {_SCHEMA}.pattern_rules (pattern_id, rule_name, rule_text, member_count, created_by)
            VALUES (?, ?, ?, ?, ?)
        """, (pattern_id, rule_name, rule_text, member_count, created_by))


def get_rules(pattern_id=None):
    """Fetch rules, optionally filtered by pattern."""
    with get_connection() as conn:
        cur = conn.cursor()
        if pattern_id:
            cur.execute(
                f"SELECT * FROM {_SCHEMA}.pattern_rules WHERE pattern_id = ? ORDER BY created_at DESC",
                (pattern_id,)
            )
        else:
            cur.execute(f"SELECT * FROM {_SCHEMA}.pattern_rules ORDER BY created_at DESC")
        return _dict_rows(cur)


# ── Pattern Tags ─────────────────────────────────────────────────────────────

def add_tags(pattern_id, tags, source="auto"):
    """Add multiple tags to a pattern. Skips duplicates."""
    with get_connection() as conn:
        cur = conn.cursor()
        for tag in tags:
            tag_clean = tag.lower().strip()
            # Check if exists first
            cur.execute(
                f"SELECT id FROM {_SCHEMA}.pattern_tags WHERE pattern_id = ? AND tag = ?",
                (pattern_id, tag_clean)
            )
            if cur.fetchone() is None:
                cur.execute(f"""
                    INSERT INTO {_SCHEMA}.pattern_tags (pattern_id, tag, tag_source)
                    VALUES (?, ?, ?)
                """, (pattern_id, tag_clean, source))


def get_tags(pattern_id):
    """Get all tags for a pattern."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT tag, tag_source, confidence, confirmed_by "
            f"FROM {_SCHEMA}.pattern_tags WHERE pattern_id = ? ORDER BY tag",
            (pattern_id,)
        )
        return _dict_rows(cur)


def confirm_tag(pattern_id, tag, confirmed_by="SME"):
    """Mark a tag as SME-confirmed."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE {_SCHEMA}.pattern_tags
            SET confirmed_by = ?, confirmed_at = GETDATE()
            WHERE pattern_id = ? AND tag = ?
        """, (confirmed_by, pattern_id, tag.lower().strip()))


def search_patterns_by_tags(tags):
    """Find patterns that match ALL given tags."""
    if not tags:
        return []
    clean = [t.lower().strip() for t in tags]
    placeholders = ",".join("?" * len(clean))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT p.*, COUNT(pt.tag) as match_count
            FROM {_SCHEMA}.patterns p
            JOIN {_SCHEMA}.pattern_tags pt ON p.id = pt.pattern_id
            WHERE pt.tag IN ({placeholders})
            GROUP BY p.id, p.pattern_key, p.contextual_name, p.grgr_ck, p.sgsg_ck,
                     p.cspd_cat, p.lobd_id, p.grgr_name, p.sgsg_name,
                     p.cspd_cat_desc, p.plds_desc, p.cluster_id, p.member_count,
                     p.pct_of_pop, p.silhouette, p.profile_json, p.ai_summary,
                     p.created_at, p.updated_at
            ORDER BY COUNT(pt.tag) DESC, p.member_count DESC
        """, clean)
        return _dict_rows(cur)


# ── Tag Vocabulary ───────────────────────────────────────────────────────────

def add_synonym(canonical_tag, synonym, category=None):
    """Add a synonym mapping to the vocabulary."""
    with get_connection() as conn:
        cur = conn.cursor()
        canonical = canonical_tag.lower()
        syn = synonym.lower()
        cur.execute(
            f"SELECT id FROM {_SCHEMA}.tag_vocabulary WHERE canonical_tag = ? AND synonym = ?",
            (canonical, syn)
        )
        if cur.fetchone() is None:
            cur.execute(f"""
                INSERT INTO {_SCHEMA}.tag_vocabulary (canonical_tag, synonym, category)
                VALUES (?, ?, ?)
            """, (canonical, syn, category))


def resolve_synonyms(terms):
    """Resolve a list of terms to canonical tags using the vocabulary."""
    if not terms:
        return []
    clean = [t.lower().strip() for t in terms]
    resolved = []

    with get_connection() as conn:
        cur = conn.cursor()
        for term in clean:
            cur.execute(
                f"SELECT canonical_tag FROM {_SCHEMA}.tag_vocabulary WHERE synonym = ?",
                (term,)
            )
            row = cur.fetchone()
            if row:
                resolved.append(row[0])
            else:
                resolved.append(term)

    return list(set(resolved))


def get_vocabulary():
    """Fetch the full tag vocabulary."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_SCHEMA}.tag_vocabulary ORDER BY canonical_tag, synonym")
        return _dict_rows(cur)


def seed_default_vocabulary():
    """Seed common synonym mappings for healthcare domain."""
    defaults = [
        ("pediatric", "pediatric", "age"), ("pediatric", "child", "age"),
        ("pediatric", "children", "age"), ("pediatric", "minor", "age"),
        ("pediatric", "under-18", "age"),
        ("adult", "adult", "age"), ("adult", "working-age", "age"),
        ("senior", "senior", "age"), ("senior", "elderly", "age"),
        ("senior", "65+", "age"), ("senior", "medicare-age", "age"),
        ("new-member", "new-member", "tenure"), ("new-member", "new", "tenure"),
        ("new-member", "recent", "tenure"),
        ("long-term", "long-term", "tenure"), ("long-term", "established", "tenure"),
        ("long-term", "loyal", "tenure"),
        ("family", "family", "family"), ("family", "with-dependents", "family"),
        ("single", "single", "family"), ("single", "individual", "family"),
        ("single", "no-dependents", "family"),
        ("dental", "dental", "plan"), ("dental", "dental-plan", "plan"),
        ("medical", "medical", "plan"), ("medical", "med", "plan"),
        ("vision", "vision", "plan"), ("vision", "eye", "plan"),
        ("medicaid", "medicaid", "lob"), ("medicare", "medicare", "lob"),
        ("medicare-advantage", "medicare-advantage", "lob"),
        ("medicare-advantage", "ma", "lob"),
        ("exchange", "exchange", "lob"), ("exchange", "marketplace", "lob"),
        ("exchange", "aca", "lob"),
        ("edge-case", "edge-case", "quality"), ("edge-case", "rare", "quality"),
        ("edge-case", "anomaly", "quality"), ("edge-case", "outlier", "quality"),
        ("edge-case", "unusual", "quality"),
        ("high-volume", "high-volume", "quality"), ("high-volume", "common", "quality"),
        ("high-volume", "dominant", "quality"),
        ("ohio", "ohio", "state"), ("ohio", "oh", "state"),
        ("kentucky", "kentucky", "state"), ("kentucky", "ky", "state"),
        ("indiana", "indiana", "state"), ("indiana", "in", "state"),
        ("georgia", "georgia", "state"), ("georgia", "ga", "state"),
        ("west-virginia", "west-virginia", "state"), ("west-virginia", "wv", "state"),
    ]
    for canonical, synonym, category in defaults:
        add_synonym(canonical, synonym, category)


# ── Generation Log ───────────────────────────────────────────────────────────

def log_generation(pattern_ids, data_type, volume, output_path=None,
                   weighting=None, requested_by="user"):
    """Log a data generation event."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO {_SCHEMA}.generation_log
                (pattern_ids, data_type, volume, output_path, weighting, requested_by)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            json.dumps(pattern_ids),
            data_type, volume, output_path,
            json.dumps(weighting) if weighting else None,
            requested_by,
        ))


def get_generation_history(limit=50):
    """Fetch recent generation events."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT TOP (?) * FROM {_SCHEMA}.generation_log ORDER BY created_at DESC",
            (limit,)
        )
        return _dict_rows(cur)


# ── Generation Rules ─────────────────────────────────────────────────────────

def upsert_generation_rule(field_name, field_label, field_category="id",
                            gen_method="sequential", data_type="alphanumeric",
                            length=10, prefix="", postfix="", start_value=1,
                            domain="", format_pattern="", lookup_source="",
                            active=True, updated_by="system"):
    """Insert or update a generation rule for a data element."""
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute(f"SELECT id FROM {_SCHEMA}.generation_rules WHERE field_name = ?", (field_name,))
        existing = cur.fetchone()

        if existing:
            cur.execute(f"""
                UPDATE {_SCHEMA}.generation_rules SET
                    field_label = ?, field_category = ?, gen_method = ?, data_type = ?,
                    length = ?, prefix = ?, postfix = ?, start_value = ?,
                    domain = ?, format_pattern = ?, lookup_source = ?,
                    active = ?, updated_by = ?, updated_at = GETDATE()
                WHERE field_name = ?
            """, (
                field_label, field_category, gen_method, data_type,
                length, prefix, postfix, start_value,
                domain, format_pattern, lookup_source,
                1 if active else 0, updated_by, field_name,
            ))
        else:
            cur.execute(f"""
                INSERT INTO {_SCHEMA}.generation_rules
                    (field_name, field_label, field_category, gen_method, data_type,
                     length, prefix, postfix, start_value, domain, format_pattern,
                     lookup_source, active, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                field_name, field_label, field_category, gen_method, data_type,
                length, prefix, postfix, start_value, domain, format_pattern,
                lookup_source, 1 if active else 0, updated_by,
            ))


def get_generation_rules():
    """Fetch all generation rules."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_SCHEMA}.generation_rules ORDER BY field_category, field_name")
        return _dict_rows(cur)


def get_generation_rule(field_name):
    """Fetch a single generation rule by field name."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_SCHEMA}.generation_rules WHERE field_name = ?", (field_name,))
        row = cur.fetchone()
        if row is None:
            return None
        columns = [col[0] for col in cur.description]
        return dict(zip(columns, row))


def delete_generation_rule(field_name):
    """Delete a generation rule."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {_SCHEMA}.generation_rules WHERE field_name = ?", (field_name,))


def seed_default_generation_rules():
    """Seed default generation rules for always-on data elements only."""
    defaults = [
        ("MEME_FIRST_NAME", "Member First Name", "default", "lookup", "text", 0, "", "", 0, "", "", "first_names"),
        ("MEME_LAST_NAME", "Member Last Name", "default", "lookup", "text", 0, "", "", 0, "", "", "last_names"),
        ("SBSB_FIRST_NAME", "Subscriber First Name", "default", "lookup", "text", 0, "", "", 0, "", "", "first_names"),
        ("SBSB_LAST_NAME", "Subscriber Last Name", "default", "lookup", "text", 0, "", "", 0, "", "", "last_names"),
        ("STREET_1", "Street Address 1", "default", "lookup", "text", 0, "", "", 0, "", "{number} {street} {type}", "street_names"),
        ("ZIP_CODE", "ZIP Code", "default", "lookup", "text", 5, "", "", 0, "", "", "zip_city_state"),
    ]
    for (field_name, label, category, method, dtype, length,
         prefix, postfix, start, domain, fmt, lookup) in defaults:
        upsert_generation_rule(
            field_name=field_name, field_label=label, field_category=category,
            gen_method=method, data_type=dtype, length=length,
            prefix=prefix, postfix=postfix, start_value=start,
            domain=domain, format_pattern=fmt, lookup_source=lookup,
        )


# ── Bootstrap ────────────────────────────────────────────────────────────────

def bootstrap():
    """Seed defaults. Safe to call multiple times.
    Tables must already exist (created by SDM_SQL_Server_Setup.sql).
    """
    seed_default_vocabulary()
    seed_default_generation_rules()
