"""SQLite backend for pattern storage, rules, tags, and keyword vocabulary.

Tables
------
patterns        – Discovered patterns with metadata and contextual names.
pattern_rules   – Saved pattern rules (hierarchy + demographic filters).
pattern_tags    – Keyword tags associated with each pattern.
tag_vocabulary  – Governed tag vocabulary with synonyms and SME status.
generation_log  – Audit log of all data generation requests.
"""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sdm.db"


def _ensure_dir():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_connection():
    """Yield a SQLite connection with row_factory enabled."""
    _ensure_dir()
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables if they don't exist."""
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS patterns (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_key     TEXT    NOT NULL UNIQUE,
                contextual_name TEXT    NOT NULL,
                grgr_ck         TEXT,
                sgsg_ck         TEXT,
                cspd_cat        TEXT,
                lobd_id         TEXT,
                grgr_name       TEXT,
                sgsg_name       TEXT,
                cspd_cat_desc   TEXT,
                plds_desc       TEXT,
                cluster_id      INTEGER NOT NULL,
                member_count    INTEGER NOT NULL,
                pct_of_pop      REAL,
                silhouette      REAL,
                profile_json    TEXT,
                ai_summary      TEXT,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS pattern_rules (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_id      INTEGER REFERENCES patterns(id),
                rule_name       TEXT    NOT NULL,
                rule_text       TEXT    NOT NULL,
                member_count    INTEGER,
                created_by      TEXT    DEFAULT 'system',
                created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS pattern_tags (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_id      INTEGER NOT NULL REFERENCES patterns(id),
                tag             TEXT    NOT NULL,
                tag_source      TEXT    NOT NULL DEFAULT 'auto',
                confidence      REAL    DEFAULT 1.0,
                confirmed_by    TEXT,
                confirmed_at    TEXT,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(pattern_id, tag)
            );

            CREATE TABLE IF NOT EXISTS tag_vocabulary (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_tag   TEXT    NOT NULL,
                synonym         TEXT    NOT NULL,
                category        TEXT,
                sme_confirmed   INTEGER DEFAULT 0,
                confirmed_by    TEXT,
                confirmed_at    TEXT,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(canonical_tag, synonym)
            );

            CREATE TABLE IF NOT EXISTS generation_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_ids     TEXT    NOT NULL,
                data_type       TEXT    NOT NULL,
                volume          INTEGER NOT NULL,
                output_path     TEXT,
                weighting       TEXT,
                requested_by    TEXT    DEFAULT 'user',
                created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_pattern_tags_tag
                ON pattern_tags(tag);
            CREATE INDEX IF NOT EXISTS idx_tag_vocab_synonym
                ON tag_vocabulary(synonym);
            CREATE INDEX IF NOT EXISTS idx_patterns_name
                ON patterns(contextual_name);
        """)


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
        conn.execute("""
            INSERT INTO patterns
                (pattern_key, contextual_name, grgr_ck, sgsg_ck, cspd_cat, lobd_id,
                 grgr_name, sgsg_name, cspd_cat_desc, plds_desc,
                 cluster_id, member_count, pct_of_pop, silhouette,
                 profile_json, ai_summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pattern_key) DO UPDATE SET
                contextual_name = excluded.contextual_name,
                member_count    = excluded.member_count,
                pct_of_pop      = excluded.pct_of_pop,
                silhouette      = excluded.silhouette,
                profile_json    = excluded.profile_json,
                ai_summary      = COALESCE(excluded.ai_summary, patterns.ai_summary),
                updated_at      = datetime('now')
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

        row = conn.execute(
            "SELECT id FROM patterns WHERE pattern_key = ?", (key,)
        ).fetchone()
        return row["id"]


def get_pattern(pattern_id):
    """Fetch a single pattern by ID."""
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM patterns WHERE id = ?", (pattern_id,)).fetchone()
        return dict(row) if row else None


def get_all_patterns():
    """Fetch all patterns ordered by member_count desc."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM patterns ORDER BY member_count DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def get_patterns_for_members(meme_cks):
    """Find all patterns whose profiles contain given member keys.

    Note: This queries the pattern_tags and profile_json for matches.
    For real-time reverse lookup, the caller should use the clustering
    engine directly. This is a DB-level convenience for persisted patterns.
    """
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM patterns ORDER BY member_count DESC").fetchall()
        return [dict(r) for r in rows]


# ── Pattern Rules ────────────────────────────────────────────────────────────

def save_rule(pattern_id, rule_name, rule_text, member_count=None, created_by="system"):
    """Save a pattern rule."""
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO pattern_rules (pattern_id, rule_name, rule_text, member_count, created_by)
            VALUES (?, ?, ?, ?, ?)
        """, (pattern_id, rule_name, rule_text, member_count, created_by))


def get_rules(pattern_id=None):
    """Fetch rules, optionally filtered by pattern."""
    with get_connection() as conn:
        if pattern_id:
            rows = conn.execute(
                "SELECT * FROM pattern_rules WHERE pattern_id = ? ORDER BY created_at DESC",
                (pattern_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM pattern_rules ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]


# ── Pattern Tags ─────────────────────────────────────────────────────────────

def add_tags(pattern_id, tags, source="auto"):
    """Add multiple tags to a pattern. Skips duplicates."""
    with get_connection() as conn:
        for tag in tags:
            conn.execute("""
                INSERT OR IGNORE INTO pattern_tags (pattern_id, tag, tag_source)
                VALUES (?, ?, ?)
            """, (pattern_id, tag.lower().strip(), source))


def get_tags(pattern_id):
    """Get all tags for a pattern."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT tag, tag_source, confidence, confirmed_by "
            "FROM pattern_tags WHERE pattern_id = ? ORDER BY tag",
            (pattern_id,)
        ).fetchall()
        return [dict(r) for r in rows]


def confirm_tag(pattern_id, tag, confirmed_by="SME"):
    """Mark a tag as SME-confirmed."""
    with get_connection() as conn:
        conn.execute("""
            UPDATE pattern_tags
            SET confirmed_by = ?, confirmed_at = datetime('now')
            WHERE pattern_id = ? AND tag = ?
        """, (confirmed_by, pattern_id, tag.lower().strip()))


def search_patterns_by_tags(tags):
    """Find patterns that match ALL given tags. Returns pattern IDs with match count."""
    if not tags:
        return []
    clean = [t.lower().strip() for t in tags]
    placeholders = ",".join("?" * len(clean))

    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT p.*, COUNT(pt.tag) as match_count
            FROM patterns p
            JOIN pattern_tags pt ON p.id = pt.pattern_id
            WHERE pt.tag IN ({placeholders})
            GROUP BY p.id
            ORDER BY match_count DESC, p.member_count DESC
        """, clean).fetchall()
        return [dict(r) for r in rows]


# ── Tag Vocabulary ───────────────────────────────────────────────────────────

def add_synonym(canonical_tag, synonym, category=None):
    """Add a synonym mapping to the vocabulary."""
    with get_connection() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO tag_vocabulary (canonical_tag, synonym, category)
            VALUES (?, ?, ?)
        """, (canonical_tag.lower(), synonym.lower(), category))


def resolve_synonyms(terms):
    """Resolve a list of terms to canonical tags using the vocabulary.

    Returns canonical tags for recognized synonyms, passes through
    unrecognized terms as-is.
    """
    if not terms:
        return []
    clean = [t.lower().strip() for t in terms]
    resolved = []

    with get_connection() as conn:
        for term in clean:
            row = conn.execute(
                "SELECT canonical_tag FROM tag_vocabulary WHERE synonym = ?",
                (term,)
            ).fetchone()
            if row:
                resolved.append(row["canonical_tag"])
            else:
                resolved.append(term)

    return list(set(resolved))


def get_vocabulary():
    """Fetch the full tag vocabulary."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM tag_vocabulary ORDER BY canonical_tag, synonym"
        ).fetchall()
        return [dict(r) for r in rows]


def seed_default_vocabulary():
    """Seed common synonym mappings for healthcare domain."""
    defaults = [
        # Age synonyms
        ("pediatric", "pediatric", "age"),
        ("pediatric", "child", "age"),
        ("pediatric", "children", "age"),
        ("pediatric", "minor", "age"),
        ("pediatric", "under-18", "age"),
        ("adult", "adult", "age"),
        ("adult", "working-age", "age"),
        ("senior", "senior", "age"),
        ("senior", "elderly", "age"),
        ("senior", "65+", "age"),
        ("senior", "medicare-age", "age"),
        # Tenure synonyms
        ("new-member", "new-member", "tenure"),
        ("new-member", "new", "tenure"),
        ("new-member", "recent", "tenure"),
        ("long-term", "long-term", "tenure"),
        ("long-term", "established", "tenure"),
        ("long-term", "loyal", "tenure"),
        # Family synonyms
        ("family", "family", "family"),
        ("family", "with-dependents", "family"),
        ("single", "single", "family"),
        ("single", "individual", "family"),
        ("single", "no-dependents", "family"),
        # Plan type synonyms
        ("dental", "dental", "plan"),
        ("dental", "dental-plan", "plan"),
        ("medical", "medical", "plan"),
        ("medical", "med", "plan"),
        ("vision", "vision", "plan"),
        ("vision", "eye", "plan"),
        # LOB synonyms
        ("medicaid", "medicaid", "lob"),
        ("medicare", "medicare", "lob"),
        ("medicare-advantage", "medicare-advantage", "lob"),
        ("medicare-advantage", "ma", "lob"),
        ("exchange", "exchange", "lob"),
        ("exchange", "marketplace", "lob"),
        ("exchange", "aca", "lob"),
        # Pattern quality synonyms
        ("edge-case", "edge-case", "quality"),
        ("edge-case", "rare", "quality"),
        ("edge-case", "anomaly", "quality"),
        ("edge-case", "outlier", "quality"),
        ("edge-case", "unusual", "quality"),
        ("high-volume", "high-volume", "quality"),
        ("high-volume", "common", "quality"),
        ("high-volume", "dominant", "quality"),
        # State synonyms
        ("ohio", "ohio", "state"),
        ("ohio", "oh", "state"),
        ("kentucky", "kentucky", "state"),
        ("kentucky", "ky", "state"),
        ("indiana", "indiana", "state"),
        ("indiana", "in", "state"),
        ("georgia", "georgia", "state"),
        ("georgia", "ga", "state"),
        ("west-virginia", "west-virginia", "state"),
        ("west-virginia", "wv", "state"),
    ]
    for canonical, synonym, category in defaults:
        add_synonym(canonical, synonym, category)


# ── Generation Log ───────────────────────────────────────────────────────────

def log_generation(pattern_ids, data_type, volume, output_path=None,
                   weighting=None, requested_by="user"):
    """Log a data generation event."""
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO generation_log
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
        rows = conn.execute(
            "SELECT * FROM generation_log ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


# ── Bootstrap ────────────────────────────────────────────────────────────────

def bootstrap():
    """Initialize DB and seed defaults. Safe to call multiple times."""
    init_db()
    seed_default_vocabulary()
