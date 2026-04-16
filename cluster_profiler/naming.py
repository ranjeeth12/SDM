"""Generate contextual pattern names from hierarchy and demographic attributes.

Instead of "Pattern 0", "Pattern 1", names are derived from the hierarchy
path and distinguishing demographic features, e.g.:
  "OH-Medicaid-Dental-Adult"
  "KY-Medicare-Advantage-Senior-Family"
"""

from typing import Optional


# ── State abbreviation lookup ────────────────────────────────────────────────

_STATE_ABBREVS = {
    "ohio": "OH", "kentucky": "KY", "indiana": "IN",
    "georgia": "GA", "west virginia": "WV", "arkansas": "AR",
    "north carolina": "NC",
}


def _abbreviate_group(name: str) -> str:
    """Extract a short label from a group name like 'CareSource Ohio'."""
    lower = name.lower()
    for state, abbrev in _STATE_ABBREVS.items():
        if state in lower:
            return abbrev
    # Fallback: first word after 'CareSource' or first 8 chars
    parts = name.replace("CareSource", "").strip().split()
    if parts:
        return parts[0][:10]
    return name[:8]


def _abbreviate_lob(desc: str) -> str:
    """Shorten a line-of-business description."""
    lower = desc.lower()
    if "medicare advantage" in lower:
        return "MA"
    if "medicare" in lower:
        return "Medicare"
    if "medicaid" in lower:
        return "Medicaid"
    if "exchange" in lower or "marketplace" in lower:
        return "Exchange"
    if "tricare" in lower:
        return "Tricare"
    return desc[:12]


def _abbreviate_plan_cat(desc: str) -> str:
    """Shorten a plan category description."""
    lower = desc.lower()
    if "dental" in lower:
        return "Dental"
    if "medical" in lower:
        return "Medical"
    if "vision" in lower:
        return "Vision"
    if "behavioral" in lower:
        return "BH"
    if "pharmacy" in lower:
        return "Rx"
    return desc[:10]


def _age_label(profile: Optional[dict]) -> Optional[str]:
    """Derive an age-bucket label from profile continuous stats."""
    if not profile:
        return None
    age_stats = profile.get("continuous", {}).get("_age", {})
    if not age_stats:
        return None
    mean_age = age_stats.get("mean", 0)
    if mean_age < 18:
        return "Pediatric"
    if mean_age < 30:
        return "YoungAdult"
    if mean_age < 50:
        return "Adult"
    if mean_age < 65:
        return "MidLife"
    return "Senior"


def _family_label(profile: Optional[dict]) -> Optional[str]:
    """Derive a family-structure label from profile."""
    if not profile:
        return None
    family = profile.get("family", {})
    avg_dep = family.get("avg_dependents", 0)
    spouse_rate = family.get("spouse_rate", 0)
    if avg_dep >= 1.0 or spouse_rate >= 0.5:
        return "Family"
    if avg_dep < 0.3 and spouse_rate < 0.2:
        return "Single"
    return None


def build_contextual_name(
    grgr_name: str = "All",
    sgsg_name: str = "All",
    cspd_cat_desc: str = "All",
    plds_desc: str = "All",
    cluster_id: int = 0,
    profile: Optional[dict] = None,
) -> str:
    """Build a human-readable pattern name from hierarchy + demographics.

    Examples:
        OH-Medicaid-Dental-Adult
        KY-MA-Medical-Senior-Family
        OH-All-All-P0
    """
    parts = []

    # Group
    if grgr_name and grgr_name != "All":
        parts.append(_abbreviate_group(grgr_name))

    # Line of Business (before plan category in display)
    if plds_desc and plds_desc != "All":
        parts.append(_abbreviate_lob(plds_desc))

    # Plan Category
    if cspd_cat_desc and cspd_cat_desc != "All":
        parts.append(_abbreviate_plan_cat(cspd_cat_desc))

    # Subgroup (only if it adds info beyond group)
    if sgsg_name and sgsg_name != "All":
        # Don't duplicate group info
        sg_short = sgsg_name.split()[-1][:8] if len(sgsg_name) > 15 else sgsg_name[:8]
        if sg_short.lower() not in [p.lower() for p in parts]:
            parts.append(sg_short)

    # Demographic labels from profile
    age_lbl = _age_label(profile)
    if age_lbl:
        parts.append(age_lbl)

    fam_lbl = _family_label(profile)
    if fam_lbl:
        parts.append(fam_lbl)

    # Cluster ID suffix if needed for disambiguation
    parts.append(f"P{cluster_id}")

    return "-".join(parts)


def build_short_label(contextual_name: str) -> str:
    """Return a compact version for UI display (drops P-suffix if unique enough)."""
    parts = contextual_name.split("-")
    if len(parts) > 2:
        return "-".join(parts[:-1])  # drop PX suffix
    return contextual_name
