"""Auto-generate keyword tags for patterns from hierarchy, demographics, and profile.

Tags are derived from three sources:
  1. Hierarchy-derived: group name, LOB, plan category, state
  2. Demographic-derived: age bucket, tenure bucket, family structure, gender skew
  3. Quality-derived: silhouette score → edge-case vs high-volume
"""

from typing import Optional


def generate_tags(
    grgr_name: str = "",
    sgsg_name: str = "",
    cspd_cat_desc: str = "",
    plds_desc: str = "",
    profile: Optional[dict] = None,
    silhouette: float = 0.0,
    pct_of_pop: float = 0.0,
) -> list[str]:
    """Return a list of keyword tags for a pattern."""
    tags = []

    # ── Hierarchy-derived tags ───────────────────────────────────────────────

    # State from group name
    _state_map = {
        "ohio": "ohio", "kentucky": "kentucky", "indiana": "indiana",
        "georgia": "georgia", "west virginia": "west-virginia",
        "arkansas": "arkansas", "north carolina": "north-carolina",
    }
    if grgr_name:
        lower_grp = grgr_name.lower()
        for state, tag in _state_map.items():
            if state in lower_grp:
                tags.append(tag)
                break
        # Also add the group name itself as a tag
        clean_grp = grgr_name.replace("CareSource", "").strip().lower()
        if clean_grp and clean_grp not in tags:
            tags.append(clean_grp)

    # Plan category
    if cspd_cat_desc and cspd_cat_desc != "All":
        lower_cat = cspd_cat_desc.lower()
        if "dental" in lower_cat:
            tags.append("dental")
        elif "medical" in lower_cat:
            tags.append("medical")
        elif "vision" in lower_cat:
            tags.append("vision")
        elif "behavioral" in lower_cat:
            tags.append("behavioral-health")
        elif "pharmacy" in lower_cat:
            tags.append("pharmacy")
        else:
            tags.append(lower_cat.replace(" ", "-"))

    # Line of business
    if plds_desc and plds_desc != "All":
        lower_lob = plds_desc.lower()
        if "medicare advantage" in lower_lob:
            tags.append("medicare-advantage")
            tags.append("medicare")
        elif "medicare" in lower_lob:
            tags.append("medicare")
        elif "medicaid" in lower_lob:
            tags.append("medicaid")
        elif "exchange" in lower_lob or "marketplace" in lower_lob:
            tags.append("exchange")
        elif "tricare" in lower_lob:
            tags.append("tricare")
        else:
            tags.append(lower_lob.replace(" ", "-")[:20])

    # Subgroup
    if sgsg_name and sgsg_name != "All":
        lower_sg = sgsg_name.lower()
        if "dual" in lower_sg:
            tags.append("dual-eligible")
        if "pension" in lower_sg or "retiree" in lower_sg:
            tags.append("retiree")

    # ── Demographic-derived tags ─────────────────────────────────────────────

    if profile:
        continuous = profile.get("continuous", {})
        age_stats = continuous.get("_age", {})
        tenure_stats = continuous.get("_tenure", {})
        family = profile.get("family", {})
        categorical = profile.get("categorical", {})

        # Age bucket
        mean_age = age_stats.get("mean", 0)
        if mean_age > 0:
            if mean_age < 18:
                tags.append("pediatric")
            elif mean_age < 30:
                tags.append("young-adult")
            elif mean_age < 50:
                tags.append("adult")
            elif mean_age < 65:
                tags.append("mid-life")
            else:
                tags.append("senior")

        # Tenure bucket
        mean_tenure = tenure_stats.get("mean", 0)
        if mean_tenure > 0:
            if mean_tenure < 12:
                tags.append("new-member")
            elif mean_tenure > 60:
                tags.append("long-term")

        # Family structure
        avg_dep = family.get("avg_dependents", 0)
        spouse_rate = family.get("spouse_rate", 0)
        if avg_dep >= 1.0 or spouse_rate >= 0.5:
            tags.append("family")
        elif avg_dep < 0.3 and spouse_rate < 0.2:
            tags.append("single")

        # Gender skew
        sex_pct = categorical.get("MEME_SEX", {}).get("pct", {})
        if sex_pct:
            m_pct = sex_pct.get("M", 0)
            f_pct = sex_pct.get("F", 0)
            if m_pct > 0.70:
                tags.append("male-dominant")
            elif f_pct > 0.70:
                tags.append("female-dominant")

        # Marital status
        marital_pct = categorical.get("MEME_MARITAL_STATUS", {}).get("pct", {})
        if marital_pct:
            married_pct = marital_pct.get("M", 0)
            if married_pct > 0.60:
                tags.append("married")
            single_pct = marital_pct.get("S", 0)
            if single_pct > 0.60:
                tags.append("unmarried")

    # ── Quality-derived tags ─────────────────────────────────────────────────

    if pct_of_pop > 0:
        if pct_of_pop >= 0.08:
            tags.append("high-volume")
        elif pct_of_pop <= 0.02:
            tags.append("edge-case")

    if silhouette > 0:
        if silhouette >= 0.5:
            tags.append("well-separated")
        elif silhouette <= 0.25:
            tags.append("overlapping")

    # Deduplicate and return
    seen = set()
    unique = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique
