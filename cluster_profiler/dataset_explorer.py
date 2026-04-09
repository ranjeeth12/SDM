"""Dataset-to-pattern reverse navigation and cross-pattern analysis.

Supports:
  - Point 4: Given a dataset/members, find which patterns they belong to
  - Point 5: Given multiple patterns, find common/overlapping members
"""

import numpy as np
import pandas as pd

from .clustering import build_features, discover_clusters
from .data_loader import apply_filters
from .discovery import enumerate_hierarchy_combos
from .profiler import profile_all_clusters


def find_patterns_for_members(
    df: pd.DataFrame,
    labels_df: pd.DataFrame,
    meme_cks: list,
    max_combos: int = 50,
) -> list[dict]:
    """Find all patterns that a set of members belong to.

    For each hierarchy combo, filters to the given members,
    runs clustering, and records which clusters they fall into.

    Parameters
    ----------
    df : pd.DataFrame
        Full member dataset.
    labels_df : pd.DataFrame
        Labels dataset.
    meme_cks : list
        List of MEME_CK values to look up.
    max_combos : int
        Maximum hierarchy combos to evaluate.

    Returns
    -------
    List of dicts with: combo, cluster_id, member_count, total_in_cluster,
    pct_in_cluster, grgr_name, sgsg_name, cspd_cat_desc, plds_desc
    """
    meme_set = set(meme_cks)
    combos = enumerate_hierarchy_combos(df)
    results = []

    for combo in combos[:max_combos]:
        try:
            subset_members, subset_labels, family_data, filters_used = apply_filters(
                df, labels_df,
                grgr_ck=combo["grgr_ck"],
                sgsg_ck=combo["sgsg_ck"],
                cspd_cat=combo["cspd_cat"],
                lobd_id=combo["lobd_id"],
            )
        except ValueError:
            continue

        # Check if any of our target members are in this subset
        overlap = subset_members[subset_members["MEME_CK"].isin(meme_set)]
        if overlap.empty:
            continue

        # Run clustering on the full subset
        assignments, metrics = discover_clusters(
            subset_members, subset_labels,
            k=None, use_labels=False,
            filters_used=filters_used,
        )

        # Find which clusters our target members fall into
        member_indices = subset_members.index.get_indexer(overlap.index)
        cluster_ids = set(assignments[member_indices[member_indices >= 0]])

        profiles = profile_all_clusters(subset_members, family_data, assignments)

        for cid in cluster_ids:
            mask = np.array(assignments) == cid
            total_in_cluster = int(mask.sum())
            members_in_cluster = int(
                subset_members.iloc[mask]["MEME_CK"].isin(meme_set).sum()
            )

            # Find matching profile
            profile = next((p for p in profiles if p["cluster_id"] == cid), None)

            results.append({
                "combo": {
                    "grgr_ck": combo["grgr_ck"],
                    "sgsg_ck": combo["sgsg_ck"],
                    "cspd_cat": combo["cspd_cat"],
                    "lobd_id": combo["lobd_id"],
                },
                "cluster_id": cid,
                "member_count": members_in_cluster,
                "total_in_cluster": total_in_cluster,
                "pct_in_cluster": round(members_in_cluster / total_in_cluster, 4)
                    if total_in_cluster > 0 else 0,
                "grgr_name": combo["grgr_name"],
                "sgsg_name": combo["sgsg_name"],
                "cspd_cat_desc": combo["cspd_cat_desc"],
                "plds_desc": combo["plds_desc"],
                "profile": profile,
            })

    # Sort by member_count descending
    results.sort(key=lambda r: r["member_count"], reverse=True)
    return results


def find_common_members(
    df: pd.DataFrame,
    labels_df: pd.DataFrame,
    pattern_specs: list[dict],
) -> dict:
    """Find members common to multiple patterns.

    Parameters
    ----------
    df : pd.DataFrame
        Full member dataset.
    labels_df : pd.DataFrame
        Labels dataset.
    pattern_specs : list of dict
        Each dict has: combo (hierarchy filters), cluster_id.

    Returns
    -------
    Dict with:
        common_members: DataFrame of members in ALL patterns
        per_pattern: list of sets of MEME_CKs per pattern
        overlap_count: number of members in intersection
        union_count: number of members in union
        jaccard: Jaccard similarity coefficient
    """
    if len(pattern_specs) < 2:
        raise ValueError("Need at least 2 patterns for cross-pattern analysis")

    member_sets = []

    for spec in pattern_specs:
        combo = spec["combo"]
        target_cluster = spec["cluster_id"]

        try:
            subset_members, subset_labels, family_data, filters_used = apply_filters(
                df, labels_df,
                grgr_ck=combo.get("grgr_ck"),
                sgsg_ck=combo.get("sgsg_ck"),
                cspd_cat=combo.get("cspd_cat"),
                lobd_id=combo.get("lobd_id"),
            )
        except ValueError:
            member_sets.append(set())
            continue

        assignments, _ = discover_clusters(
            subset_members, subset_labels,
            k=None, use_labels=False,
            filters_used=filters_used,
        )

        mask = np.array(assignments) == target_cluster
        cluster_members = set(subset_members.iloc[mask]["MEME_CK"].tolist())
        member_sets.append(cluster_members)

    # Compute intersection and union
    intersection = member_sets[0]
    union = member_sets[0]
    for s in member_sets[1:]:
        intersection = intersection & s
        union = union | s

    # Get the actual member rows for the intersection
    common_df = df[df["MEME_CK"].isin(intersection)].drop_duplicates(subset=["MEME_CK"])

    jaccard = len(intersection) / len(union) if union else 0.0

    return {
        "common_members": common_df,
        "per_pattern": member_sets,
        "overlap_count": len(intersection),
        "union_count": len(union),
        "jaccard": round(jaccard, 4),
    }


def compare_patterns(profiles: list[dict]) -> pd.DataFrame:
    """Build a comparison table across multiple pattern profiles.

    Returns a DataFrame with one row per feature and one column per pattern.
    """
    if not profiles:
        return pd.DataFrame()

    rows = []

    # Member count
    row = {"Feature": "Members"}
    for i, p in enumerate(profiles):
        row[f"Pattern {p['cluster_id']}"] = p["size"]
    rows.append(row)

    # % of subset
    row = {"Feature": "% of Subset"}
    for i, p in enumerate(profiles):
        row[f"Pattern {p['cluster_id']}"] = f"{p['pct_of_subset'] * 100:.1f}%"
    rows.append(row)

    # Continuous features
    for feat in ["_age", "_tenure"]:
        label = "Age (mean)" if feat == "_age" else "Tenure months (mean)"
        row = {"Feature": label}
        for p in profiles:
            stats = p.get("continuous", {}).get(feat, {})
            row[f"Pattern {p['cluster_id']}"] = f"{stats.get('mean', 0):.1f}"
        rows.append(row)

    # Categorical features
    for feat in ["MEME_SEX", "MEME_MARITAL_STATUS"]:
        row = {"Feature": feat}
        for p in profiles:
            pct = p.get("categorical", {}).get(feat, {}).get("pct", {})
            parts = [f"{k}:{v*100:.0f}%" for k, v in sorted(pct.items())]
            row[f"Pattern {p['cluster_id']}"] = " | ".join(parts)
        rows.append(row)

    # Family
    row = {"Feature": "Avg Dependents"}
    for p in profiles:
        row[f"Pattern {p['cluster_id']}"] = p.get("family", {}).get("avg_dependents", 0)
    rows.append(row)

    row = {"Feature": "Spouse Rate"}
    for p in profiles:
        sr = p.get("family", {}).get("spouse_rate", 0)
        row[f"Pattern {p['cluster_id']}"] = f"{sr * 100:.1f}%"
    rows.append(row)

    return pd.DataFrame(rows)
