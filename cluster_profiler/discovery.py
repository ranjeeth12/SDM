"""Enumerate hierarchy combinations and batch-discover top K-means patterns."""

import numpy as np
import pandas as pd
import streamlit as st

from cluster_profiler.clustering import discover_clusters
from cluster_profiler.data_loader import apply_filters
from cluster_profiler.profiler import profile_all_clusters


def enumerate_hierarchy_combos(df):
    """Return all valid hierarchy filter combos at every depth.

    Each combo is a dict with keys: grgr_ck, sgsg_ck, cspd_cat, lobd_id, label.
    Filter values are single-element lists or None (meaning "All").
    """
    combos = []

    # Depth 1: Group only
    for _, row in (
        df[["GRGR_CK", "GRGR_NAME"]].drop_duplicates().iterrows()
    ):
        combos.append({
            "grgr_ck": [int(row["GRGR_CK"])],
            "sgsg_ck": None,
            "cspd_cat": None,
            "lobd_id": None,
            "grgr_name": row["GRGR_NAME"],
            "sgsg_name": "All",
            "cspd_cat_desc": "All",
            "plds_desc": "All",
        })

    # Depth 2: Group + Subgroup
    for _, row in (
        df[["GRGR_CK", "GRGR_NAME", "SGSG_CK", "SGSG_NAME"]]
        .drop_duplicates()
        .iterrows()
    ):
        combos.append({
            "grgr_ck": [int(row["GRGR_CK"])],
            "sgsg_ck": [int(row["SGSG_CK"])],
            "cspd_cat": None,
            "lobd_id": None,
            "grgr_name": row["GRGR_NAME"],
            "sgsg_name": row["SGSG_NAME"],
            "cspd_cat_desc": "All",
            "plds_desc": "All",
        })

    # Depth 3: Group + Subgroup + Plan Category
    for _, row in (
        df[["GRGR_CK", "GRGR_NAME", "SGSG_CK", "SGSG_NAME",
            "CSPD_CAT", "CSPD_CAT_DESC"]]
        .drop_duplicates()
        .iterrows()
    ):
        combos.append({
            "grgr_ck": [int(row["GRGR_CK"])],
            "sgsg_ck": [int(row["SGSG_CK"])],
            "cspd_cat": [row["CSPD_CAT"]],
            "lobd_id": None,
            "grgr_name": row["GRGR_NAME"],
            "sgsg_name": row["SGSG_NAME"],
            "cspd_cat_desc": row["CSPD_CAT_DESC"],
            "plds_desc": "All",
        })

    # Depth 4: Full hierarchy
    for _, row in (
        df[["GRGR_CK", "GRGR_NAME", "SGSG_CK", "SGSG_NAME",
            "CSPD_CAT", "CSPD_CAT_DESC", "LOBD_ID", "PLDS_DESC"]]
        .drop_duplicates()
        .iterrows()
    ):
        combos.append({
            "grgr_ck": [int(row["GRGR_CK"])],
            "sgsg_ck": [int(row["SGSG_CK"])],
            "cspd_cat": [row["CSPD_CAT"]],
            "lobd_id": [row["LOBD_ID"]],
            "grgr_name": row["GRGR_NAME"],
            "sgsg_name": row["SGSG_NAME"],
            "cspd_cat_desc": row["CSPD_CAT_DESC"],
            "plds_desc": row["PLDS_DESC"],
        })

    return combos


def discover_top_patterns(df, labels_df, top_n=50, progress_callback=None):
    """Run K-means on every hierarchy combo and return the top_n patterns by size.

    Returns a list of dicts with keys:
        combo, cluster_id, size, n_patterns, grgr_name, sgsg_name,
        cspd_cat_desc, plds_desc
    """
    combos = enumerate_hierarchy_combos(df)
    all_patterns = []

    for i, combo in enumerate(combos):
        if progress_callback:
            progress_callback(i, len(combos), combo)

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

        assignments, metrics = discover_clusters(
            subset_members, subset_labels,
            k=None, use_labels=False,
            filters_used=filters_used,
        )

        profiles = profile_all_clusters(subset_members, family_data, assignments)
        n_patterns = metrics.get("n_clusters", 1)

        silhouette = metrics.get("silhouette", 0.0)

        for profile in profiles:
            all_patterns.append({
                "combo": {
                    "grgr_ck": combo["grgr_ck"],
                    "sgsg_ck": combo["sgsg_ck"],
                    "cspd_cat": combo["cspd_cat"],
                    "lobd_id": combo["lobd_id"],
                },
                "cluster_id": profile["cluster_id"],
                "size": profile["size"],
                "silhouette": silhouette,
                "n_patterns": n_patterns,
                "grgr_name": combo["grgr_name"],
                "sgsg_name": combo["sgsg_name"],
                "cspd_cat_desc": combo["cspd_cat_desc"],
                "plds_desc": combo["plds_desc"],
            })

    # Sort by size descending, take top_n
    all_patterns.sort(key=lambda p: p["size"], reverse=True)
    return all_patterns[:top_n]
