"""
Join Facets lookup sheets into denormalized parquet files.

Output:
  data/lookups_joined/plans.parquet      — CSPI + GRGR + CSPD + PDPD + PDDS + PLDS
  data/lookups_joined/subgroups.parquet  — SGSG (standalone)
  data/lookups_joined/providers.parquet  — PRPR (standalone)
"""

import pathlib
import pandas as pd

SRC = pathlib.Path(__file__).parent / "lookupSheets"
OUT = pathlib.Path(__file__).parent / "lookups_joined"
OUT.mkdir(exist_ok=True)

AUDIT_SUFFIXES = ("_LOCK_TOKEN",)
AUDIT_COLS = {"ATXR_SOURCE_ID", "SYS_LAST_UPD_DTM", "SYS_USUS_ID", "SYS_DBUSER_ID"}


def read(name: str) -> pd.DataFrame:
    df = pd.read_excel(SRC / f"{name}.xlsx")
    # strip whitespace from string columns
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].astype(str).str.strip().replace("nan", pd.NA)
    # drop audit columns
    drop = [c for c in df.columns if c.endswith(AUDIT_SUFFIXES) or c in AUDIT_COLS]
    df.drop(columns=drop, inplace=True)
    return df


def main():
    # --- read tables ---
    cspi = read("CSPI")
    grgr = read("GRGR")
    cspd = read("CSPD")
    pdpd = read("PDPD").drop_duplicates(subset="PDPD_ID", keep="last")
    pdds = read("PDDS")
    plds = read("PLDS")
    sgsg = read("SGSG")
    prpr = read("PRPR")

    # --- build plans table ---
    plans = cspi.merge(grgr, on="GRGR_CK", how="left", suffixes=("", "_grgr"))
    plans = plans.merge(cspd, on="CSPD_CAT", how="left", suffixes=("", "_cspd"))
    plans = plans.merge(pdpd, on="PDPD_ID", how="left", suffixes=("", "_pdpd"))
    plans = plans.merge(pdds, on="PDPD_ID", how="left", suffixes=("", "_pdds"))
    plans = plans.merge(plds, on="CSPI_ID", how="left", suffixes=("", "_plds"))

    # --- write parquet ---
    plans.to_parquet(OUT / "plans.parquet", index=False)
    sgsg.to_parquet(OUT / "subgroups.parquet", index=False)
    prpr.to_parquet(OUT / "providers.parquet", index=False)

    # --- summary ---
    print("=== Lookup Join Summary ===\n")
    for label, df, path in [
        ("plans", plans, "plans.parquet"),
        ("subgroups", sgsg, "subgroups.parquet"),
        ("providers", prpr, "providers.parquet"),
    ]:
        print(f"{label:12s}  {df.shape[0]:>6,} rows × {df.shape[1]:>3} cols  → {path}")

    print(f"\nPlans key columns sample ({plans.shape[0]} rows):")
    key_cols = [c for c in ["CSPI_ID", "GRGR_CK", "GRGR_NAME", "CSPD_CAT",
                            "CSPD_CAT_DESC", "PDPD_ID", "LOBD_ID", "PLDS_DESC"]
                if c in plans.columns]
    print(plans[key_cols].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
