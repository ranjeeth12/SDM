# Synthetic Data Generator for MEMBER_GROUP_PLAN_FLAT

## Overview

The synthetic generator creates realistic Facets-style enrollment data with embedded
**per-level cluster structure**. Each generated record conforms to the 56-column
`MEMBER_GROUP_PLAN_FLAT` schema (defined in `POC/facets_denorm_extraction.sql`), and
members are assigned to independent clusters at **four hierarchy levels** — group,
subgroup, plan, and product — whose signals are blended via a weighted Gaussian
mixture. The clusters are designed to be recoverable from the raw data using standard
unsupervised techniques (K-means) when validated within the appropriate hierarchical
partition.

The generator operates in two modes:

- **Config mode** (default) — reads explicit group definitions with nested
  subgroups/plans/products and per-level cluster centroids from a YAML file.
- **Auto mode** — programmatically creates groups with well-separated per-level
  cluster centroids without any input file.

All generated output is written to the `data/` directory.

## Prerequisites

```
pip install numpy pandas scikit-learn pyyaml matplotlib
```

Or from the project root:

```
pip install -r synthetic_generator/requirements.txt
```

## Quick Start

All commands are run from the project root (`CareSourcePoC/`).

### Generate with default config

```bash
python3 -m synthetic_generator generate
```

### Generate with auto-selected clusters and validate

```bash
python3 -m synthetic_generator generate --auto --validate
```

### Generate with custom parameters

```bash
python3 -m synthetic_generator generate \
    --subscribers 1000 \
    --seed 99 \
    --validate
```

### Validate a previously generated dataset

```bash
python3 -m synthetic_generator validate \
    --data data/MEMBER_GROUP_PLAN_FLAT_generated.csv \
    --labels data/MEMBER_GROUP_PLAN_FLAT_generated_labels.csv \
    --plot data/cluster_plots.png
```

## CLI Reference

### `generate` subcommand

```
python3 -m synthetic_generator generate [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config PATH` | `synthetic_generator/config/default.yaml` | Path to YAML cluster config. Ignored when `--auto` is set. |
| `--auto` | off | Auto-select cluster count and centroids instead of reading config. |
| `--auto-groups N` | `6` | Number of explicit groups in auto mode. |
| `--auto-clusters-per-level N` | `3` | Number of member clusters per hierarchy level in auto mode. |
| `--subscribers N` | from config (600) | Total subscribers to generate. Overrides the config value. |
| `--seed N` | `42` | Random seed for reproducibility. |
| `--output PATH` | `data/MEMBER_GROUP_PLAN_FLAT_generated.csv` | Output CSV path. |
| `--validate` | off | Run cluster recovery validation immediately after generation. |

**Output files** (written to `data/` by default):

| File | Contents |
|------|----------|
| `MEMBER_GROUP_PLAN_FLAT_generated.csv` | Main dataset, 56 columns matching the extraction SQL. |
| `MEMBER_GROUP_PLAN_FLAT_generated_labels.csv` | Ground-truth cluster assignments per member per plan. |
| `MEMBER_GROUP_PLAN_FLAT_generated_clusters_composite.png` | PCA scatter plot of composite clustering (when `--validate` is used). |

### `validate` subcommand

```
python3 -m synthetic_generator validate [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data PATH` | *required* | Path to the generated CSV. |
| `--labels PATH` | *required* | Path to the labels CSV. |
| `--reference-date DATE` | `2025-01-01` | Reference date for age/tenure derivation. |
| `--plot PATH` | none | Output path prefix for PCA scatter PNGs. |


## Data Model

### Output grain

One row per **member x enrollment period x plan**. A single subscriber enrolled in
two plans produces two rows. A termed-and-reinstated subscriber with two plans
produces four rows (2 periods x 2 plans).

### Hierarchy

```
Group (GRGR)
  └─ Sub Group (SGSG)
       └─ Coverage Section (CSCS)
            └─ Plan (CSPI)   ← row-expanding join
Subscriber (SBSB)
  └─ Member (MEME)
       └─ Enrollment Bridge (SBSG)  ← defines enrollment period grain
```

### 56-Column Schema

The output columns match the SELECT list in `POC/facets_denorm_extraction.sql`
exactly, in order:

| # | Column | Source Table | Type |
|---|--------|-------------|------|
| 1 | `MEME_CK` | CMC_MEME_MEMBER | int |
| 2 | `SBSB_CK` | CMC_MEME_MEMBER | int |
| 3 | `GRGR_CK` | CMC_MEME_MEMBER | int |
| 4 | `MEME_SFX` | CMC_MEME_MEMBER | smallint |
| 5 | `MEME_REL` | CMC_MEME_MEMBER | char(1) |
| 6 | `MEME_LAST_NAME` | CMC_MEME_MEMBER | char(35) |
| 7 | `MEME_FIRST_NAME` | CMC_MEME_MEMBER | char(15) |
| 8 | `MEME_MID_INIT` | CMC_MEME_MEMBER | char(1) |
| 9 | `MEME_SEX` | CMC_MEME_MEMBER | char(1) |
| 10 | `MEME_BIRTH_DT` | CMC_MEME_MEMBER | date |
| 11 | `MEME_SSN` | CMC_MEME_MEMBER | char(9) |
| 12 | `MEME_MCTR_STS` | CMC_MEME_MEMBER | char(4) |
| 13 | `MEME_ORIG_EFF_DT` | CMC_MEME_MEMBER | date |
| 14 | `MEME_MARITAL_STATUS` | CMC_MEME_MEMBER | char(1) |
| 15 | `MEME_MEDCD_NO` | CMC_MEME_MEMBER | char(20) |
| 16 | `MEME_HICN` | CMC_MEME_MEMBER | char(12) |
| 17 | `MEME_MCTR_RACE_NVL` | CMC_MEME_MEMBER | char(4) |
| 18 | `MEME_MCTR_ETHN_NVL` | CMC_MEME_MEMBER | char(4) |
| 19 | `SBSB_ID` | CMC_SBSB_SUBSC | char(9) |
| 20 | `SBSB_LAST_NAME` | CMC_SBSB_SUBSC | char(35) |
| 21 | `SBSB_FIRST_NAME` | CMC_SBSB_SUBSC | char(15) |
| 22 | `SBSB_ORIG_EFF_DT` | CMC_SBSB_SUBSC | date |
| 23 | `SBSB_MCTR_STS` | CMC_SBSB_SUBSC | char(4) |
| 24 | `SBSB_EMPLOY_ID` | CMC_SBSB_SUBSC | char(10) |
| 25 | `SGSG_CK` | CMC_SBSG_RELATION | int |
| 26 | `SBSG_EFF_DT` | CMC_SBSG_RELATION | date |
| 27 | `SBSG_TERM_DT` | CMC_SBSG_RELATION | date |
| 28 | `SBSG_MCTR_TRSN` | CMC_SBSG_RELATION | char(4) |
| 29 | `SGSG_ID` | CMC_SGSG_SUB_GROUP | char(4) |
| 30 | `SGSG_NAME` | CMC_SGSG_SUB_GROUP | char(50) |
| 31 | `CSCS_ID` | CMC_SGSG_SUB_GROUP | char(4) |
| 32 | `SGSG_STATE` | CMC_SGSG_SUB_GROUP | char(2) |
| 33 | `SGSG_STS` | CMC_SGSG_SUB_GROUP | char(2) |
| 34 | `SGSG_ORIG_EFF_DT` | CMC_SGSG_SUB_GROUP | date |
| 35 | `SGSG_TERM_DT` | CMC_SGSG_SUB_GROUP | date |
| 36 | `GRGR_ID` | CMC_GRGR_GROUP | char(8) |
| 37 | `GRGR_NAME` | CMC_GRGR_GROUP | char(50) |
| 38 | `GRGR_STATE` | CMC_GRGR_GROUP | char(2) |
| 39 | `GRGR_COUNTY` | CMC_GRGR_GROUP | char(20) |
| 40 | `GRGR_STS` | CMC_GRGR_GROUP | char(2) |
| 41 | `GRGR_ORIG_EFF_DT` | CMC_GRGR_GROUP | date |
| 42 | `GRGR_TERM_DT` | CMC_GRGR_GROUP | date |
| 43 | `GRGR_MCTR_TYPE` | CMC_GRGR_GROUP | char(4) |
| 44 | `PAGR_CK` | CMC_GRGR_GROUP | int |
| 45 | `CSPI_ID` | CMC_CSPI_CS_PLAN | char(8) |
| 46 | `CSPD_CAT` | CMC_CSPI_CS_PLAN | char(1) |
| 47 | `CSPI_EFF_DT` | CMC_CSPI_CS_PLAN | date |
| 48 | `CSPI_TERM_DT` | CMC_CSPI_CS_PLAN | date |
| 49 | `PDPD_ID` | CMC_CSPI_CS_PLAN | char(8) |
| 50 | `CSPI_SEL_IND` | CMC_CSPI_CS_PLAN | char(1) |
| 51 | `CSPI_HIOS_ID_NVL` | CMC_CSPI_CS_PLAN | varchar(16) |
| 52 | `CSPD_CAT_DESC` | CMC_CSPD_DESC | char(70) |
| 53 | `CSPD_TYPE` | CMC_CSPD_DESC | char(1) |
| 54 | `LOBD_ID` | CMC_PDPD_PRODUCT | char(4) |
| 55 | `PDPD_RISK_IND` | CMC_PDPD_PRODUCT | char(1) |
| 56 | `PDPD_MCTR_CCAT` | CMC_PDPD_PRODUCT | char(4) |


## Per-Level Clustering Architecture

Data generation embeds cluster structure independently at **four hierarchy levels**.
Each level defines its own set of member clusters with Gaussian centroids on continuous
attributes and weighted categorical distributions. A member's final demographics are a
**weighted blend** of samples drawn independently from each level's assigned cluster.

### The Four Levels

| Level | Scope | What It Represents |
|-------|-------|--------------------|
| **Group** | Per group (GRGR_CK) | Employer-specific demographic segments (e.g., blue-collar veterans vs. young hires) |
| **Subgroup** | Per subgroup (SGSG_CK) | Benefit-tier demographic differences (e.g., standard tenure vs. new enrollees) |
| **Plan** | Per plan type (CSPD_CAT) | Plan-selection demographic patterns (e.g., high vs. low utilizers for medical) |
| **Product** | Per product (LOBD_ID) | Product-line demographic patterns (e.g., standard vs. entry-level for MED) |

### Weighted Gaussian Mixture

When generating a subscriber, the engine:

1. **Independently picks a cluster** at each level (weighted random by cluster weights)
2. **Samples from each level's Gaussian** independently (age, tenure)
3. **Blends via weighted average**:
   ```
   final_age = w_group * age_group + w_subgroup * age_subgroup + w_plan * age_plan + w_product * age_product
   ```
4. **Blends categorical distributions** (sex, marital status) using the same weights,
   then samples once from the combined distribution
5. **Family structure** (spouse/children) comes from the group-level cluster only
6. Demographics are fixed once per member — they do not change across plans

The level weights are a global config parameter:

```yaml
level_weights:
  group: 0.40
  subgroup: 0.20
  plan: 0.20
  product: 0.20
```

### Why This Works for Validation

Each level's cluster signal contributes a known fraction of the final demographic
values. Within a hierarchical partition where all parent-level signals are held
constant, the target level's signal becomes dominant:

- **Group-level clusters** are recovered within each group (GRGR_CK), where the group
  identity is constant and group-level clusters have the highest weight (40%).
- **Subgroup-level clusters** are recovered within each (group, group_cluster)
  partition, where the group signal is constant.
- **Plan-level clusters** are recovered within each (group, group_cluster,
  subgroup_cluster, plan_type) partition.
- **Product-level clusters** are recovered within each (group, group_cluster,
  subgroup_cluster, plan_cluster, LOBD_ID) partition.

### Shared vs. Unique Cluster Definitions

- **Group-level clusters** are **unique per group** — each group defines its own
  demographic segments (e.g., Buckeye Manufacturing has different clusters than Great
  Lakes Healthcare). This means `group_cluster_idx=0` in one group is not comparable
  to `group_cluster_idx=0` in another group.
- **Subgroup, plan, and product clusters** are **shared** across all instances — all
  subgroups use the same cluster definitions, all medical plans use the same plan
  clusters, etc. This makes cluster indices globally comparable: `plan_cluster_idx=0`
  means the same thing everywhere.

### Default Configuration (config/default.yaml)

The default config defines 6 explicit groups with per-level clusters:

**Groups:**

| Group | State | Subscribers | Character |
|-------|-------|-------------|-----------|
| Buckeye Manufacturing LLC | OH | 80 | Blue-collar veteran + young hire |
| Great Lakes Healthcare | OH | 100 | Clinical staff + admin |
| Midwest Technologies Inc | IN | 80 | Senior engineer + junior dev |
| Heritage Financial Group | PA | 100 | Financial advisor + support staff |
| Summit Energy Corp | WV | 90 | Plant worker + field tech |
| Cascade Logistics LLC | KY | 100 | Driver + warehouse |

**Shared subgroup clusters (2):** standard_tenure (older, longer tenure) vs. new_enrollee (younger, shorter tenure)

**Shared plan clusters (2 per type):** medical high_utilizer vs. low_utilizer; dental regular vs. basic; vision regular vs. basic

**Shared product clusters (2 per LOB):** standard (older, longer tenure) vs. entry (younger, shorter tenure)


## Configuration Reference

The YAML configuration file has the following top-level sections. See
`synthetic_generator/config/default.yaml` for a complete working example.

### `seed`

Integer. Random seed for reproducible generation.

### `reference_date`

String (`YYYY-MM-DD`). The date used to convert age and tenure values into calendar
dates (`MEME_BIRTH_DT = reference_date - age`, etc.).

### `total_subscribers`

Integer. Total number of subscribers (MEME_REL='M') to generate. The actual number
of output rows will be larger due to dependents, multiple plans, and enrollment
period expansion.

### `level_weights`

Controls the relative influence of each hierarchy level on final demographics:

```yaml
level_weights:
  group: 0.40
  subgroup: 0.20
  plan: 0.20
  product: 0.20
```

Weights should sum to 1.0. Higher weight means that level's clusters contribute more
to the final demographic values and are easier to recover.

### `groups`

List of explicit group definitions. Each group contains nested subgroups, plans, and
products with member clusters at every level:

```yaml
groups:
  - name: "Buckeye Manufacturing LLC"
    grgr_id: "GRP01001"
    state: OH
    county: Franklin
    mctr_type: COMM
    orig_eff_dt: "2016-01-01"
    target_subscribers: 80
    member_clusters:               # Group-level clusters
      - name: blue_collar_veteran
        weight: 0.50
        continuous:
          age: {mean: 48, std: 3.0}
          tenure_months: {mean: 120, std: 15}
        categorical:
          meme_sex: {M: 0.70, F: 0.30}
          meme_marital_status: {M: 0.65, S: 0.20, D: 0.15}
        family:
          spouse_prob: 0.55
          children: {mean: 1.2, std: 0.8}
      - name: young_hire
        weight: 0.50
        ...
    subgroups:
      - name: "Standard Benefits"
        sgsg_id: "SG01"
        cscs_id: "CS01"
        member_clusters: *sg_clusters   # Can use YAML anchors for sharing
        plans:
          - type: medical
            cspi_id: "CSPI0101"
            pdpd_id: "MEDP0101"
            member_clusters: *med_plan_clusters
            products:
              - lobd_id: MED
                member_clusters: *med_product_clusters
          - type: dental
            ...
```

Each `member_clusters` entry defines:

- **`name`**: Cluster label (appears in labels file)
- **`weight`**: Relative probability for assignment (weights are normalized)
- **`continuous`**: Gaussian centroids with `mean` and `std` for `age` and
  `tenure_months`
- **`categorical`**: Probability distributions for `meme_sex` and
  `meme_marital_status`
- **`family`** (group-level only): `spouse_prob` and `children` (mean, std)

### `plan_enrollment_weights`

Controls which plans subscribers enroll in beyond their primary plan:

```yaml
plan_enrollment_weights:
  medical: 1.0    # All subscribers get medical
  dental: 0.60    # 60% also get dental
  vision: 0.30    # 30% also get vision
```

### `enrollment`

Controls enrollment period patterns:

```yaml
enrollment:
  term_rate: 0.10                 # 10% of subscribers get a terminated enrollment
  reinstate_rate: 0.40            # 40% of terminated subscribers get re-enrolled
  term_reasons: [VTRM, NPAY, MOVE]  # Randomly selected termination reason codes
```

### `group_name_prefixes`

List of strings used for generating group names in auto mode.


## Generation Flow

For each subscriber:

1. **Pick group** (weighted by `target_subscribers`)
2. **Pick subgroup** within group (equal weight)
3. **Pick primary plan** from subgroup's plans (medical first)
4. **Get product** from primary plan
5. **Pick a cluster independently at each level:**
   - `group_cluster_idx` from group's `member_clusters`
   - `subgroup_cluster_idx` from subgroup's `member_clusters`
   - `plan_cluster_idx` from plan's `member_clusters`
   - `product_cluster_idx` from product's `member_clusters`
6. **Blend continuous attributes** using level weights:
   ```
   age = clip(w_g * N(gc.age) + w_sg * N(sgc.age) + w_p * N(pc.age) + w_pr * N(prc.age), 18, 70)
   ```
7. **Blend categorical attributes** — normalize weighted distribution, sample once
8. **Generate family** from group-level cluster's family config
9. **Determine additional plan enrollments** via `plan_enrollment_weights`
10. **For additional plans**, pick plan/product clusters (for labels); demographics
    unchanged
11. **Generate enrollment periods** (term/reinstate logic)
12. **Expand**: member x enrollment_period x enrolled_plans -> rows
13. **Record labels** with cluster assignment at all four levels


## Validation

The validation module proves that the embedded per-level cluster structure is
recoverable from the raw generated CSV using K-means clustering. It validates
**hierarchically** — each level is validated within a partition where all parent-level
signals are held constant.

### Level 1: Group-Level

Within each group (GRGR_CK), cluster subscribers by demographics (age, tenure, sex,
marital status) and compare to `group_cluster_idx` labels. Since each group has unique
clusters, this validates that within-group demographic segments are separable.

### Level 2: Subgroup-Level

Within each (GRGR_CK, group_cluster_idx) partition, cluster subscribers and compare
to `subgroup_cluster_idx`. Holding the group-level cluster constant removes the
dominant 40%-weight signal, making the 20%-weight subgroup signal recoverable.

### Level 3: Plan-Level

Within each (GRGR_CK, group_cluster_idx, subgroup_cluster_idx, CSPD_CAT) partition,
cluster subscribers and compare to `plan_cluster_idx`. With group and subgroup signals
constant, the plan-level signal is dominant.

### Level 4: Product-Level

Within each (GRGR_CK, group_cluster_idx, subgroup_cluster_idx, plan_cluster_idx,
LOBD_ID) partition, cluster subscribers and compare to `product_cluster_idx`.

### Level 5: Composite

Cluster all subscribers globally using GRGR_CK as the label. Tests whether the
blended signal is separable at the group-identity level. This level depends on how
different the groups' demographics are — it may be weak when groups have overlapping
profiles.

### Interpreting Results

| ARI Range | Quality | Meaning |
|-----------|---------|---------|
| > 0.8 | EXCELLENT | Near-perfect cluster recovery |
| 0.6 - 0.8 | GOOD | Most members correctly assigned |
| 0.4 - 0.6 | MODERATE | Partial recovery, some cluster overlap |
| < 0.4 | WEAK | Clusters not clearly separable |

Results use a **size-weighted mean** across partitions, giving more influence to
larger partitions where K-means is more reliable. Small partitions (< 15 members for
plan level, < 8 for product level) are excluded.

### Benchmark Results

With the default configuration (seed=42, 600 subscribers, 6 groups, 2 clusters/level):

| Level | Weighted Mean ARI | Quality |
|-------|-------------------|---------|
| Group | 0.45 | MODERATE |
| Subgroup | 0.59 | MODERATE |
| Plan | 0.47 | MODERATE |
| Product | 0.72 | GOOD |

With auto mode (seed=42, 600 subscribers, 6 groups, 3 clusters/level):

| Level | Weighted Mean ARI | Quality |
|-------|-------------------|---------|
| Group | 0.75 | GOOD |
| Subgroup | 0.70 | GOOD |
| Plan | 0.69 | GOOD |
| Product | 0.64 | GOOD |


## Labels File

The labels CSV (`*_labels.csv`) contains one row per **member per plan enrollment**
with ground-truth metadata:

| Column | Description |
|--------|-------------|
| `MEME_CK` | Member key (joins to main CSV) |
| `SBSB_CK` | Subscriber key |
| `GRGR_CK` | Group key |
| `SGSG_CK` | Subgroup key |
| `CSPD_CAT` | Plan category (M/D/V) |
| `LOBD_ID` | Product/line of business |
| `group_cluster` | Group-level cluster name |
| `group_cluster_idx` | Group-level cluster index (0-based) |
| `subgroup_cluster` | Subgroup-level cluster name |
| `subgroup_cluster_idx` | Subgroup-level cluster index (0-based) |
| `plan_cluster` | Plan-level cluster name |
| `plan_cluster_idx` | Plan-level cluster index (0-based) |
| `product_cluster` | Product-level cluster name |
| `product_cluster_idx` | Product-level cluster index (0-based) |
| `age` | Final blended age value |
| `tenure_months` | Final blended tenure value |
| `meme_rel` | Relationship code (M=subscriber, S=spouse, D=dependent) |

This file is the answer key for validation. It is not part of the synthetic dataset
itself.


## Auto Mode

Auto mode (`--auto`) generates the full hierarchy programmatically:

- Creates `--auto-groups` groups (default: 6) with varied states and names
- Each group gets 1-2 subgroups, each with 2-3 plans from [medical, dental, vision]
- At each level, places `--auto-clusters-per-level` cluster centroids (default: 3)
  with separation > 2 sigma in age x tenure space
- **Group-level clusters** are unique per group with shifted base centroids
- **Subgroup, plan, and product clusters** are shared across all instances for
  globally comparable indices
- Level weights default to `{group: 0.40, subgroup: 0.20, plan: 0.20, product: 0.20}`

```bash
python3 -m synthetic_generator generate --auto --auto-groups 8 --auto-clusters-per-level 4 --validate
```


## Project Structure

```
CareSourcePoC/
├── synthetic_generator/
│   ├── __init__.py
│   ├── __main__.py           # CLI entry point
│   ├── engine.py             # SyntheticGenerator class and auto-config function
│   ├── schema.py             # 56-column definitions, plan types, name pools
│   ├── validate.py           # Hierarchical K-means cluster recovery validation
│   ├── requirements.txt      # Python dependencies
│   └── config/
│       └── default.yaml      # Default cluster configuration (6 groups, per-level clusters)
├── data/                     # Generated output (CSV, labels, plots)
├── docs/
│   └── synthetic_generator.md
└── POC/
    ├── facets_denorm_extraction.sql   # Source SQL defining the 56-column schema
    └── DDL.txt                        # Facets table DDL with field types
```


## Examples

### Generate a large dataset for model training

```bash
python3 -m synthetic_generator generate \
    --auto \
    --subscribers 5000 \
    --auto-groups 10 \
    --auto-clusters-per-level 4 \
    --seed 123 \
    --output data/large_dataset.csv \
    --validate
```

### Use the generator as a library

```python
import sys
sys.path.insert(0, 'synthetic_generator')
from engine import SyntheticGenerator, generate_auto_config

# Auto mode
config = generate_auto_config(seed=99, total_subscribers=200,
                              n_groups=6, n_clusters_per_level=3)
gen = SyntheticGenerator(config)
df, labels = gen.generate()

# Config mode
import yaml
with open('synthetic_generator/config/default.yaml') as f:
    config = yaml.safe_load(f)
gen = SyntheticGenerator(config)
df, labels = gen.generate()

# df is a pandas DataFrame with 56 columns
# labels is a DataFrame with ground-truth cluster assignments at all 4 levels
```


## Data Integrity Guarantees

The generator enforces the following referential integrity rules, matching the join
logic in `facets_denorm_extraction.sql`:

- All members within a family share the same `SBSB_CK`, `GRGR_CK`, `SGSG_CK`, and
  `CSCS_ID`.
- Subscriber members (`MEME_REL = 'M'`) have `MEME_LAST_NAME = SBSB_LAST_NAME` and
  `MEME_FIRST_NAME = SBSB_FIRST_NAME`.
- `CSPD_CAT`, `CSPD_CAT_DESC`, `CSPD_TYPE`, and `LOBD_ID` are always consistent:
  M/Medical/M/MED, D/Dental/D/DEN, V/Vision/V/VIS.
- `CSPI_HIOS_ID_NVL` is populated only for Medical plans; blank for Dental and
  Vision.
- `MEME_MEDCD_NO` and `MEME_HICN` are blank (commercial members).
- SSNs use the 900-series range (obviously synthetic).
- Active enrollments have `SBSG_TERM_DT = 9999-12-31` and empty `SBSG_MCTR_TRSN`.
- Terminated enrollments have `SBSG_TERM_DT < 9999-12-31` and a non-empty
  `SBSG_MCTR_TRSN` (one of VTRM, NPAY, MOVE).
- Reinstated enrollments start the day after the prior termination date.
- A member's demographics are fixed once generated and do not change across plans.
