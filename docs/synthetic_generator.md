# Synthetic Data Generator for MEMBER_GROUP_PLAN_FLAT

## Overview

The synthetic generator creates realistic Facets-style enrollment data with embedded
cluster structure. Each generated record conforms to the 56-column
`MEMBER_GROUP_PLAN_FLAT` schema (defined in `POC/facets_denorm_extraction.sql`), and
members are assigned to pre-defined demographic clusters with Gaussian noise on
continuous attributes. The clusters are designed to be recoverable from the raw data
using standard unsupervised techniques.

The generator operates in two modes:

- **Config mode** (default) -- reads cluster centroids and hierarchy definitions from
  a YAML file.
- **Auto mode** -- programmatically selects the number of clusters, places
  well-separated centroids, and generates the full configuration without any input
  file.

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
| `--auto-member-clusters N` | `5` | Number of member-level clusters in auto mode. |
| `--auto-group-clusters N` | `3` | Number of group-level clusters in auto mode. |
| `--subscribers N` | from config (600) | Total subscribers to generate. Overrides the config value. |
| `--seed N` | `42` | Random seed for reproducibility. |
| `--output PATH` | `data/MEMBER_GROUP_PLAN_FLAT_generated.csv` | Output CSV path. |
| `--validate` | off | Run cluster recovery validation immediately after generation. |

**Output files** (written to `data/` by default):

| File | Contents |
|------|----------|
| `MEMBER_GROUP_PLAN_FLAT_generated.csv` | Main dataset, 56 columns matching the extraction SQL. |
| `MEMBER_GROUP_PLAN_FLAT_generated_labels.csv` | Ground-truth cluster assignments per member. |
| `MEMBER_GROUP_PLAN_FLAT_generated_clusters_subscribers.png` | PCA scatter plot of subscriber clusters (when `--validate` is used). |
| `MEMBER_GROUP_PLAN_FLAT_generated_clusters_groups.png` | PCA scatter plot of group clusters (when `--validate` is used). |

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


## Clustering Architecture

Data generation embeds cluster structure at two hierarchy levels. Clusters are
defined by Gaussian centroids on continuous attributes and weighted categorical
distributions. The Gaussian noise ensures natural variation within each cluster while
maintaining enough separation for unsupervised recovery.

### Level 1: Group Clusters

Group clusters represent employer archetypes. Each group cluster defines:

- **Number of groups** to generate (e.g., 5 large manufacturers, 6 small tech firms)
- **Group attributes**: state, county, plan offerings, origination year
- **Subscriber count**: Gaussian-sampled headcount per group
- **Member cluster weights**: the demographic mix of employees (e.g., manufacturing
  skews toward mid-career; tech skews toward young professionals)

Different group clusters produce different member demographic mixes, which is what
makes group-level clustering recoverable from aggregated member features.

### Level 2: Member Clusters

Member clusters represent demographic segments of subscribers (MEME_REL='M'). Each
cluster defines:

- **Continuous centroids with Gaussian noise**:
  - `age` (mean, std) -- drives `MEME_BIRTH_DT`
  - `tenure_months` (mean, std) -- drives `MEME_ORIG_EFF_DT`
- **Categorical distributions**:
  - `meme_sex` -- probability weights for M/F
  - `meme_marital_status` -- probability weights for S/M/D/W
- **Family structure**:
  - `spouse_prob` -- probability of generating a spouse (MEME_REL='S')
  - `children` (mean, std) -- Gaussian-sampled number of dependents (MEME_REL='D')
- **Plan preference**: which plan profile (medical_only, medical_dental, full_suite)
  members of this cluster tend to enroll in

Dependents (spouses and children) inherit their subscriber's group, enrollment
period, and cluster label. Spouse ages are correlated with the subscriber
(subscriber age +/- small noise). Child ages are derived from the subscriber's age.

### Default Clusters (config/default.yaml)

The default configuration defines 3 group clusters and 5 member clusters:

**Group clusters:**

| Cluster | Groups | State | Plans Offered | Character |
|---------|--------|-------|---------------|-----------|
| `large_manufacturing` | 5 | OH / Franklin | Medical, Dental | Older workforce, more families |
| `healthcare_system` | 5 | OH / Cuyahoga | Medical, Dental, Vision | Balanced demographics |
| `small_tech` | 6 | IN / Marion | Medical, Dental | Young, many recent graduates |

**Member clusters:**

| Cluster | Age | Tenure (mo) | Marital | Family | Plan Pref |
|---------|-----|-------------|---------|--------|-----------|
| `recent_graduate` | 22 +/- 1.0 | 3 +/- 1.5 | 95% single | Rare | Medical only |
| `young_professional` | 28 +/- 2.0 | 20 +/- 4 | 80% single | 15% spouse | Medical only |
| `established_family` | 36 +/- 2.5 | 48 +/- 8 | 88% married | 85% spouse, ~2 children | Full suite |
| `mid_career` | 47 +/- 2.5 | 96 +/- 10 | 70% married | 60% spouse, ~1 child | Medical + Dental |
| `pre_retirement` | 59 +/- 2.0 | 168 +/- 18 | 60% married | 50% spouse | Medical + Dental |

### Centroid Separation

For reliable unsupervised recovery, cluster centroids should be separated by at least
2x the maximum standard deviation on at least one continuous dimension. The default
config satisfies this:

| Pair | Age gap | Tenure gap | Separable? |
|------|---------|------------|------------|
| recent_graduate vs young_professional | 6 (3x max std) | 17 (4x max std) | Yes |
| young_professional vs established_family | 8 (3x max std) | 28 (3.5x max std) | Yes |
| established_family vs mid_career | 11 (4x max std) | 48 (5x max std) | Yes |
| mid_career vs pre_retirement | 12 (5x max std) | 72 (4x max std) | Yes |


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

### `group_clusters`

List of group cluster definitions. Each entry:

```yaml
- name: large_manufacturing       # Cluster label (appears in labels file)
  weight: 0.35                    # Relative weight for subscriber assignment
  count: 5                        # Number of groups to create in this cluster
  attributes:
    subscriber_count:
      mean: 70                    # Gaussian mean for target subscriber headcount
      std: 12                     # Gaussian std
    grgr_state: OH                # State code (char 2)
    grgr_county: Franklin         # County name (char 20)
    grgr_mctr_type: COMM          # Group type code (char 4)
    grgr_orig_year:
      mean: 2016                  # Gaussian mean for group origination year
      std: 2
  name_templates:                 # Group name templates ({prefix} is replaced)
    - "{prefix} Manufacturing LLC"
    - "{prefix} Industrial Corp"
  subgroup:
    plans: [medical, dental]      # Plan types offered by this group cluster
  member_cluster_weights:         # Demographic mix (must reference member_clusters by name)
    young_professional: 0.20
    established_family: 0.30
    mid_career: 0.30
    pre_retirement: 0.15
    recent_graduate: 0.05
```

### `member_clusters`

List of member cluster definitions. Each entry:

```yaml
- name: young_professional        # Cluster label
  continuous:
    age:
      mean: 28                    # Gaussian centroid for subscriber age
      std: 2.0                    # Gaussian std
    tenure_months:
      mean: 20                    # Gaussian centroid for enrollment tenure (months)
      std: 4
  categorical:
    meme_sex:                     # Probability weights for sex
      M: 0.55
      F: 0.45
    meme_marital_status:          # Probability weights for marital status
      S: 0.80
      M: 0.20
  family:
    spouse_prob: 0.15             # Probability of generating a spouse member
    children:
      mean: 0.1                   # Gaussian mean for number of child dependents
      std: 0.3
  plan_preference: medical_only   # Key into plan_profiles
```

### `plan_profiles`

Maps profile names to lists of plan types:

```yaml
plan_profiles:
  medical_only: [medical]
  medical_dental: [medical, dental]
  full_suite: [medical, dental, vision]
```

When a member's preferred plan profile includes plans not offered by their group, the
generator falls back to the plans the group does offer.

### `enrollment`

Controls enrollment period patterns:

```yaml
enrollment:
  term_rate: 0.10                 # 10% of subscribers get a terminated enrollment
  reinstate_rate: 0.40            # 40% of terminated subscribers get re-enrolled
  term_reasons: [VTRM, NPAY, MOVE]  # Randomly selected termination reason codes
```

Terminated enrollments produce rows with `SBSG_TERM_DT` before `9999-12-31` and a
non-empty `SBSG_MCTR_TRSN`. Reinstated enrollments produce a second enrollment period
starting the day after termination with `SBSG_TERM_DT = 9999-12-31`.

### `group_name_prefixes`

List of strings used to populate the `{prefix}` placeholder in group name templates.
Provide at least as many prefixes as the total number of groups across all clusters.


## Validation

The validation module proves that the embedded cluster structure is recoverable from
the raw generated CSV using K-means clustering. It runs at two levels:

### Level 1: Subscriber-Level

1. Loads the generated CSV and derives two continuous features:
   - `age` = (`reference_date` - `MEME_BIRTH_DT`) / 365.25
   - `tenure` = (`reference_date` - `MEME_ORIG_EFF_DT`) / 30.44
2. Deduplicates to one row per member, then filters to subscribers only
   (`MEME_REL = 'M'`), since subscribers are the independent unit of cluster
   assignment.
3. Builds a feature matrix: standardized continuous features (weighted 2x) plus
   one-hot encoded categoricals (`MEME_SEX`, `MEME_MARITAL_STATUS`).
4. Runs K-means for a range of k values around the true cluster count.
5. Reports **Adjusted Rand Index (ARI)** and **silhouette score** for each k.

### Level 2: Group-Level

1. Aggregates subscriber features per `GRGR_CK`: mean age, age std, mean tenure,
   percent male, percent married, subscriber count.
2. Standardizes and runs K-means across a k range.
3. Reports ARI and silhouette for group cluster recovery.

### Interpreting Results

| ARI Range | Quality | Meaning |
|-----------|---------|---------|
| > 0.8 | EXCELLENT | Near-perfect cluster recovery |
| 0.6 - 0.8 | GOOD | Most members correctly assigned |
| 0.4 - 0.6 | MODERATE | Partial recovery, some cluster overlap |
| < 0.4 | WEAK | Clusters not clearly separable |

The **silhouette score** measures how well-separated the clusters are in feature
space. Higher is better (range -1 to 1). When the best-k-by-silhouette matches the
true k, it means the correct number of clusters is discoverable from the data alone.

### PCA Scatter Plots

When `--validate` is used with `generate`, or `--plot` is passed to `validate`, the
tool saves side-by-side PCA scatter plots showing true cluster assignments (left) vs
K-means recovered assignments (right). These provide a visual confirmation of cluster
separability.

### Benchmark Results

With the default configuration (seed=42, 600 subscribers, 5 member clusters, 3 group
clusters):

| Level | True k | ARI | Quality |
|-------|--------|-----|---------|
| Subscriber | 5 | 0.82 | EXCELLENT |
| Group | 3 | 0.42 | MODERATE (16 groups is limited data) |

With auto mode (seed=42, 600 subscribers):

| Level | True k | ARI | Quality |
|-------|--------|-----|---------|
| Subscriber | 5 | 0.98 | EXCELLENT |
| Group | 3 | 1.00 | EXCELLENT |


## Labels File

The labels CSV (`*_labels.csv`) contains one row per member with ground-truth
metadata:

| Column | Description |
|--------|-------------|
| `MEME_CK` | Member key (joins to main CSV) |
| `SBSB_CK` | Subscriber key |
| `member_cluster` | Member cluster name (e.g., `young_professional`) |
| `member_cluster_idx` | Member cluster numeric index (0-based) |
| `group_cluster` | Group cluster name (e.g., `large_manufacturing`) |
| `group_cluster_idx` | Group cluster numeric index (0-based) |
| `age` | Sampled age value (before date conversion) |
| `tenure_months` | Sampled tenure value (before date conversion) |
| `meme_rel` | Relationship code (M=subscriber, S=spouse, D=dependent) |

This file is the answer key for validation. It is not part of the synthetic dataset
itself.


## Project Structure

```
CareSourcePoC/
├── synthetic_generator/
│   ├── __init__.py
│   ├── __main__.py           # CLI entry point
│   ├── engine.py             # SyntheticGenerator class and auto-config function
│   ├── schema.py             # 56-column definitions, plan types, name pools
│   ├── validate.py           # K-means cluster recovery validation
│   ├── requirements.txt      # Python dependencies
│   └── config/
│       └── default.yaml      # Default cluster configuration
├── data/                     # Generated output (CSV, labels, plots)
├── docs/
│   └── synthetic_generator.md
└── POC/
    ├── facets_denorm_extraction.sql   # Source SQL defining the 56-column schema
    └── DDL.txt                        # Facets table DDL with field types
```


## Examples

### Create a custom configuration with 7 member clusters

Copy the default config and modify:

```bash
cp synthetic_generator/config/default.yaml synthetic_generator/config/custom.yaml
# Edit custom.yaml: add member_clusters entries, adjust group_cluster weights
python3 -m synthetic_generator generate \
    --config synthetic_generator/config/custom.yaml \
    --validate
```

### Generate a large dataset for model training

```bash
python3 -m synthetic_generator generate \
    --auto \
    --subscribers 5000 \
    --auto-member-clusters 8 \
    --auto-group-clusters 4 \
    --seed 123 \
    --output data/large_dataset.csv \
    --validate
```

### Validate with a custom reference date

```bash
python3 -m synthetic_generator validate \
    --data data/MEMBER_GROUP_PLAN_FLAT_generated.csv \
    --labels data/MEMBER_GROUP_PLAN_FLAT_generated_labels.csv \
    --reference-date 2025-06-01 \
    --plot data/validation_plots.png
```

### Use the generator as a library

```python
import sys
sys.path.insert(0, 'synthetic_generator')
from engine import SyntheticGenerator, generate_auto_config

# Auto mode
config = generate_auto_config(seed=99, total_subscribers=200, n_member_clusters=4)
gen = SyntheticGenerator(config)
df, labels = gen.generate()

# Config mode
import yaml
with open('synthetic_generator/config/default.yaml') as f:
    config = yaml.safe_load(f)
gen = SyntheticGenerator(config)
df, labels = gen.generate()

# df is a pandas DataFrame with 56 columns
# labels is a DataFrame with ground-truth cluster assignments
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
