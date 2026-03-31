# Cluster Profiler

Interactive tool for exploring member cluster structure across the 4-level hierarchy (group → subgroup → plan → product).

## Usage

```bash
# Profile a single group
python -m cluster_profiler --grgr-ck 1

# Filter by multiple groups and plan type
python -m cluster_profiler --grgr-ck 1 7 --cspd-cat M

# Use true cluster labels instead of K-means
python -m cluster_profiler --grgr-ck 12 --use-labels

# Export JSON
python -m cluster_profiler --grgr-ck 12 --use-labels --output-json data/profiles.json

# Export visualization
python -m cluster_profiler --grgr-ck 16 --cspd-cat M --output-plot data/profiles.png
```

## CLI Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--data` | str | `data/MEMBER_GROUP_PLAN_FLAT_generated.csv` | Path to generated member CSV |
| `--labels` | str | `data/..._labels.csv` | Path to labels CSV |
| `--grgr-ck` | int+ | None | Group key(s) |
| `--sgsg-ck` | int+ | None | Subgroup key(s) |
| `--cspd-cat` | M/D/C+ | None | Plan type(s) |
| `--lobd-id` | str+ | None | Product/LOB ID(s) |
| `--k` | int | auto | Force cluster count |
| `--use-labels` | flag | false | Use true labels from the labels CSV |
| `--output-json` | str | None | Write JSON report to path |
| `--output-plot` | str | None | Write PNG visualization to path |
| `--reference-date` | str | 2025-01-01 | Reference date for age/tenure derivation |

Filters are AND-combined. Omitting a filter means "all values."

## Clustering Modes

**K-means (default):** Builds a feature matrix from standardized age/tenure (weighted 2×) and one-hot encoded sex/marital status. Selects optimal k via silhouette score sweep over k=2..10, or uses `--k` if specified. Subsets with fewer than 8 members are treated as a single cluster.

**Label-based (`--use-labels`):** Uses the true cluster assignments from the labels CSV. Automatically picks the most specific hierarchy level based on which filters are active (product > plan > subgroup > group). Computes ARI between true labels and K-means as validation.

## Output

### Console Report

Per-cluster sections showing:
- **Size** and percentage of subset
- **Age/tenure stats**: mean, std, median, min, max
- **Categorical distributions**: sex and marital status counts/percentages
- **Family structure**: average dependents per subscriber, spouse rate
- **Hierarchy descriptions**: group names, subgroup names, plan/product descriptions

### JSON (`--output-json`)

```json
{
  "summary": { "total_members": 150, "filters": {...}, ... },
  "metrics": { "method": "kmeans", "n_clusters": 3, "silhouette": 0.45 },
  "clusters": [ { "cluster_id": 0, "size": 50, ... }, ... ]
}
```

### Visualization (`--output-plot`)

2×2 PNG figure:
1. PCA scatter colored by cluster
2. Age histogram by cluster
3. Tenure histogram by cluster
4. Sex distribution bar chart by cluster

## Module Structure

| Module | Purpose |
|--------|---------|
| `config.py` | Constants, column definitions, defaults |
| `data_loader.py` | CSV loading, feature derivation, hierarchy filtering |
| `clustering.py` | Feature matrix construction, K-means, label-based clustering |
| `profiler.py` | Per-cluster demographic profile computation |
| `formatters.py` | Console text and JSON output formatting |
| `visualization.py` | PCA scatter and distribution plots |
| `__main__.py` | CLI argument parsing and orchestration |

## Tests

```bash
python -m pytest cluster_profiler/tests/ -v
```
