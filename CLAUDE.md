# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks data migration tool for identifying table relationships by analyzing actual data content rather than metadata. It's specifically designed for Oracle-to-Databricks migrations where:
- Tables have been migrated with changed modeling logic
- Source tables have been split into multiple target tables
- Multiple source tables have been combined
- Column names or data types have changed

## Development Environment

### Running Locally vs Databricks
The codebase supports both local development (using Databricks Connect) and execution within Databricks notebooks:
- **Local**: Uses `DatabricksSession` with cluster_id from `utils/config/config.yml`
- **Databricks**: Detects runtime via `DATABRICKS_RUNTIME_VERSION` environment variable
- Session initialization is handled automatically in `utils/dbx/session.py:initialize_dbx_session()`

### Configuration
All configuration is managed through `utils/config/config.yml`:
```yaml
databricks:
  cluster_id: "..."           # Only used for local development
  finder_catalog:
    name: "catalog_name"      # Catalog for storing results
    schema: "schema_name"     # Schema for storing results
    volume: "volume_name"     # Volume for temporary data

oracle:
  host: "hostname"
  port: 1521
  service_name: "service"
```

Oracle credentials must be set via environment variables:
- `ORACLE_USER`
- `ORACLE_PASSWORD`

In Databricks, these come from secrets (see `finder/column_matcher_notebook.py:71-72` for example).

## Core Architecture

### Three-Layer Processing Pipeline

1. **ColumnSimilarityCalculator** (`finder/column_similarity.py`)
   - Calculates similarity metrics between individual columns
   - Supports Jaccard similarity, value overlap, and statistical comparisons
   - Handles type mismatches by casting to strings for comparison
   - Returns metrics: `overall`, `source_in_target`, `target_in_source`

2. **ColumnContentMatcher** (`finder/column_matcher.py`)
   - Orchestrates the entire comparison process
   - Manages Oracle data loading with three processing modes
   - Coordinates with ColumnMapper for predefined mappings
   - Produces table relationship analysis (subset, superset, etc.)

3. **ResultsWriter** (`finder/observability/results_writer.py`)
   - Handles output in two modes: `deltalake` (save to Delta tables) or `text` (log only)
   - Saves results to `/Volumes/{catalog}/{schema}/{volume}/{run_timestamp}/results/`

### Oracle Data Processing Modes

The matcher supports three modes for handling Oracle data (via `processing_mode` parameter):

- **`deltalake`** (recommended): Saves Oracle data to Delta Lake first, then reads back
  - Best for large datasets (>100k rows)
  - More reliable and better performance
  - Uses volume path: `/Volumes/{catalog}/{schema}/{volume}/{run_timestamp}/{table_name}`

- **`direct`**: JDBC connection directly to Oracle
  - Can impact Oracle database performance
  - No intermediate storage required
  - Good for quick tests with small data

- **`memory`**: Loads data via Pandas
  - Fast for small datasets (<100k rows)
  - Risk of out-of-memory errors with large data
  - Uses `pandas_api()` conversion

### Column Mapping System

The `ColumnMapper` (`finder/column_mapper.py`) loads predefined column mappings from CSV files:
- Format: `source_catalog,source_schema,source_table,source_column,target_catalog,target_schema,target_table,target_column`
- Oracle tables can have empty `source_catalog` (Oracle has no catalog concept)
- Manual mappings are always included in results, even with 0% similarity
- See `finder/example_column_mappings.csv` for example format

## Running Analysis

### Main Entry Point
`finder/column_matcher_notebook.py` is the primary Databricks notebook for running analysis.

### Key Configuration Parameters (in notebook)
```python
column_mapping_csv_name = "mappings.csv"         # CSV file with predefined mappings
source_table = "SCHEMA.TABLE"                     # Oracle table (SCHEMA.TABLE format)
candidate_tables = ["catalog.schema.table"]       # Databricks tables to compare
processing_mode = "deltalake"                     # How to handle Oracle data
output_mode = "text"                              # "text" or "deltalake"
column_data_match_threshold = 0.6                 # Min similarity for ✓ status (0.0-1.0)
table_column_match_ratio = 0.3                    # Min ratio of matching columns
sample_size = 10000                               # Rows to sample per table
source_key_columns = ["ID"]                       # Columns for ordered sampling
target_key_columns = ["ID"]
```

### Oracle Performance Tuning (for `direct` mode)
When using `processing_mode="direct"`, configure partitioning for parallel reads:
```python
oracle_partition_column = "ID"        # Column to partition on
oracle_lower_bound = 1                # Min value
oracle_upper_bound = 10000            # Max value
oracle_num_partitions = 10            # Number of parallel reads
```

## Module Organization

```
dbrxdw/
├── finder/                          # Core analysis modules
│   ├── column_matcher.py           # Main orchestration (ColumnContentMatcher)
│   ├── column_similarity.py        # Similarity calculations
│   ├── column_mapper.py            # CSV mapping loader
│   ├── column_matcher_notebook.py  # Databricks notebook entry point
│   └── observability/
│       └── results_writer.py       # Output handling (text/Delta)
└── utils/                          # Shared utilities
    ├── config/
    │   ├── config.yml              # Main configuration file
    │   └── config.py               # Config loader
    ├── oracle/
    │   └── connection.py           # Oracle JDBC connection utilities
    ├── dbx/
    │   └── session.py              # Databricks session initialization
    └── logger/                     # Logging configuration
```

## Important Implementation Details

### Column Name Normalization
All column names are normalized to uppercase (`_normalize_columns()`) for cross-platform compatibility between Oracle and Databricks.

### Type Handling in Similarity Calculations
When column types differ between source and target:
- Both columns are cast to strings for Jaccard similarity calculation
- Allows comparison of semantically similar data with different types (e.g., NUMBER vs STRING)
- See `finder/column_similarity.py:67-94`

### Output Modes
- **`deltalake`**: Results saved to Delta tables in volume (default, recommended for large analyses)
- **`text`**: Results printed to logs only, no disk I/O (good for quick tests)

### Relationship Types Detected
- Subset: Source is contained within target
- Superset: Target is contained within source
- Overlap: Partial overlap between source and target
- Based on `avg_source_in_target` and `avg_target_in_source` metrics

## Extending the Tool

### Adding New Similarity Metrics
Add methods to `ColumnSimilarityCalculator` in `finder/column_similarity.py`. Current metrics:
- Jaccard similarity (set overlap)
- Source-in-target percentage
- Target-in-source percentage
- Statistical comparisons for numeric columns

### Adding New Output Formats
Extend `ResultsWriter` in `finder/observability/results_writer.py`. Current modes: `text`, `deltalake`.

### Custom Processing Modes
The `ProcessingMode` type is defined in `finder/column_matcher.py:40`. To add new modes, extend the Literal type and implement handling in the `find_subset_tables()` method.
