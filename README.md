# Databricks Data Similarity Finder

A tool for identifying tables with matching data content by analyzing actual column values rather than relying on metadata or schema information.

## Overview

This tool helps you discover which Databricks tables contain the same data as your source tables (typically from Oracle). It's designed for data migration scenarios where:
- Tables have been migrated with different schemas or column names
- One source table has been split into multiple target tables
- Multiple source tables have been combined into a single target table
- You need to verify that migrated data matches the original source

The tool compares actual data values between tables and identifies:
- **Exact matches**: Tables containing identical data
- **Subsets**: Target tables that contain a subset of the source table's columns/data
- **Supersets**: Target tables that contain all source data plus additional columns/data
- **Partial overlaps**: Tables with some matching columns and data

## Features

- Finds tables with matching data content by comparing column values
- Identifies tables that are subsets or supersets of source tables
- Supports comparison between Oracle and Databricks tables
- Handles large tables efficiently through intelligent sampling
- Provides detailed similarity metrics for each column comparison
- Support for different output modes:
  - `deltalake`: Save results as Delta tables (default)
  - `text`: Print results to logs without saving to disk
- Multiple processing modes for Oracle data:
  - `deltalake`: Save to Delta Lake and read back (best for large datasets)
  - `direct`: Use JDBC connection directly (impacts Oracle performance)
  - `memory`: Load data into memory via Pandas (for small datasets)

## Prerequisites

Before running this tool, ensure you have the following set up:

### 1. Databricks Connect Configuration (for Local Development)
If you plan to run this tool locally (outside of Databricks), you must configure a Databricks Connect default profile:

1. Install Databricks Connect:
   ```bash
   pip install databricks-connect
   ```

2. Configure the default profile with your workspace:
   ```bash
   databricks configure
   ```
   You'll need to provide:
   - Databricks workspace URL
   - Personal access token

The session initialization in `utils/dbx/session.py` uses this profile to connect to your Databricks workspace and the cluster specified in `utils/config/config.yml`.

**Note**: This step is NOT required if you're running the tool directly in Databricks notebooks.

### 2. Oracle JDBC Driver
Your Databricks cluster must have the Oracle JDBC driver installed.

To install the driver:
1. Go to your Databricks workspace
2. Navigate to **Compute** > Select your cluster > **Libraries**
3. Click **Install New**
4. Select **Maven** and enter the coordinates: `com.oracle.database.jdbc:ojdbc8:21.9.0.0` (or your preferred version)
5. Click **Install**

Alternatively, you can download the JDBC driver JAR file from Oracle and upload it directly to your cluster.

### 3. Databricks Resources
The following resources must be pre-created in your Databricks workspace:
- **Catalog**: A catalog to store the analysis results and temporary data
- **Schema**: A schema within the catalog
- **Volume**: A volume within the schema for temporary file storage

You can create these using SQL commands in Databricks:
```sql
CREATE CATALOG IF NOT EXISTS your_catalog_name;
CREATE SCHEMA IF NOT EXISTS your_catalog_name.your_schema_name;
CREATE VOLUME IF NOT EXISTS your_catalog_name.your_schema_name.your_volume_name;
```

### 4. Configuration File Setup
1. Copy the example configuration file:
   ```bash
   cp utils/config/config.yml.example utils/config/config.yml
   ```

2. Edit `utils/config/config.yml` and update with your actual values:
   - Oracle connection details (host, port, service_name)
   - Databricks catalog, schema, and volume names
   - Cluster ID (only needed for local development)

**Important**: The `config.yml` file is ignored by git to prevent committing sensitive credentials. Never commit this file to version control.

## Running in Databricks

### 1. Set Up Oracle Credentials
1. Create Databricks secrets for Oracle credentials (or reuse an existing scope):
   ```bash
   databricks secrets create-scope oracle-credentials
   databricks secrets put-secret oracle-credentials user
   databricks secrets put-secret oracle-credentials password
   ```

If you changed the default scope and secret names, you need to update the credentials section in `column_matcher_notebook.py` with the new scope and secret names.
2. Update the credentials section in `column_matcher_notebook.py`:
   ```python
   os.environ["ORACLE_USER"] = dbutils.secrets.get("oracle-credentials", "user")
   os.environ["ORACLE_PASSWORD"] = dbutils.secrets.get("oracle-credentials", "password")
   ```

### 2. Configure Column Mappings (Optional)
You can provide manual column mappings using a CSV file to help guide the analysis, especially for columns that might not match automatically due to data type differences or low similarity scores.

#### Creating a Column Mappings CSV File
Create a CSV file with the following columns:
```csv
source_catalog,source_schema,source_table,source_column,target_catalog,target_schema,target_table,target_column,notes
```

#### Example Column Mappings CSV:
```csv
source_catalog,source_schema,source_table,source_column,target_catalog,target_schema,target_table,target_column,notes
,source_schema,oracle_table_b,ID,target_catalog,target_schema,oracle_table_b_subset_2,ID,Primary key mapping from Oracle source table
,source_schema,oracle_table_b,uuid,target_catalog,target_schema,oracle_table_b_subset_2,uuid,UUID field mapping
,source_schema,oracle_table_b,IP_ADDRESS,target_catalog,target_schema,oracle_table_b_subset_2,IP_ADDRESS,IP address field
,source_schema,oracle_table_b,NU_TLFN,target_catalog,target_schema,oracle_table_b_subset_2,NU_TLFN,Phone number field
,source_schema,oracle_table_b,nu_cep,target_catalog,target_schema,oracle_table_b_subset_2,nu_cep,Postal code field
```

#### Key Features of Column Mappings:
- **Manual Override**: Predefined mappings will be processed even if they have 0% similarity
- **Mixed Case Support**: Column names are automatically normalized to uppercase
- **Catalog/Schema Flexibility**: Catalog fields can be empty for simpler configurations
- **Detailed Reporting**: Manual mappings are shown in the column analysis table with their actual similarity scores
- **Threshold Independence**: Manual mappings are displayed regardless of similarity thresholds

#### Using Column Mappings in Code:
```python
# Specify the path to your column mappings CSV file
mapping_file_path = "./finder/example_column_mappings.csv"

# Initialize matcher with column mappings
matcher = ColumnContentMatcher(
    spark=spark,
    finder_catalog=finder_catalog,
    run_timestamp=run_timestamp,
    output_mode=output_mode,
    column_mappings_path=mapping_file_path  # Add this parameter
)

# Or override mappings for specific analysis
results = matcher.find_subset_tables(
    source_table=source_table,
    candidate_tables=candidate_tables,
    processing_mode=processing_mode,
    column_mappings_path=mapping_file_path  # Can also be specified here
)
```

When column mappings are provided:
- The analysis will always show mapped columns in the results table
- Each mapped column displays its actual calculated similarity score (which may be 0.0%)
- Columns that meet the individual `column_data_match_threshold` will show ✓ status
- Columns that don't meet the threshold will show ✗ status, but you can still see their target mapping

### 3. Run the Analysis
1. Open `finder/column_matcher_notebook.py` in your Databricks workspace
2. Update the example configuration with your table names:
   ```python
   source_table = "YOUR_SCHEMA.SOURCE_TABLE"  # Oracle table
   candidate_tables = [
       "catalog.schema.target_table1",
       "catalog.schema.target_table2"
   ]
   
   # Choose output mode
   output_mode = "deltalake"  # Options: "deltalake", "text"
   
   # Choose processing mode for Oracle data
   processing_mode = "deltalake"  # Options: "deltalake", "direct", "memory"
   
   # Initialize matcher with output mode and optional column mappings
   matcher = ColumnContentMatcher(
       spark=spark,
       finder_catalog=finder_catalog,
       run_timestamp=run_timestamp,
       output_mode=output_mode,
       column_mappings_path="./finder/example_column_mappings.csv"  # Optional
   )
   
   # Run analysis with chosen processing mode
   results = matcher.find_subset_tables(
       source_table=source_table,
       candidate_tables=candidate_tables,
       processing_mode=processing_mode
   )
   ```
3. Run the notebook

## Example Output

The analysis provides detailed metrics including:
- **Column-level similarity scores**: Percentage match for each column pair
- **Table classification**: Whether the target is a subset, superset, or partial match of the source
- **Content overlap percentages**: How much of the source data exists in the target (and vice versa)
- **Value distribution comparisons**: Statistical analysis for numeric columns

Results will be saved according to the chosen output mode:
- `deltalake`: Saved as Delta tables in the specified volume
- `text`: Printed to logs with detailed information

## Customizing the Analysis
Key parameters you can adjust:
- `column_data_match_threshold` (default: 0.7) - Minimum similarity for column matches
- `table_column_match_ratio` (default: 0.3) - Minimum ratio of matching columns
- `sample_size` (default: 10000) - Number of rows to sample
- `source_key_columns`/`target_key_columns` - Columns for ordered comparison
- `output_mode` - How to save/display results ("deltalake" or "text")
- `processing_mode` - How to handle Oracle data:
  - `deltalake`: Save Oracle data to Delta Lake and read back. Best for large datasets as it provides better performance and reliability.
  - `direct`: Use JDBC connection directly. Can impact Oracle performance but useful for quick tests.
  - `memory`: Load data into memory via Pandas. Best for small datasets (less than 100k rows).

## Performance Considerations
The choice of processing_mode can significantly impact performance:
- `deltalake` mode provides the best performance for large datasets by leveraging Delta Lake's optimizations
- `direct` mode might be slower but requires less storage space as it reads directly from Oracle
- `memory` mode is fastest for small datasets but can cause out-of-memory errors with large data

## Similarity Calculation Algorithms

The tool uses different algorithms depending on the data type being compared. Below are ASCII diagrams showing how similarity scores are calculated for each type.

### String Columns

```
┌─────────────────────────────────────────────────────────────────┐
│                    STRING SIMILARITY                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Step 1: Extract Distinct Values                                │
│  ┌──────────────┐              ┌──────────────┐                 │
│  │ Source Col   │              │ Target Col   │                 │
│  │ ["A","B","C"]│              │ ["B","C","D"]│                 │
│  └──────────────┘              └──────────────┘                 │
│                                                                  │
│  Step 2: Calculate Jaccard Similarity                           │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Intersection = {B, C}           = 2 values        │         │
│  │  Union = {A, B, C, D}            = 4 values        │         │
│  │  Jaccard = Intersection / Union  = 2/4 = 0.50     │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 3: Calculate Containment Metrics                          │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Source in Target = Intersection / Source_Count    │         │
│  │                   = 2/3 = 0.67                     │         │
│  │                                                     │         │
│  │  Target in Source = Intersection / Target_Count    │         │
│  │                   = 2/3 = 0.67                     │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Final Score: overall = jaccard = 0.50                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Numeric Columns

```
┌─────────────────────────────────────────────────────────────────┐
│                   NUMERIC SIMILARITY                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Step 1: Calculate Jaccard (Same as Strings)                    │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Source: [1, 2, 3, 4, 5]                           │         │
│  │  Target: [3, 4, 5, 6, 7]                           │         │
│  │  Intersection: {3, 4, 5} = 3                       │         │
│  │  Union: {1,2,3,4,5,6,7} = 7                        │         │
│  │  Jaccard = 3/7 = 0.43                              │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 2: Calculate Distribution Similarity                      │
│  ┌────────────────────────────────────────────────────┐         │
│  │  A) Range Overlap                                  │         │
│  │     Source: min=1, max=5                           │         │
│  │     Target: min=3, max=7                           │         │
│  │     ───1═══════5───  (Source Range)                │         │
│  │         ───3═══════7───  (Target Range)            │         │
│  │         ───3═══5───  (Overlap = 2)                 │         │
│  │     Total Range = 7-1 = 6                          │         │
│  │     Range Similarity = 2/6 = 0.33                  │         │
│  │                                                     │         │
│  │  B) Mean Comparison                                │         │
│  │     Source Mean = 3.0                              │         │
│  │     Target Mean = 5.0                              │         │
│  │     Mean Diff = 1 - |3-5|/max(3,5) = 0.60         │         │
│  │                                                     │         │
│  │  C) Standard Deviation Comparison                  │         │
│  │     Source StdDev = 1.41                           │         │
│  │     Target StdDev = 1.41                           │         │
│  │     StdDev Diff = 1 - |1.41-1.41|/1.41 = 1.00     │         │
│  │                                                     │         │
│  │  Distribution = (0.33 + 0.60 + 1.00) / 3 = 0.64   │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 3: Combine Metrics                                        │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Overall = (Jaccard + Distribution) / 2            │         │
│  │          = (0.43 + 0.64) / 2 = 0.54               │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Final Score: overall = 0.54                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Date/Timestamp Columns

```
┌─────────────────────────────────────────────────────────────────┐
│                DATE/TIMESTAMP SIMILARITY                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Step 1: Normalize to Common Format                             │
│  ┌────────────────────────────────────────────────────┐         │
│  │  If types differ (Timestamp vs Date):              │         │
│  │    - Normalize both to DATE (drops time component) │         │
│  │  Else:                                              │         │
│  │    - Use original format                            │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 2: Calculate Jaccard on Distinct Dates                    │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Source: [2024-01-01, 2024-01-02, 2024-01-03]     │         │
│  │  Target: [2024-01-02, 2024-01-03, 2024-01-04]     │         │
│  │  Intersection = 2, Union = 4                       │         │
│  │  Jaccard = 0.50                                    │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 3: Calculate Temporal Range Overlap                       │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Convert to Unix timestamps (epoch)                │         │
│  │  Source: min=1704067200, max=1704240000           │         │
│  │  Target: min=1704153600, max=1704326400           │         │
│  │                                                     │         │
│  │  Timeline:                                          │         │
│  │  ───[Source─────────]─── (3 days)                 │         │
│  │       ───[Target─────────]─── (3 days)            │         │
│  │       ───[Overlap───]─── (2 days)                 │         │
│  │                                                     │         │
│  │  Source Range = 1704240000 - 1704067200 = 172800  │         │
│  │  Target Range = 1704326400 - 1704153600 = 172800  │         │
│  │  Overlap Range = 1704240000 - 1704153600 = 86400  │         │
│  │  Max Range = 1704326400 - 1704067200 = 259200     │         │
│  │                                                     │         │
│  │  Temporal Overlap = 86400 / 259200 = 0.33         │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 4: Weighted Combination                                   │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Overall = (Temporal_Overlap × 0.7) +              │         │
│  │            (Jaccard × 0.3)                         │         │
│  │          = (0.33 × 0.7) + (0.50 × 0.3)            │         │
│  │          = 0.23 + 0.15 = 0.38                      │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Final Score: overall = 0.38                                    │
│  (70% weight on temporal range, 30% on exact matches)           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Type Mismatches (Non-Temporal)

```
┌─────────────────────────────────────────────────────────────────┐
│            MIXED TYPE SIMILARITY (Non-Temporal)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Example: Number column vs String column                        │
│                                                                  │
│  Step 1: Cast Both to String                                    │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Source (Number): [1, 2, 3]                        │         │
│  │  Target (String): ["1", "2", "4"]                  │         │
│  │                                                     │         │
│  │  After casting:                                     │         │
│  │  Source: ["1", "2", "3"]                            │         │
│  │  Target: ["1", "2", "4"]                            │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 2: Calculate Jaccard on String Values                     │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Intersection = {"1", "2"} = 2                     │         │
│  │  Union = {"1", "2", "3", "4"} = 4                  │         │
│  │  Jaccard = 2/4 = 0.50                              │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 3: Calculate Containment                                  │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Source in Target = 2/3 = 0.67                     │         │
│  │  Target in Source = 2/3 = 0.67                     │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Final Score: overall = jaccard = 0.50                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Metrics Explained

- **Jaccard Similarity**: Measures overlap between distinct values
  - Formula: `|Intersection| / |Union|`
  - Range: 0.0 (no overlap) to 1.0 (identical sets)

- **Containment Metrics**:
  - `source_in_target`: What % of source values exist in target
  - `target_in_source`: What % of target values exist in source
  - Used to identify if a table is a subset or superset of another

- **Distribution Similarity** (Numeric only):
  - Range overlap: How much do the min-max ranges overlap?
  - Mean comparison: How similar are the average values?
  - StdDev comparison: How similar is the spread of values?

- **Temporal Range Overlap** (Dates/Timestamps):
  - Calculates what % of the time range overlaps
  - Weighted 70% (range) + 30% (exact date matches)
  - Better for comparing temporal datasets that may have shifted time periods
