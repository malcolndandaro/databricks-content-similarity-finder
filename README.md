# Databricks Data Migration Finder

A tool for identifying table relationships by data content during data warehouse migrations to Databricks.

## Overview
This module helps identify similarities between tables based on actual data content rather than metadata. It's particularly useful when:
- Tables have been migrated from Oracle to Databricks with changed modeling logic
- One source table has been split into multiple target tables
- Multiple source tables have been combined into one target table
- You need to find table relationships by analyzing the actual data content

## Features
- Identifies table relationships by analyzing data content
- Supports comparison between Oracle and Databricks tables
- Handles large tables efficiently through sampling
- Provides detailed similarity metrics and reports
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
The analysis will provide detailed metrics including:
- Column-level similarity scores
- Table relationship type (subset, superset, etc.)
- Content overlap percentages
- Value distribution comparisons

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
