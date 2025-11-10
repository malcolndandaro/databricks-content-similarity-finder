# Databricks notebook source
# MAGIC %md
# MAGIC # Table Similarity Finder
# MAGIC 
# MAGIC A tool for discovering relationships between tables by comparing **actual data content**, not just metadata.  
# MAGIC Useful for **data migrations** where tables may have been **renamed, split, or restructured**.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## How It Works
# MAGIC 
# MAGIC 1. Compares data content between **source** and **target** tables  
# MAGIC 2. Calculates **similarity scores** for matching columns  
# MAGIC 3. Generates a **relationship analysis report** summarizing detected mappings
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## How to Run
# MAGIC 
# MAGIC 1. (Optional) Upload a `.csv` file containing predefined column mappings  
# MAGIC 2. Update configuration options in **cell 2**  
# MAGIC 3. Run all subsequent cells to start the analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## (Optional) CSV Mapping File Format
# MAGIC 
# MAGIC The mapping CSV should include the following **8 columns**:
# MAGIC 
# MAGIC | source_catalog | source_schema | source_table | source_column | target_catalog | target_schema | target_table | target_column |
# MAGIC |----------------|----------------|---------------|----------------|----------------|----------------|---------------|----------------|
# MAGIC 
# MAGIC **Notes:**
# MAGIC - `source_catalog` can be left empty since Oracle does not have a catalog level (unlike Databricks).
# MAGIC 
# MAGIC **Example:**
# MAGIC ```csv
# MAGIC source_catalog,source_schema,source_table,source_column,target_catalog,target_schema,target_table,target_column
# MAGIC ,BCP,oracle_table_b,ID,malcoln,bcp,oracle_table_b_subset_2,ID
# MAGIC ```
# MAGIC 

# COMMAND ----------

# Change those configurations
column_mapping_csv_name = "example_column_mappings.csv" # file name with the column mapping
source_table = "porto_poc.transactions_feip"  # Oracle source table to analyze (format: SCHEMA.TABLE)
candidate_tables = ["users.malcoln_dandaro.transactions_feip"]  # List of Databricks tables to compare against
source_key_columns = ["ID"]  # Columns to order source data for consistent sampling
target_key_columns = ["ID"]  # Columns to order target data for consistent sampling
oracle_partition_column = "id"  # Column to use for partitioning Oracle reads (improves performance)
oracle_lower_bound = 1  # Lower bound value for partition column
oracle_upper_bound = 1000000000  # Upper bound value for partition column
oracle_num_partitions = 8  # Number of partitions to split Oracle reads (parallel processing)
processing_mode = "direct"  # How to handle Oracle data: "direct", "deltalake", "memory"
output_mode =  "text"  # Output format: "text" (logs only) or "deltalake" (save results)
column_data_match_threshold = 0.6  # Minimum similarity score (0.0-1.0) for column matches to show ✓
table_column_match_ratio = 0.3  # Minimum ratio (0.0-1.0) of columns that must match for table relationship
sample_size = 100000  # Number of rows to sample from each table for analysis

# COMMAND ----------

# Setup
import os
import sys
from pathlib import Path
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
if os.environ.get("DATABRICKS_RUNTIME_VERSION") is None:
  absolute_path = str(Path(__file__ ).parent.parent)
  mapping_file_path = f"./finder/{column_mapping_csv_name}"
else:
  absolute_path = str(Path(os.getcwd()).parent)
  mapping_file_path = column_mapping_csv_name

sys.path.append(str(Path(absolute_path)))

# Secrets
os.environ["ORACLE_USER"] = w.dbutils.secrets.get("oracle-credentials", "user")
os.environ["ORACLE_PASSWORD"] = w.dbutils.secrets.get("oracle-credentials", "password")

# COMMAND ----------

# Imports
from utils.logger import get_finder_logger
from utils.dbx.session import initialize_dbx_session
from column_matcher import ColumnContentMatcher
from utils.config.config import load_config
import datetime

logger = get_finder_logger(level="INFO", include_debug=True)

# COMMAND ----------

print(f"Mapping file: {mapping_file_path}")
with open(mapping_file_path, 'r') as f:
    print(f.read())

# COMMAND ----------

def run_analysis():
    # Initialize
    spark = initialize_dbx_session()
    config = load_config()
    finder_catalog = config["databricks"]["finder_catalog"]
    run_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create matcher with CSV mappings
    matcher = ColumnContentMatcher(
        spark=spark,
        finder_catalog=finder_catalog,
        run_timestamp=run_timestamp,
        output_mode="text",
        column_mappings_path=mapping_file_path
    )
    
    logger.info(f"Analyzing: {source_table} -> {candidate_tables[0]}")
    
    # Run analysis
    import time
    start_time = time.time()
    
    results = matcher.find_subset_tables(
        source_table=source_table,
        candidate_tables=candidate_tables,
        column_data_match_threshold=column_data_match_threshold,
        table_column_match_ratio=table_column_match_ratio,
        sample_size=sample_size,
        source_key_columns=source_key_columns,
        target_key_columns=target_key_columns,
        source_type="oracle",
        partition_column=oracle_partition_column,
        lower_bound=oracle_lower_bound,
        upper_bound=oracle_upper_bound,
        num_partitions=oracle_num_partitions,
        processing_mode=processing_mode,
        output_mode=output_mode
    )
    
    execution_time = time.time() - start_time
    logger.info(f"Analysis completed in {execution_time:.2f} seconds")
    
    # Print results
    print_results(results)
    
    return results

def print_results(results):
    if not results:
        print("No relationships found")
        return
    
    for table, info in results.items():
        print(f"\nTable: {table}")
        print(f"Match Ratio: {info['match_ratio']:.2%}")
        print(f"Row Counts: {info['source_row_count']:,} -> {info['candidate_row_count']:,}")
        print(f"Relationship: {info['relationship']}")
        print(f"Source in Target: {info['avg_source_in_target']*100:.1f}%")
        print(f"Target in Source: {info['avg_target_in_source']*100:.1f}%")
        
        print("\nColumn Matches:")
        for src_col, matches in info["matching_columns"].items():
            for tgt_col, metrics in matches.items():
                print(f"  {src_col} -> {tgt_col}: {metrics['overall']:.1%}")

# COMMAND ----------

# Execute
if __name__ == "__main__":
    results = run_analysis()
