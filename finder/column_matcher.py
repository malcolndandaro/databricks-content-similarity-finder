"""
Column Content Matcher

This module provides functionality to match columns between tables based on their data content
rather than metadata like column names or data types.
"""

import sys
from pathlib import Path

# Add the parent directory to Python path to allow imports from utils
sys.path.append(str(Path(__file__).parent.parent))
import os
from typing import Dict, List, Tuple, Optional, Any, Literal
import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    TimestampType,
    DateType,
    StringType,
    DataType,
)
import datetime
import pandas as pd
from utils.config.config import load_config
from utils.logger import get_finder_logger
from utils.dbx.session import initialize_dbx_session
from column_similarity import ColumnSimilarityCalculator
from utils.oracle import get_oracle_connection_properties, read_oracle_table
from observability.results_writer import ResultsWriter, OutputMode
from column_mapper import ColumnMapper

# Define valid processing modes
ProcessingMode = Literal['deltalake', 'direct', 'memory']

class ColumnContentMatcher:
    """
    A class to identify similar columns across tables based on data content.

    This class implements various strategies to compare column contents across
    different tables and determine if they contain the same or similar data.
    """

    def __init__(self, spark: SparkSession, finder_catalog: Dict[str, str], run_timestamp: str, output_mode: OutputMode = 'deltalake', column_mappings_path: Optional[str] = None):
        """
        Initialize the ColumnContentMatcher.

        Args:
            spark: SparkSession to use
            finder_catalog: Dictionary with catalog configuration
            run_timestamp: Timestamp for this run
            output_mode: Output mode to use ('text', 'deltalake', or 'csv')
            column_mappings_path: Optional path to CSV file containing column mappings
        """
        self.logger = get_finder_logger(level="INFO", include_debug=True)
        start_time = time.time()
        self.logger.info("Initializing ColumnContentMatcher...")
        self.spark = spark
        self.similarity_calculator = ColumnSimilarityCalculator(self.spark)
        self.config = load_config()
        self.finder_catalog = finder_catalog
        self.run_timestamp = run_timestamp
        self.output_mode = output_mode
        
        # Initialize column mapper
        self.column_mapper = ColumnMapper(column_mappings_path) if column_mappings_path else None
        if self.column_mapper:
            self.logger.info(f"Column mappings loaded from: {column_mappings_path}")
        else:
            self.logger.info("No column mappings provided - will use automatic column matching")
        
        self.results_writer = ResultsWriter(
            spark=self.spark,
            catalog=finder_catalog["name"],
            schema=finder_catalog["schema"],
            volume=finder_catalog["volume"],
            run_timestamp=run_timestamp,
            output_mode=output_mode
        )
        elapsed = time.time() - start_time
        self.logger.info(f"ColumnContentMatcher initialized successfully in {elapsed:.2f}s")

    def _normalize_columns(self, columns: List[str]) -> List[str]:
        """Utility to normalize column names to uppercase for cross-platform compatibility."""
        return [col.upper() for col in columns]

    def _oracle_to_delta_path(self, oracle_table: str) -> str:
        catalog = self.finder_catalog["name"]
        volume = self.finder_catalog["volume"]
        schema = self.finder_catalog["schema"]
        # Remove schema prefix if present in oracle_table
        table_name = oracle_table.split(".")[-1]
        return f"/Volumes/{catalog}/{schema}/{volume}/{self.run_timestamp}/{table_name}"

    def _get_results_path(self, results_type: str) -> str:
        """Get path for results Delta tables."""
        catalog = self.finder_catalog["name"]
        volume = self.finder_catalog["volume"]
        schema = self.finder_catalog["schema"]
        return f"/Volumes/{catalog}/{schema}/{volume}/{self.run_timestamp}/results/{results_type}"

    def _persist_oracle_df_as_delta(self, df: DataFrame, oracle_table: str) -> str:
        path = self._oracle_to_delta_path(oracle_table)
        self.logger.info(f"Persisting Oracle table '{oracle_table}' as Delta to: {path}")
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).option("optimizeWrite", "True").option("path", path).save()

        return path

    def load_source_dataframe(
        self,
        source_table: str,
        source_type: str,
        processing_mode: ProcessingMode,
        sample_size: int,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: Optional[int] = None,
        source_key_columns: Optional[List[str]] = None,
        target_key_columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Load source DataFrame using the specified processing mode.

        Args:
            source_table: Name of the source table
            source_type: Type of source ('databricks' or 'oracle')
            processing_mode: Mode to process the data ('deltalake', 'direct', or 'memory')
            sample_size: Number of rows to sample
            partition_column: Column to use for partitioning (Oracle only)
            lower_bound: Lower bound for partitioning (Oracle only)
            upper_bound: Upper bound for partitioning (Oracle only)
            num_partitions: Number of partitions (Oracle only)
            source_key_columns: Columns to order by (Oracle only)

        Returns:
            DataFrame: Loaded source DataFrame
        """
        if source_type == "oracle":
            # Get Oracle connection properties
            oracle_props = get_oracle_connection_properties()
            
            # Read from Oracle
            oracle_df = read_oracle_table(
                self.spark,
                source_table,
                oracle_props,
                sample_size=sample_size,
                partition_column=partition_column,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                num_partitions=num_partitions,
                order_by_columns=source_key_columns
            )

            if processing_mode == 'deltalake':
                self.logger.info("Using DeltaLake mode: Saving to Delta and reading back")
                # Save to Delta and read back
                delta_path = self._persist_oracle_df_as_delta(oracle_df, source_table)
                source_df = self.spark.read.format("delta").load(delta_path)
                oracle_df.unpersist()
            elif processing_mode == 'direct':
                self.logger.info("Using Direct mode: Using JDBC DataFrame directly")
                # Use the JDBC DataFrame directly
                source_df = oracle_df
            else:  # memory mode
                self.logger.info("Using Memory mode: Converting to Pandas and back to Spark")
                # Convert to pandas and back to Spark
                self.logger.info("  Converting to Pandas DataFrame...")
                pandas_df = oracle_df.toPandas()
                self.logger.info("  Converting back to Spark DataFrame...")
                source_df = self.spark.createDataFrame(pandas_df)
                oracle_df.unpersist()
                del pandas_df  # Help garbage collection
        else:
            # For Databricks source, just read the table normally
            source_df = self.spark.table(source_table)

        # Normalize column names to uppercase
        source_df = source_df.select(*[col.upper() for col in source_df.columns])
        return source_df

    def find_matching_columns_parallel(
        self,
        source_table: str,
        target_table: str,
        column_data_match_threshold: float = 0.9,
        sample_size: int = 1000,
        source_key_columns: Optional[List[str]] = None,
        target_key_columns: Optional[List[str]] = None,
        source_df: Optional[DataFrame] = None,
        column_mappings: Optional[List[Tuple[str, str]]] = None,
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Find all matching columns between two tables in parallel using Spark.

        Args:
            source_table: Name of the source table
            target_table: Name of the target table
            column_data_match_threshold: Threshold above which columns are considered matching
            sample_size: Number of rows to use for comparison
            source_key_columns: If provided, will order source table by these columns before limiting
            target_key_columns: If provided, will order target table by these columns before limiting.
                              If not provided but source_key_columns is, will try to use those
            source_df: Optional DataFrame for the source table (if already loaded/cached)
            column_mappings: Optional list of (source_column, target_column) tuples for known mappings.
                           When provided, only these column pairs will be compared instead of all combinations.
        Returns:
            Dict mapping source columns to matching target columns with similarity scores
        """
        self.logger.info(
            f"\nFinding matching columns between {source_table} and {target_table} (parallel mode)..."
        )
        if source_key_columns:
            self.logger.info(f"  Using source key columns for ordering: {', '.join(source_key_columns)}")
        if target_key_columns:
            self.logger.info(f"  Using target key columns for ordering: {', '.join(target_key_columns)}")
        if column_mappings:
            self.logger.info(f"  Using predefined column mappings: {len(column_mappings)} column pairs")

        start_time = time.time()

        # Get source DataFrame
        self.logger.info("  Loading source DataFrame...")
        if source_df is not None:
            source_df_local = source_df

        source_df_local = source_df_local.select(
            *[col.upper() for col in source_df_local.columns]
        )

        self.logger.info("  Loading target DataFrame...")
        target_df = self.spark.table(target_table)
        target_df = target_df.select(*[col.upper() for col in target_df.columns])

        # Get column names and types
        source_columns = [(f.name, f.dataType) for f in source_df_local.schema.fields]
        target_columns = [(f.name, f.dataType) for f in target_df.schema.fields]
        self.logger.info(f"  Source columns: {len(source_columns)}")
        self.logger.info(f"  Target columns: {len(target_columns)}")
        self.logger.info(f"  Source column details: {[(name, str(dtype)) for name, dtype in source_columns]}")
        self.logger.info(f"  Target column details: {[(name, str(dtype)) for name, dtype in target_columns]}")

        def are_types_compatible(source_type: DataType, target_type: DataType) -> bool:
            """Check if two column types are compatible for comparison."""
            # Same type is always compatible
            if source_type == target_type:
                return True

            # Numeric types are compatible with each other
            numeric_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)
            if isinstance(source_type, numeric_types) and isinstance(
                target_type, numeric_types
            ):
                return True

            # Timestamp and Date types are compatible
            temporal_types = (TimestampType, DateType)
            if isinstance(source_type, temporal_types) and isinstance(
                target_type, temporal_types
            ):
                return True

            # String type can be compared with any type (we'll convert to string)
            if isinstance(source_type, StringType) or isinstance(
                target_type, StringType
            ):
                return True

            return False

        # Create a single sample for source and target, to be reused for all comparisons
        if source_key_columns or target_key_columns:
            self.logger.info(f"  Creating ordered samples by key columns...")
            source_cols = set(name for name, _ in source_columns)
            target_cols = set(name for name, _ in target_columns)
            
            # Check source key columns
            self.logger.info(f"  Source key columns: {source_key_columns}")
            if source_key_columns:
                source_key_columns = self._normalize_columns(source_key_columns)
                missing_in_source = [col for col in source_key_columns if col not in source_cols]
                if missing_in_source:
                    self.logger.warning(f"  Warning: Some source key columns missing. Using random sampling instead.")
                    self.logger.warning(f"  Missing in source: {missing_in_source}")
                    self.logger.info("  Creating random sample of source table...")
                    source_sample = source_df_local.sample(
                        False, min(1.0, sample_size / max(1, source_df_local.count()))
                    )
                else:
                    self.logger.info("  Creating ordered sample of source table...")
                    order_cols = [F.col(col) for col in source_key_columns]
                    source_sample = source_df_local.orderBy(*order_cols).limit(sample_size)
            else:
                self.logger.info("  Creating random sample of source table...")
                source_sample = source_df_local.sample(
                    False, min(1.0, sample_size / max(1, source_df_local.count()))
                )
            
            source_sample_count = source_sample.count()
            self.logger.info(f"  Source sample created: {source_sample_count:,} rows (materialized in Spark)")
            
            # Check target key columns
            self.logger.info(f"  Target key columns: {target_key_columns}")
            if target_key_columns:
                target_key_columns = self._normalize_columns(target_key_columns)
                missing_in_target = [col for col in target_key_columns if col not in target_cols]
                if missing_in_target:
                    self.logger.warning(f"  Warning: Some target key columns missing. Using random sampling instead.")
                    self.logger.warning(f"  Missing in target: {missing_in_target}")
                    self.logger.info("  Creating random sample of target table...")
                    # If any of the specified target key columns are not present in the actual target columns,
                    # log a warning and use random sampling for the target table rather than an ordered sample.
                    # The 'sample' method here takes a sample fraction (rows to select / total rows), but ensures
                    # the fraction does not exceed 1.0 (i.e., you can't sample more rows than exist).
                    # 'False' means sampling without replacement.
                    target_sample = target_df.sample(
                        False, min(1.0, sample_size / max(1, target_df.count()))
                    )
                else:
                    self.logger.info("  Creating ordered sample of target table...")
                    order_cols = [F.col(col) for col in target_key_columns]
                    target_sample = target_df.orderBy(*order_cols).limit(sample_size)
            else:
                self.logger.info("  Creating random sample of target table...")
                target_sample = target_df.sample(
                    False, min(1.0, sample_size / max(1, target_df.count()))
                )
            
            target_sample_count = target_sample.count()
            self.logger.info(f"  Target sample created: {target_sample_count:,} rows (materialized in Spark)")
        else:
            self.logger.info(f"  Creating random samples...")
            self.logger.info("  Creating random sample of source table...")
            source_sample = source_df_local.sample(
                False, min(1.0, sample_size / max(1, source_df_local.count()))
            )
            source_sample_count = source_sample.count()
            self.logger.info(
                f"  Source sample created: {source_sample_count:,} rows (materialized in Spark)"
            )
            self.logger.info("  Creating random sample of target table...")
            target_sample = target_df.sample(
                False, min(1.0, sample_size / max(1, target_df.count()))
            )
            target_sample_count = target_sample.count()
            self.logger.info(
                f"  Target sample created: {target_sample_count:,} rows (materialized in Spark)"
            )

        # Initialize stats dictionaries
        source_stats = {}
        target_stats = {}

        # Pre-calculate stats for all numeric columns in source
        self.logger.info("\nPre-calculating source numeric stats...")
        for s_col, s_type in source_columns:
            if isinstance(
                s_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)
            ):
                # Calculate stats using summary
                stats = (
                    source_sample.select(s_col)
                    .summary("min", "max", "mean", "stddev")
                    .collect()
                )
                source_stats[s_col] = {
                    "row_count": source_df_local.count(),
                    "distinct_count": source_sample.select(s_col).distinct().count(),
                    "null_count": source_sample.filter(F.col(s_col).isNull()).count(),
                    "numeric_stats": {
                        "min": (
                            float(stats[0][s_col])
                            if stats[0][s_col] is not None
                            else None
                        ),
                        "max": (
                            float(stats[1][s_col])
                            if stats[1][s_col] is not None
                            else None
                        ),
                        "mean": (
                            float(stats[2][s_col])
                            if stats[2][s_col] is not None
                            else None
                        ),
                        "stddev": (
                            float(stats[3][s_col])
                            if stats[3][s_col] is not None
                            else None
                        ),
                    },
                }
                self.logger.debug(f"  Collected stats for {s_col}: {source_stats[s_col]}")

        # Pre-calculate stats for all numeric columns in target
        self.logger.info("\nPre-calculating target numeric stats...")
        for t_col, t_type in target_columns:
            if isinstance(
                t_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)
            ):
                # Calculate stats using summary
                stats = (
                    target_sample.select(t_col)
                    .summary("min", "max", "mean", "stddev")
                    .collect()
                )
                target_stats[t_col] = {
                    "row_count": target_df.count(),
                    "distinct_count": target_sample.select(t_col).distinct().count(),
                    "null_count": target_sample.filter(F.col(t_col).isNull()).count(),
                    "numeric_stats": {
                        "min": (
                            float(stats[0][t_col])
                            if stats[0][t_col] is not None
                            else None
                        ),
                        "max": (
                            float(stats[1][t_col])
                            if stats[1][t_col] is not None
                            else None
                        ),
                        "mean": (
                            float(stats[2][t_col])
                            if stats[2][t_col] is not None
                            else None
                        ),
                        "stddev": (
                            float(stats[3][t_col])
                            if stats[3][t_col] is not None
                            else None
                        ),
                    },
                }
                self.logger.debug(f"  Collected stats for {t_col}: {target_stats[t_col]}")

        # Create a list of column pairs to compare
        column_pairs = []
        mapped_pairs = set()  # Track which pairs are already mapped
        
        # Initialize stats for all columns first
        for s_col, s_type in source_columns:
            if s_col not in source_stats:
                source_stats[s_col] = {
                    "row_count": source_df_local.count(),
                    "distinct_count": source_sample.select(s_col).distinct().count(),
                    "null_count": source_sample.filter(F.col(s_col).isNull()).count(),
                }

        for t_col, t_type in target_columns:
            if t_col not in target_stats:
                target_stats[t_col] = {
                    "row_count": target_df.count(),
                    "distinct_count": target_sample.select(t_col).distinct().count(),
                    "null_count": target_sample.filter(F.col(t_col).isNull()).count(),
                }
        
        if column_mappings:
            # STEP 1: Add predefined column mappings
            self.logger.info("STEP 1: Processing predefined column mappings...")
            source_col_dict = {f.name: f.dataType for f in source_df_local.schema.fields}
            target_col_dict = {f.name: f.dataType for f in target_df.schema.fields}
            
            # Debug: Show available columns
            self.logger.info(f"  Available source columns: {sorted(source_col_dict.keys())}")
            self.logger.info(f"  Available target columns: {sorted(target_col_dict.keys())}")
            self.logger.info(f"  Processing {len(column_mappings)} predefined mappings...")
            
            for i, (s_col, t_col) in enumerate(column_mappings, 1):
                self.logger.info(f"  Processing mapping {i}/{len(column_mappings)}: '{s_col}' -> '{t_col}'")
                
                # Check if both columns exist in their respective tables
                if s_col not in source_col_dict:
                    self.logger.warning(f"    ❌ Source column '{s_col}' not found in table {source_table}")
                    self.logger.warning(f"    Available source columns: {sorted(source_col_dict.keys())}")
                    continue
                if t_col not in target_col_dict:
                    self.logger.warning(f"    ❌ Target column '{t_col}' not found in table {target_table}")
                    self.logger.warning(f"    Available target columns: {sorted(target_col_dict.keys())}")
                    continue
                
                s_type = source_col_dict[s_col]
                t_type = target_col_dict[t_col]
                
                self.logger.info(f"    Column types: {s_col}({s_type}) -> {t_col}({t_type})")
                
                if are_types_compatible(s_type, t_type):
                    column_pairs.append((s_col, t_col))
                    mapped_pairs.add((s_col, t_col))
                    self.logger.info(f"    ✅ Successfully added mapping: {s_col} -> {t_col}")
                else:
                    self.logger.warning(
                        f"    ⚠️ Skipping incompatible types: {s_col}({s_type}) ⟷ {t_col}({t_type})"
                    )
            
            predefined_count = len(column_pairs)
            self.logger.info(f"  Predefined mappings added: {predefined_count}")
            
            # STEP 2: Add automatic discovery for unmapped columns ONLY
            self.logger.info("STEP 2: Adding automatic discovery for unmapped columns...")
            
            # Get mapped source and target columns
            mapped_source_cols = {pair[0] for pair in mapped_pairs}
            mapped_target_cols = {pair[1] for pair in mapped_pairs}
            
            self.logger.info(f"  Excluding {len(mapped_source_cols)} already-mapped source columns: {sorted(mapped_source_cols)}")
            self.logger.info(f"  Excluding {len(mapped_target_cols)} already-mapped target columns: {sorted(mapped_target_cols)}")
            
            # Add automatic discovery for remaining combinations
            # ONLY between unmapped source and unmapped target columns
            auto_discovery_count = 0
            skipped_mapped_source = 0
            skipped_mapped_target = 0
            
            for s_col, s_type in source_columns:
                # Skip source columns that are already mapped
                if s_col in mapped_source_cols:
                    skipped_mapped_source += len(target_columns)  # This source col would have been compared to all targets
                    continue
                    
                for t_col, t_type in target_columns:
                    # Skip target columns that are already mapped
                    if t_col in mapped_target_cols:
                        skipped_mapped_target += 1
                        continue
                        
                    if are_types_compatible(s_type, t_type):
                        column_pairs.append((s_col, t_col))
                        auto_discovery_count += 1
                    else:
                        self.logger.debug(
                            f"  Skipping incompatible types: {s_col}({s_type}) ⟷ {t_col}({t_type})"
                        )
            
            self.logger.info(f"  Automatic discovery pairs added: {auto_discovery_count}")
            self.logger.info(f"  Skipped comparisons (mapped source cols): {skipped_mapped_source}")
            self.logger.info(f"  Skipped comparisons (mapped target cols): {skipped_mapped_target}")
            self.logger.info(f"  Unmapped source columns: {len(source_columns) - len(mapped_source_cols)}")
            self.logger.info(f"  Unmapped target columns: {len(target_columns) - len(mapped_target_cols)}")
                    
        else:
            # Use automatic column matching only (original behavior)
            self.logger.info("Using automatic column matching for all columns...")
            
            for s_col, s_type in source_columns:
                for t_col, t_type in target_columns:
                    if are_types_compatible(s_type, t_type):
                        column_pairs.append((s_col, t_col))
                    else:
                        self.logger.info(
                            f"  Skipping incompatible types: {s_col}({s_type}) ⟷ {t_col}({t_type})"
                        )

        # Summary logging
        total_possible_combinations = len(source_columns) * len(target_columns)
        self.logger.info(f"\nColumn Comparison Summary:")
        self.logger.info(f"  Total possible combinations: {total_possible_combinations:,}")
        self.logger.info(f"  Compatible pairs to compare: {len(column_pairs):,}")
        
        if column_mappings:
            predefined_count = len([pair for pair in column_pairs if pair in mapped_pairs])
            automatic_count = len(column_pairs) - predefined_count
            self.logger.info(f"  - Using predefined mappings: {predefined_count}")
            self.logger.info(f"  - Using automatic discovery: {automatic_count}")
            
            # Calculate efficiency gain
            efficiency_gain = ((total_possible_combinations - len(column_pairs)) / total_possible_combinations) * 100
            if efficiency_gain > 0:
                self.logger.info(f"  - Efficiency gain: {efficiency_gain:.1f}% fewer comparisons")
        else:
            self.logger.info(f"  - All automatic discovery: {len(column_pairs)}")

        from concurrent.futures import ThreadPoolExecutor, as_completed
        import math

        # Function to process a batch of column pairs
        def process_batch(batch_id: int, batch: List[Tuple[str, str]]):
            batch_results = []
            batch_start = time.time()
            processed = 0
            batch_size = len(batch)
            for s_col, t_col in batch:
                try:
                    # Convert both columns to string for comparison if types don't match
                    s_type = [
                        f.dataType
                        for f in source_sample.schema.fields
                        if f.name == s_col
                    ][0]
                    t_type = [
                        f.dataType
                        for f in target_sample.schema.fields
                        if f.name == t_col
                    ][0]

                    # Only cast to string if types are incompatible AND not both numeric/temporal
                    # Check if both are numeric types
                    numeric_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)
                    both_numeric = isinstance(s_type, numeric_types) and isinstance(t_type, numeric_types)

                    # Check if both are temporal types
                    temporal_types = (TimestampType, DateType)
                    both_temporal = isinstance(s_type, temporal_types) and isinstance(t_type, temporal_types)

                    if s_type != t_type and not both_numeric and not both_temporal:
                        # Cast to string only for truly incompatible types
                        self.logger.debug(f"Casting {s_col}({s_type}) and {t_col}({t_type}) to string for comparison")
                        source_col = F.col(s_col).cast(StringType())
                        target_col = F.col(t_col).cast(StringType())
                        source_sample_cast = source_sample.withColumn(s_col, source_col)
                        target_sample_cast = target_sample.withColumn(t_col, target_col)
                    else:
                        # Use original types (compatible numeric or temporal types, or exact match)
                        if both_numeric and s_type != t_type:
                            self.logger.debug(f"Preserving numeric types for {s_col}({s_type}) and {t_col}({t_type})")
                        elif both_temporal and s_type != t_type:
                            self.logger.debug(f"Preserving temporal types for {s_col}({s_type}) and {t_col}({t_type})")
                        source_sample_cast = source_sample
                        target_sample_cast = target_sample

                    # Use the pre-sampled, cached DataFrames for all comparisons
                    similarities = self.similarity_calculator.calculate_column_similarity_from_samples(
                        source_sample=source_sample_cast,
                        source_column=s_col,
                        target_sample=target_sample_cast,
                        target_column=t_col,
                        method="auto",
                    )

                    # Store the stats from similarity calculation
                    if s_col not in source_stats:
                        source_stats[s_col] = {
                            "row_count": source_df_local.count(),
                            "jaccard": similarities["jaccard"],
                            "value_distribution": similarities.get(
                                "value_distribution", similarities["jaccard"]
                            ),
                        }
                        # Get numeric stats if they were calculated
                        if "source_stats" in similarities:
                            self.logger.debug(f"\nDebug - Storing source stats for {s_col}:")
                            self.logger.debug(f"  Raw similarities: {similarities}")
                            source_stats[s_col]["numeric_stats"] = similarities[
                                "source_stats"
                            ]
                            source_stats[s_col]["distinct_count"] = similarities.get(
                                "source_distinct_count"
                            )
                            source_stats[s_col]["null_count"] = similarities.get(
                                "source_null_count"
                            )
                            self.logger.debug(f"  Stored stats: {source_stats[s_col]}")

                    if t_col not in target_stats:
                        target_stats[t_col] = {
                            "row_count": target_df.count(),
                            "jaccard": similarities["jaccard"],
                            "value_distribution": similarities.get(
                                "value_distribution", similarities["jaccard"]
                            ),
                        }
                        # Get numeric stats if they were calculated
                        if "target_stats" in similarities:
                            self.logger.debug(f"\nDebug - Storing target stats for {t_col}:")
                            self.logger.debug(f"  Raw similarities: {similarities}")
                            target_stats[t_col]["numeric_stats"] = similarities[
                                "target_stats"
                            ]
                            target_stats[t_col]["distinct_count"] = similarities.get(
                                "target_distinct_count"
                            )
                            target_stats[t_col]["null_count"] = similarities.get(
                                "target_null_count"
                            )
                            self.logger.debug(f"  Stored stats: {target_stats[t_col]}")

                    if similarities["overall"] >= column_data_match_threshold:
                        batch_results.append((s_col, t_col, similarities))
                        self.logger.info(
                            f"    ✓ Match found: {s_col} -> {t_col} (score: {similarities['overall']:.4f})"
                        )
                    else:
                        self.logger.info(
                            f"    ✗ Below threshold: {s_col} -> {t_col} (score: {similarities['overall']:.4f}, threshold: {column_data_match_threshold})"
                        )
                except Exception as e:
                    self.logger.warning(f"    ⚠️ Error comparing {s_col} and {t_col}: {str(e)}")
                processed += 1
                if processed % max(1, batch_size // 10) == 0:
                    pass
            batch_time = time.time() - batch_start
            self.logger.debug(
                f"  ✓ Batch {batch_id} complete: {len(batch_results)} matches found in {batch_time:.2f}s"
            )
            return batch_results

        # Determine optimal batch size and number of workers
        num_cores = os.cpu_count() or 4
        num_workers = min(max(num_cores - 1, 2), 8)
        actual_comparisons = len(column_pairs)
        batch_size = math.ceil(actual_comparisons / (num_workers * 4)) if actual_comparisons > 0 else 1
        batches = [
            column_pairs[i : i + batch_size]
            for i in range(0, len(column_pairs), batch_size)
        ]
        num_workers = len(batches)
        self.logger.info(f"\nStarting parallel processing:")
        self.logger.info(f"  Using {num_workers} parallel workers")
        self.logger.info(f"  {len(batches)} batches of ~{batch_size} comparisons each")
        all_results = []
        completed_comparisons = 0
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_batch = {
                executor.submit(process_batch, i, batch): (i, batch)
                for i, batch in enumerate(batches, 1)
            }
            for future in as_completed(future_to_batch):
                batch_id, batch = future_to_batch[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                    completed_comparisons += len(batch)
                    progress = completed_comparisons / actual_comparisons * 100 if actual_comparisons > 0 else 100
                    elapsed = time.time() - start_time
                    est_total = elapsed / progress * 100 if progress > 0 else 0
                    remaining = max(0, est_total - elapsed)
                    self.logger.info(
                        f"Progress: {completed_comparisons:,}/{actual_comparisons:,} comparisons ({progress:.1f}%)"
                    )
                    self.logger.debug(
                        f"Elapsed: {elapsed:.1f}s, Estimated remaining: {remaining:.1f}s"
                    )
                    self.logger.debug(f"Matches found so far: {len(all_results)}")
                except Exception as e:
                    self.logger.error(f"⚠️ Error processing batch {batch_id}: {str(e)}")
        results: Dict[str, Dict[str, Dict[str, float]]] = {}
        for s_col, t_col, similarities in all_results:
            if s_col not in results:
                results[s_col] = {}
            results[s_col][t_col] = similarities
        elapsed_time = time.time() - start_time
        matches_found = sum(len(matches) for matches in results.values())
        self.logger.info(f"\nMatching complete in {elapsed_time:.2f}s")
        self.logger.info(
            f"Found {matches_found:,} total matches across {len(results)} source columns"
        )
        self.logger.info(f"Average time per comparison: {elapsed_time/actual_comparisons:.4f}s" if actual_comparisons > 0 else "No comparisons performed")
        total_similarity_score = 0
        total_matched_columns = 0
        for s_col, t_matches in results.items():
            if t_matches:
                best_match = max(t_matches.items(), key=lambda x: x[1]["overall"])
                best_target_col, best_similarity = best_match
                total_similarity_score += best_similarity["overall"]
                total_matched_columns += 1
        source_coverage = (
            total_matched_columns / len(source_columns) if source_columns else 0
        )
        if total_matched_columns > 0:
            avg_match_quality = total_similarity_score / total_matched_columns
        else:
            avg_match_quality = 0
        overall_compatibility = (source_coverage + avg_match_quality) / 2 * 100
        self.logger.info("\nTable Compatibility Summary:")
        self.logger.info(
            f"  {source_table} ⟷ {target_table}: {overall_compatibility:.1f}% compatible"
        )
        self.logger.info(
            f"  • Columns with matches: {total_matched_columns}/{len(source_columns)} ({source_coverage*100:.1f}%)"
        )
        self.logger.info(f"  • Average match quality: {avg_match_quality:.4f}")
        if total_matched_columns > 0:
            self.logger.info("\nTop 5 highest quality matches:")
            all_matches = [
                (s, t, m["overall"])
                for s, t_dict in results.items()
                for t, m in t_dict.items()
            ]
            all_matches.sort(key=lambda x: x[2], reverse=True)
            for idx, (source_col, target_col, score) in enumerate(all_matches[:5], 1):
                self.logger.info(f"  {idx}. {source_col} ⟷ {target_col}: {score:.4f}")
        self.logger.info("\nCleaning up cached DataFrames...")
        source_sample.unpersist()
        target_sample.unpersist()
        if source_df is None:
            source_df_local.unpersist()
        target_df.unpersist()

        # After calculating overall_compatibility, source_coverage, and avg_match_quality
        self.logger.info("\nSaving results to Delta tables...")

        # Save individual table statistics
        self.logger.info("\nSaving source table statistics...")
        self.results_writer.save_table_column_stats(
            table_name=source_table,
            table_type="source",
            table_df=source_df_local,
            column_stats=source_stats,
        )

        self.logger.info("\nSaving target table statistics...")
        self.results_writer.save_table_column_stats(
            table_name=target_table,
            table_type="target",
            table_df=target_df,
            column_stats=target_stats,
        )

        # Save comparison details and analysis
        self.results_writer.save_column_comparison_details(
            source_table=source_table,
            target_table=target_table,
            source_columns=[col for col, _ in source_columns],
            target_columns=[col for col, _ in target_columns],
            matches=results,
            source_sample=source_sample,
            target_sample=target_sample,
        )

        self.results_writer.save_table_analysis(
            source_table=source_table,
            target_table=target_table,
            source_columns=[col for col, _ in source_columns],
            target_columns=[col for col, _ in target_columns],
            matches=results,
            overall_compatibility=overall_compatibility,
            source_coverage=source_coverage,
            avg_match_quality=avg_match_quality,
            source_count=source_df_local.count(),
            target_count=target_df.count(),
        )

        return results

    def find_subset_tables(
        self,
        source_table: str,
        candidate_tables: List[str],
        column_data_match_threshold: float = 0.9,
        table_column_match_ratio: float = 0.7,
        sample_size: int = 1000,
        source_key_columns: Optional[List[str]] = None,
        target_key_columns: Optional[List[str]] = None,
        source_type: str = "databricks",
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: Optional[int] = None,
        processing_mode: ProcessingMode = 'deltalake',
        output_mode: Optional[OutputMode] = None,
        column_mappings_path: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Find tables that might be subsets, supersets, or related to the source table.

        Args:
            source_table: Name of the source table
            candidate_tables: List of candidate table names to check
            column_data_match_threshold: Threshold above which columns are considered matching
            table_column_match_ratio: Minimum ratio of matching columns required
            sample_size: Number of rows to sample for comparison
            source_key_columns: If provided, will order source table by these columns before limiting
            target_key_columns: If provided, will order target tables by these columns before limiting
            source_type: 'databricks' (default) or 'oracle'. If 'oracle', reads source from Oracle DB
            partition_column, lower_bound, upper_bound, num_partitions: Partitioning options for Oracle JDBC
            processing_mode: How to process Oracle data. Options:
                - 'deltalake' (default): Save to Delta Lake and read back for better performance
                - 'direct': Use JDBC connection directly (may impact Oracle performance)
                - 'memory': Load data into memory via Pandas (for small datasets)
            output_mode: Output mode to use ('text', 'deltalake', or 'csv'). If None, uses the mode set in constructor.
            column_mappings_path: Optional path to CSV file containing column mappings. Overrides the instance mappings.

        Returns:
            Dict mapping table names to their analysis results
        """
        # If output_mode is provided and different from current mode, temporarily change it
        original_results_writer = None
        if output_mode is not None and output_mode != self.output_mode:
            self.logger.info(f"Temporarily switching to output mode: {output_mode}")
            original_results_writer = self.results_writer
            self.results_writer = ResultsWriter(
                spark=self.spark,
                catalog=self.finder_catalog["name"],
                schema=self.finder_catalog["schema"],
                volume=self.finder_catalog["volume"],
                run_timestamp=self.run_timestamp,
                output_mode=output_mode
            )

        try:
            overall_start = time.time()
            self.logger.info(f"\nAnalyzing {len(candidate_tables)} potential subset tables...")
            self.logger.info(f"Source table: {source_table}")
            if source_key_columns:
                self.logger.info(f"Source key columns: {', '.join(source_key_columns)}")
            if target_key_columns:
                self.logger.info(f"Target key columns: {', '.join(target_key_columns)}")

            # Initialize column mapper for this run if provided
            run_column_mapper = None
            if column_mappings_path:
                self.logger.info(f"Loading column mappings from: {column_mappings_path}")
                run_column_mapper = ColumnMapper(column_mappings_path)
            elif self.column_mapper:
                self.logger.info("Using instance column mappings")
                run_column_mapper = self.column_mapper
            else:
                self.logger.info("No column mappings available - using automatic discovery")

            results = {}

            # Load and cache the source table once
            self.logger.info(f"\nLoading source table...")
            source_df = self.load_source_dataframe(
                source_table=source_table,
                source_type=source_type,
                processing_mode=processing_mode,
                sample_size=sample_size,
                partition_column=partition_column,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                num_partitions=num_partitions,
                source_key_columns=source_key_columns,
                target_key_columns=target_key_columns
            )
            source_df_count = source_df.count()
            source_columns = [f.name.upper() for f in source_df.schema.fields]
            norm_source_key_columns = self._normalize_columns(source_key_columns) if source_key_columns else None
            norm_target_key_columns = self._normalize_columns(target_key_columns) if target_key_columns else None
            self.logger.info(f"  • Table: {source_table}")
            self.logger.info(f"  • Rows: {source_df_count:,}")
            self.logger.info(f"  • Columns: {len(source_columns)}")

            # Store compatibility scores for all tables
            compatibility_scores = []

            for i, candidate in enumerate(candidate_tables, 1):
                candidate_start = time.time()
                self.logger.info(f"\n{'-'*100}")
                self.logger.info(f"Analyzing Candidate {i}/{len(candidate_tables)}: {candidate}")
                self.logger.info(f"{'-'*100}")

                try:
                    # Get column mappings for this specific table pair
                    candidate_column_mappings = None
                    if run_column_mapper:
                        candidate_column_mappings = run_column_mapper.get_column_mappings(source_table, candidate)
                        if candidate_column_mappings:
                            self.logger.info(f"  Using {len(candidate_column_mappings)} predefined column mappings for {candidate}")
                        else:
                            self.logger.info(f"  No predefined mappings found for {candidate}, using automatic discovery")

                    # Find matching columns using parallel implementation
                    matches = self.find_matching_columns_parallel(
                        source_table=source_table,
                        target_table=candidate,
                        column_data_match_threshold=column_data_match_threshold,
                        sample_size=sample_size,
                        source_key_columns=norm_source_key_columns,
                        target_key_columns=norm_target_key_columns,
                        source_df=source_df,  # Pass the Delta DataFrame
                        column_mappings=candidate_column_mappings,
                    )

                    # Get target columns for unmatched analysis
                    target_df = self.spark.table(candidate)
                    target_columns = [f.name.upper() for f in target_df.schema.fields]

                    # Calculate table-level compatibility metrics
                    total_similarity_score = 0
                    total_matched_columns = 0

                    # Track best matches for each column
                    best_matches = {}
                    for s_col, t_matches in matches.items():
                        if t_matches:
                            best_match = max(
                                t_matches.items(), key=lambda x: x[1]["overall"]
                            )
                            best_target_col, best_similarity = best_match
                            total_similarity_score += best_similarity["overall"]
                            total_matched_columns += 1
                            best_matches[s_col] = (
                                best_target_col,
                                best_similarity["overall"],
                            )

                    # Calculate compatibility scores
                    source_coverage = (
                        total_matched_columns / len(source_columns) if source_columns else 0
                    )

                    if total_matched_columns > 0:
                        avg_match_quality = total_similarity_score / total_matched_columns
                    else:
                        avg_match_quality = 0

                    overall_compatibility = (
                        (source_coverage + avg_match_quality) / 2 * 100
                    )  # as percentage

                    # Store for later summary
                    compatibility_scores.append(
                        (
                            candidate,
                            overall_compatibility,
                            source_coverage,
                            avg_match_quality,
                        )
                    )

                    # Get row counts
                    candidate_count = target_df.count()

                    # Calculate containment scores
                    avg_source_in_target = 0
                    avg_target_in_source = 0
                    count = 0

                    for s_col, t_col_matches in matches.items():
                        for t_col, similarities in t_col_matches.items():
                            avg_source_in_target += similarities.get(
                                "containment_source_in_target", 0
                            )
                            avg_target_in_source += similarities.get(
                                "containment_target_in_source", 0
                            )
                            count += 1

                    if count > 0:
                        avg_source_in_target /= count
                        avg_target_in_source /= count

                    relationship = (
                        "subset"
                        if avg_target_in_source > 0.8
                        else "superset" if avg_source_in_target > 0.8 else "related"
                    )

                    # Check if thresholds are met
                    meets_column_threshold = len(matches) >= len(source_columns) * table_column_match_ratio
                    
                    self.logger.info("\nRelationship Analysis:")
                    self.logger.info(f"  • Type: {relationship.upper()}")
                    self.logger.info(f"  • Overall Compatibility: {overall_compatibility:.1f}%")
                    self.logger.info(f"  • Rows: {candidate_count:,} / {source_df_count:,}")
                    self.logger.info(
                        f"  • Matched Columns: {total_matched_columns} / {len(source_columns)} ({source_coverage*100:.1f}%)"
                    )
                    self.logger.info(f"  • Average Match Quality: {avg_match_quality:.4f}")
                    self.logger.info(f"  • Content Overlap:")
                    self.logger.info(
                        f"    - Source content in target: {avg_source_in_target*100:.1f}%"
                    )
                    self.logger.info(
                        f"    - Target content in source: {avg_target_in_source*100:.1f}%"
                    )

                    # If enough columns match, consider it a potential subset
                    if meets_column_threshold:
                        results[candidate] = {
                            "matching_columns": matches,
                            "source_row_count": source_df_count,
                            "candidate_row_count": candidate_count,
                            "is_potential_subset": candidate_count <= source_df_count,
                            "match_ratio": len(matches) / len(source_columns),
                            "avg_source_in_target": avg_source_in_target,
                            "avg_target_in_source": avg_target_in_source,
                            "relationship": relationship,
                            "compatibility_score": overall_compatibility,
                            "best_matches": best_matches,
                            "source_columns": source_columns,
                            "target_columns": target_columns,
                        }
                    else:
                        self.logger.info(f"\n❌ Column Match Threshold Not Met:")
                        self.logger.info(f"  • Found: {len(matches)} matches")
                        self.logger.info(
                            f"  • Required: {len(source_columns) * table_column_match_ratio:.0f} columns"
                        )

                    # Always print detailed column analysis
                    self.logger.info("\nColumn Analysis:")
                    self.logger.info(f"{'='*100}")
                    self.logger.info(
                        "{:<30} {:<30} {:<10} {:<10} {:<15}".format(
                            "Source Column",
                            "Target Match",
                            "Score",
                            "Status",
                            "Match %",
                        )
                    )
                    self.logger.info("-" * 100)

                    # Create a mapping from manual mappings for this specific table pair
                    manual_mappings_dict = {}
                    if candidate_column_mappings:
                        for source_col, target_col in candidate_column_mappings:
                            manual_mappings_dict[source_col.upper()] = target_col.upper()

                    # Sort source columns by match status and score
                    sorted_source_cols = []
                    for col in source_columns:
                        if col in best_matches:
                            target_col, score = best_matches[col]
                            # Get containment score for this column pair
                            containment = (
                                matches[col][target_col].get(
                                    "containment_source_in_target", 0
                                )
                                * 100
                            )
                            # Check if this specific column meets its individual threshold
                            column_meets_threshold = score >= column_data_match_threshold
                            sorted_source_cols.append(
                                (col, target_col, score, True, containment, column_meets_threshold)
                            )
                        elif col in manual_mappings_dict:
                            # Column has manual mapping but no/low automated match
                            target_col = manual_mappings_dict[col]
                            # Check if this column was processed but had 0 similarity
                            score = 0.0
                            containment = 0.0
                            if col in matches and target_col in matches[col]:
                                score = matches[col][target_col].get("overall", 0.0)
                                containment = matches[col][target_col].get("containment_source_in_target", 0.0) * 100
                            
                            # Check if this specific column meets its individual threshold
                            column_meets_threshold = score >= column_data_match_threshold
                            sorted_source_cols.append(
                                (col, target_col, score, True, containment, column_meets_threshold)
                            )
                        else:
                            sorted_source_cols.append(
                                (col, "NO MATCH", 0.0, False, 0.0, False)
                            )

                    # Sort by matched status (matched first) and then by score
                    sorted_source_cols.sort(key=lambda x: (-x[3], -x[2]))

                    for (
                        col,
                        match,
                        score,
                        is_matched,
                        containment,
                        meets_threshold,
                    ) in sorted_source_cols:
                        # Show ✓ if individual column meets its threshold
                        status = "✓" if meets_threshold else "✗"
                        if is_matched:
                            self.logger.info(
                                "{:<30} {:<30} {:<10.4f} {:<10} {:<15.1f}%".format(
                                    col, match, score, status, containment
                                )
                            )
                        else:
                            self.logger.info(
                                "{:<30} {:<30} {:<10} {:<10} {:<15}".format(
                                    col, "-", "-", status, "-"
                                )
                            )

                    # Show unmatched target columns
                    matched_target_cols = set()
                    for matches_dict in matches.values():
                        matched_target_cols.update(matches_dict.keys())

                    unmatched_target = [
                        col for col in target_columns if col not in matched_target_cols
                    ]
                    if unmatched_target:
                        self.logger.info(f"\nUnmatched Target Columns ({len(unmatched_target)}):")
                        self.logger.info("  " + ", ".join(unmatched_target))

                except Exception as e:
                    self.logger.error(f"\n⚠️ Error analyzing table:")
                    self.logger.error(f"  {str(e)}")

                candidate_time = time.time() - candidate_start
                self.logger.info(f"\nProcessing time: {candidate_time:.2f}s")

            # Clean up source DataFrame
            source_df.unpersist()

            overall_time = time.time() - overall_start

            # Print final summary
            self.logger.info(f"\n{'='*100}")
            self.logger.info("Final Analysis Summary")
            self.logger.info(f"{'='*100}")
            self.logger.info(f"Processing time: {overall_time:.2f}s")
            self.logger.info(f"Found {len(results)} potential related tables")

            # Sort tables by compatibility score
            compatibility_scores.sort(key=lambda x: x[1], reverse=True)

            self.logger.info("\nTable Compatibility Ranking:")
            self.logger.info("Compatibility scores are calculated based on column coverage and match quality.")
            self.logger.info("Higher scores indicate better potential relationships between tables.\n")
            
            for table, score, coverage, quality in compatibility_scores:
                relationship = results.get(table, {}).get("relationship", "-")
                status = "✓" if table in results else "✗"
                
                # Create explanatory text
                explanation = (
                    f"Table {table} has {score:.1f}% compatibility "
                    f"(calculated as the average of column coverage and match quality) "
                    f"with {coverage*100:.1f}% column coverage "
                    f"(percentage of source table columns that have matching counterparts in this table) "
                    f"and {quality:.4f} match quality "
                    f"(average similarity score of the matched columns)."
                )
                
                self.logger.info(f"  {status} {table}")
                self.logger.info(f"    {explanation}")
                self.logger.info(f"    • Compatibility: {score:.1f}%")
                self.logger.info(f"    • Column Coverage: {coverage*100:.1f}%")
                self.logger.info(f"    • Match Quality: {quality:.4f}")
                self.logger.info(f"    • Relationship: {relationship.upper()}")
                self.logger.info("")  # Add blank line for better readability

            return results

        finally:
            # Restore original results_writer if we changed it
            if original_results_writer is not None:
                self.results_writer = original_results_writer
                self.logger.info("Restored original output mode")
