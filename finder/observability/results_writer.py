"""
Results Writer

This module handles saving comparison results and analysis data to Delta tables
for observability and reporting purposes.
"""
from typing import Dict, List, Any, Optional, Tuple, Literal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, FloatType, DoubleType, DecimalType,
    TimestampType, DateType, StringType, BooleanType, StructType, 
    StructField, ArrayType
)
import sys
from pathlib import Path

# Add the parent directory to Python path to allow imports from utils
sys.path.append(str(Path(__file__).parent.parent.parent))

from utils.logger import get_finder_logger

# Define valid output modes
OutputMode = Literal['text', 'deltalake']

class ResultsWriter:
    """Handles writing comparison results and analysis data in different formats."""
    
    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str, run_timestamp: str, output_mode: OutputMode = 'deltalake'):
        """
        Initialize the ResultsWriter.
        
        Args:
            spark: SparkSession to use
            catalog: Databricks catalog name
            schema: Schema name
            volume: Volume name
            run_timestamp: Timestamp for this run
            output_mode: Output mode to use ('text' or 'deltalake')
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.run_timestamp = run_timestamp
        self.output_mode = output_mode
        self.logger = get_finder_logger(level="INFO", include_debug=True)
    
    def _get_results_path(self, results_type: str) -> str:
        """Get path for results."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.run_timestamp}/results/{results_type}"

    def _save_dataframe(self, df: DataFrame, results_type: str) -> None:
        """Save DataFrame using the configured output mode."""
        path = self._get_results_path(results_type)
        
        if self.output_mode == 'deltalake':
            df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
            self.logger.info(f"Saved results to Delta table: {path}")
        else:  # text mode
            # For text mode, we log the results and don't save to disk
            self.logger.info(f"\nResults for {results_type}:")
            df.show(truncate=False)
            self.logger.info("\nDetailed results (all rows):")
            for row in df.collect():
                self.logger.info(row.asDict())

    def save_column_comparison_details(
        self,
        source_table: str,
        target_table: str,
        source_columns: List[str],
        target_columns: List[str],
        matches: Dict[str, Dict[str, Dict[str, float]]],
        source_sample: DataFrame,
        target_sample: DataFrame
    ) -> None:
        """Save detailed column comparison results as Delta table."""
        try:
            # Define schema for the results DataFrame
            details_schema = StructType([
                StructField("run_timestamp", StringType(), False),
                StructField("source_table", StringType(), False),
                StructField("target_table", StringType(), False),
                StructField("source_column", StringType(), True),
                StructField("target_column", StringType(), True),
                StructField("match_score", DoubleType(), False),
                StructField("jaccard_similarity", DoubleType(), False),
                StructField("value_distribution_similarity", DoubleType(), False),
                StructField("source_in_target", DoubleType(), False),
                StructField("target_in_source", DoubleType(), False),
                # Store all stats as strings to handle mixed types
                StructField("source_min", StringType(), True),
                StructField("source_max", StringType(), True),
                StructField("source_mean", StringType(), True),
                StructField("source_stddev", StringType(), True),
                StructField("source_distinct_values", LongType(), True),
                StructField("target_min", StringType(), True),
                StructField("target_max", StringType(), True),
                StructField("target_mean", StringType(), True),
                StructField("target_stddev", StringType(), True),
                StructField("target_distinct_values", LongType(), True),
                StructField("is_matched", BooleanType(), False),
                # Add column type information
                StructField("source_column_type", StringType(), True),
                StructField("target_column_type", StringType(), True)
            ])
            
            # Create rows for detailed analysis
            detailed_rows = []
            
            def get_column_stats(df: DataFrame, col_name: str) -> Tuple[str, str, str, str, int, str]:
                """Safely get column statistics handling different data types."""
                try:
                    # Get column data type
                    col_type = [f.dataType for f in df.schema.fields if f.name == col_name][0]
                    type_name = str(col_type)
                    
                    # For numeric types, get full stats
                    if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                        stats = df.select(
                            F.min(col_name).alias("min"),
                            F.max(col_name).alias("max"),
                            F.mean(col_name).alias("mean"),
                            F.stddev(col_name).alias("stddev")
                        ).collect()[0]
                        return (
                            str(stats["min"]) if stats["min"] is not None else None,
                            str(stats["max"]) if stats["max"] is not None else None,
                            str(stats["mean"]) if stats["mean"] is not None else None,
                            str(stats["stddev"]) if stats["stddev"] is not None else None,
                            df.select(col_name).distinct().count(),
                            type_name
                        )
                    
                    # For timestamp/date types, get min/max only
                    elif isinstance(col_type, (TimestampType, DateType)):
                        stats = df.select(
                            F.min(col_name).alias("min"),
                            F.max(col_name).alias("max")
                        ).collect()[0]
                        return (
                            str(stats["min"]) if stats["min"] is not None else None,
                            str(stats["max"]) if stats["max"] is not None else None,
                            None,
                            None,
                            df.select(col_name).distinct().count(),
                            type_name
                        )
                    
                    # For other types (string, boolean, etc.), get distinct count only
                    else:
                        return (
                            None,
                            None,
                            None,
                            None,
                            df.select(col_name).distinct().count(),
                            type_name
                        )
                except Exception as e:
                    self.logger.warning(f"Could not compute stats for column {col_name}: {str(e)}")
                    return None, None, None, None, 0, "unknown"
            
            for s_col in source_columns:
                # Get source column stats
                s_min, s_max, s_mean, s_stddev, s_distinct, s_type = get_column_stats(source_sample, s_col)
                
                # Process matches if any
                if s_col in matches:
                    for t_col, metrics in matches[s_col].items():
                        # Get target column stats
                        t_min, t_max, t_mean, t_stddev, t_distinct, t_type = get_column_stats(target_sample, t_col)
                        
                        detailed_rows.append({
                            "run_timestamp": self.run_timestamp,
                            "source_table": source_table,
                            "target_table": target_table,
                            "source_column": s_col,
                            "target_column": t_col,
                            "match_score": metrics.get("overall", 0.0),
                            "jaccard_similarity": metrics.get("jaccard", 0.0),
                            "value_distribution_similarity": metrics.get("value_distribution", 0.0),
                            "source_in_target": metrics.get("containment_source_in_target", 0.0),
                            "target_in_source": metrics.get("containment_target_in_source", 0.0),
                            "source_min": s_min,
                            "source_max": s_max,
                            "source_mean": s_mean,
                            "source_stddev": s_stddev,
                            "source_distinct_values": s_distinct,
                            "target_min": t_min,
                            "target_max": t_max,
                            "target_mean": t_mean,
                            "target_stddev": t_stddev,
                            "target_distinct_values": t_distinct,
                            "is_matched": True,
                            "source_column_type": s_type,
                            "target_column_type": t_type
                        })
                else:
                    # Add unmatched source column
                    detailed_rows.append({
                        "run_timestamp": self.run_timestamp,
                        "source_table": source_table,
                        "target_table": target_table,
                        "source_column": s_col,
                        "target_column": None,
                        "match_score": 0.0,
                        "jaccard_similarity": 0.0,
                        "value_distribution_similarity": 0.0,
                        "source_in_target": 0.0,
                        "target_in_source": 0.0,
                        "source_min": s_min,
                        "source_max": s_max,
                        "source_mean": s_mean,
                        "source_stddev": s_stddev,
                        "source_distinct_values": s_distinct,
                        "target_min": None,
                        "target_max": None,
                        "target_mean": None,
                        "target_stddev": None,
                        "target_distinct_values": None,
                        "is_matched": False,
                        "source_column_type": s_type,
                        "target_column_type": None
                    })
            
            # Add unmatched target columns
            matched_target_cols = set()
            for matches_dict in matches.values():
                matched_target_cols.update(matches_dict.keys())
            
            for t_col in target_columns:
                if t_col not in matched_target_cols:
                    t_min, t_max, t_mean, t_stddev, t_distinct, t_type = get_column_stats(target_sample, t_col)
                    
                    detailed_rows.append({
                        "run_timestamp": self.run_timestamp,
                        "source_table": source_table,
                        "target_table": target_table,
                        "source_column": None,
                        "target_column": t_col,
                        "match_score": 0.0,
                        "jaccard_similarity": 0.0,
                        "value_distribution_similarity": 0.0,
                        "source_in_target": 0.0,
                        "target_in_source": 0.0,
                        "source_min": None,
                        "source_max": None,
                        "source_mean": None,
                        "source_stddev": None,
                        "source_distinct_values": None,
                        "target_min": t_min,
                        "target_max": t_max,
                        "target_mean": t_mean,
                        "target_stddev": t_stddev,
                        "target_distinct_values": t_distinct,
                        "is_matched": False,
                        "source_column_type": None,
                        "target_column_type": t_type
                    })
            
            if not detailed_rows:
                self.logger.warning("No comparison details to save")
                return
                
            # Create DataFrame with explicit schema
            details_df = self.spark.createDataFrame(detailed_rows, schema=details_schema)
            self._save_dataframe(details_df, "results_table_details")
            
        except Exception as e:
            self.logger.error(f"Error saving column comparison details: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def save_table_analysis(
        self,
        source_table: str,
        target_table: str,
        source_columns: List[str],
        target_columns: List[str],
        matches: Dict[str, Dict[str, Dict[str, float]]],
        overall_compatibility: float,
        source_coverage: float,
        avg_match_quality: float,
        source_count: int,
        target_count: int
    ) -> None:
        """Save table-level analysis results as Delta table."""
        try:
            # Define schema for analysis results
            analysis_schema = StructType([
                StructField("run_timestamp", StringType(), False),
                StructField("source_table", StringType(), False),
                StructField("target_table", StringType(), False),
                StructField("source_column_count", IntegerType(), False),
                StructField("target_column_count", IntegerType(), False),
                StructField("matched_column_count", IntegerType(), False),
                StructField("source_coverage_pct", DoubleType(), False),
                StructField("avg_match_quality", DoubleType(), False),
                StructField("overall_compatibility_pct", DoubleType(), False),
                StructField("source_row_count", LongType(), False),
                StructField("target_row_count", LongType(), False),
                StructField("unmatched_source_columns", ArrayType(StringType(), True), True),
                StructField("unmatched_target_columns", ArrayType(StringType(), True), True),
                StructField("best_matches", ArrayType(
                    StructType([
                        StructField("source_column", StringType(), False),
                        StructField("target_column", StringType(), False),
                        StructField("score", DoubleType(), False)
                    ]),
                    True
                ), True)
            ])

            # Create analysis row with proper types
            analysis_row = {
                "run_timestamp": self.run_timestamp,
                "source_table": source_table,
                "target_table": target_table,
                "source_column_count": len(source_columns),
                "target_column_count": len(target_columns),
                "matched_column_count": len(matches),
                "source_coverage_pct": source_coverage * 100,
                "avg_match_quality": avg_match_quality,
                "overall_compatibility_pct": overall_compatibility,
                "source_row_count": source_count,
                "target_row_count": target_count,
                "unmatched_source_columns": [col for col in source_columns if col not in matches],
                "unmatched_target_columns": [col for col in target_columns if col not in set(t_col for m in matches.values() for t_col in m.keys())],
                "best_matches": [
                    {
                        "source_column": s_col,
                        "target_column": max(t_matches.items(), key=lambda x: x[1]["overall"])[0],
                        "score": float(max(t_matches.items(), key=lambda x: x[1]["overall"])[1]["overall"])
                    }
                    for s_col, t_matches in matches.items()
                ] if matches else []
            }
            
            # Create and save DataFrame with explicit schema
            analysis_df = self.spark.createDataFrame([analysis_row], schema=analysis_schema)
            self._save_dataframe(analysis_df, "results_table_analysis")
            
        except Exception as e:
            self.logger.error(f"Error saving table analysis: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    def save_table_column_stats(
        self,
        table_name: str,
        table_type: str,  # 'source' or 'target'
        table_df: DataFrame,
        column_stats: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> None:
        """
        Save detailed statistics for each column in a table, reusing statistics
        collected during the column matching process.
        
        Args:
            table_name: Name of the table being analyzed
            table_type: Either 'source' or 'target'
            table_df: DataFrame containing the table data
            column_stats: Optional dictionary containing pre-calculated column statistics
                        from the column matching process
        """
        try:
            # Define schema for column statistics based on metrics we collect
            stats_schema = StructType([
                StructField("run_timestamp", StringType(), False),
                StructField("table_name", StringType(), False),
                StructField("table_type", StringType(), False),
                StructField("column_name", StringType(), False),
                StructField("data_type", StringType(), False),
                # Basic counts
                StructField("row_count", LongType(), True),
                StructField("distinct_count", LongType(), True),
                StructField("null_count", LongType(), True),
                # Distribution metrics from column matching
                StructField("min_value", StringType(), True),
                StructField("max_value", StringType(), True),
                StructField("mean_value", StringType(), True),
                StructField("stddev_value", StringType(), True),
                # Value distribution metrics
                StructField("jaccard_distinct_ratio", DoubleType(), True),
                StructField("value_distribution_score", DoubleType(), True)
            ])

            stats_rows = []
            
            for column in table_df.columns:
                # Get column type
                col_type = [f.dataType for f in table_df.schema.fields if f.name == column][0]
                type_name = str(col_type)
                
                # Get pre-calculated stats if available
                col_stats = column_stats.get(column, {}) if column_stats else {}
                
                # Base statistics
                stats_dict = {
                    "run_timestamp": self.run_timestamp,
                    "table_name": table_name,
                    "table_type": table_type,
                    "column_name": column,
                    "data_type": type_name,
                    "row_count": col_stats.get("row_count", table_df.count()),
                    "distinct_count": col_stats.get("distinct_count"),
                    "null_count": col_stats.get("null_count")
                }
                
                # Get distribution metrics that were calculated during matching
                numeric_stats = col_stats.get("numeric_stats", {})
                self.logger.debug(f"Processing numeric stats for {column}:")
                self.logger.debug(f"Raw numeric_stats: {numeric_stats}")
                
                if numeric_stats and any(v is not None for v in numeric_stats.values()):
                    # Convert numeric values to strings, handling None values
                    stats_dict.update({
                        "min_value": str(numeric_stats["min"]) if numeric_stats.get("min") is not None else None,
                        "max_value": str(numeric_stats["max"]) if numeric_stats.get("max") is not None else None,
                        "mean_value": str(numeric_stats["mean"]) if numeric_stats.get("mean") is not None else None,
                        "stddev_value": str(numeric_stats["stddev"]) if numeric_stats.get("stddev") is not None else None
                    })
                    self.logger.debug(f"Processed stats: {stats_dict}")
                else:
                    self.logger.debug(f"No valid numeric stats found for column {column}")
                    stats_dict.update({
                        "min_value": None,
                        "max_value": None,
                        "mean_value": None,
                        "stddev_value": None
                    })
                
                # Get similarity metrics that were calculated during matching
                stats_dict.update({
                    "jaccard_distinct_ratio": col_stats.get("jaccard", 0.0),
                    "value_distribution_score": col_stats.get("value_distribution", 0.0)
                })
                
                # If we're missing any required stats, calculate them
                if stats_dict["distinct_count"] is None:
                    stats_dict["distinct_count"] = table_df.select(column).distinct().count()
                if stats_dict["null_count"] is None:
                    stats_dict["null_count"] = table_df.filter(F.col(column).isNull()).count()
                
                # Add debug logging to see what stats we're getting
                self.logger.debug(f"Stats for column {column}:")
                self.logger.debug(f"Input col_stats: {col_stats}")
                self.logger.debug(f"Numeric stats: {numeric_stats}")
                self.logger.debug(f"Final stats dict: {stats_dict}")
                
                stats_rows.append(stats_dict)
            
            if not stats_rows:
                self.logger.warning(f"No statistics to save for table {table_name}")
                return
                
            # Create DataFrame with explicit schema
            stats_df = self.spark.createDataFrame(stats_rows, schema=stats_schema)
            self._save_dataframe(stats_df, "results_column_stats")
            
        except Exception as e:
            self.logger.error(f"Error saving column statistics for table {table_name}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc()) 