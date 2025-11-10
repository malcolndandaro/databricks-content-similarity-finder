"""
Column Similarity Calculator

This module provides functionality to calculate similarity between columns
based on their data content.
"""

import sys
from pathlib import Path

# Add the parent directory to Python path to allow imports from utils
sys.path.append(str(Path(__file__).parent.parent))

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType,
    StringType,
    BooleanType,
    TimestampType,
    DateType,
)
from utils.logger import get_finder_logger


class ColumnSimilarityCalculator:
    """
    A class to calculate similarity between columns based on their content.
    """

    def __init__(self, spark: SparkSession):
        """Initialize the calculator with a SparkSession."""
        self.spark = spark
        self.logger = get_finder_logger(level="INFO", include_debug=True)

    def calculate_column_similarity_from_samples(
        self,
        source_sample: DataFrame,
        source_column: str,
        target_sample: DataFrame,
        target_column: str,
        method: str = "auto",
    ) -> Dict[str, float]:
        """
        Calculate similarity between two columns using pre-sampled DataFrames.
        """
        # Determine column types from the samples
        source_type = source_sample.schema[source_column].dataType
        target_type = target_sample.schema[target_column].dataType

        # Check if columns are numeric or strings
        is_source_numeric = str(source_type).startswith(
            ("IntegerType", "LongType", "DoubleType", "FloatType", "DecimalType")
        )
        is_target_numeric = str(target_type).startswith(
            ("IntegerType", "LongType", "DoubleType", "FloatType", "DecimalType")
        )
        is_source_string = str(source_type).startswith("StringType")
        is_target_string = str(target_type).startswith("StringType")

        results = {}

        # Handle type mismatch for Jaccard similarity
        try:
            # For type mismatches, convert both to strings for comparison
            # UNLESS both are temporal types (timestamp/date) - then normalize to dates
            is_source_temporal = isinstance(source_type, (TimestampType, DateType))
            is_target_temporal = isinstance(target_type, (TimestampType, DateType))
            both_temporal = is_source_temporal and is_target_temporal

            if str(source_type) != str(target_type):
                if both_temporal:
                    # Both are temporal but different types (Timestamp vs Date)
                    # Normalize both to DATE for comparison (drops time component)
                    self.logger.info(f"  Normalizing temporal types to DATE for Jaccard comparison")
                    source_distinct = (
                        source_sample.select(
                            F.to_date(F.col(source_column)).alias("value")
                        )
                        .where("value is not null")
                        .distinct()
                    )

                    target_distinct = (
                        target_sample.select(
                            F.to_date(F.col(target_column)).alias("value")
                        )
                        .where("value is not null")
                        .distinct()
                    )

                    # Get counts for Jaccard calculation
                    source_count = source_distinct.count()
                    target_count = target_distinct.count()

                    # Calculate intersection using join on date values
                    intersection_count = source_distinct.join(
                        target_distinct,
                        source_distinct["value"] == target_distinct["value"],
                        "inner",
                    ).count()

                    self.logger.info(f"  Date-normalized Jaccard: source={source_count}, target={target_count}, intersection={intersection_count}")
                else:
                    # Different non-temporal types - convert to strings
                    source_distinct = (
                        source_sample.select(
                            F.col(source_column).cast("string").alias("value")
                        )
                        .where("value is not null")
                        .distinct()
                    )

                    target_distinct = (
                        target_sample.select(
                            F.col(target_column).cast("string").alias("value")
                        )
                        .where("value is not null")
                        .distinct()
                    )

                    # Get counts for Jaccard calculation
                    source_count = source_distinct.count()
                    target_count = target_distinct.count()

                    # Calculate intersection using join on string values
                    intersection_count = source_distinct.join(
                        target_distinct,
                        source_distinct["value"] == target_distinct["value"],
                        "inner",
                    ).count()
            else:
                # Same types - direct comparison
                source_distinct = (
                    source_sample.select(source_column)
                    .where(f"{source_column} is not null")
                    .distinct()
                )
                target_distinct = (
                    target_sample.select(target_column)
                    .where(f"{target_column} is not null")
                    .distinct()
                )

                # Get counts for Jaccard calculation
                source_count = source_distinct.count()
                target_count = target_distinct.count()

                # Calculate intersection using join
                intersection_count = source_distinct.join(
                    target_distinct,
                    source_distinct[source_column] == target_distinct[source_column],
                    "inner",
                ).count()

                # Debug logging for temporal columns
                if isinstance(source_type, (TimestampType, DateType)):
                    self.logger.info(f"  Jaccard calculation for temporal column {source_column}:")
                    self.logger.info(f"    Source distinct count: {source_count}")
                    self.logger.info(f"    Target distinct count: {target_count}")
                    self.logger.info(f"    Intersection count: {intersection_count}")

                    # Show sample values
                    self.logger.info(f"    Sample source values (first 5):")
                    for row in source_distinct.limit(5).collect():
                        self.logger.info(f"      {row[source_column]}")
                    self.logger.info(f"    Sample target values (first 5):")
                    for row in target_distinct.limit(5).collect():
                        self.logger.info(f"      {row[target_column]}")

            # Calculate union count
            union_count = source_count + target_count - intersection_count

            # Calculate Jaccard similarity
            results["jaccard"] = (
                intersection_count / union_count if union_count > 0 else 0.0
            )

            # Containment metrics
            results["containment_source_in_target"] = (
                intersection_count / source_count if source_count > 0 else 0.0
            )
            results["containment_target_in_source"] = (
                intersection_count / target_count if target_count > 0 else 0.0
            )
        except Exception:
            # Default values if calculation fails
            results["jaccard"] = 0.0
            results["containment_source_in_target"] = 0.0
            results["containment_target_in_source"] = 0.0

        # Special handling for timestamp/date columns
        is_source_temporal = isinstance(source_type, (TimestampType, DateType))
        is_target_temporal = isinstance(target_type, (TimestampType, DateType))

        self.logger.info(f"Type check for {source_column} -> {target_column}:")
        self.logger.info(f"  Source type: {source_type}, is_temporal={is_source_temporal}")
        self.logger.info(f"  Target type: {target_type}, is_temporal={is_target_temporal}")

        if is_source_temporal and is_target_temporal:
            try:
                self.logger.info(f"Detected temporal columns, using range overlap comparison")

                # Convert timestamps to Unix epoch for comparison
                source_epoch = source_sample.select(
                    F.unix_timestamp(F.col(source_column)).alias("epoch")
                ).where("epoch is not null")

                target_epoch = target_sample.select(
                    F.unix_timestamp(F.col(target_column)).alias("epoch")
                ).where("epoch is not null")

                # Calculate range overlap
                source_stats_result = source_epoch.agg(
                    F.min("epoch").alias("min"),
                    F.max("epoch").alias("max"),
                    F.count("epoch").alias("count")
                ).collect()[0]

                target_stats_result = target_epoch.agg(
                    F.min("epoch").alias("min"),
                    F.max("epoch").alias("max"),
                    F.count("epoch").alias("count")
                ).collect()[0]

                # Check if we have valid data
                if (source_stats_result["count"] > 0 and target_stats_result["count"] > 0 and
                    source_stats_result["min"] is not None and target_stats_result["min"] is not None):

                    source_min = source_stats_result["min"]
                    source_max = source_stats_result["max"]
                    target_min = target_stats_result["min"]
                    target_max = target_stats_result["max"]

                    # Calculate overlap percentage
                    overlap_start = max(source_min, target_min)
                    overlap_end = min(source_max, target_max)

                    if overlap_end >= overlap_start:
                        source_range = source_max - source_min if source_max != source_min else 1
                        target_range = target_max - target_min if target_max != target_min else 1
                        overlap_range = overlap_end - overlap_start

                        # Calculate overlap as percentage of the larger range
                        max_range = max(source_range, target_range)
                        temporal_similarity = overlap_range / max_range if max_range > 0 else 1.0

                        # Combine with Jaccard for a balanced score
                        # Give more weight to range overlap (70%) than exact value matches (30%)
                        results["temporal_range_overlap"] = temporal_similarity
                        results["overall"] = (temporal_similarity * 0.7) + (results["jaccard"] * 0.3)

                        self.logger.info(f"  Temporal comparison: range_overlap={temporal_similarity:.4f}, jaccard={results['jaccard']:.4f}, overall={results['overall']:.4f}")
                        return results
                    else:
                        # No overlap in time ranges
                        self.logger.info(f"  No temporal overlap detected")
                        results["temporal_range_overlap"] = 0.0
                        results["overall"] = 0.0
                        return results

            except Exception as e:
                self.logger.warning(f"Timestamp comparison failed: {e}, falling back to Jaccard similarity")
                # Fall through to use Jaccard similarity

        # For string columns, use Jaccard similarity as the primary metric
        if is_source_string and is_target_string:
            results["overall"] = results["jaccard"]
            return results

        # For numeric columns, calculate distribution similarity
        if (
            is_source_numeric
            and is_target_numeric
            and (method in ["distribution", "auto"])
        ):
            try:
                # Use Spark's built-in statistics functions
                source_stats = (
                    source_sample.select(source_column)
                    .summary("min", "max", "mean", "stddev")
                    .collect()
                )
                target_stats = (
                    target_sample.select(target_column)
                    .summary("min", "max", "mean", "stddev")
                    .collect()
                )

                # Add debug logging
                self.logger.debug(f"\nDebug - Raw stats from summary:")
                self.logger.debug(f"  Source stats: {source_stats}")
                self.logger.debug(f"  Target stats: {target_stats}")

                # Safely convert statistics to float, handling None values
                def safe_float(val, default=0.0):
                    try:
                        return float(val) if val is not None else default
                    except (ValueError, TypeError):
                        return default

                # Calculate normalized ranges
                source_min = safe_float(source_stats[0][source_column])
                source_max = safe_float(source_stats[1][source_column])
                target_min = safe_float(target_stats[0][target_column])
                target_max = safe_float(target_stats[1][target_column])
                source_mean = safe_float(source_stats[2][source_column])
                source_stddev = safe_float(source_stats[3][source_column])
                target_mean = safe_float(target_stats[2][target_column])
                target_stddev = safe_float(target_stats[3][target_column])

                # Store the stats for reuse
                results["source_stats"] = {
                    "min": source_min,
                    "max": source_max,
                    "mean": source_mean,
                    "stddev": source_stddev,
                }
                results["target_stats"] = {
                    "min": target_min,
                    "max": target_max,
                    "mean": target_mean,
                    "stddev": target_stddev,
                }

                # Add debug logging for processed stats
                self.logger.debug(f"\nDebug - Processed numeric stats:")
                self.logger.debug(f"  Source stats: {results['source_stats']}")
                self.logger.debug(f"  Target stats: {results['target_stats']}")

                # Store distinct and null counts
                results["source_distinct_count"] = source_distinct.count()
                results["target_distinct_count"] = target_distinct.count()
                results["source_null_count"] = source_sample.filter(
                    F.col(source_column).isNull()
                ).count()
                results["target_null_count"] = target_sample.filter(
                    F.col(target_column).isNull()
                ).count()

                # Check if we have valid ranges
                if source_min == source_max or target_min == target_max:
                    # Constant values or insufficient data
                    range_similarity = (
                        1.0
                        if source_min == target_min and source_max == target_max
                        else 0.0
                    )
                else:
                    # Calculate distribution similarity using ranges and statistics
                    range_overlap = min(source_max, target_max) - max(
                        source_min, target_min
                    )
                    total_range = max(source_max, target_max) - min(
                        source_min, target_min
                    )
                    range_similarity = max(
                        0, range_overlap / total_range if total_range > 0 else 0
                    )

                # Compare means and standard deviations
                source_mean = safe_float(source_stats[2][source_column])
                target_mean = safe_float(target_stats[2][target_column])
                source_stddev = safe_float(source_stats[3][source_column])
                target_stddev = safe_float(target_stats[3][target_column])

                # Avoid division by zero and handle the case where stddev is 0 (constant values)
                if source_mean == 0 and target_mean == 0:
                    mean_diff = 1.0  # Both means are 0, perfect match
                else:
                    mean_diff = 1 - min(
                        abs(source_mean - target_mean)
                        / max(abs(source_mean), abs(target_mean), 1),
                        1,
                    )

                if source_stddev == 0 and target_stddev == 0:
                    stddev_diff = 1.0  # Both stddevs are 0, perfect match
                else:
                    stddev_diff = 1 - min(
                        abs(source_stddev - target_stddev)
                        / max(source_stddev, target_stddev, 1),
                        1,
                    )

                results["distribution"] = (
                    range_similarity + mean_diff + stddev_diff
                ) / 3
                self.logger.info(f"  Distribution calculated: range={range_similarity:.4f}, mean={mean_diff:.4f}, stddev={stddev_diff:.4f}, overall={results['distribution']:.4f}")
            except Exception as e:
                # Just use Jaccard similarity if distribution fails
                self.logger.warning(f"Distribution similarity calculation failed for {source_column} -> {target_column}: {e}")
                import traceback
                self.logger.debug(f"Stack trace: {traceback.format_exc()}")
                results["distribution"] = results["jaccard"]

        # For non-string, non-numeric columns, use value distribution
        if (
            not (is_source_string and is_target_string)
            and not (is_source_numeric and is_target_numeric)
            and (method in ["value_distribution", "auto"])
        ):
            try:
                # Convert to strings for frequency distribution if types differ
                if str(source_type) != str(target_type):
                    source_freqs = (
                        source_sample.select(
                            F.col(source_column).cast("string").alias("value")
                        )
                        .groupBy("value")
                        .count()
                    )

                    target_freqs = (
                        target_sample.select(
                            F.col(target_column).cast("string").alias("value")
                        )
                        .groupBy("value")
                        .count()
                    )

                    join_column = "value"
                else:
                    source_freqs = source_sample.groupBy(source_column).count()
                    target_freqs = target_sample.groupBy(target_column).count()
                    join_column = source_column

                # Skip if either DataFrame is empty
                if source_sample.count() > 0 and target_sample.count() > 0:
                    # Join the frequency distributions
                    if str(source_type) != str(target_type):
                        joined_freqs = source_freqs.join(
                            target_freqs,
                            source_freqs["value"] == target_freqs["value"],
                            "outer",
                        ).select(
                            source_freqs["value"],
                            source_freqs["count"].alias("source_count"),
                            target_freqs["count"].alias("target_count"),
                        )
                    else:
                        joined_freqs = source_freqs.join(
                            target_freqs,
                            source_freqs[join_column] == target_freqs[join_column],
                            "outer",
                        ).select(
                            source_freqs[join_column],
                            source_freqs["count"].alias("source_count"),
                            target_freqs["count"].alias("target_count"),
                        )

                    # Replace nulls with 0 for non-matching values
                    joined_freqs = joined_freqs.fillna(0)

                    # Calculate frequency similarity using cosine similarity
                    source_total = source_sample.count()
                    target_total = target_sample.count()

                    if source_total > 0 and target_total > 0:
                        joined_freqs = joined_freqs.withColumn(
                            "source_ratio", joined_freqs["source_count"] / source_total
                        ).withColumn(
                            "target_ratio", joined_freqs["target_count"] / target_total
                        )

                        # Calculate dot product, magnitudes
                        similarity_data = (
                            joined_freqs.withColumn(
                                "dot_product",
                                joined_freqs["source_ratio"]
                                * joined_freqs["target_ratio"],
                            )
                            .withColumn(
                                "source_squared",
                                joined_freqs["source_ratio"]
                                * joined_freqs["source_ratio"],
                            )
                            .withColumn(
                                "target_squared",
                                joined_freqs["target_ratio"]
                                * joined_freqs["target_ratio"],
                            )
                            .agg(
                                {
                                    "dot_product": "sum",
                                    "source_squared": "sum",
                                    "target_squared": "sum",
                                }
                            )
                            .collect()[0]
                        )

                        # Calculate cosine similarity
                        dot_product = (
                            similarity_data[0] if similarity_data[0] is not None else 0
                        )
                        source_magnitude = (
                            (similarity_data[1] ** 0.5)
                            if similarity_data[1] is not None
                            else 0
                        )
                        target_magnitude = (
                            (similarity_data[2] ** 0.5)
                            if similarity_data[2] is not None
                            else 0
                        )

                        results["value_distribution"] = (
                            dot_product / (source_magnitude * target_magnitude)
                            if (source_magnitude * target_magnitude) > 0
                            else 0
                        )
                    else:
                        results["value_distribution"] = 0.0
                else:
                    results["value_distribution"] = 0.0
            except Exception:
                # Fail gracefully
                results["value_distribution"] = results["jaccard"]

        # Calculate overall similarity based on column types
        if is_source_numeric and is_target_numeric:
            # For numeric columns, combine Jaccard and distribution
            distribution_val = results.get("distribution", results["jaccard"])
            results["overall"] = (results["jaccard"] + distribution_val) / 2
            self.logger.info(f"Numeric columns {source_column} -> {target_column}: jaccard={results['jaccard']:.4f}, distribution={distribution_val:.4f}, overall={results['overall']:.4f}")
        else:
            # For other types, use Jaccard as primary metric
            results["overall"] = results["jaccard"]
            self.logger.info(f"Non-numeric columns {source_column} -> {target_column}: overall={results['overall']:.4f} (using jaccard)")

        return results
