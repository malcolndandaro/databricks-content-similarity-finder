from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from utils.config.config import load_config
import os

def get_oracle_connection_properties() -> Dict[str, str]:
    """
    Get Oracle connection properties from configuration.
    
    Returns:
        Dict[str, str]: Dictionary containing Oracle connection properties
    """
    config = load_config()
    oracle_config = config["oracle"]
    
    # Construct JDBC URL from components
    jdbc_url = f"jdbc:oracle:thin:@//{oracle_config['host']}:{oracle_config['port']}/{oracle_config['service_name']}"
    
    return {
        "user": os.environ.get("ORACLE_USER"),  
        "password": os.environ.get("ORACLE_PASSWORD"),
        "url": jdbc_url,
        "driver": "oracle.jdbc.driver.OracleDriver"
    }

def write_to_oracle(df: DataFrame, table_name: str, properties: Dict[str, str]) -> None:
    """
    Write DataFrame to Oracle table.
    
    Args:
        df (DataFrame): Spark DataFrame to write
        table_name (str): Target Oracle table name
        properties (Dict[str, str]): Oracle connection properties
    """

    from pyspark.sql import DataFrame

    def to_uppercase_columns(df: DataFrame) -> DataFrame:
        return df.select([df[col].alias(col.upper()) for col in df.columns])

    df.transform(to_uppercase_columns).write \
        .format("jdbc") \
        .option("url", properties["url"]) \
        .option("dbtable", table_name) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .mode("append") \
        .save()

def read_oracle_table(
    spark: 'SparkSession',
    table_name: str,
    properties: Dict[str, str],
    columns: Optional[List[str]] = None,
    predicates: Optional[List[str]] = None,
    sample_size: Optional[int] = None,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: Optional[int] = None,
    order_by_columns: Optional[List[str]] = None
) -> 'DataFrame':
    """
    Read an Oracle table into a Spark DataFrame using JDBC.

    Args:
        spark (SparkSession): The Spark session.
        table_name (str): The Oracle table name (optionally schema-qualified).
        properties (Dict[str, str]): Oracle connection properties (from get_oracle_connection_properties).
        columns (Optional[List[str]]): List of columns to select. If None, selects all columns.
        predicates (Optional[List[str]]): List of predicates for partitioned reads (optional).
        sample_size (Optional[int]): If provided, limits the number of rows returned.
        partition_column (Optional[str]): Column to partition by (must be numeric or date).
        lower_bound (Optional[int]): Lower bound of the partition column.
        upper_bound (Optional[int]): Upper bound of the partition column.
        num_partitions (Optional[int]): Number of partitions.
        order_by_columns (Optional[List[str]]): List of columns to order by. If provided, results will be ordered by these columns.

    Returns:
        DataFrame: The loaded Spark DataFrame.
    """
    # Build the query with ORDER BY if specified
    order_by_clause = f" ORDER BY {', '.join(order_by_columns)}" if order_by_columns else ""
    
    options = {
        "url": properties["url"],
        "dbtable": f"(SELECT {', '.join(columns) if columns else '*'} FROM {table_name}{f' WHERE ROWNUM <= {sample_size}' if sample_size else ''}{order_by_clause}) t",
        "user": properties["user"],
        "password": properties["password"],
        "driver": properties["driver"],
        "fetchsize": "5000"
    }
    if predicates:
        options["predicates"] = predicates
    if partition_column and lower_bound is not None and upper_bound is not None and num_partitions:
        options.update({
            "partitionColumn": partition_column,
            "lowerBound": str(lower_bound),
            "upperBound": str(upper_bound),
            "numPartitions": str(num_partitions)
        })
    return spark.read.format("jdbc").options(**options).load() 