#!/usr/bin/env python3
"""Debug script to investigate AMOUNT column mismatch"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import get_finder_logger
from utils.dbx.session import initialize_dbx_session

logger = get_finder_logger(level="INFO", include_debug=True)

# Initialize
spark = initialize_dbx_session()

print("=" * 80)
print("INVESTIGATING AMOUNT COLUMN MISMATCH")
print("=" * 80)

# Get distinct counts
print("\n1. DISTINCT VALUE COUNTS:")
source_distinct = spark.sql('SELECT COUNT(DISTINCT AMOUNT) as cnt FROM porto_poc.transactions_feip').collect()[0]['cnt']
target_distinct = spark.sql('SELECT COUNT(DISTINCT AMOUNT) as cnt FROM users.malcoln_dandaro.transactions_feip').collect()[0]['cnt']
print(f"   Source (Oracle): {source_distinct:,} distinct values")
print(f"   Target (Databricks): {target_distinct:,} distinct values")

# Sample values from source
print("\n2. SOURCE (Oracle) - First 20 AMOUNT values:")
source_sample = spark.sql('''
    SELECT AMOUNT, COUNT(*) as frequency
    FROM porto_poc.transactions_feip
    WHERE AMOUNT IS NOT NULL
    GROUP BY AMOUNT
    ORDER BY AMOUNT
    LIMIT 20
''')
source_sample.show(20, False)

# Sample values from target
print("\n3. TARGET (Databricks) - First 20 AMOUNT values:")
target_sample = spark.sql('''
    SELECT AMOUNT, COUNT(*) as frequency
    FROM users.malcoln_dandaro.transactions_feip
    WHERE AMOUNT IS NOT NULL
    GROUP BY AMOUNT
    ORDER BY AMOUNT
    LIMIT 20
''')
target_sample.show(20, False)

# Check for exact matches
print("\n4. CHECKING FOR EXACT VALUE MATCHES:")
print("   Comparing first 10 rows ordered by ID...")
comparison = spark.sql('''
    SELECT
        s.ID,
        s.AMOUNT as source_amount,
        t.AMOUNT as target_amount,
        CASE
            WHEN s.AMOUNT = t.AMOUNT THEN 'EXACT MATCH'
            WHEN ABS(s.AMOUNT - t.AMOUNT) < 0.0001 THEN 'CLOSE MATCH'
            ELSE 'DIFFERENT'
        END as comparison_result,
        ABS(s.AMOUNT - t.AMOUNT) as difference
    FROM porto_poc.transactions_feip s
    JOIN users.malcoln_dandaro.transactions_feip t ON s.ID = t.ID
    ORDER BY s.ID
    LIMIT 10
''')
comparison.show(10, False)

# Statistical summary
print("\n5. STATISTICAL COMPARISON:")
stats = spark.sql('''
    SELECT
        'SOURCE' as table_name,
        MIN(AMOUNT) as min_val,
        MAX(AMOUNT) as max_val,
        AVG(AMOUNT) as mean_val,
        STDDEV(AMOUNT) as stddev_val,
        COUNT(DISTINCT AMOUNT) as distinct_count
    FROM porto_poc.transactions_feip
    UNION ALL
    SELECT
        'TARGET' as table_name,
        MIN(AMOUNT) as min_val,
        MAX(AMOUNT) as max_val,
        AVG(AMOUNT) as mean_val,
        STDDEV(AMOUNT) as stddev_val,
        COUNT(DISTINCT AMOUNT) as distinct_count
    FROM users.malcoln_dandaro.transactions_feip
''')
stats.show(10, False)

# Check data types
print("\n6. DATA TYPES:")
source_schema = spark.table('porto_poc.transactions_feip').schema
target_schema = spark.table('users.malcoln_dandaro.transactions_feip').schema
for field in source_schema:
    if field.name == 'AMOUNT':
        print(f"   Source AMOUNT type: {field.dataType}")
for field in target_schema:
    if field.name == 'AMOUNT':
        print(f"   Target AMOUNT type: {field.dataType}")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
