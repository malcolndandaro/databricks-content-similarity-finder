#!/usr/bin/env python3
"""Debug script to investigate target AMOUNT column"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.dbx.session import initialize_dbx_session

spark = initialize_dbx_session()

print("=" * 80)
print("TARGET TABLE ANALYSIS")
print("=" * 80)

# Get distinct counts
print("\n1. ROW AND DISTINCT VALUE COUNTS:")
stats = spark.sql('''
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT ID) as distinct_ids,
        COUNT(DISTINCT AMOUNT) as distinct_amounts
    FROM users.malcoln_dandaro.transactions_feip
''').collect()[0]
print(f"   Total rows: {stats['total_rows']:,}")
print(f"   Distinct IDs: {stats['distinct_ids']:,}")
print(f"   Distinct AMOUNT values: {stats['distinct_amounts']:,}")

# Sample values
print("\n2. FIRST 20 AMOUNT VALUES (sorted):")
spark.sql('''
    SELECT AMOUNT, COUNT(*) as frequency
    FROM users.malcoln_dandaro.transactions_feip
    GROUP BY AMOUNT
    ORDER BY AMOUNT
    LIMIT 20
''').show(20, False)

# Check if amounts are unique per ID
print("\n3. AMOUNT VALUE DISTRIBUTION:")
spark.sql('''
    SELECT
        CASE
            WHEN dup_count = 1 THEN 'Unique (1 row)'
            WHEN dup_count BETWEEN 2 AND 10 THEN '2-10 rows'
            WHEN dup_count BETWEEN 11 AND 100 THEN '11-100 rows'
            ELSE '100+ rows'
        END as frequency_bucket,
        COUNT(*) as num_distinct_amounts
    FROM (
        SELECT AMOUNT, COUNT(*) as dup_count
        FROM users.malcoln_dandaro.transactions_feip
        GROUP BY AMOUNT
    )
    GROUP BY frequency_bucket
    ORDER BY frequency_bucket
''').show(10, False)

# Check data type
print("\n4. DATA TYPE:")
schema = spark.table('users.malcoln_dandaro.transactions_feip').schema
for field in schema:
    if field.name == 'AMOUNT':
        print(f"   AMOUNT type: {field.dataType}")

# Sample actual values
print("\n5. SAMPLE OF 10 ROWS (ordered by ID):")
spark.sql('''
    SELECT ID, TRANSACTION_ID, AMOUNT, TRANSACTION_TIMESTAMP
    FROM users.malcoln_dandaro.transactions_feip
    ORDER BY ID
    LIMIT 10
''').show(10, False)

print("\n" + "=" * 80)
