#!/usr/bin/env python3
"""Debug script to investigate why timestamps have 0% Jaccard match"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.dbx.session import initialize_dbx_session

spark = initialize_dbx_session()

print("=" * 80)
print("TIMESTAMP MATCHING INVESTIGATION")
print("=" * 80)

# Get samples ordered by ID (same as column_matcher does)
print("\n1. SAMPLE FIRST 10 ROWS (ordered by ID):")
comparison = spark.sql('''
    SELECT
        s.ID,
        s.TRANSACTION_TIMESTAMP as source_ts,
        t.TRANSACTION_TIMESTAMP as target_ts,
        CASE
            WHEN s.TRANSACTION_TIMESTAMP = t.TRANSACTION_TIMESTAMP THEN 'EXACT MATCH'
            ELSE 'DIFFERENT'
        END as match_status,
        CASE
            WHEN s.TRANSACTION_TIMESTAMP != t.TRANSACTION_TIMESTAMP
            THEN unix_timestamp(t.TRANSACTION_TIMESTAMP) - unix_timestamp(s.TRANSACTION_TIMESTAMP)
            ELSE 0
        END as seconds_diff
    FROM porto_poc.transactions_feip s
    JOIN users.malcoln_dandaro.transactions_feip t ON s.ID = t.ID
    ORDER BY s.ID
    LIMIT 10
''')
comparison.show(10, False)

# Check distinct timestamp counts
print("\n2. DISTINCT TIMESTAMP COUNTS:")
source_distinct = spark.sql('''
    SELECT COUNT(DISTINCT TRANSACTION_TIMESTAMP) as cnt
    FROM (
        SELECT TRANSACTION_TIMESTAMP
        FROM porto_poc.transactions_feip
        ORDER BY ID
        LIMIT 100000
    )
''').collect()[0]['cnt']

target_distinct = spark.sql('''
    SELECT COUNT(DISTINCT TRANSACTION_TIMESTAMP) as cnt
    FROM (
        SELECT TRANSACTION_TIMESTAMP
        FROM users.malcoln_dandaro.transactions_feip
        ORDER BY ID
        LIMIT 100000
    )
''').collect()[0]['cnt']

print(f"   Source distinct timestamps (first 100k by ID): {source_distinct:,}")
print(f"   Target distinct timestamps (first 100k by ID): {target_distinct:,}")

# Check exact match count
print("\n3. EXACT MATCH ANALYSIS (first 100k rows by ID):")
match_stats = spark.sql('''
    SELECT
        COUNT(*) as total_compared,
        SUM(CASE WHEN s.TRANSACTION_TIMESTAMP = t.TRANSACTION_TIMESTAMP THEN 1 ELSE 0 END) as exact_matches,
        SUM(CASE WHEN s.TRANSACTION_TIMESTAMP != t.TRANSACTION_TIMESTAMP THEN 1 ELSE 0 END) as mismatches,
        SUM(CASE WHEN ABS(unix_timestamp(s.TRANSACTION_TIMESTAMP) - unix_timestamp(t.TRANSACTION_TIMESTAMP)) < 1 THEN 1 ELSE 0 END) as within_1_second
    FROM (
        SELECT ID, TRANSACTION_TIMESTAMP
        FROM porto_poc.transactions_feip
        ORDER BY ID
        LIMIT 100000
    ) s
    JOIN (
        SELECT ID, TRANSACTION_TIMESTAMP
        FROM users.malcoln_dandaro.transactions_feip
        ORDER BY ID
        LIMIT 100000
    ) t ON s.ID = t.ID
''').collect()[0]

total = match_stats['total_compared']
exact = match_stats['exact_matches']
mismatch = match_stats['mismatches']
within_1s = match_stats['within_1_second']

print(f"   Total rows compared: {total:,}")
print(f"   Exact matches: {exact:,} ({exact/total*100:.1f}%)")
print(f"   Mismatches: {mismatch:,} ({mismatch/total*100:.1f}%)")
print(f"   Within 1 second: {within_1s:,} ({within_1s/total*100:.1f}%)")

# Check data types
print("\n4. DATA TYPES:")
source_schema = spark.table('porto_poc.transactions_feip').schema
target_schema = spark.table('users.malcoln_dandaro.transactions_feip').schema
for field in source_schema:
    if field.name == 'TRANSACTION_TIMESTAMP':
        print(f"   Source: {field.dataType}")
for field in target_schema:
    if field.name == 'TRANSACTION_TIMESTAMP':
        print(f"   Target: {field.dataType}")

# Show sample of mismatches if any
print("\n5. SAMPLE OF MISMATCHES (if any):")
spark.sql('''
    SELECT
        s.ID,
        s.TRANSACTION_TIMESTAMP as source_ts,
        t.TRANSACTION_TIMESTAMP as target_ts,
        unix_timestamp(s.TRANSACTION_TIMESTAMP) as source_epoch,
        unix_timestamp(t.TRANSACTION_TIMESTAMP) as target_epoch,
        unix_timestamp(t.TRANSACTION_TIMESTAMP) - unix_timestamp(s.TRANSACTION_TIMESTAMP) as diff_seconds
    FROM (
        SELECT ID, TRANSACTION_TIMESTAMP
        FROM porto_poc.transactions_feip
        ORDER BY ID
        LIMIT 100000
    ) s
    JOIN (
        SELECT ID, TRANSACTION_TIMESTAMP
        FROM users.malcoln_dandaro.transactions_feip
        ORDER BY ID
        LIMIT 100000
    ) t ON s.ID = t.ID
    WHERE s.TRANSACTION_TIMESTAMP != t.TRANSACTION_TIMESTAMP
    LIMIT 10
''').show(10, False)

print("\n" + "=" * 80)
