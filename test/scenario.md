# Test Scenario: FUSE Mount + DuckDB Query

## Setup

### 1. Mount the OCFL object

```bash
mkdir -p /tmp/ocfl-object

./ocfl-mount -version v4 ucsb-dreamlab-data/ocfl proquest-historical-news /tmp/ocfl-object
```

| Parameter | Value |
|-----------|-------|
| Bucket | `ucsb-dreamlab-data` |
| Prefix | `ocfl` |
| Object ID | `proquest-historical-news` |
| Version | `v4` |
| Mount path | `/tmp/ocfl-object` |

### 2. Verify the mount

```bash
ls /tmp/ocfl-object/hive/
```

Expect hive-partitioned parquet files with `pub_name=.../year=.../data.parquet` structure.

## Test: Count Records by Type

Using DuckDB, time a query that counts the number of records grouped by the `type` column.

```bash
time duckdb -c "
  SELECT type, count(*) AS record_count
  FROM read_parquet('/tmp/ocfl-object/hive/**/*.parquet', hive_partitioning=true)
  GROUP BY type
  ORDER BY record_count DESC;
"
```

### Results (2026-02-16)

- **250 files** mounted from OCFL object
- **30 distinct types**, **~25.3M total records**
- Query run 3 times with fresh mount + cache drop between each run

| Run | Time |
|-----|------|
| 1 | 2m 02.3s |
| 2 | 1m 54.2s |
| 3 | 1m 51.2s |
| **Average** | **1m 55.9s** |

Top types by record count:

| Type | Count |
|------|-------|
| Feature; Article | 12,413,867 |
| Advertisement | 4,331,654 |
| Classified Advertisement; Advertisement | 1,884,158 |
| Front Page/Cover Story | 1,588,812 |
| General Information | 1,522,331 |
| News | 662,900 |
| Stock Quote | 630,576 |
| Editorial; Commentary | 598,970 |
| Obituary | 314,692 |
| Letter to the Editor; Correspondence | 248,262 |

## Teardown

```bash
fusermount -u /tmp/ocfl-object
```
