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

Expect hive-partitioned parquet files (e.g. `type=.../` subdirectories).

## Test: Count Records by Type

Using DuckDB, time a query that counts the number of records grouped by the `type` partition column.

```bash
time duckdb -c "
  SELECT type, count(*) AS record_count
  FROM read_parquet('/tmp/ocfl-object/hive/**/*.parquet', hive_partitioning=true)
  GROUP BY type
  ORDER BY record_count DESC;
"
```

## Teardown

```bash
fusermount -u /tmp/ocfl-object
```
