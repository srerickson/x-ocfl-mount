# Test Scenario: DuckDB Direct S3 Query (No FUSE)

Query the same dataset directly from S3 using DuckDB's native S3 support,
for comparison with the FUSE mount approach in [scenario.md](scenario.md).

## Resolving OCFL Content Paths

The OCFL object uses `0003-hash-and-id-n-tuple-storage-layout` (sha256, tupleSize=3, numberOfTuples=1).

| Parameter | Value |
|-----------|-------|
| Bucket | `ucsb-dreamlab-data` |
| Prefix | `ocfl` |
| Object ID | `proquest-historical-news` |
| Object path | `ocfl/4b5/proquest-historical-news` |
| Version | `v4` |

The v4 inventory state maps all 250 hive parquet files to content stored
under the `v3/content/` directory (forward-delta, no files changed between v3 and v4).

Full S3 glob pattern:
```
s3://ucsb-dreamlab-data/ocfl/4b5/proquest-historical-news/v3/content/hive/**/*.parquet
```

## Test: Count Records by Type

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
export AWS_DEFAULT_REGION=us-west-2

time duckdb -c "
  CREATE SECRET s3_secret (
    TYPE S3,
    KEY_ID '\$AWS_ACCESS_KEY_ID',
    SECRET '\$AWS_SECRET_ACCESS_KEY',
    SESSION_TOKEN '\$AWS_SESSION_TOKEN',
    REGION 'us-west-2'
  );

  SELECT type, count(*) AS record_count
  FROM read_parquet(
    's3://ucsb-dreamlab-data/ocfl/4b5/proquest-historical-news/v3/content/hive/**/*.parquet',
    hive_partitioning=true
  )
  GROUP BY type
  ORDER BY record_count DESC;
"
```

## Results (2026-02-16)

- **250 files** queried directly from S3
- **30 distinct types**, **~25.3M total records** (identical to FUSE scenario)
- Query run 3 times

| Run | Time |
|-----|------|
| 1 | 2m 02.4s |
| 2 | 2m 03.2s |
| 3 | 2m 05.3s |
| **Average** | **2m 03.6s** |

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
