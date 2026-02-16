# Test Scenario: FUSE Mount vs Direct S3 — DuckDB Parquet Query

Compare querying a hive-partitioned parquet dataset via the FUSE mount
against querying S3 directly with DuckDB's native S3 support.

## Dataset

| Parameter | Value |
|-----------|-------|
| Bucket | `ucsb-dreamlab-data` |
| Prefix | `ocfl` |
| Object ID | `proquest-historical-news` |
| Version | `v4` |
| Files | 250 hive-partitioned parquet files |
| Records | ~25.3M |
| Partitioning | `pub_name=.../year=.../data.parquet` |

The OCFL object uses `0003-hash-and-id-n-tuple-storage-layout`
(sha256, tupleSize=3, numberOfTuples=1), which resolves the object ID
`proquest-historical-news` to storage path `4b5/proquest-historical-news`.

The v4 inventory state maps all 250 hive files to content stored under
`v3/content/` (forward-delta — no files changed between v3 and v4).

## Prerequisites

### Build ocfl-mount

```bash
go build -o ocfl-mount .
```

### Install DuckDB

```bash
curl -fsSL https://github.com/duckdb/duckdb/releases/download/v1.3.0/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip
unzip -o /tmp/duckdb.zip -d /usr/local/bin/
```

### AWS credentials

Configure AWS credentials for the SDK:

```bash
mkdir -p ~/.aws

cat > ~/.aws/credentials <<EOF
[default]
aws_access_key_id = <AccessKeyId>
aws_secret_access_key = <SecretAccessKey>
aws_session_token = <SessionToken>
EOF

cat > ~/.aws/config <<EOF
[default]
region = us-west-2
EOF
```

### Query

All parts use the same query:

```sql
SELECT type, count(*) AS record_count
FROM read_parquet('<path>', hive_partitioning=true)
GROUP BY type
ORDER BY record_count DESC;
```

---

## Part 1: FUSE Mount

### Run

For each of the 3 runs:

```bash
# 1. Ensure clean state (skip for first run)
fusermount -u /tmp/ocfl-object 2>/dev/null
sleep 1
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

# 2. Mount the OCFL object
mkdir -p /tmp/ocfl-object
./ocfl-mount -version v4 ucsb-dreamlab-data/ocfl proquest-historical-news /tmp/ocfl-object &
sleep 4

# 3. Verify
ls /tmp/ocfl-object/hive/
# Expect: pub_name=latimes  pub_name=nytimes  pub_name=wapost

# 4. Query
time duckdb -c "
  SELECT type, count(*) AS record_count
  FROM read_parquet('/tmp/ocfl-object/hive/**/*.parquet', hive_partitioning=true)
  GROUP BY type
  ORDER BY record_count DESC;
"
```

Between runs, the FUSE filesystem is unmounted and kernel page caches are
dropped (`/proc/sys/vm/drop_caches`) so each run starts cold — no cached
file data or FUSE metadata from a previous run.

### Teardown

```bash
fusermount -u /tmp/ocfl-object
```

### Results (2026-02-16)

| Run | Time |
|-----|------|
| 1 | 2m 02.3s |
| 2 | 1m 54.2s |
| 3 | 1m 51.2s |
| **Average** | **1m 55.9s** |

---

## Part 2: DuckDB Direct S3

Query the same parquet files directly from S3 using DuckDB's native S3
support, bypassing the FUSE mount entirely. This requires resolving the
OCFL content paths to a concrete S3 glob:

```
s3://ucsb-dreamlab-data/ocfl/4b5/proquest-historical-news/v3/content/hive/**/*.parquet
```

### Run

```bash
export AWS_ACCESS_KEY_ID=<AccessKeyId>
export AWS_SECRET_ACCESS_KEY=<SecretAccessKey>
export AWS_SESSION_TOKEN=<SessionToken>

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

No special cache clearing is needed — DuckDB does not cache S3 data between
invocations.

### Results (2026-02-16)

| Run | Time |
|-----|------|
| 1 | 2m 02.4s |
| 2 | 2m 03.2s |
| 3 | 2m 05.3s |
| **Average** | **2m 03.6s** |

---

## Query Output (both parts identical)

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
| Illustration | 220,351 |
| News; Legal Notice | 165,644 |
| News; Marriage Announcement | 163,187 |
| Image/Photograph | 155,183 |
| Review | 98,578 |
| Table of Contents; Front Matter | 92,453 |
| Credit/Acknowledgement | 69,549 |
| Editorial Cartoon/Comic | 58,744 |
| Birth Notice | 32,119 |
| Undefined | 1,154 |
| Article | 611 |
| Article; Feature | 594 |
| Advertisement; Classified Advertisement | 54 |
| Commentary; Editorial | 30 |
| Marriage Announcement; News | 14 |
| Correspondence; Letter to the Editor | 7 |
| News; Military/War News | 6 |
| Table Of Contents; Front Matter | 4 |
| Front Matter; Table of Contents | 4 |
| Legal Notice; News | 1 |
