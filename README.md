# ocfl-mount

A Go CLI tool that mounts an OCFL object from S3 as a read-only FUSE filesystem.

## Usage

```
ocfl-mount [options] <bucket/prefix> <object-id> <mountpoint>
```

**Arguments:**
- `bucket/prefix` - S3 bucket and optional prefix (e.g., `mybucket/ocfl-root`)
- `object-id` - OCFL object identifier
- `mountpoint` - Local directory to mount the filesystem

**Flags:**
- `-version <v1|v2|...>` - Object version to mount (optional, defaults to head)
- `-debug` - Enable FUSE debug output

## Example

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-west-2

# Mount latest version
./ocfl-mount mybucket/ocfl-root my-object-id /mnt/ocfl

# Mount specific version
./ocfl-mount -version v3 mybucket/ocfl-root my-object-id /mnt/ocfl

# Unmount
fusermount -u /mnt/ocfl
```

## Features

- Parses OCFL `inventory.json` to map logical paths → content paths
- Supports `0003-hash-and-id-n-tuple-storage-layout` extension (auto-detected from `ocfl_layout.json`)
- Uses AWS SDK v2 (credentials from environment/config)
- Efficient S3 range requests for random access reads
- go-fuse for filesystem implementation
- Clean unmount on SIGINT/SIGTERM

## Building

```bash
go build -o ocfl-mount .
```

## Requirements

- Go 1.22+
- FUSE (libfuse/fuse3)
- AWS credentials with S3 read access

## Performance

Tested with a 250-file hive-partitioned parquet dataset (~25M records):

| Method | Time |
|--------|------|
| FUSE mount | 1m 27s |
| DuckDB native S3 | 1m 10s |

The FUSE mount adds ~20% overhead compared to native S3 access, but works with any file-based tool.
