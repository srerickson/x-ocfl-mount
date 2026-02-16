# ocfl-fuse

A Go library and CLI tool that mounts an OCFL object as a read-only FUSE filesystem. Supports both S3 and local storage roots.

## Library Usage

The `ocflfuse` package exports a single entry point, `NewObjectFS`, which resolves an OCFL object version and returns an `*ObjectFS`:

```go
import (
    ocflfuse "github.com/srerickson/ocfl-fuse"
    "github.com/hanwen/go-fuse/v2/fs"
)

objFS, err := ocflfuse.NewObjectFS(ctx, "s3://bucket/prefix", "my-object-id", "v1")
// objFS.Root is an fs.InodeEmbedder for use with fs.Mount
// objFS.Info has metadata (ObjectID, Version, FileCount, etc.)

server, err := fs.Mount(mountpoint, objFS.Root, opts)
```

## CLI Usage

```
mount-ocfl [options] <storage-root> <object-id> <mountpoint>
```

**Arguments:**
- `storage-root` - S3 URI (`s3://bucket/prefix`) or local filesystem path
- `object-id` - OCFL object identifier
- `mountpoint` - Local directory to mount the filesystem

**Flags:**
- `-version <v1|v2|...>` - Object version to mount (optional, defaults to head)
- `-debug` - Enable FUSE debug output

## Examples

### Local filesystem

```bash
# Mount latest version from a local OCFL storage root
./mount-ocfl /data/ocfl-root my-object-id /mnt/ocfl

# Mount specific version
./mount-ocfl -version v3 /data/ocfl-root my-object-id /mnt/ocfl

# Unmount
fusermount -u /mnt/ocfl
```

### S3

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-west-2

# Mount latest version from S3
./mount-ocfl s3://mybucket/ocfl-root my-object-id /mnt/ocfl

# Mount specific version
./mount-ocfl -version v3 s3://mybucket/ocfl-root my-object-id /mnt/ocfl

# Unmount
fusermount -u /mnt/ocfl
```

## Features

- Parses OCFL `inventory.json` to map logical paths → content paths
- **S3 backend**: Uses AWS SDK v2 with efficient range requests for random access reads
- **Local backend**: Direct filesystem reads with `ReadAt` for random access
- go-fuse for filesystem implementation
- Clean unmount on SIGINT/SIGTERM

## Building

```bash
go build -o mount-ocfl ./cmd/mount-ocfl
```

## Testing

```bash
go test -v ./...
```

Tests use the [reg-extension-dir-root](https://github.com/srerickson/ocfl-go/tree/main/testdata/store-fixtures/1.0/good-stores/reg-extension-dir-root) fixture from ocfl-go.

## Requirements

- Go 1.22+
- FUSE (libfuse/fuse3)
- AWS credentials with S3 read access (for S3 backend)

## Performance

Tested with a 250-file hive-partitioned parquet dataset (~25M records) over S3:

| Method | Time |
|--------|------|
| FUSE mount | 1m 27s |
| DuckDB native S3 | 1m 10s |

The FUSE mount adds ~20% overhead compared to native S3 access, but works with any file-based tool.
