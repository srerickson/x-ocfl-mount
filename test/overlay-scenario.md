# Test Scenario: OverlayFS with Copy-on-Write over OCFL FUSE Mount

Create a writable overlay filesystem on top of a read-only OCFL FUSE mount,
then write a README into the overlay to demonstrate copy-on-write behavior.

## Dataset

| Parameter | Value |
|-----------|-------|
| Bucket | `ucsb-dreamlab-data` |
| Prefix | `ocfl` |
| Object ID | `proquest-historical-news` |
| Version | `v4` |

## Prerequisites

### Build ocfl-mount

```bash
go build -o ocfl-mount .
```

### AWS credentials

Configure `~/.aws/credentials` and `~/.aws/config` with valid credentials
for the `ucsb-dreamlab-data` bucket (region `us-west-2`). Copy to
`/root/.aws/` as well since the FUSE mount runs as root.

```bash
sudo mkdir -p /root/.aws
sudo cp ~/.aws/credentials /root/.aws/credentials
sudo cp ~/.aws/config /root/.aws/config
```

### FUSE configuration

No changes to `/etc/fuse.conf` are needed. The FUSE mount runs as root so
that the kernel overlay driver can traverse it.

---

## Part 1: Mount the OCFL Object

Mount the FUSE filesystem as root so the overlay can access the lower layer.

```bash
mkdir -p /tmp/ocfl-object
sudo ./ocfl-mount -version v4 ucsb-dreamlab-data/ocfl proquest-historical-news /tmp/ocfl-object &
sleep 5
```

Verify:

```bash
sudo ls /tmp/ocfl-object/hive/
# Expect: pub_name=latimes  pub_name=nytimes  pub_name=wapost
```

---

## Part 2: Create the OverlayFS

OverlayFS requires three directories:

- **lowerdir** — the read-only FUSE mount
- **upperdir** — where writes are stored (must be on a local filesystem)
- **workdir** — internal scratch (must be on the same filesystem as upperdir)

```bash
mkdir -p /tmp/overlay-upper /tmp/overlay-work /tmp/ocfl-merged

sudo mount -t overlay overlay \
  -o lowerdir=/tmp/ocfl-object,upperdir=/tmp/overlay-upper,workdir=/tmp/overlay-work \
  /tmp/ocfl-merged
```

Verify the merged view shows the OCFL content and is writable:

```bash
sudo ls /tmp/ocfl-merged/hive/
# Expect: pub_name=latimes  pub_name=nytimes  pub_name=wapost

sudo touch /tmp/ocfl-merged/test-write && echo "Writable" && sudo rm /tmp/ocfl-merged/test-write
```

---

## Part 3: Write a README into the Overlay

Write a README.md describing the object into the merged mount:

```bash
sudo tee /tmp/ocfl-merged/README.md << 'EOF'
# ProQuest Historical Newspapers
...
EOF
```

Verify copy-on-write behavior — the file should exist in the upper layer
and the merged view, but not in the FUSE lower layer:

```bash
# Present in upperdir (local disk)
ls -la /tmp/overlay-upper/README.md

# Visible in merged view
sudo ls /tmp/ocfl-merged/README.md

# Absent from FUSE mount (read-only, untouched)
sudo ls /tmp/ocfl-object/README.md
# Expect: No such file or directory
```

---

## Teardown

Order matters — unmount the overlay first, then the FUSE mount:

```bash
sudo umount /tmp/ocfl-merged
sudo fusermount -u /tmp/ocfl-object
```

After teardown, the upper layer persists on disk:

```bash
ls /tmp/overlay-upper/README.md
# Still present — can be re-used with a future overlay mount
```

---

## Notes

- The FUSE mount must run as root for the kernel overlay driver to traverse
  the lower layer. An alternative is enabling `allow_other` in the FUSE
  mount and `/etc/fuse.conf`, which this tool does not currently support.
- `upperdir` and `workdir` must be on the same local filesystem (not
  another FUSE mount).
- Modifying an existing file from the lower layer triggers a full copy-up
  to the upper layer before the write proceeds.
- The overlay can be re-mounted later with the same `upperdir` to resume
  a previous session.
