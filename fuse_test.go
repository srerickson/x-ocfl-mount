package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ocfl "github.com/srerickson/ocfl-go"
	ocfllocal "github.com/srerickson/ocfl-go/fs/local"
)

const testStoreRoot = "testdata/good-stores/reg-extension-dir-root"
const testObjectID = "ark:123/abc"

func TestLocalMount(t *testing.T) {
	ctx := context.Background()

	absRoot, err := filepath.Abs(testStoreRoot)
	if err != nil {
		t.Fatal(err)
	}

	fsys, err := ocfllocal.NewFS(absRoot)
	if err != nil {
		t.Fatal(err)
	}

	root, err := ocfl.NewRoot(ctx, fsys, ".")
	if err != nil {
		t.Fatal(err)
	}

	if root.Layout() == nil {
		t.Fatal("expected layout to be detected")
	}
	t.Logf("OCFL root: spec=%s layout=%v", root.Spec(), root.Layout())

	obj, err := root.NewObject(ctx, testObjectID, ocfl.ObjectMustExist())
	if err != nil {
		t.Fatal(err)
	}

	ver := obj.Version(0) // HEAD
	if ver == nil {
		t.Fatal("no HEAD version")
	}

	// Build the file map with absolute paths
	state := ver.State()
	manifest := obj.Manifest()
	objPath := obj.Path()

	files := make(map[string]string, state.NumPaths())
	for logicalPath, digest := range state.Paths() {
		contentPaths := manifest[digest]
		if len(contentPaths) == 0 {
			t.Fatalf("missing manifest entry for digest %s", digest)
		}
		files[logicalPath] = filepath.Join(absRoot, filepath.FromSlash(objPath+"/"+contentPaths[0]))
	}

	if len(files) == 0 {
		t.Fatal("no files in version state")
	}
	t.Logf("object %q version %s: %d files", obj.ID(), ver.VNum(), len(files))
	for lp, cp := range files {
		t.Logf("  %s -> %s", lp, cp)
	}

	// Mount via FUSE and read file contents
	mountpoint := t.TempDir()
	fuseRoot := &localRoot{files: files}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "ocfl-test",
			Name:   "ocfl",
		},
	}

	server, err := fs.Mount(mountpoint, fuseRoot, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Unmount()

	// Give FUSE a moment to initialize
	time.Sleep(50 * time.Millisecond)

	// Verify we can read the mounted file
	data, err := os.ReadFile(filepath.Join(mountpoint, "a_file.txt"))
	if err != nil {
		t.Fatal(err)
	}

	expected := "Hello! I am a file.\n"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}

	// Verify directory listing
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Name() != "a_file.txt" {
		t.Errorf("expected a_file.txt, got %s", entries[0].Name())
	}
}

func TestMountLocal(t *testing.T) {
	ctx := context.Background()

	// Use the mountLocal function (same code path as CLI)
	fuseRoot := mountLocal(ctx, testStoreRoot, testObjectID, "")

	mountpoint := t.TempDir()
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "ocfl-test",
			Name:   "ocfl",
		},
	}

	server, err := fs.Mount(mountpoint, fuseRoot, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Unmount()

	time.Sleep(50 * time.Millisecond)

	data, err := os.ReadFile(filepath.Join(mountpoint, "a_file.txt"))
	if err != nil {
		t.Fatal(err)
	}

	expected := "Hello! I am a file.\n"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}

func TestMountLocalWithVersion(t *testing.T) {
	ctx := context.Background()

	// Explicitly request v1
	fuseRoot := mountLocal(ctx, testStoreRoot, testObjectID, "v1")

	mountpoint := t.TempDir()
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "ocfl-test",
			Name:   "ocfl",
		},
	}

	server, err := fs.Mount(mountpoint, fuseRoot, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Unmount()

	time.Sleep(50 * time.Millisecond)

	data, err := os.ReadFile(filepath.Join(mountpoint, "a_file.txt"))
	if err != nil {
		t.Fatal(err)
	}

	expected := "Hello! I am a file.\n"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}
