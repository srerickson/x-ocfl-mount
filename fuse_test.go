package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ocfl "github.com/srerickson/ocfl-go"
	ocfllocal "github.com/srerickson/ocfl-go/fs/local"
)

const testStoreRoot = "testdata/good-stores/reg-extension-dir-root"
const testObjectID = "ark:123/abc"

// mountForTest mounts a FUSE root at a temp directory and registers cleanup.
// Returns the mountpoint path.
func mountForTest(t *testing.T, root fs.InodeEmbedder) string {
	t.Helper()
	mountpoint := t.TempDir()
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:  "ocfl-test",
			Name:    "ocfl",
			Options: []string{"ro"},
		},
	}
	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Unmount() })
	if err := server.WaitMount(); err != nil {
		t.Fatal(err)
	}
	return mountpoint
}

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

	mountpoint := mountForTest(t, &localRoot{files: files})

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

	fuseRoot, err := mountLocal(ctx, testStoreRoot, testObjectID, "")
	if err != nil {
		t.Fatal(err)
	}

	mountpoint := mountForTest(t, fuseRoot)

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

	fuseRoot, err := mountLocal(ctx, testStoreRoot, testObjectID, "v1")
	if err != nil {
		t.Fatal(err)
	}

	mountpoint := mountForTest(t, fuseRoot)

	data, err := os.ReadFile(filepath.Join(mountpoint, "a_file.txt"))
	if err != nil {
		t.Fatal(err)
	}

	expected := "Hello! I am a file.\n"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}

func TestResolveVersionErrors(t *testing.T) {
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
	obj, err := root.NewObject(ctx, testObjectID, ocfl.ObjectMustExist())
	if err != nil {
		t.Fatal(err)
	}

	// Invalid version string
	if _, err := resolveVersion(obj, "abc"); err == nil {
		t.Error("expected error for invalid version")
	}

	// Non-existent version
	if _, err := resolveVersion(obj, "v99"); err == nil {
		t.Error("expected error for non-existent version")
	}

	// Valid
	if _, err := resolveVersion(obj, "v1"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if _, err := resolveVersion(obj, ""); err != nil {
		t.Errorf("unexpected error for HEAD: %v", err)
	}
}
