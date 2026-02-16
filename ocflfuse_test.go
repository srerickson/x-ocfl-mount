package ocflfuse

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const testStoreRoot = "testdata/good-stores/reg-extension-dir-root"
const testObjectID = "ark:123/abc"

// mountForTest mounts a FUSE root at a temp directory and registers cleanup.
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

func TestNewObjectFS(t *testing.T) {
	ctx := context.Background()

	result, err := NewObjectFS(ctx, testStoreRoot, testObjectID, "")
	if err != nil {
		t.Fatal(err)
	}

	if result.Info.ObjectID != testObjectID {
		t.Errorf("got object ID %q, want %q", result.Info.ObjectID, testObjectID)
	}
	if result.Info.FileCount == 0 {
		t.Fatal("expected files")
	}
	t.Logf("object %q version %s: %d files (spec %s, layout %s)",
		result.Info.ObjectID, result.Info.Version, result.Info.FileCount,
		result.Info.RootSpec, result.Info.Layout)

	mountpoint := mountForTest(t, result.Root)

	data, err := os.ReadFile(filepath.Join(mountpoint, "a_file.txt"))
	if err != nil {
		t.Fatal(err)
	}

	expected := "Hello! I am a file.\n"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}

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

func TestNewObjectFSWithVersion(t *testing.T) {
	ctx := context.Background()

	result, err := NewObjectFS(ctx, testStoreRoot, testObjectID, "v1")
	if err != nil {
		t.Fatal(err)
	}

	mountpoint := mountForTest(t, result.Root)

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

	// Invalid version
	if _, err := NewObjectFS(ctx, testStoreRoot, testObjectID, "abc"); err == nil {
		t.Error("expected error for invalid version")
	}

	// Non-existent version
	if _, err := NewObjectFS(ctx, testStoreRoot, testObjectID, "v99"); err == nil {
		t.Error("expected error for non-existent version")
	}

	// Valid
	if _, err := NewObjectFS(ctx, testStoreRoot, testObjectID, "v1"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if _, err := NewObjectFS(ctx, testStoreRoot, testObjectID, ""); err != nil {
		t.Errorf("unexpected error for HEAD: %v", err)
	}
}
