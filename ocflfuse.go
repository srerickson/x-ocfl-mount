// Package ocflfuse provides a read-only FUSE filesystem for OCFL objects.
//
// The primary entry point is [NewRoot], which resolves an OCFL object version
// and returns an [fs.InodeEmbedder] suitable for use with [fs.Mount].
package ocflfuse

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	ocfl "github.com/srerickson/ocfl-go"
	ocfllocal "github.com/srerickson/ocfl-go/fs/local"
	ocfls3 "github.com/srerickson/ocfl-go/fs/s3"
)

// Info describes the resolved OCFL object and version.
type Info struct {
	ObjectID string
	Version  string
	FileCount int
	RootSpec  string
	Layout   string
}

// Result is returned by NewRoot and contains the FUSE root node
// along with metadata about the resolved OCFL object.
type Result struct {
	// Root is the FUSE inode tree root, ready to pass to fs.Mount.
	Root fs.InodeEmbedder
	// Info describes the resolved OCFL object and version.
	Info Info
}

// NewRoot resolves an OCFL object version and returns a FUSE root node.
//
// storageRoot is an S3 URI (s3://bucket/prefix) or a local filesystem path.
// objectID is the OCFL object identifier. version is the version to mount
// (e.g. "v1", "v2"); pass "" for the head/latest version.
func NewRoot(ctx context.Context, storageRoot, objectID, version string) (*Result, error) {
	if strings.HasPrefix(storageRoot, "s3://") {
		return newS3Root(ctx, storageRoot, objectID, version)
	}
	return newLocalRoot(ctx, storageRoot, objectID, version)
}

// resolveVersion parses a version flag and returns the OCFL object version.
func resolveVersion(obj *ocfl.Object, versionFlag string) (*ocfl.ObjectVersion, error) {
	vnum := 0 // HEAD
	if versionFlag != "" {
		v := versionFlag
		if strings.HasPrefix(v, "v") {
			v = v[1:]
		}
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n < 1 {
			return nil, fmt.Errorf("invalid version %q", versionFlag)
		}
		vnum = n
	}
	ver := obj.Version(vnum)
	if ver == nil {
		return nil, fmt.Errorf("version not found")
	}
	return ver, nil
}

// buildFileMap builds the logical path -> content path mapping for an object version.
func buildFileMap(obj *ocfl.Object, ver *ocfl.ObjectVersion) (map[string]string, error) {
	state := ver.State()
	manifest := obj.Manifest()
	objPath := obj.Path()

	files := make(map[string]string, state.NumPaths())
	for logicalPath, digest := range state.Paths() {
		contentPaths := manifest[digest]
		if len(contentPaths) == 0 {
			return nil, fmt.Errorf("missing manifest entry for digest %s", digest)
		}
		files[logicalPath] = objPath + "/" + contentPaths[0]
	}
	return files, nil
}

func layoutString(root *ocfl.Root) string {
	if l := root.Layout(); l != nil {
		return fmt.Sprintf("%v", l)
	}
	return ""
}

func newS3Root(ctx context.Context, storageRoot, objectID, versionFlag string) (*Result, error) {
	after := strings.TrimPrefix(storageRoot, "s3://")
	bucket, prefix, _ := strings.Cut(after, "/")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	fsys := ocfls3.NewBucketFS(s3Client, bucket)
	root, err := ocfl.NewRoot(ctx, fsys, prefix)
	if err != nil {
		return nil, fmt.Errorf("opening OCFL root: %w", err)
	}

	obj, err := root.NewObject(ctx, objectID, ocfl.ObjectMustExist())
	if err != nil {
		return nil, fmt.Errorf("loading OCFL object: %w", err)
	}

	ver, err := resolveVersion(obj, versionFlag)
	if err != nil {
		return nil, err
	}
	files, err := buildFileMap(obj, ver)
	if err != nil {
		return nil, err
	}

	return &Result{
		Root: &s3Root{
			s3Client: s3Client,
			bucket:   bucket,
			files:    files,
		},
		Info: Info{
			ObjectID:  obj.ID(),
			Version:   ver.VNum().String(),
			FileCount: len(files),
			RootSpec:  string(root.Spec()),
			Layout:    layoutString(root),
		},
	}, nil
}

func newLocalRoot(ctx context.Context, storageRoot, objectID, versionFlag string) (*Result, error) {
	absRoot, err := filepath.Abs(storageRoot)
	if err != nil {
		return nil, fmt.Errorf("resolving path: %w", err)
	}

	fsys, err := ocfllocal.NewFS(absRoot)
	if err != nil {
		return nil, fmt.Errorf("opening local FS: %w", err)
	}

	root, err := ocfl.NewRoot(ctx, fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("opening OCFL root: %w", err)
	}

	obj, err := root.NewObject(ctx, objectID, ocfl.ObjectMustExist())
	if err != nil {
		return nil, fmt.Errorf("loading OCFL object: %w", err)
	}

	ver, err := resolveVersion(obj, versionFlag)
	if err != nil {
		return nil, err
	}
	relFiles, err := buildFileMap(obj, ver)
	if err != nil {
		return nil, err
	}

	// Convert relative content paths to absolute paths on disk
	files := make(map[string]string, len(relFiles))
	for logicalPath, relPath := range relFiles {
		files[logicalPath] = filepath.Join(absRoot, filepath.FromSlash(relPath))
	}

	return &Result{
		Root: &localRoot{files: files},
		Info: Info{
			ObjectID:  obj.ID(),
			Version:   ver.VNum().String(),
			FileCount: len(files),
			RootSpec:  string(root.Spec()),
			Layout:    layoutString(root),
		},
	}, nil
}
