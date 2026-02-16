// Package ocflfuse provides a read-only FUSE filesystem for OCFL objects.
//
// The primary entry point is [NewRoot], which resolves an OCFL object version
// and returns an [fs.InodeEmbedder] suitable for use with [fs.Mount].
package ocflfuse

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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

// fileNodeCreator creates a persistent FUSE inode for a file.
type fileNodeCreator func(ctx context.Context, parent *fs.Inode, contentPath string) *fs.Inode

// buildFuseTree populates a FUSE inode tree from logical paths using the given
// file node creator. This is shared between S3 and local backends.
func buildFuseTree(ctx context.Context, root *fs.Inode, files map[string]string, newFile fileNodeCreator) {
	for logicalPath, contentPath := range files {
		dir := root
		parts := strings.Split(logicalPath, "/")

		// Create parent directories
		for _, part := range parts[:len(parts)-1] {
			child := dir.GetChild(part)
			if child == nil {
				dirNode := &ocflDir{}
				child = dir.NewPersistentInode(ctx, dirNode, fs.StableAttr{Mode: syscall.S_IFDIR})
				dir.AddChild(part, child, false)
			}
			dir = child
		}

		// Add file node
		filename := parts[len(parts)-1]
		child := newFile(ctx, dir, contentPath)
		dir.AddChild(filename, child, false)
	}
}

// ocflDir is a read-only directory node in the FUSE tree.
type ocflDir struct {
	fs.Inode
}

var _ = (fs.NodeGetattrer)((*ocflDir)(nil))

func (d *ocflDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555 | syscall.S_IFDIR
	return 0
}

// --- S3 backend ---

// s3Root is the FUSE root node for S3-backed OCFL objects.
type s3Root struct {
	fs.Inode
	s3Client *s3.Client
	bucket   string
	files    map[string]string // logicalPath -> S3 key
}

var _ = (fs.NodeOnAdder)((*s3Root)(nil))

func (r *s3Root) OnAdd(ctx context.Context) {
	buildFuseTree(ctx, &r.Inode, r.files, func(ctx context.Context, parent *fs.Inode, contentPath string) *fs.Inode {
		node := &s3File{
			s3Client: r.s3Client,
			bucket:   r.bucket,
			s3Key:    contentPath,
		}
		return parent.NewPersistentInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	})
}

// s3File is a file node backed by an S3 object.
type s3File struct {
	fs.Inode
	s3Client *s3.Client
	bucket   string
	s3Key    string
	sizeOnce sync.Once
	size     int64
	sizeErr  error
}

var _ = (fs.NodeGetattrer)((*s3File)(nil))
var _ = (fs.NodeOpener)((*s3File)(nil))
var _ = (fs.NodeReader)((*s3File)(nil))

func (f *s3File) fetchSize(ctx context.Context) (int64, error) {
	f.sizeOnce.Do(func() {
		resp, err := f.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &f.bucket,
			Key:    &f.s3Key,
		})
		if err != nil {
			f.sizeErr = err
			return
		}
		if resp.ContentLength != nil {
			f.size = *resp.ContentLength
		}
	})
	return f.size, f.sizeErr
}

func (f *s3File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	size, err := f.fetchSize(ctx)
	if err != nil {
		log.Printf("HeadObject error for %s: %v", f.s3Key, err)
		return syscall.EIO
	}
	out.Mode = 0444 | syscall.S_IFREG
	out.Size = uint64(size)
	return 0
}

func (f *s3File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *s3File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(dest))-1)
	resp, err := f.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &f.bucket,
		Key:    &f.s3Key,
		Range:  &rangeHeader,
	})
	if err != nil {
		log.Printf("GetObject range error for %s: %v", f.s3Key, err)
		return nil, syscall.EIO
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Read error for %s: %v", f.s3Key, err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(data), 0
}

// --- Local filesystem backend ---

// localRoot is the FUSE root node for locally-stored OCFL objects.
type localRoot struct {
	fs.Inode
	files map[string]string // logicalPath -> absolute file path on disk
}

var _ = (fs.NodeOnAdder)((*localRoot)(nil))

func (r *localRoot) OnAdd(ctx context.Context) {
	buildFuseTree(ctx, &r.Inode, r.files, func(ctx context.Context, parent *fs.Inode, contentPath string) *fs.Inode {
		node := &localFile{path: contentPath}
		return parent.NewPersistentInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	})
}

// localFile is a file node backed by a local file.
type localFile struct {
	fs.Inode
	path string // absolute path on disk
}

var _ = (fs.NodeGetattrer)((*localFile)(nil))
var _ = (fs.NodeOpener)((*localFile)(nil))

func (f *localFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	info, err := os.Stat(f.path)
	if err != nil {
		log.Printf("stat error for %s: %v", f.path, err)
		return syscall.EIO
	}
	out.Mode = 0444 | syscall.S_IFREG
	out.Size = uint64(info.Size())
	return 0
}

func (f *localFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	fh, err := os.Open(f.path)
	if err != nil {
		log.Printf("open error for %s: %v", f.path, err)
		return nil, 0, syscall.EIO
	}
	return &localFileHandle{file: fh}, fuse.FOPEN_KEEP_CACHE, 0
}

// localFileHandle holds an open file descriptor for a local file.
type localFileHandle struct {
	file *os.File
}

var _ = (fs.FileReader)((*localFileHandle)(nil))
var _ = (fs.FileReleaser)((*localFileHandle)(nil))

func (fh *localFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := fh.file.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		log.Printf("read error for %s: %v", fh.file.Name(), err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (fh *localFileHandle) Release(ctx context.Context) syscall.Errno {
	if err := fh.file.Close(); err != nil {
		log.Printf("close error for %s: %v", fh.file.Name(), err)
		return syscall.EIO
	}
	return 0
}
