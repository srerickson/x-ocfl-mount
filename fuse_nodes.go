package ocflfuse

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

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
