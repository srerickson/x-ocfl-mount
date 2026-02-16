package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// OCFLRoot is the FUSE root node. It holds the S3 client and the
// logical-path-to-S3-key mapping derived from the OCFL inventory.
type OCFLRoot struct {
	fs.Inode
	s3Client *s3.Client
	bucket   string
	files    map[string]string // logicalPath -> S3 key
}

var _ = (fs.NodeOnAdder)((*OCFLRoot)(nil))

func (r *OCFLRoot) OnAdd(ctx context.Context) {
	for logicalPath, s3Key := range r.files {
		dir := &r.Inode
		parts := strings.Split(logicalPath, "/")

		// Create parent directories
		for _, part := range parts[:len(parts)-1] {
			child := dir.GetChild(part)
			if child == nil {
				dirNode := &OCFLDir{}
				child = dir.NewPersistentInode(ctx, dirNode, fs.StableAttr{Mode: syscall.S_IFDIR})
				dir.AddChild(part, child, false)
			}
			dir = child
		}

		// Add file node
		filename := parts[len(parts)-1]
		fileNode := &OCFLFile{
			s3Client: r.s3Client,
			bucket:   r.bucket,
			s3Key:    s3Key,
		}
		child := dir.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG})
		dir.AddChild(filename, child, false)
	}
}

// OCFLDir is a directory node in the FUSE tree.
type OCFLDir struct {
	fs.Inode
}

var _ = (fs.NodeGetattrer)((*OCFLDir)(nil))

func (d *OCFLDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | syscall.S_IFDIR
	return 0
}

// OCFLFile is a file node backed by an S3 object.
type OCFLFile struct {
	fs.Inode
	s3Client *s3.Client
	bucket   string
	s3Key    string
	size     int64
	sizeOK   bool
}

var _ = (fs.NodeGetattrer)((*OCFLFile)(nil))
var _ = (fs.NodeOpener)((*OCFLFile)(nil))
var _ = (fs.NodeReader)((*OCFLFile)(nil))

func (f *OCFLFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if !f.sizeOK {
		resp, err := f.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &f.bucket,
			Key:    &f.s3Key,
		})
		if err != nil {
			log.Printf("HeadObject error for %s: %v", f.s3Key, err)
			return syscall.EIO
		}
		if resp.ContentLength != nil {
			f.size = *resp.ContentLength
		}
		f.sizeOK = true
	}
	out.Mode = 0644 | syscall.S_IFREG
	out.Size = uint64(f.size)
	return 0
}

func (f *OCFLFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *OCFLFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
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
