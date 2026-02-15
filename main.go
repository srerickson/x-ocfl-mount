package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// OCFLInventory represents the OCFL inventory.json structure
type OCFLInventory struct {
	ID              string                       `json:"id"`
	Type            string                       `json:"type"`
	DigestAlgorithm string                       `json:"digestAlgorithm"`
	Head            string                       `json:"head"`
	Manifest        map[string][]string          `json:"manifest"`
	Versions        map[string]OCFLVersion       `json:"versions"`
}

// OCFLVersion represents a version entry in the inventory
type OCFLVersion struct {
	Created string              `json:"created"`
	Message string              `json:"message"`
	User    *OCFLUser           `json:"user,omitempty"`
	State   map[string][]string `json:"state"`
}

// OCFLUser represents a user in the inventory
type OCFLUser struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

// S3Backend handles S3 operations
type S3Backend struct {
	client *s3.Client
	bucket string
	prefix string
}

func NewS3Backend(ctx context.Context, bucketPrefix string) (*S3Backend, error) {
	parts := strings.SplitN(bucketPrefix, "/", 2)
	bucket := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = parts[1]
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	return &S3Backend{
		client: s3.NewFromConfig(cfg),
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (s *S3Backend) GetObject(ctx context.Context, key string) ([]byte, error) {
	fullKey := key
	if s.prefix != "" {
		fullKey = s.prefix + "/" + key
	}

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &fullKey,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (s *S3Backend) GetObjectReader(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	fullKey := key
	if s.prefix != "" {
		fullKey = s.prefix + "/" + key
	}

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &fullKey,
	})
	if err != nil {
		return nil, 0, err
	}

	size := int64(0)
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}

	return resp.Body, size, nil
}

// OCFLObject represents a mounted OCFL object
type OCFLObject struct {
	backend   *S3Backend
	objectID  string
	version   string
	inventory *OCFLInventory
	// Map from logical path to content path (relative to object root)
	files map[string]string
	// Map from logical path to file size
	sizes map[string]int64
}

func NewOCFLObject(ctx context.Context, backend *S3Backend, objectID, version string) (*OCFLObject, error) {
	// OCFL object path is derived from the object ID
	// For simplicity, we'll use the object ID directly as the path
	// In practice, you might need pairtree or other path encoding
	objectPath := objectID

	// Fetch the inventory
	inventoryPath := objectPath + "/inventory.json"
	data, err := backend.GetObject(ctx, inventoryPath)
	if err != nil {
		return nil, fmt.Errorf("fetching inventory: %w", err)
	}

	var inventory OCFLInventory
	if err := json.Unmarshal(data, &inventory); err != nil {
		return nil, fmt.Errorf("parsing inventory: %w", err)
	}

	// Use head version if not specified
	if version == "" {
		version = inventory.Head
	}

	// Validate version exists
	versionData, ok := inventory.Versions[version]
	if !ok {
		var versions []string
		for v := range inventory.Versions {
			versions = append(versions, v)
		}
		sort.Strings(versions)
		return nil, fmt.Errorf("version %q not found; available versions: %v", version, versions)
	}

	// Build reverse manifest lookup (digest -> content path)
	digestToPath := make(map[string]string)
	for digest, paths := range inventory.Manifest {
		if len(paths) > 0 {
			digestToPath[digest] = paths[0]
		}
	}

	// Build file map from version state
	files := make(map[string]string)
	for digest, logicalPaths := range versionData.State {
		contentPath, ok := digestToPath[digest]
		if !ok {
			return nil, fmt.Errorf("digest %q in state not found in manifest", digest)
		}
		for _, logicalPath := range logicalPaths {
			files[logicalPath] = objectPath + "/" + contentPath
		}
	}

	log.Printf("Loaded OCFL object %q version %s with %d files", objectID, version, len(files))

	return &OCFLObject{
		backend:   backend,
		objectID:  objectID,
		version:   version,
		inventory: &inventory,
		files:     files,
		sizes:     make(map[string]int64),
	}, nil
}

// FUSE filesystem implementation

type OCFLRoot struct {
	fs.Inode
	obj *OCFLObject
}

var _ = (fs.NodeOnAdder)((*OCFLRoot)(nil))

func (r *OCFLRoot) OnAdd(ctx context.Context) {
	// Build the directory tree from the file list
	for logicalPath, contentPath := range r.obj.files {
		// Create parent directories as needed
		dir := &r.Inode
		parts := strings.Split(logicalPath, "/")

		for i, part := range parts[:len(parts)-1] {
			child := dir.GetChild(part)
			if child == nil {
				dirNode := &OCFLDir{}
				child = dir.NewPersistentInode(ctx, dirNode, fs.StableAttr{Mode: syscall.S_IFDIR})
				dir.AddChild(part, child, false)
			}
			dir = child
			_ = i
		}

		// Add the file
		filename := parts[len(parts)-1]
		fileNode := &OCFLFile{
			obj:         r.obj,
			logicalPath: logicalPath,
			contentPath: contentPath,
		}
		child := dir.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG})
		dir.AddChild(filename, child, false)
	}
}

type OCFLDir struct {
	fs.Inode
}

var _ = (fs.NodeGetattrer)((*OCFLDir)(nil))

func (d *OCFLDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | syscall.S_IFDIR
	return 0
}

type OCFLFile struct {
	fs.Inode
	obj         *OCFLObject
	logicalPath string
	contentPath string
	size        int64
	sizeKnown   bool
}

var _ = (fs.NodeGetattrer)((*OCFLFile)(nil))
var _ = (fs.NodeOpener)((*OCFLFile)(nil))
var _ = (fs.NodeReader)((*OCFLFile)(nil))

func (f *OCFLFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if !f.sizeKnown {
		// Fetch size from S3
		_, size, err := f.obj.backend.GetObjectReader(ctx, f.contentPath)
		if err != nil {
			log.Printf("Error getting size for %s: %v", f.contentPath, err)
			return syscall.EIO
		}
		f.size = size
		f.sizeKnown = true
	}
	out.Mode = 0644 | syscall.S_IFREG
	out.Size = uint64(f.size)
	return 0
}

func (f *OCFLFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return &OCFLFileHandle{file: f}, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *OCFLFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	reader, _, err := f.obj.backend.GetObjectReader(ctx, f.contentPath)
	if err != nil {
		log.Printf("Error reading %s: %v", f.contentPath, err)
		return nil, syscall.EIO
	}
	defer reader.Close()

	// Skip to offset
	if off > 0 {
		if _, err := io.CopyN(io.Discard, reader, off); err != nil {
			if err == io.EOF {
				return fuse.ReadResultData(nil), 0
			}
			log.Printf("Error seeking %s: %v", f.contentPath, err)
			return nil, syscall.EIO
		}
	}

	n, err := reader.Read(dest)
	if err != nil && err != io.EOF {
		log.Printf("Error reading %s: %v", f.contentPath, err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest[:n]), 0
}

type OCFLFileHandle struct {
	file *OCFLFile
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <bucket/prefix> <object-id> <mountpoint>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Mount an OCFL object from S3 as a read-only filesystem.\n\n")
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  bucket/prefix   S3 bucket and optional prefix (e.g., mybucket/ocfl-root)\n")
		fmt.Fprintf(os.Stderr, "  object-id       OCFL object identifier\n")
		fmt.Fprintf(os.Stderr, "  mountpoint      Local directory to mount the filesystem\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	version := flag.String("version", "", "Object version to mount (default: head/latest)")
	debug := flag.Bool("debug", false, "Enable FUSE debug output")
	flag.Parse()

	if flag.NArg() != 3 {
		flag.Usage()
		os.Exit(1)
	}

	bucketPrefix := flag.Arg(0)
	objectID := flag.Arg(1)
	mountpoint := flag.Arg(2)

	ctx := context.Background()

	// Create S3 backend
	backend, err := NewS3Backend(ctx, bucketPrefix)
	if err != nil {
		log.Fatalf("Failed to create S3 backend: %v", err)
	}

	// Load OCFL object
	obj, err := NewOCFLObject(ctx, backend, objectID, *version)
	if err != nil {
		log.Fatalf("Failed to load OCFL object: %v", err)
	}

	// Create mountpoint if it doesn't exist
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		log.Fatalf("Failed to create mountpoint: %v", err)
	}

	// Mount the filesystem
	root := &OCFLRoot{obj: obj}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "ocfl-" + path.Base(objectID),
			Name:   "ocfl",
			Debug:  *debug,
		},
	}

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		log.Fatalf("Failed to mount: %v", err)
	}

	log.Printf("Mounted OCFL object %q version %s at %s", objectID, obj.version, mountpoint)
	log.Printf("Press Ctrl+C to unmount")

	// Handle signals for clean unmount
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Unmounting...")
		server.Unmount()
	}()

	server.Wait()
	log.Println("Unmounted")
}
