package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ocfl "github.com/srerickson/ocfl-go"
	ocfllocal "github.com/srerickson/ocfl-go/fs/local"
	ocfls3 "github.com/srerickson/ocfl-go/fs/s3"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <storage-root> <object-id> <mountpoint>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Mount an OCFL object as a read-only filesystem.\n\n")
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  storage-root   S3 URI (s3://bucket/prefix) or local path\n")
		fmt.Fprintf(os.Stderr, "  object-id      OCFL object identifier\n")
		fmt.Fprintf(os.Stderr, "  mountpoint     Local directory to mount the filesystem\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	versionFlag := flag.String("version", "", "Object version to mount (default: head/latest)")
	debug := flag.Bool("debug", false, "Enable FUSE debug output")
	flag.Parse()

	if flag.NArg() != 3 {
		flag.Usage()
		os.Exit(1)
	}

	storageRoot := flag.Arg(0)
	objectID := flag.Arg(1)
	mountpoint := flag.Arg(2)

	ctx := context.Background()

	var (
		fuseRoot fs.InodeEmbedder
		err      error
	)
	if strings.HasPrefix(storageRoot, "s3://") {
		fuseRoot, err = mountS3(ctx, storageRoot, objectID, *versionFlag)
	} else {
		fuseRoot, err = mountLocal(ctx, storageRoot, objectID, *versionFlag)
	}
	if err != nil {
		log.Fatalf("%v", err)
	}

	// Create mountpoint if it doesn't exist
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		log.Fatalf("Failed to create mountpoint: %v", err)
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:  "ocfl-" + path.Base(objectID),
			Name:    "ocfl",
			Debug:   *debug,
			Options: []string{"ro"},
		},
	}

	server, err := fs.Mount(mountpoint, fuseRoot, opts)
	if err != nil {
		log.Fatalf("Failed to mount: %v", err)
	}

	log.Printf("Mounted at %s", mountpoint)
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
// Content paths are relative to the FS root (e.g. "objPath/v1/content/file.txt").
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

func mountS3(ctx context.Context, storageRoot, objectID, versionFlag string) (fs.InodeEmbedder, error) {
	// Parse s3://bucket/prefix
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
	log.Printf("Opened OCFL root (spec %s, layout %v)", root.Spec(), root.Layout())

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
	log.Printf("OCFL object %q version %s: %d files", obj.ID(), ver.VNum(), len(files))

	return &s3Root{
		s3Client: s3Client,
		bucket:   bucket,
		files:    files,
	}, nil
}

func mountLocal(ctx context.Context, storageRoot, objectID, versionFlag string) (fs.InodeEmbedder, error) {
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
	log.Printf("Opened OCFL root (spec %s, layout %v)", root.Spec(), root.Layout())

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

	log.Printf("OCFL object %q version %s: %d files", obj.ID(), ver.VNum(), len(files))

	return &localRoot{files: files}, nil
}
