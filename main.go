package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ocfl "github.com/srerickson/ocfl-go"
	ocfls3 "github.com/srerickson/ocfl-go/fs/s3"
)

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

	versionFlag := flag.String("version", "", "Object version to mount (default: head/latest)")
	debug := flag.Bool("debug", false, "Enable FUSE debug output")
	flag.Parse()

	if flag.NArg() != 3 {
		flag.Usage()
		os.Exit(1)
	}

	bucketPrefix := flag.Arg(0)
	objectID := flag.Arg(1)
	mountpoint := flag.Arg(2)

	// Parse bucket and prefix
	bucket, prefix, _ := strings.Cut(bucketPrefix, "/")

	ctx := context.Background()

	// Create AWS S3 client
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	// Create BucketFS for the OCFL root
	fsys := ocfls3.NewBucketFS(s3Client, bucket)

	// Open the OCFL storage root
	root, err := ocfl.NewRoot(ctx, fsys, prefix)
	if err != nil {
		log.Fatalf("Failed to open OCFL root: %v", err)
	}
	log.Printf("Opened OCFL root (spec %s, layout %v)", root.Spec(), root.Layout())

	// Resolve and load the object
	obj, err := root.NewObject(ctx, objectID, ocfl.ObjectMustExist())
	if err != nil {
		log.Fatalf("Failed to load OCFL object: %v", err)
	}

	// Determine version to mount
	vnum := 0 // HEAD
	if *versionFlag != "" {
		// Parse "v3" -> 3
		v := *versionFlag
		if strings.HasPrefix(v, "v") {
			v = v[1:]
		}
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n < 1 {
			log.Fatalf("Invalid version %q", *versionFlag)
		}
		vnum = n
	}

	ver := obj.Version(vnum)
	if ver == nil {
		log.Fatalf("Version not found")
	}

	// Build logical path -> content S3 key mapping
	state := ver.State()
	manifest := obj.Manifest()
	objPath := obj.Path()

	files := make(map[string]string, state.NumPaths())
	for logicalPath, digest := range state.Paths() {
		contentPaths := manifest[digest]
		if len(contentPaths) == 0 {
			log.Fatalf("Missing manifest entry for digest %s", digest)
		}
		// Content path is relative to the FS root (bucket), so join objPath + contentPath
		files[logicalPath] = objPath + "/" + contentPaths[0]
	}

	log.Printf("OCFL object %q version %s: %d files", obj.ID(), ver.VNum(), len(files))

	// Create mountpoint if it doesn't exist
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		log.Fatalf("Failed to create mountpoint: %v", err)
	}

	// Mount the filesystem
	fuseRoot := &OCFLRoot{
		s3Client: s3Client,
		bucket:   bucket,
		files:    files,
	}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "ocfl-" + path.Base(objectID),
			Name:   "ocfl",
			Debug:  *debug,
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
