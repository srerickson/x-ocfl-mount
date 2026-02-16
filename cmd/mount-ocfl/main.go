package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	ocflfuse "github.com/srerickson/ocfl-fuse"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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

	result, err := ocflfuse.NewRoot(ctx, storageRoot, objectID, *versionFlag)
	if err != nil {
		log.Fatalf("%v", err)
	}

	log.Printf("Opened OCFL root (spec %s, layout %s)", result.Info.RootSpec, result.Info.Layout)
	log.Printf("OCFL object %q version %s: %d files", result.Info.ObjectID, result.Info.Version, result.Info.FileCount)

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

	server, err := fs.Mount(mountpoint, result.Root, opts)
	if err != nil {
		log.Fatalf("Failed to mount: %v", err)
	}

	log.Printf("Mounted at %s", mountpoint)
	log.Printf("Press Ctrl+C to unmount")

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
