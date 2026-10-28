package cfsrclone_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// The way editors and many programs save: write a temporary file, rename it
// over the real one. On Windows, with rclone's VFS in cache mode full and a
// file that was read before, the rename of the cache file failed against the
// open handle, the error was swallowed, and the drive ended up with an empty
// file (rclone issues #4293, #9943; PRs #9574, #9975). The rclone checkout in
// ../rclone-fork carries the fixes; this test fails on unpatched v1.75.2 on
// Windows, for the plain local backend as much as for cfs.
func TestVFSAtomicSave(t *testing.T) {
	for _, backend := range []string{"local", "8Mi"} {
		for _, wb := range []time.Duration{0, time.Second} {
			name := fmt.Sprintf("%s/write_back=%v", map[string]string{"local": "local", "8Mi": "cfs"}[backend], wb)
			t.Run(name, func(t *testing.T) {
				v, direct, drive, _ := setupVFSOpts(t, backend, func(o *vfscommon.Options) {
					o.WriteBack = fs.Duration(wb)
				})
				onDrive := func() []byte {
					if backend == "local" {
						b, _ := os.ReadFile(filepath.Join(drive, "a.txt"))
						return b
					}
					return direct("a.txt")
				}
				old, fresh := []byte("old-content"), []byte("NEW-CONTENT-LONGER-THAN-THE-OLD")
				if err := v.WriteFile("a.txt", old, 0o644); err != nil {
					t.Fatal(err)
				}
				settle(v)
				time.Sleep(wb + 500*time.Millisecond)
				if b, err := v.ReadFile("a.txt"); err != nil || string(b) != string(old) {
					t.Fatalf("first read: %q %v", b, err)
				}
				if err := v.WriteFile("a.txt.tmp", fresh, 0o644); err != nil {
					t.Fatal(err)
				}
				if err := v.Rename("a.txt.tmp", "a.txt"); err != nil {
					t.Fatalf("rename over the file that was read: %v", err)
				}
				if b, err := v.ReadFile("a.txt"); err != nil || string(b) != string(fresh) {
					t.Fatalf("read right after the rename: %q %v", b, err)
				}
				settle(v)
				deadline := time.Now().Add(wb + 15*time.Second)
				for time.Now().Before(deadline) && string(onDrive()) != string(fresh) {
					time.Sleep(100 * time.Millisecond)
				}
				if got := onDrive(); string(got) != string(fresh) {
					t.Fatalf("the drive holds %q (%d bytes), want the new content (%d bytes)", got, len(got), len(fresh))
				}
				if b, _ := v.ReadFile("a.txt"); string(b) != string(fresh) {
					t.Fatalf("read at the end: %q", b)
				}
			})
		}
	}
}
