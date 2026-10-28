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

// Two behaviours of rclone's VFS that the model test used to work around
// (remounting after a directory rename, never renaming over an existing file).
// They are asserted here as they should be, for the plain local backend and for
// cfs, so a regression or a fix in the rclone checkout shows up as a result.
func TestVFSRenameQuirks(t *testing.T) {
	for _, backend := range []string{"local", "8Mi"} {
		for _, wb := range []time.Duration{0, time.Second} {
			t.Run(fmt.Sprintf("%s/write_back=%v", backend, wb), func(t *testing.T) {
				v, direct, drive, _ := setupVFSOpts(t, backend, func(o *vfscommon.Options) { o.WriteBack = fs.Duration(wb) })
				onDrive := func(p string) []byte {
					if backend == "local" {
						b, _ := os.ReadFile(filepath.Join(drive, filepath.FromSlash(p)))
						return b
					}
					return direct(p)
				}
				t.Run("directory rename keeps the destination's siblings", func(t *testing.T) {
					for _, d := range []string{"d1/s1", "d2/s1"} {
						if err := v.MkdirAll(d, 0o755); err != nil {
							t.Fatal(err)
						}
					}
					v.WriteFile("d1/s1/a", []byte("a"), 0o644)
					v.WriteFile("d2/s1/b", []byte("b"), 0o644)
					settle(v)
					if err := v.Rename("d1/s1", "d2/s4"); err != nil {
						t.Fatal(err)
					}
					infos, err := v.ReadDir("d2")
					if err != nil {
						t.Fatal(err)
					}
					var names []string
					for _, fi := range infos {
						names = append(names, fi.Name())
					}
					if fmt.Sprint(names) != "[s1 s4]" {
						t.Errorf("d2 holds %v in the VFS right after the rename, want [s1 s4]", names)
					}
				})
				t.Run("rename over an existing file", func(t *testing.T) {
					v.WriteFile("x", []byte("XXXX-new"), 0o644)
					v.WriteFile("y", []byte("yy-old"), 0o644)
					settle(v)
					time.Sleep(wb + 300*time.Millisecond)
					if b, _ := v.ReadFile("y"); string(b) != "yy-old" {
						t.Fatalf("setup: %q", b)
					}
					if err := v.Rename("x", "y"); err != nil {
						t.Fatal(err)
					}
					if b, _ := v.ReadFile("y"); string(b) != "XXXX-new" {
						t.Errorf("the VFS serves %q for y after the rename, want the renamed content", b)
					}
					settle(v)
					deadline := time.Now().Add(wb + 10*time.Second)
					for time.Now().Before(deadline) && string(onDrive("y")) != "XXXX-new" {
						time.Sleep(100 * time.Millisecond)
					}
					if got := onDrive("y"); string(got) != "XXXX-new" {
						t.Errorf("the drive holds %q for y, want the renamed content", got)
					}
				})
			})
		}
	}
}
