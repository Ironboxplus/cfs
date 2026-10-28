package cfsrclone_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cfsrclone "github.com/Ironboxplus/cfs/frontend/rclone"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

func countFiles(dir string) int {
	ents, _ := os.ReadDir(dir)
	return len(ents)
}

// The VFS path (what "rclone mount" and "serve webdav" run on): three callers
// write small files through the VFS, rclone's write-back uploads them with its
// own concurrency, and the packs on the drive follow the bytes, not the files
// or the callers. Both ack modes are run and counted.
func TestVFSPackEfficiency(t *testing.T) {
	const files, callers = 500, 3
	const packMax = 64 << 10
	rng := rand.New(rand.NewPCG(5, 6))
	sizes := make([]int, files)
	var total int64
	for i := range sizes {
		sizes[i] = 200 + rng.IntN(1800)
		total += int64(sizes[i])
	}
	expect := int((total + packMax - 1) / packMax)
	counts := map[string][2]int{}
	for _, mode := range []string{"staged", "durable"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			drive, state, cache := t.TempDir(), t.TempDir(), t.TempDir()
			config.SetCacheDir(cache)
			quiet, max := "1h", "1h"
			if mode == "durable" {
				quiet, max = "40ms", "400ms"
			}
			f, err := cfsrclone.NewFs(ctx, "vfspacks", "", configmap.Simple{
				"remote": drive, "state_dir": state, "ack": mode, "pack_max_bytes": "64Ki",
				"seal_quiet": quiet, "seal_max": max, "dir_cache": "1h", "lease_ttl": "30s",
			})
			if err != nil {
				t.Fatal(err)
			}
			opt := vfscommon.Opt
			opt.CacheMode = vfscommon.CacheModeFull
			opt.WriteBack = fs.Duration(30 * time.Millisecond)
			opt.CachePollInterval = fs.Duration(time.Hour)
			v := vfs.New(ctx, f, &opt)
			t.Cleanup(func() {
				v.Shutdown()
				cfsrclone.CloseAll(ctx)
			})
			for d := 0; d < 10; d++ {
				if err := v.MkdirAll(fmt.Sprintf("eff/d%d", d), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			var wg sync.WaitGroup
			var next atomic.Int64
			for c := 0; c < callers; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(next.Add(1)) - 1
						if i >= files {
							return
						}
						d := fileData(i, sizes[i])
						if err := v.WriteFile(fmt.Sprintf("eff/d%d/f%04d", i%10, i), d, 0o644); err != nil {
							t.Errorf("write %d: %v", i, err)
							return
						}
					}
				}()
			}
			wg.Wait()
			settle(v)
			if err := cfsrclone.Volume(f).Flush(ctx); err != nil {
				t.Fatal(err)
			}
			packs := countFiles(filepath.Join(drive, ".cfs", "packs"))
			segs := countFiles(filepath.Join(drive, ".cfs", "log"))
			counts[mode] = [2]int{packs, segs}
			t.Logf("%s through the VFS: %d files, %d bytes, %d callers: %d packs, %d log segments (bytes / pack size = %d)", mode, files, total, callers, packs, segs, expect)
			for i := 0; i < files; i += 53 {
				got, err := v.ReadFile(fmt.Sprintf("eff/d%d/f%04d", i%10, i))
				if err != nil || !bytes.Equal(got, fileData(i, sizes[i])) {
					t.Fatalf("file %d wrong: %v", i, err)
				}
			}
			if mode == "staged" && (packs > expect+3 || packs < expect-1 || segs > packs+1) {
				t.Errorf("staged through the VFS: %d packs and %d segments for %d bytes, want about %d packs", packs, segs, total, expect)
			}
		})
	}
	s, d := counts["staged"], counts["durable"]
	t.Logf("packs / segments: staged %d / %d, durable %d / %d", s[0], s[1], d[0], d[1])
}

// fileData is the (incompressible) content of file i.
func fileData(i, size int) []byte {
	rng := rand.New(rand.NewPCG(uint64(i), 77))
	b := make([]byte, size)
	for j := range b {
		b[j] = byte(rng.Uint32())
	}
	return b
}
