package cfsrclone_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"
	"time"

	cfsrclone "github.com/Ironboxplus/cfs/frontend/rclone"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// The same VFS code that backs "rclone mount" and "serve webdav", driven
// in-process with POSIX-style operations on a cfs remote and on a local
// reference directory in parallel. The drive's view (read through the volume,
// not through the VFS cache) is compared after the write-back has completed.

func setupVFS(t *testing.T, threshold string) (*vfs.VFS, func(string) []byte, string, func() *vfs.VFS) {
	t.Helper()
	ctx := context.Background()
	drive, state, cache := t.TempDir(), t.TempDir(), t.TempDir()
	config.SetCacheDir(cache)
	var f fs.Fs
	var err error
	if threshold == "local" { // control: the same VFS on rclone's own local backend
		f, err = fs.NewFs(ctx, drive)
	} else {
		f, err = cfsrclone.NewFs(ctx, "vfscfs", "", configmap.Simple{
			"remote": drive, "state_dir": state, "threshold": threshold,
			"seal_quiet": "10ms", "seal_max": "100ms", "dir_cache": "1h", "lease_ttl": "30s",
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeFull
	opt.WriteBack = fs.Duration(30 * time.Millisecond)
	opt.CachePollInterval = fs.Duration(time.Hour)
	cur := vfs.New(ctx, f, &opt)
	t.Cleanup(func() {
		cur.Shutdown()
		cfsrclone.CloseAll(ctx)
	})
	// remount throws the VFS away (after its uploads finished) and starts a
	// fresh one on the same remote, like unmounting and mounting again.
	remount := func() *vfs.VFS {
		cur.WaitForWriters(30 * time.Second)
		cur.Shutdown()
		cur = vfs.New(ctx, f, &opt)
		return cur
	}
	vol := cfsrclone.Volume(f)
	direct := func(p string) []byte {
		if vol == nil {
			b, _ := os.ReadFile(filepath.Join(drive, filepath.FromSlash(p)))
			return b
		}
		rc, err := vol.Open(ctx, p, 0, -1)
		if err != nil {
			return nil
		}
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		return b
	}
	return cur, direct, drive, remount
}

type vfsRef struct{ root string }

func (r vfsRef) abs(p string) string { return filepath.Join(r.root, filepath.FromSlash(p)) }

func TestVFSModel(t *testing.T) {
	for _, thr := range []string{"local", "8Mi", "2000"} {
		t.Run("threshold="+thr, func(t *testing.T) { vfsModel(t, thr) })
	}
}

func vfsModel(t *testing.T, threshold string) {
	v, direct, _, remount := setupVFS(t, threshold)
	ref := vfsRef{t.TempDir()}
	rng := rand.New(rand.NewPCG(5, 6))
	rnd := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		return b
	}
	slot := func() string { return fmt.Sprintf("d%d/s%d/f%d", rng.IntN(3), rng.IntN(3), rng.IntN(6)) }
	existing := func() string {
		var names []string
		filepath.WalkDir(ref.root, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				rel, _ := filepath.Rel(ref.root, p)
				names = append(names, filepath.ToSlash(rel))
			}
			return nil
		})
		if len(names) == 0 {
			return ""
		}
		sort.Strings(names)
		return names[rng.IntN(len(names))]
	}
	mkdirs := func(p string) {
		dir := filepath.Dir(p)
		if dir != "." {
			if err := v.MkdirAll(filepath.ToSlash(dir), 0o755); err != nil {
				t.Fatalf("mkdirall %s: %v", dir, err)
			}
			os.MkdirAll(ref.abs(filepath.ToSlash(dir)), 0o755)
		}
	}
	counts := map[string]int{}
	check := func(step int) {
		t.Helper()
		v.WaitForWriters(30 * time.Second)
		// The VFS view and the drive's view agree with the reference.
		filepath.WalkDir(ref.root, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			rel, _ := filepath.Rel(ref.root, p)
			rel = filepath.ToSlash(rel)
			want, _ := os.ReadFile(p)
			if got, err := v.ReadFile(rel); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("step %d: %s via VFS differs (%v)", step, rel, err)
			}
			if got := direct(rel); !bytes.Equal(got, want) {
				t.Fatalf("step %d: %s on the volume differs: %d bytes, want %d", step, rel, len(got), len(want))
			}
			return nil
		})
		// No stray names in the VFS tree.
		var walk func(dir string)
		walk = func(dir string) {
			infos, err := v.ReadDir(dir)
			if err != nil {
				t.Fatalf("step %d: readdir %q: %v", step, dir, err)
			}
			for _, fi := range infos {
				p := filepath.ToSlash(filepath.Join(dir, fi.Name()))
				if _, err := os.Stat(ref.abs(p)); err != nil {
					t.Fatalf("step %d: %s exists in the VFS but not in the reference", step, p)
				}
				if fi.IsDir() {
					walk(p)
				}
			}
		}
		walk("")
	}
	for step := 1; step <= 400; step++ {
		if step >= 50 && step <= 62 && os.Getenv("VFS_TRACE") != "" {
			t.Logf("step %d starting", step)
		}
		switch r := rng.IntN(100); {
		case r < 25: // write a whole file
			p := slot()
			mkdirs(p)
			data := rnd([]int{0, 5, 700, 1999, 2000, 2001, 6000, 70000}[rng.IntN(8)])
			if err := v.WriteFile(p, data, 0o644); err != nil {
				t.Fatalf("step %d: write %s: %v", step, p, err)
			}
			os.WriteFile(ref.abs(p), data, 0o644)
			counts["write"]++
			vtrace(t, step, "write "+p)
		case r < 40: // write in the middle / beyond the end
			p := existing()
			if p == "" {
				break
			}
			old, _ := os.ReadFile(ref.abs(p))
			off := rng.IntN(len(old) + 100)
			data := rnd(1 + rng.IntN(3000))
			fd, err := v.OpenFile(p, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("step %d: open %s: %v", step, p, err)
			}
			if _, err := fd.WriteAt(data, int64(off)); err != nil {
				t.Fatalf("step %d: writeat: %v", step, err)
			}
			if err := fd.Close(); err != nil {
				t.Fatalf("step %d: close: %v", step, err)
			}
			if off+len(data) > len(old) {
				old = append(old, make([]byte, off+len(data)-len(old))...)
			}
			copy(old[off:], data)
			os.WriteFile(ref.abs(p), old, 0o644)
			counts["pwrite"]++
			vtrace(t, step, fmt.Sprintf("pwrite %s off=%d len=%d oldlen=%d", p, off, len(data), len(old)))
		case r < 50: // append
			p := existing()
			if p == "" {
				break
			}
			data := rnd(1 + rng.IntN(4000))
			fd, err := v.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				infos, derr := v.ReadDir(path.Dir(p))
				names := []string{}
				for _, fi := range infos {
					names = append(names, fi.Name())
				}
				t.Logf("VFS dir %s: %v (%v); direct read %d bytes", path.Dir(p), names, derr, len(direct(p)))
				pinfos, perr := v.ReadDir("d2")
				var pn []string
				for _, fi := range pinfos {
					pn = append(pn, fi.Name())
				}
				t.Logf("VFS d2: %v (%v)", pn, perr)
				t.Fatalf("step %d: open append %s: %v", step, p, err)
			}
			fd.Write(data)
			if err := fd.Close(); err != nil {
				t.Fatalf("step %d: close: %v", step, err)
			}
			f, _ := os.OpenFile(ref.abs(p), os.O_APPEND|os.O_WRONLY, 0)
			f.Write(data)
			f.Close()
			counts["append"]++
			vtrace(t, step, fmt.Sprintf("append %s len=%d", p, len(data)))
		case r < 58: // truncate
			p := existing()
			if p == "" {
				break
			}
			old, _ := os.ReadFile(ref.abs(p))
			n := rng.IntN(len(old) + 500)
			fd, err := v.OpenFile(p, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := fd.Truncate(int64(n)); err != nil {
				t.Fatalf("step %d: truncate: %v", step, err)
			}
			if err := fd.Close(); err != nil {
				t.Fatal(err)
			}
			os.Truncate(ref.abs(p), int64(n))
			counts["truncate"]++
			vtrace(t, step, fmt.Sprintf("truncate %s to %d", p, n))
		case r < 68: // delete
			p := existing()
			if p == "" {
				break
			}
			if err := v.Remove(p); err != nil {
				t.Fatalf("step %d: remove %s: %v", step, p, err)
			}
			os.Remove(ref.abs(p))
			counts["remove"]++
			vtrace(t, step, "remove "+p)
		case r < 82: // rename file
			p := existing()
			to := slot()
			if p == "" || p == to {
				break
			}
			if _, err := os.Stat(ref.abs(to)); err == nil {
				break // rename over an existing file: rclone's VFS keeps serving the old cached bytes (same with its local backend)
			}
			v.WaitForWriters(30 * time.Second)
			mkdirs(to)
			if err := v.Rename(p, to); err != nil {
				t.Fatalf("step %d: rename %s -> %s: %v", step, p, to, err)
			}
			os.Rename(ref.abs(p), ref.abs(to))
			counts["rename"]++
			vtrace(t, step, "rename "+p+" -> "+to)
		case r < 88: // rename directory
			src := fmt.Sprintf("d%d/s%d", rng.IntN(3), rng.IntN(3))
			dst := fmt.Sprintf("d%d/s%d", rng.IntN(3), rng.IntN(3)+3)
			if _, err := os.Stat(ref.abs(src)); err != nil {
				break
			}
			if _, err := os.Stat(ref.abs(dst)); err == nil {
				break
			}
			v.WaitForWriters(30 * time.Second)
			mkdirs(dst)
			if err := v.Rename(src, dst); err != nil {
				t.Fatalf("step %d: rename dir %s -> %s: %v", step, src, dst, err)
			}
			os.Rename(ref.abs(src), ref.abs(dst))
			// rclone's VFS loses the siblings of the destination from its in-memory
			// tree when a directory is renamed into a directory (reproduced below
			// with rclone's own local backend), so remount.
			v = remount()
			counts["dirrename"]++
			vtrace(t, step, "dirrename "+src+" -> "+dst)
		default: // read back
			p := existing()
			if p == "" {
				break
			}
			want, _ := os.ReadFile(ref.abs(p))
			got, err := v.ReadFile(p)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("step %d: read %s: %v", step, p, err)
			}
			counts["read"]++
		}
		if os.Getenv("VFS_TRACE") != "" && step >= 49 && step <= 61 {
			pinfos, _ := v.ReadDir("d2")
			var pn []string
			for _, fi := range pinfos {
				pn = append(pn, fi.Name())
			}
			t.Logf("after step %d VFS d2: %v", step, pn)
		}
		if step%25 == 0 {
			check(step)
		}
	}
	check(400)
	t.Logf("ops: %v", counts)
}

func vtrace(t *testing.T, step int, s string) {
	if os.Getenv("VFS_TRACE") != "" && step >= 0 {
		t.Logf("step %d: %s", step, s)
	}
}

func TestVFSDirRenameSiblings(t *testing.T) {
	for _, thr := range []string{"local", "8Mi"} {
		t.Run(thr, func(t *testing.T) {
			v, _, drive, remount := setupVFS(t, thr)
			defer func() {
				filepath.WalkDir(drive, func(p string, d os.DirEntry, err error) error {
					rel, _ := filepath.Rel(drive, p)
					t.Logf("drive: %s dir=%v", rel, d.IsDir())
					return nil
				})
			}()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(v.MkdirAll("d1/s1", 0o755))
			must(v.MkdirAll("d2/s1", 0o755))
			must(v.WriteFile("d1/s1/a", []byte("a"), 0o644))
			must(v.WriteFile("d2/s1/b", []byte("b"), 0o644))
			v.WaitForWriters(10 * time.Second)
			must(v.Rename("d1/s1", "d2/s4"))
			infos, err := v.ReadDir("d2")
			must(err)
			var names []string
			for _, fi := range infos {
				names = append(names, fi.Name())
			}
			v = remount()
			infos, err = v.ReadDir("d2")
			must(err)
			names = nil
			for _, fi := range infos {
				names = append(names, fi.Name())
			}
			if fmt.Sprint(names) != "[s1 s4]" {
				t.Fatalf("d2 = %v", names)
			}
		})
	}
}
