package core_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/store"
)

// A model test applies random operations (create, overwrite, append, truncate,
// delete, rename, directory move, read) to a cfs volume and to a plain local
// directory, and compares them after every step. Thresholds are tiny so that
// files cross it all the time, and the volume is regularly reopened,
// checkpointed, compacted and rebuilt from the drive.

type refFS struct{ root string }

func (r refFS) abs(p string) string { return filepath.Join(r.root, filepath.FromSlash(p)) }

func (r refFS) files() (map[string][]byte, map[string]bool) {
	files, dirs := map[string][]byte{}, map[string]bool{}
	filepath.WalkDir(r.root, func(p string, d os.DirEntry, err error) error {
		rel, _ := filepath.Rel(r.root, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			dirs[rel] = true
		} else {
			b, _ := os.ReadFile(p)
			files[rel] = b
		}
		return nil
	})
	return files, dirs
}

func volFiles(t *testing.T, v *core.Volume) (map[string][]byte, map[string]bool) {
	files, dirs := map[string][]byte{}, map[string]bool{}
	var rec func(dir string)
	rec = func(dir string) {
		ents, err := v.List(context.Background(), dir)
		if err != nil {
			t.Fatalf("list %q: %v", dir, err)
		}
		for _, e := range ents {
			if e.IsDir {
				dirs[e.Path] = true
				rec(e.Path)
			} else {
				b, err := readAllVol(v, e.Path)
				if err != nil {
					t.Fatalf("read %s: %v", e.Path, err)
				}
				if int64(len(b)) != e.Size {
					t.Fatalf("%s: listing says %d bytes, read %d", e.Path, e.Size, len(b))
				}
				files[e.Path] = b
			}
		}
	}
	rec("")
	return files, dirs
}

func compareAll(t *testing.T, step int, v *core.Volume, ref refFS) {
	t.Helper()
	rf, rd := ref.files()
	vf, vd := volFiles(t, v)
	for p, b := range rf {
		g, ok := vf[p]
		if !ok {
			t.Fatalf("step %d: %s missing from the volume", step, p)
		}
		if !bytes.Equal(g, b) {
			t.Fatalf("step %d: %s differs (volume %d bytes, reference %d)", step, p, len(g), len(b))
		}
	}
	for p := range vf {
		if _, ok := rf[p]; !ok {
			t.Fatalf("step %d: unexpected file %s in the volume", step, p)
		}
	}
	for p := range rd {
		if !vd[p] {
			t.Fatalf("step %d: directory %s missing from the volume", step, p)
		}
	}
	for p := range vd {
		if !rd[p] {
			t.Fatalf("step %d: unexpected directory %s in the volume", step, p)
		}
	}
}

func TestModel(t *testing.T) {
	// Long runs only on request: CFS_MODEL_OPS=30000.
	n := 300
	if testing.Short() {
		n = 100
	}
	if s := os.Getenv("CFS_MODEL_OPS"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	seed := uint64(1)
	if s := os.Getenv("CFS_MODEL_SEED"); s != "" {
		x, _ := strconv.Atoi(s)
		seed = uint64(x)
	}
	e := newEnv(t, aspectOpts(), func(c *core.Config) {
		c.Threshold = 4096
		c.SealQuiet = 5 * time.Millisecond
		c.SealMax = 100 * time.Millisecond
		c.GCGrace = 50 * time.Millisecond
		c.CheckpointEvery = 25
	})
	ref := refFS{t.TempDir()}
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, 77))
	slot := func() string { return fmt.Sprintf("d%d/s%d/f%d", rng.IntN(4), rng.IntN(8), rng.IntN(5)) }
	size := func() int {
		switch rng.IntN(6) {
		case 0:
			return 0
		case 1:
			return 1 + rng.IntN(100)
		case 2:
			return 3000 + rng.IntN(1500) // straddles the threshold
		case 3:
			return 4095 + rng.IntN(3)
		case 4:
			return 4096 + rng.IntN(9000)
		}
		return 100 + rng.IntN(3000)
	}
	existing := func() string {
		f, _ := ref.files()
		if len(f) == 0 {
			return ""
		}
		names := make([]string, 0, len(f))
		for p := range f {
			names = append(names, p)
		}
		sort.Strings(names)
		return names[rng.IntN(len(names))]
	}
	counts := map[string]int{}
	put := func(p string, data []byte) {
		if _, err := e.v.Put(ctx, p, bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
			t.Fatalf("put %s: %v", p, err)
		}
		os.MkdirAll(filepath.Dir(ref.abs(p)), 0o755)
		if err := os.WriteFile(ref.abs(p), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for step := 1; step <= n; step++ {
		var touched []string
		switch r := rng.IntN(100); {
		case r < 25: // create or overwrite
			p := slot()
			put(p, randBytes(size()))
			touched = []string{p}
			counts["put"]++
		case r < 35: // append
			if p := existing(); p != "" {
				old, _ := os.ReadFile(ref.abs(p))
				put(p, append(old, randBytes(1+rng.IntN(5000))...))
				touched = []string{p}
				counts["append"]++
			}
		case r < 42: // truncate
			if p := existing(); p != "" {
				old, _ := os.ReadFile(ref.abs(p))
				put(p, old[:rng.IntN(len(old)+1)])
				touched = []string{p}
				counts["truncate"]++
			}
		case r < 55: // delete
			if p := existing(); p != "" {
				if err := e.v.Remove(ctx, p); err != nil {
					t.Fatalf("step %d: remove %s: %v", step, p, err)
				}
				os.Remove(ref.abs(p))
				touched = []string{p}
				counts["remove"]++
			}
		case r < 72: // rename a file
			if p := existing(); p != "" {
				to := slot()
				if to == p {
					break
				}
				if err := e.v.Move(ctx, p, to); err != nil {
					t.Fatalf("step %d: move %s -> %s: %v", step, p, to, err)
				}
				os.MkdirAll(filepath.Dir(ref.abs(to)), 0o755)
				if err := os.Rename(ref.abs(p), ref.abs(to)); err != nil {
					t.Fatal(err)
				}
				touched = []string{p, to}
				counts["rename"]++
			}
		case r < 80: // move a directory
			src := fmt.Sprintf("d%d/%c%d", rng.IntN(4), "st"[rng.IntN(2)], rng.IntN(8))
			dst := fmt.Sprintf("d%d/%c%d", rng.IntN(4), "st"[rng.IntN(2)], rng.IntN(8))
			if _, err := os.Stat(ref.abs(src)); err != nil || src == dst {
				break
			}
			if _, err := os.Stat(ref.abs(dst)); err == nil {
				break
			}
			if err := e.v.DirMove(ctx, src, dst); err != nil {
				t.Fatalf("step %d: dirmove %s -> %s: %v", step, src, dst, err)
			}
			os.MkdirAll(filepath.Dir(ref.abs(dst)), 0o755)
			if err := os.Rename(ref.abs(src), ref.abs(dst)); err != nil {
				t.Fatal(err)
			}
			counts["dirmove"]++
			touched = nil
			compareAll(t, step, e.v, ref)
		case r < 84: // remove an empty directory
			d := fmt.Sprintf("d%d/%c%d", rng.IntN(4), "se"[rng.IntN(2)], rng.IntN(8))
			if _, err := os.Stat(ref.abs(d)); err != nil {
				break
			}
			rerr := os.Remove(ref.abs(d)) // fails when not empty
			verr := e.v.Rmdir(ctx, d)
			if (rerr == nil) != (verr == nil) {
				t.Fatalf("step %d: rmdir %s: reference err=%v, volume err=%v", step, d, rerr, verr)
			}
			if rerr == nil {
				counts["rmdir"]++
			}
		case r < 88: // make an empty directory, sometimes with an empty child
			d := fmt.Sprintf("d%d/e%d", rng.IntN(4), rng.IntN(8))
			if rng.IntN(3) == 0 {
				d += fmt.Sprintf("/x%d", rng.IntN(3))
			}
			if err := e.v.Mkdir(ctx, d); err != nil {
				t.Fatalf("step %d: mkdir %s: %v", step, d, err)
			}
			if err := os.MkdirAll(ref.abs(d), 0o755); err != nil {
				t.Fatal(err)
			}
			counts["mkdir"]++
		default: // read
			if p := existing(); p != "" {
				got, err := readAllVol(e.v, p)
				want, _ := os.ReadFile(ref.abs(p))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("step %d: read %s: %v", step, p, err)
				}
				if len(want) > 10 {
					off := rng.IntN(len(want) - 5)
					rc, err := e.v.Open(ctx, p, int64(off), 5)
					if err != nil {
						t.Fatal(err)
					}
					part := make([]byte, 5)
					k, _ := rc.Read(part)
					rc.Close()
					if !bytes.Equal(part[:k], want[off:off+k]) {
						t.Fatalf("step %d: range read of %s differs", step, p)
					}
				}
				counts["read"]++
			}
		}
		for _, p := range touched {
			want, werr := os.ReadFile(ref.abs(p))
			got, gerr := readAllVol(e.v, p)
			switch {
			case werr != nil && gerr == nil:
				t.Fatalf("step %d: %s should not exist", step, p)
			case werr != nil && !errors.Is(gerr, store.ErrNotFound):
				t.Fatalf("step %d: %s: %v", step, p, gerr)
			case werr == nil && (gerr != nil || !bytes.Equal(got, want)):
				t.Fatalf("step %d: %s differs: %v", step, p, gerr)
			}
		}
		if step%100 == 0 {
			compareAll(t, step, e.v, ref)
		}
		switch {
		case step%250 == 0: // reopen with the local state intact
			e.reopen()
			compareAll(t, step, e.v, ref)
		case step%400 == 0: // lose the local state
			if err := e.v.Close(ctx); err != nil {
				t.Fatal(err)
			}
			e.v = nil
			removeAll(t, e.state)
			e.newState()
			e.open()
			compareAll(t, step, e.v, ref)
			counts["rebuild"]++
		case step%300 == 0: // compaction, checkpoint, GC
			cr, gr, err := e.v.Maintain(ctx)
			if err != nil {
				t.Fatalf("step %d: maintain: %v", step, err)
			}
			time.Sleep(80 * time.Millisecond)
			if _, err := e.v.GC(ctx); err != nil {
				t.Fatal(err)
			}
			compareAll(t, step, e.v, ref)
			counts["maintain"]++
			counts["compacted_packs"] += cr.PacksCompacted
			counts["packs_deleted"] += gr.PacksDeleted
		}
	}
	compareAll(t, n, e.v, ref)
	fsckClean(t, e.v, true)
	t.Logf("%d steps, op counts: %v", n, counts)
	e.report()
	e.assertPeaks(1, 8)
}

var _ = path.Join
var _ store.Entry
