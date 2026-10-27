package core_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

type env struct {
	t     *testing.T
	drive string
	state string
	meter *aspect.Meter
	v     *core.Volume
	opts  aspect.Options
	tweak func(*core.Config)
}

func newEnv(t *testing.T, opts aspect.Options, tweak func(*core.Config)) *env {
	e := &env{t: t, drive: t.TempDir(), state: t.TempDir(), opts: opts, tweak: tweak}
	e.open()
	t.Cleanup(func() {
		if e.v != nil {
			e.v.Close(context.Background())
		}
	})
	return e
}

func (e *env) open() {
	e.t.Helper()
	base := testutil.LocalStore(e.t, e.drive)
	st, m := testutil.Stacked(base, e.opts)
	cfg := testutil.FastConfig(e.state)
	if e.tweak != nil {
		e.tweak(&cfg)
	}
	v, err := core.Open(context.Background(), st, cfg)
	if err != nil {
		e.t.Fatalf("open volume: %v", err)
	}
	e.v, e.meter = v, m
}

func (e *env) reopen() {
	e.t.Helper()
	if err := e.v.Close(context.Background()); err != nil {
		e.t.Fatalf("close: %v", err)
	}
	e.v = nil
	e.open()
}

func (e *env) put(p string, data []byte) core.Entry {
	e.t.Helper()
	ent, err := e.v.Put(context.Background(), p, bytes.NewReader(data), int64(len(data)), time.Now())
	if err != nil {
		e.t.Fatalf("put %s: %v", p, err)
	}
	return ent
}

func (e *env) read(p string) []byte {
	e.t.Helper()
	rc, err := e.v.Open(context.Background(), p, 0, -1)
	if err != nil {
		e.t.Fatalf("open %s: %v", p, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		e.t.Fatalf("read %s: %v", p, err)
	}
	return b
}

func (e *env) report() {
	s := e.meter.Snapshot()
	e.t.Logf("underlying: %s", s)
}

func (e *env) assertPeaks(meta, transfer int) {
	e.t.Helper()
	s := e.meter.Snapshot()
	if s.PeakMeta > meta {
		e.t.Errorf("meta in-flight peak %d > %d", s.PeakMeta, meta)
	}
	if s.PeakTransfer > transfer {
		e.t.Errorf("transfer in-flight peak %d > %d", s.PeakTransfer, transfer)
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func sum(b []byte) string { s := sha1.Sum(b); return hex.EncodeToString(s[:]) }

func (e *env) listNames(dir string) []string {
	e.t.Helper()
	ents, err := e.v.List(context.Background(), dir)
	if err != nil {
		e.t.Fatalf("list %q: %v", dir, err)
	}
	var out []string
	for _, x := range ents {
		n := x.Name()
		if x.IsDir {
			n += "/"
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func TestBasicSmallAndLarge(t *testing.T) {
	e := newEnv(t, aspect.Options{}, func(c *core.Config) { c.Threshold = 1 << 20 })
	small := randBytes(1000)
	big := randBytes(3 << 20)
	e.put("a/b/small.txt", small)
	e.put("a/b/big.bin", big)
	e.put("top.txt", []byte("hello"))
	e.put("empty", nil)

	if got := e.read("a/b/small.txt"); !bytes.Equal(got, small) {
		t.Fatal("small content differs")
	}
	if got := e.read("a/b/big.bin"); !bytes.Equal(got, big) {
		t.Fatal("big content differs")
	}
	if got := e.read("empty"); len(got) != 0 {
		t.Fatal("empty file not empty")
	}
	if n := e.listNames("a/b"); fmt.Sprint(n) != "[big.bin small.txt]" {
		t.Fatalf("list a/b = %v", n)
	}
	if n := e.listNames(""); fmt.Sprint(n) != "[a/ empty top.txt]" {
		t.Fatalf("list root = %v", n)
	}
	st, err := e.v.Stat(context.Background(), "a/b/small.txt")
	if err != nil || !st.Packed || st.Size != 1000 {
		t.Fatalf("stat small: %+v %v", st, err)
	}
	st, err = e.v.Stat(context.Background(), "a/b/big.bin")
	if err != nil || st.Packed || st.Size != 3<<20 {
		t.Fatalf("stat big: %+v %v", st, err)
	}
	// Small files are not on the drive at their own path; the big one is.
	if _, err := os.Stat(filepath.Join(e.drive, "a", "b", "small.txt")); err == nil {
		t.Fatal("small file stored loose")
	}
	if b, err := os.ReadFile(filepath.Join(e.drive, "a", "b", "big.bin")); err != nil || !bytes.Equal(b, big) {
		t.Fatalf("big file not 1:1 on the drive: %v", err)
	}
	// Range reads.
	rc, err := e.v.Open(context.Background(), "a/b/big.bin", 1<<20+5, 100)
	if err != nil {
		t.Fatal(err)
	}
	part, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(part, big[1<<20+5:1<<20+105]) {
		t.Fatal("range read differs")
	}
	e.report()
	e.assertPeaks(1, 8)
}

func TestMetadataOps(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspect.Options{}, func(c *core.Config) { c.Threshold = 1 << 20 })
	e.put("d/one.txt", []byte("one"))
	e.put("d/two.txt", []byte("two"))
	e.put("d/sub/three.txt", []byte("three"))
	e.put("d/sub/big.bin", randBytes(2<<20))

	if err := e.v.Move(ctx, "d/one.txt", "d/uno.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.v.Stat(ctx, "d/one.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("old name still there: %v", err)
	}
	if string(e.read("d/uno.txt")) != "one" {
		t.Fatal("moved content")
	}
	if err := e.v.Remove(ctx, "d/two.txt"); err != nil {
		t.Fatal(err)
	}
	if err := e.v.Remove(ctx, "d/two.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second remove: %v", err)
	}
	if err := e.v.DirMove(ctx, "d/sub", "d/moved"); err != nil {
		t.Fatal(err)
	}
	if string(e.read("d/moved/three.txt")) != "three" {
		t.Fatal("dirmove small")
	}
	if _, err := e.v.Stat(ctx, "d/moved/big.bin"); err != nil {
		t.Fatalf("dirmove big: %v", err)
	}
	if _, err := e.v.Stat(ctx, "d/sub/three.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("old dir still has files")
	}
	if err := e.v.Rmdir(ctx, "d"); !errors.Is(err, store.ErrNotEmpty) {
		t.Fatalf("rmdir non-empty: %v", err)
	}
	// Overwrite across the threshold, both ways.
	e.put("d/uno.txt", randBytes(2<<20))
	if st, _ := e.v.Stat(ctx, "d/uno.txt"); st.Packed || st.Size != 2<<20 {
		t.Fatalf("grown file: %+v", st)
	}
	e.put("d/moved/big.bin", []byte("tiny"))
	// Hysteresis: only 4 bytes, far below threshold*0.5, so it moves into a pack.
	if st, _ := e.v.Stat(ctx, "d/moved/big.bin"); !st.Packed {
		t.Fatalf("shrunk file should be packed: %+v", st)
	}
	if string(e.read("d/moved/big.bin")) != "tiny" {
		t.Fatal("shrunk content")
	}
	if _, err := os.Stat(filepath.Join(e.drive, "d", "moved", "big.bin")); err == nil {
		t.Fatal("old large version still on the drive")
	}
	e.report()
	e.assertPeaks(1, 8)
}

func TestSurvivesReopenAndRebuild(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspect.Options{}, func(c *core.Config) { c.Threshold = 1 << 20 })
	want := map[string][]byte{}
	for i := 0; i < 50; i++ {
		p := fmt.Sprintf("dir%d/f%d.dat", i%5, i)
		want[p] = randBytes(100 + i*37)
		e.put(p, want[p])
	}
	e.put("dir0/large.bin", randBytes(2<<20))
	e.v.Remove(ctx, "dir1/f1.dat")
	delete(want, "dir1/f1.dat")
	e.v.Move(ctx, "dir2/f2.dat", "dir3/moved.dat")
	want["dir3/moved.dat"] = want["dir2/f2.dat"]
	delete(want, "dir2/f2.dat")
	e.reopen()
	for p, d := range want {
		if !bytes.Equal(e.read(p), d) {
			t.Fatalf("%s differs after reopen", p)
		}
	}
	// Lose all local state: rebuild from the drive alone.
	if err := e.v.Close(ctx); err != nil {
		t.Fatal(err)
	}
	e.v = nil
	os.RemoveAll(e.state)
	e.state = t.TempDir()
	e.open()
	for p, d := range want {
		if !bytes.Equal(e.read(p), d) {
			t.Fatalf("%s differs after rebuild", p)
		}
	}
	if _, err := e.v.Stat(ctx, "dir1/f1.dat"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("removed file came back")
	}
	rep, err := e.v.Fsck(ctx, core.FsckOpts{Deep: true})
	if err != nil || !rep.OK() {
		t.Fatalf("fsck: %v %v", err, rep.Problems)
	}
	t.Logf("fsck: %d packs, %d members", rep.Packs, rep.Members)
}

func TestTransfersOverlap(t *testing.T) {
	// Many files written concurrently: packs and large files upload at the
	// same time, up to the transfers limit, metadata stays at 1.
	e := newEnv(t, aspect.Options{Gate: aspect.GateOpts{MetaInflight: 1, Transfers: 8}},
		func(c *core.Config) { c.Threshold = 64 << 10; c.PackMaxFiles = 20 })
	var wg sync.WaitGroup
	for i := 0; i < 160; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sz := 1000
			if i%10 == 0 {
				sz = 300 << 10
			}
			p := fmt.Sprintf("x/%d/f%d", i%7, i)
			if _, err := e.v.Put(context.Background(), p, bytes.NewReader(make([]byte, sz)), int64(sz), time.Now()); err != nil {
				t.Errorf("put %s: %v", p, err)
			}
		}()
	}
	wg.Wait()
	e.report()
	s := e.meter.Snapshot()
	if s.PeakTransfer <= 1 {
		t.Errorf("expected overlapping transfers, peak = %d", s.PeakTransfer)
	}
	e.assertPeaks(1, 8)
}
func aspectOpts() aspect.Options { return aspect.Options{} }

func TestListAfterDirMoveKeepsSiblings(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspect.Options{}, nil)
	e.put("d2/s1/f5", []byte("x"))
	e.put("d1/s1/f", []byte("y"))
	if n := e.listNames("d2"); fmt.Sprint(n) != "[s1/]" {
		t.Fatalf("before: %v", n)
	}
	if err := e.v.DirMove(ctx, "d1/s1", "d2/s4"); err != nil {
		t.Fatal(err)
	}
	if n := e.listNames("d2"); fmt.Sprint(n) != "[s1/ s4/]" {
		t.Fatalf("after dirmove: %v", n)
	}
}
