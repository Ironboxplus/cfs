package core_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

// pattern is deterministic pseudo-random content with random access, so huge
// files can be verified without keeping a reference copy.
type pattern struct {
	seed uint64
	pos  int64
	size int64
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (p *pattern) fill(b []byte, off int64) {
	for i := 0; i < len(b); {
		pos := off + int64(i)
		w := splitmix(p.seed ^ uint64(pos>>3))
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], w)
		n := copy(b[i:], tmp[pos&7:])
		i += n
	}
}

func (p *pattern) Read(b []byte) (int, error) {
	if p.pos >= p.size {
		return 0, io.EOF
	}
	if int64(len(b)) > p.size-p.pos {
		b = b[:p.size-p.pos]
	}
	p.fill(b, p.pos)
	p.pos += int64(len(b))
	return len(b), nil
}

func (p *pattern) sum() string {
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for off := int64(0); off < p.size; off += int64(len(buf)) {
		n := min(int64(len(buf)), p.size-off)
		p.fill(buf[:n], off)
		h.Write(buf[:n])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func hashStream(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// bigVolume opens a volume on st with the default 8 MiB threshold.
func bigVolume(t *testing.T, st store.Store, transfers int) (*core.Volume, *aspect.Meter) {
	s, m := testutil.Stacked(st, aspect.Options{Gate: aspect.GateOpts{MetaInflight: 1, Transfers: transfers}})
	cfg := testutil.FastConfig(t.TempDir())
	v, err := core.Open(context.Background(), s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close(context.Background()) })
	return v, m
}

func testBigFile(t *testing.T, size int64, mkStore func(t *testing.T, drive string) store.Store) {
	if testing.Short() {
		t.Skip("big files skipped in -short mode")
	}
	drive := t.TempDir()
	v, m := bigVolume(t, mkStore(t, drive), 8)
	ctx := context.Background()
	src := &pattern{seed: 0xC0FFEE + uint64(size), size: size}
	want := src.sum()
	t0 := time.Now()
	e, err := v.Put(ctx, "huge/file.bin", src, size, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	up := time.Since(t0)
	if e.Packed || e.Size != size {
		t.Fatalf("entry: %+v", e)
	}
	// 1:1 on the drive at its own path.
	if fi, err := os.Stat(filepath.Join(drive, "huge", "file.bin")); err != nil || fi.Size() != size {
		t.Fatalf("object on the drive: %v", err)
	}
	t0 = time.Now()
	rc, err := v.Open(ctx, "huge/file.bin", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, n, err := hashStream(rc)
	rc.Close()
	if err != nil || n != size || got != want {
		t.Fatalf("full read: n=%d err=%v hash ok=%v", n, err, got == want)
	}
	down := time.Since(t0)
	// Random ranges, including ones that straddle chunk boundaries.
	rng := rand.New(rand.NewPCG(1, 2))
	chunk := v.Config().ReadChunk
	offs := []int64{0, chunk - 7, chunk, 2*chunk + 11, size - 100, size - chunk - 5}
	for i := 0; i < 12; i++ {
		offs = append(offs, rng.Int64N(size-1))
	}
	ref := &pattern{seed: src.seed, size: size}
	for _, off := range offs {
		ln := int64(1 + rng.IntN(3<<20))
		if off+ln > size {
			ln = size - off
		}
		rc, err := v.Open(ctx, "huge/file.bin", off, ln)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		w := make([]byte, ln)
		ref.fill(w, off)
		if err != nil || !bytes.Equal(b, w) {
			t.Fatalf("range %d+%d differs: %v", off, ln, err)
		}
	}
	s := m.Snapshot()
	t.Logf("%d MiB: upload %v (%.0f MiB/s), full read %v (%.0f MiB/s)", size>>20, up, float64(size>>20)/up.Seconds(), down, float64(size>>20)/down.Seconds())
	t.Logf("underlying: %s", s)
	if s.PeakMeta > 1 || s.PeakTransfer > 8 {
		t.Errorf("in-flight peaks exceed limits: %s", s)
	}
	// Sequential ranged reads of ReadChunk: the number of opens is what the chunk size says.
	if min := (size + chunk - 1) / chunk; s.Calls["open"] < min {
		t.Errorf("a %d byte file read in %d byte chunks needs at least %d range requests, saw %d", size, chunk, min, s.Calls["open"])
	}
}

func localStoreFn(t *testing.T, drive string) store.Store { return testutil.LocalStore(t, drive) }

func davStoreFn(t *testing.T, drive string) store.Store {
	url, _ := testutil.WebDAVServer(t, drive)
	return testutil.WebDAVStore(t, url)
}

func TestBigFile1GiBLocal(t *testing.T)  { testBigFile(t, 1<<30, localStoreFn) }
func TestBigFile4GiBLocal(t *testing.T)  { testBigFile(t, 4<<30, localStoreFn) }
func TestBigFile1GiBWebDAV(t *testing.T) { testBigFile(t, 1<<30, davStoreFn) }
func TestBigFile4GiBWebDAV(t *testing.T) { testBigFile(t, 4<<30, davStoreFn) }

// Files of exactly the threshold and one byte either side, and a file that
// grows across it and shrinks back.
func TestThresholdEdges(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, _ := bigVolume(t, testutil.LocalStore(t, drive), 8)
	const T = 8 << 20
	loose := func(p string) bool {
		_, err := os.Stat(filepath.Join(drive, filepath.FromSlash(p)))
		return err == nil
	}
	put := func(p string, size int) []byte {
		b := randBytes(size)
		if _, err := v.Put(ctx, p, bytes.NewReader(b), int64(size), time.Now()); err != nil {
			t.Fatalf("put %s (%d): %v", p, size, err)
		}
		return b
	}
	check := func(p string, want []byte, wantLarge bool) {
		t.Helper()
		got, err := readAllVol(v, p)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: content wrong (%v)", p, err)
		}
		e, _ := v.Stat(ctx, p)
		if e.Packed == wantLarge {
			t.Fatalf("%s (%d bytes): packed=%v, want large=%v", p, len(want), e.Packed, wantLarge)
		}
		if loose(p) != wantLarge {
			t.Fatalf("%s (%d bytes): stored loose on the drive = %v, want %v", p, len(want), loose(p), wantLarge)
		}
	}
	for _, c := range []struct {
		size  int
		large bool
	}{{T - 1, false}, {T, true}, {T + 1, true}, {0, false}, {1, false}} {
		p := fmt.Sprintf("edge/f%d", c.size)
		b := put(p, c.size)
		check(p, b, c.large)
	}
	// Grow step by step across the threshold, then shrink back.
	p := "grow/file"
	for _, s := range []struct {
		size  int
		large bool
	}{{1000, false}, {T - 1, false}, {T, true}, {T + 1, true}, {T - 1, true}, {T/2 + 1, true}, {T/2 - 1, false}, {10, false}, {T + 5, true}} {
		b := put(p, s.size)
		check(p, b, s.large)
	}
	rep, err := v.Fsck(ctx, core.FsckOpts{Deep: true})
	if err != nil || !rep.OK() {
		t.Fatalf("fsck: %v %v", err, rep.Problems)
	}
}
