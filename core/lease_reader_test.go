package core_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
)

// A volume is a reader until its first change: it takes the lease for that, and
// gives it back once it has been idle.

// driveBytes is every file of the drive directory, by relative path.
func driveBytes(t *testing.T, drive string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(drive, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(drive, p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sameDrive(t *testing.T, what string, before, after map[string]string) {
	t.Helper()
	for p, b := range before {
		a, ok := after[p]
		switch {
		case !ok:
			t.Errorf("%s: %s was removed from the drive", what, p)
		case a != b:
			t.Errorf("%s: %s changed on the drive", what, p)
		}
	}
	for p := range after {
		if _, ok := before[p]; !ok {
			t.Errorf("%s: %s appeared on the drive", what, p)
		}
	}
}

type leaseFile struct {
	Writer   string
	Epoch    uint64
	Released bool
}

// readLeaseFile is the lease on the drive: the last file of the lease directory.
func readLeaseFile(t *testing.T, drive string) (leaseFile, bool) {
	t.Helper()
	files := leaseFiles(t, drive)
	if len(files) == 0 {
		return leaseFile{}, false
	}
	n := parseLeaseFile(t, files[len(files)-1])
	return leaseFile{Writer: n.writer, Epoch: n.epoch, Released: n.state == "released"}, true
}

// waitFor polls until cond holds.
func waitFor(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("after %v: %s", within, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func bigTweak(c *core.Config) { c.Threshold = 64 << 10 }

// writeLibrary writes a small (packed) and a big (stored 1:1) file.
func writeLibrary(t *testing.T, v *core.Volume) (small, big []byte) {
	t.Helper()
	ctx := context.Background()
	small = []byte("a packed file")
	big = make([]byte, 300<<10)
	rand.New(rand.NewSource(7)).Read(big)
	if _, err := v.Put(ctx, "lib/small.txt", bytes.NewReader(small), int64(len(small)), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Put(ctx, "lib/big.bin", bytes.NewReader(big), int64(len(big)), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	return small, big
}

func readsLibrary(t *testing.T, v *core.Volume, small, big []byte) {
	t.Helper()
	ctx := context.Background()
	ents, err := v.List(ctx, "lib")
	if err != nil || len(ents) < 2 {
		t.Fatalf("list: %v %v", ents, err)
	}
	if got, err := readAllVol(v, "lib/small.txt"); err != nil || !bytes.Equal(got, small) {
		t.Fatalf("the packed file: %v", err)
	}
	if got, err := readAllVol(v, "lib/big.bin"); err != nil || !bytes.Equal(got, big) {
		t.Fatalf("the big file: %v", err)
	}
}

// Opening, listing and reading a volume (a packed and a big file) writes nothing
// to the drive: the lease file is the same bytes, and no segment, checkpoint or
// pack appears. Waiting longer than a heartbeat makes no difference.
func TestLeaseOnlyReadingWritesNothing(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	w, err := openAt(t, drive, t.TempDir(), bigTweak)
	if err != nil {
		t.Fatal(err)
	}
	small, big := writeLibrary(t, w)
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before := driveBytes(t, drive)

	r, err := openAt(t, drive, t.TempDir(), bigTweak)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ReadOnly() {
		t.Fatal("a volume that has made no change holds the lease")
	}
	readsLibrary(t, r, small, big)
	time.Sleep(1500 * time.Millisecond) // a heartbeat would be due (TTL 3s, every third)
	sameDrive(t, "open and read", before, driveBytes(t, drive))
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	sameDrive(t, "after Close", before, driveBytes(t, drive))
}

// The first change takes the lease and the data lands; once the volume has been
// idle (nothing left to upload) it writes a released lease and the heartbeat
// stops; the next change takes the lease again.
func TestLeaseFirstChangeTakesItAndIdleHandsItBack(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, err := openAt(t, drive, t.TempDir(), func(c *core.Config) { c.LeaseIdle = time.Second })
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close(ctx)
	if _, ok := readLeaseFile(t, drive); ok {
		t.Fatal("opening wrote a lease")
	}
	if _, err := v.Put(ctx, "a.txt", strings.NewReader("one"), 3, time.Now()); err != nil {
		t.Fatal(err)
	}
	l, ok := readLeaseFile(t, drive)
	if !ok || l.Released || l.Epoch == 0 {
		t.Fatalf("after the first change the lease is %+v (present %v)", l, ok)
	}
	first := l.Epoch

	waitFor(t, "the idle volume hands the lease back", 10*time.Second, func() bool {
		l, _ := readLeaseFile(t, drive)
		return l.Released
	})
	if !v.ReadOnly() {
		t.Error("the volume still thinks it is the writer")
	}
	handed := driveBytes(t, drive)
	time.Sleep(2500 * time.Millisecond) // two heartbeats would be due
	sameDrive(t, "after the hand-back", handed, driveBytes(t, drive))
	if got, err := readAllVol(v, "a.txt"); err != nil || string(got) != "one" {
		t.Fatalf("the file after the hand-back: %q %v", got, err)
	}

	if _, err := v.Put(ctx, "b.txt", strings.NewReader("two"), 3, time.Now()); err != nil {
		t.Fatal(err)
	}
	l, _ = readLeaseFile(t, drive)
	if l.Released || l.Epoch < first {
		t.Fatalf("the next change did not take the lease again: %+v", l)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

// While A writes, B opens and reads but its changes are refused, and B stays a
// reader. After A hands the lease back, B's change succeeds, and A, a reader
// again, comes to see what B wrote.
func TestLeaseOtherWriterIsRefusedUntilItHandsBack(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	a, err := openAt(t, drive, t.TempDir(), func(c *core.Config) { c.LeaseIdle = 2 * time.Second; c.DirCacheTTL = 100 * time.Millisecond })
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)
	if _, err := a.Put(ctx, "a.txt", strings.NewReader("from a"), 6, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	b, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(ctx)
	if got, err := readAllVol(b, "a.txt"); err != nil || string(got) != "from a" {
		t.Fatalf("B reading while A writes: %q %v", got, err)
	}
	if _, err := b.Put(ctx, "b.txt", strings.NewReader("no"), 2, time.Now()); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("B's put while A holds the lease: %v", err)
	}
	if err := b.Mkdir(ctx, "bdir"); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("B's mkdir while A holds the lease: %v", err)
	}
	if err := b.Remove(ctx, "a.txt"); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("B's remove while A holds the lease: %v", err)
	}
	if !b.ReadOnly() {
		t.Fatal("B is not a reader after its refused changes")
	}
	if l, _ := readLeaseFile(t, drive); l.Released {
		t.Fatalf("the refused changes disturbed A's lease: %+v", l)
	}

	waitFor(t, "A hands the lease back", 10*time.Second, func() bool {
		l, _ := readLeaseFile(t, drive)
		return l.Released
	})
	if _, err := b.Put(ctx, "b.txt", strings.NewReader("from b"), 6, time.Now()); err != nil {
		t.Fatalf("B's put after A handed the lease back: %v", err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "A, a reader again, sees B's file", 10*time.Second, func() bool {
		got, err := readAllVol(a, "b.txt")
		return err == nil && string(got) == "from b"
	})
	// and A's own change is refused in its turn
	if _, err := a.Put(ctx, "a2.txt", strings.NewReader("no"), 2, time.Now()); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("A's put while B holds the lease: %v", err)
	}
}

// Staged files left by an earlier run: the volume opens as a writer, uploads
// them, and once idle hands the lease back.
func TestLeaseStagedLeftoversAreUploadedAtOpenThenHandedBack(t *testing.T) {
	if os.Getenv("CFS_TEST_ACK") == "durable" {
		t.Skip("nothing is staged in durable mode")
	}
	ctx := context.Background()
	drive := t.TempDir()
	state := t.TempDir()
	lc := &leaseCounter{Store: testutil.LocalStore(t, drive)}
	st, _ := testutil.Stacked(lc, aspect.Options{Retry: aspect.RetryOpts{Attempts: 2}})
	cfg := testutil.FastConfig(state)
	cfg.SealQuiet, cfg.SealMax = time.Hour, time.Hour // the file stays staged
	v, err := core.Open(ctx, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("acknowledged, not yet on the drive")
	if _, err := v.Put(ctx, "left/f.txt", bytes.NewReader(want), int64(len(want)), time.Now()); err != nil {
		t.Fatal(err)
	}
	lc.cut.Store(true) // the drive goes away: Close cannot upload it
	_ = v.Close(ctx)
	lc.cut.Store(false)

	cfg2 := testutil.FastConfig(state)
	cfg2.LeaseIdle = 300 * time.Millisecond
	st2, _ := testutil.Stacked(testutil.LocalStore(t, drive), aspectOpts())
	v2, err := core.Open(ctx, st2, cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close(ctx)
	if got, err := readAllVol(v2, "left/f.txt"); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("the leftover after the restart: %v", err)
	}
	waitFor(t, "the leftover is uploaded", 10*time.Second, func() bool { n, _ := v2.Staged(); return n == 0 })
	waitFor(t, "the lease is handed back once it is done", 10*time.Second, func() bool {
		l, _ := readLeaseFile(t, drive)
		return l.Released && v2.ReadOnly()
	})
	// a volume that opens later, from nothing, finds the file on the drive
	v3, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer v3.Close(ctx)
	if got, err := readAllVol(v3, "left/f.txt"); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("the leftover on the drive: %v", err)
	}
}

// Writers that keep pausing for about as long as the idle time: the lease goes
// back and forth, and everything acknowledged is on the drive at the end.
func TestLeaseHandBackRacingWithWritesLosesNothing(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	var handbacks, takes atomic.Int64
	v, err := openAt(t, drive, t.TempDir(), func(c *core.Config) {
		c.LeaseIdle = 25 * time.Millisecond
		c.Logf = func(f string, a ...any) {
			switch msg := fmt.Sprintf(f, a...); {
			case strings.Contains(msg, "lease handed back"):
				handbacks.Add(1)
			case strings.Contains(msg, "took the writer lease"):
				takes.Add(1)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	acked := map[string][]byte{}
	var wg sync.WaitGroup
	deadline := time.Now().Add(5 * time.Second)
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; time.Now().Before(deadline); i++ {
				p := fmt.Sprintf("g%d/f%d.txt", g, i)
				data := make([]byte, 20+rng.Intn(300))
				rng.Read(data)
				if _, err := v.Put(ctx, p, bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
					t.Errorf("put %s: %v", p, err)
					return
				}
				mu.Lock()
				acked[p] = data
				mu.Unlock()
				time.Sleep(time.Duration(10+rng.Intn(80)) * time.Millisecond)
			}
		}(g)
	}
	wg.Wait()
	if err := v.Close(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d files, %d hand-backs, %d takes", len(acked), handbacks.Load(), takes.Load())
	if handbacks.Load() < 1 {
		t.Errorf("only %d hand-backs: the race was not exercised", handbacks.Load())
	}
	check, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close(ctx)
	for p, want := range acked {
		if got, err := readAllVol(check, p); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s acknowledged but not on the drive: %v", p, err)
		}
	}
}

// Many first changes at once take the lease once, and all of them succeed.
func TestLeaseConcurrentFirstChangesTakeItOnce(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	var takes atomic.Int64
	v, err := openAt(t, drive, t.TempDir(), func(c *core.Config) {
		c.Logf = func(f string, a ...any) {
			if strings.Contains(fmt.Sprintf(f, a...), "took the writer lease") {
				takes.Add(1)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := v.Put(ctx, fmt.Sprintf("c/f%d", i), strings.NewReader("x"), 1, time.Now()); err != nil {
				t.Errorf("put %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if n := takes.Load(); n != 1 {
		t.Errorf("the lease was taken %d times", n)
	}
	if l, _ := readLeaseFile(t, drive); l.Released || l.Epoch == 0 {
		t.Errorf("lease on the drive: %+v", l)
	}
}
