package core_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

// leaseCounter counts the reads and writes of lease.json reaching the drive, and
// can cut the connection: while cut, every call fails like a dead network.
type leaseCounter struct {
	store.Store
	reads, writes atomic.Int64
	cut           atomic.Bool
}

var errNetDown = errors.New("network down")

func (l *leaseCounter) Put(ctx context.Context, p string, src store.Source, o store.PutOpts) error {
	if l.cut.Load() {
		return errNetDown
	}
	if p == core.CtlDir+"/lease.json" {
		l.writes.Add(1)
	}
	return l.Store.Put(ctx, p, src, o)
}

func (l *leaseCounter) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	if l.cut.Load() {
		return nil, errNetDown
	}
	if p == core.CtlDir+"/lease.json" {
		l.reads.Add(1)
	}
	return l.Store.Open(ctx, p, off, n)
}

func (l *leaseCounter) List(ctx context.Context, dir string) ([]store.Entry, error) {
	if l.cut.Load() {
		return nil, errNetDown
	}
	return l.Store.List(ctx, dir)
}

func (l *leaseCounter) Stat(ctx context.Context, p string) (store.Entry, error) {
	if l.cut.Load() {
		return store.Entry{}, errNetDown
	}
	return l.Store.Stat(ctx, p)
}

func openCounted(t *testing.T, drive string, tweak func(*core.Config)) (*core.Volume, *leaseCounter, error) {
	t.Helper()
	lc := &leaseCounter{Store: testutil.LocalStore(t, drive)}
	st, _ := testutil.Stacked(lc, aspectOpts())
	cfg := testutil.FastConfig(t.TempDir())
	if tweak != nil {
		tweak(&cfg)
	}
	v, err := core.Open(context.Background(), st, cfg)
	return v, lc, err
}

// Committing continuously for a minute costs one lease write per heartbeat
// (TTL/3) and a few reads: commits within TTL/6 of the last lease access don't
// read it again.
func TestLeaseHeartbeatCost(t *testing.T) {
	if os.Getenv("CFS_LONG") == "" || testing.Short() {
		t.Skip("a minute of mostly sleeping: set CFS_LONG=1")
	}
	ctx := context.Background()
	v, lc, err := openCounted(t, t.TempDir(), func(c *core.Config) { c.LeaseTTL = 6 * time.Second })
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close(ctx)
	start := time.Now()
	commits := 0
	for time.Since(start) < 60*time.Second {
		p := fmt.Sprintf("d/f%d.txt", commits)
		if _, err := v.Put(ctx, p, strings.NewReader("x"), 1, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		commits++
		time.Sleep(300 * time.Millisecond)
	}
	w, r := lc.writes.Load(), lc.reads.Load()
	t.Logf("60s, %d commits, TTL 6s: lease writes=%d reads=%d", commits, w, r)
	if w < 28 || w > 33 { // 60/2 heartbeats + the one at open
		t.Fatalf("lease writes %d, want about 31", w)
	}
	if r > w+60 {
		t.Fatalf("lease reads %d > writes %d + 60", r, w)
	}
}

// With the default TTL (30 minutes) the lease is written once at open and not
// again for a long while, however many commits there are.
func TestLeaseDefaultTTLWritesOnce(t *testing.T) {
	ctx := context.Background()
	v, lc, err := openCounted(t, t.TempDir(), func(c *core.Config) { c.LeaseTTL = 0 })
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close(ctx)
	if got := v.Config().LeaseTTL; got != 30*time.Minute {
		t.Fatalf("default LeaseTTL %v, want 30m", got)
	}
	start := time.Now()
	for i := 0; time.Since(start) < 5*time.Second; i++ {
		if _, err := v.Put(ctx, fmt.Sprintf("f%d", i), strings.NewReader("x"), 1, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	w, r := lc.writes.Load(), lc.reads.Load()
	t.Logf("5s of commits, default TTL: lease writes=%d reads=%d", w, r)
	if w != 1 {
		t.Fatalf("lease writes %d, want 1 (the open)", w)
	}
}

// A writer cut off from the drive for longer than the TTL is replaced; when the
// network is back its next commit is fenced and writes no segment.
func TestSilentWriterIsFencedAfterTakeover(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	a, lc, err := openCounted(t, drive, func(c *core.Config) { c.LeaseTTL = 3 * time.Second })
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)
	if _, err := a.Put(ctx, "before.txt", strings.NewReader("one"), 3, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	segs := func() map[string]bool {
		m := map[string]bool{}
		ents, _ := os.ReadDir(filepath.Join(drive, ".cfs", "log"))
		for _, e := range ents {
			m[e.Name()] = true
		}
		return m
	}
	before := segs()

	lc.cut.Store(true)
	time.Sleep(3500 * time.Millisecond) // longer than the TTL: the heartbeat can't run
	b, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(ctx)
	if b.ReadOnly() || b.Epoch() <= a.Epoch() {
		t.Fatalf("takeover: readonly=%v epoch %d (silent writer had %d)", b.ReadOnly(), b.Epoch(), a.Epoch())
	}
	lc.cut.Store(false)

	_, err = a.Put(ctx, "after.txt", strings.NewReader("two"), 3, time.Now())
	if err == nil {
		err = a.Flush(ctx)
	}
	if !errors.Is(err, core.ErrFenced) {
		t.Fatalf("silent writer's next commit: %v, want ErrFenced", err)
	}
	prefix := fmt.Sprintf("%06d-", a.Epoch())
	for name := range segs() {
		if strings.HasPrefix(name, prefix) && !before[name] {
			t.Fatalf("fenced writer wrote segment %s", name)
		}
	}
}
