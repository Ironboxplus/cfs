package core_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

// packGate holds the first pack upload until released.
type packGate struct {
	store.Store
	first   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *packGate) Put(ctx context.Context, p string, src store.Source, o store.PutOpts) error {
	if strings.HasPrefix(p, core.CtlDir+"/packs/") {
		if g.first.CompareAndSwap(false, true) { // not sync.Once: it would hold the other packs back too
			close(g.entered)
			select {
			case <-g.release:
			case <-ctx.Done():
			}
		}
	}
	return g.Store.Put(ctx, p, src, o)
}

// Two versions of one name must reach the log, and the index, in the order they
// were written, however the commits of their packs are scheduled. The older
// version sits in a pack that is sealed and still uploading; the newer one
// arrives now. It must not get into a pack of its own that can commit first,
// or the older version would be applied over it.
func TestStagedNewerVersionNeverCommitsBeforeOlderOne(t *testing.T) {
	ctx := context.Background()
	g := &packGate{Store: testutil.LocalStore(t, t.TempDir()), entered: make(chan struct{}), release: make(chan struct{})}
	st, _ := testutil.Stacked(g, aspectOpts())
	v, err := core.Open(ctx, st, testutil.FastConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	var released sync.Once
	releaseGate := func() { released.Do(func() { close(g.release) }) }
	defer v.Close(ctx)
	defer releaseGate()

	if _, err := v.PackAdd(ctx, "d/f.txt", []byte("old"), time.Now()); err != nil {
		t.Fatal(err)
	}
	v.SealCur()
	select {
	case <-g.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the first pack was never uploaded")
	}
	newer := []byte("the newer and longer version")
	done := make(chan error, 1)
	go func() {
		wait, err := v.PackAdd(ctx, "d/f.txt", newer, time.Now().Add(time.Second))
		if err == nil {
			err = wait()
		}
		done <- err
	}()
	time.Sleep(500 * time.Millisecond) // long enough for a pack of its own to seal and commit
	releaseGate()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the newer version never committed")
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	rc, err := v.Open(ctx, "d/f.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(newer) {
		t.Fatalf("d/f.txt holds %q, want the newer version %q", got, newer)
	}
}
