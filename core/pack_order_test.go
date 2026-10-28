package core_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
)

// Two versions of one name must reach the log, and the index, in the order they
// were written. The commit of a sealed pack reserves its sequence number when
// its goroutine gets to run, so two packs sealed one after the other can
// reserve them in either order (the goroutine started last runs first); the
// index keeps the row with the higher sequence number. A newer version must
// therefore never get into a pack of its own while a pack with the older one is
// still in commit.
func TestStagedNewerVersionNeverCommitsBeforeOlderOne(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Stacked(testutil.LocalStore(t, t.TempDir()), aspectOpts())
	v, err := core.Open(ctx, st, testutil.FastConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close(ctx)

	const n = 300
	var waits []func() error
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("d%d/f.txt", i%7)
		if _, err := v.PackAdd(ctx, p, []byte("old"), time.Now()); err != nil {
			t.Fatal(err)
		}
		v.SealCur() // the older version is on its way to the drive...
		wait, err := v.PackAdd(ctx, p, []byte(fmt.Sprintf("newer version %d", i)), time.Now().Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		v.SealCur() // ...when the newer one is packed and sent
		waits = append(waits, wait)
		if i%7 == 6 { // one path per directory; settle before a path is used again
			for _, w := range waits {
				if err := w(); err != nil {
					t.Fatal(err)
				}
			}
			waits = nil
			for j := 0; j < 7; j++ {
				q := fmt.Sprintf("d%d/f.txt", j)
				rc, err := v.Open(ctx, q, 0, -1)
				if err != nil {
					t.Fatal(err)
				}
				got, _ := io.ReadAll(rc)
				rc.Close()
				want := fmt.Sprintf("newer version %d", i-6+j)
				if string(got) != want {
					t.Fatalf("round %d: %s holds %q, want the newer version %q", i, q, got, want)
				}
			}
		}
	}
}
