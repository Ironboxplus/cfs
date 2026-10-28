package core_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

// A volume that is closed leaves its lease released, even when a heartbeat is
// in the middle of writing the lease at that moment. The heartbeat's write is
// slow here (a drive that takes its time) and the release is quick, so the
// heartbeat's write reaches the drive after the release's unless Close sees to
// the order; the next writer would then find a live lease and open read-only
// (bb-dataset: "volume held by writer ... opening read-only").
func TestLeaseReleasedByCloseDespiteAHeartbeatInFlight(t *testing.T) {
	for i := 0; i < 6; i++ {
		drive := t.TempDir()
		pr := &probe{Store: testutil.LocalStore(t, drive)}
		pr.putHook = func(path string, src store.Source) time.Duration {
			if path != core.CtlDir+"/lease.json" {
				return 0
			}
			rc, err := src.Open()
			if err != nil {
				return 0
			}
			b, _ := io.ReadAll(rc)
			rc.Close()
			if bytes.Contains(b, []byte(`"released":true`)) {
				return time.Millisecond
			}
			return 60 * time.Millisecond // a heartbeat
		}
		st, _ := testutil.Stacked(pr, aspect.Options{})
		cfg := testutil.FastConfig(t.TempDir())
		cfg.LeaseTTL = 90 * time.Millisecond // a heartbeat every 30 ms, each taking 60 ms to land
		v, err := core.Open(context.Background(), st, cfg)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(100+i*13) * time.Millisecond)
		if err := v.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(drive, ".cfs", "lease.json"))
		if err != nil {
			t.Fatal(err)
		}
		var l struct {
			Released bool `json:"released"`
		}
		if err := json.Unmarshal(b, &l); err != nil || !l.Released {
			t.Fatalf("round %d: after Close the lease on the drive is %s", i, b)
		}
	}
}
