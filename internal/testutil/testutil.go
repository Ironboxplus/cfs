// Package testutil builds real stores for tests: a local directory, and a
// WebDAV server (rclone serve webdav in a subprocess) with a TCP fault proxy
// in front of it. There are no fakes: every store here is a real drive.
package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/store"
	"github.com/Ironboxplus/cfs/store/rclonestore"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
)

// LocalStore returns a store on a real local directory.
func LocalStore(t testing.TB, dir string) store.Store {
	t.Helper()
	f, err := fs.NewFs(context.Background(), dir)
	if err != nil {
		t.Fatalf("local fs %s: %v", dir, err)
	}
	return rclonestore.New(f)
}

// FastConfig is a configuration with short timers for tests.
func FastConfig(stateDir string) core.Config {
	return core.Config{
		StateDir:  stateDir,
		SealQuiet: 40 * time.Millisecond,
		SealMax:   400 * time.Millisecond,
		LeaseTTL:  3 * time.Second,
		GCGrace:   time.Second,
	}
}

// Stacked wraps a store in the standard aspect stack and returns the meter.
func Stacked(base store.Store, o aspect.Options) (store.Store, *aspect.Meter) {
	if o.Retry.Base == 0 {
		o.Retry.Base = 20 * time.Millisecond
		o.Retry.Max = 200 * time.Millisecond
	}
	return aspect.Stack(base, o)
}
