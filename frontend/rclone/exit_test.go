package cfsrclone_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cfsrclone "github.com/Ironboxplus/cfs/frontend/rclone"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
)

// slowLease is an Fs whose writes of the lease file take their time once slow
// is set: a drive that is slow to take the release when the volume closes.
type slowLease struct {
	fs.Fs
	slow *atomic.Bool
}

type slowObj struct {
	fs.Object
	f *slowLease
}

func (f *slowLease) wrap(o fs.Object, err error) (fs.Object, error) {
	if o == nil || err != nil {
		return o, err
	}
	return &slowObj{o, f}, nil
}

func (f *slowLease) delay(remote string) {
	if f.slow.Load() && strings.HasSuffix(remote, "lease.json") {
		time.Sleep(800 * time.Millisecond)
	}
}

func (f *slowLease) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.wrap(f.Fs.NewObject(ctx, remote))
}

func (f *slowLease) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, opts ...fs.OpenOption) (fs.Object, error) {
	f.delay(src.Remote())
	return f.wrap(f.Fs.Put(ctx, in, src, opts...))
}

func (f *slowLease) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	ents, err := f.Fs.List(ctx, dir)
	for i, e := range ents {
		if o, ok := e.(fs.Object); ok {
			ents[i] = &slowObj{o, f}
		}
	}
	return ents, err
}

func (o *slowObj) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, opts ...fs.OpenOption) error {
	o.f.delay(o.Remote())
	return o.Object.Update(ctx, in, src, opts...)
}

// When the last Fs of a volume is shut down on one goroutine (rclone clears its
// Fs cache when the command ends) while the exit handler's CloseAll runs on
// another (rc core/quit), CloseAll must not return before that close is done:
// the process exits when it returns, and an unfinished close leaves the lease
// held, so the next start opens read-only.
func TestSharedVolumeCloseAllWaitsForAShutdownInFlight(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	opt, err := cfsrclone.OptionsFrom(configmap.Simple{"state_dir": t.TempDir(), "seal_quiet": "10ms", "seal_max": "100ms"})
	if err != nil {
		t.Fatal(err)
	}
	var slow atomic.Bool
	under := func() (fs.Fs, error) {
		u, err := fs.NewFs(ctx, drive)
		return &slowLease{u, &slow}, err
	}
	sh, err := cfsrclone.Acquire(ctx, "test-exit", under, opt)
	if err != nil {
		t.Fatal(err)
	}
	f, err := sh.NewFs(ctx, "cfs", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Mkdir(ctx, "d"); err != nil { // the first change takes the lease
		t.Fatal(err)
	}
	slow.Store(true)
	sh.Release(ctx) // the Fs holds the last reference
	done := make(chan error, 1)
	go func() { done <- f.(fs.Shutdowner).Shutdown(ctx) }()
	time.Sleep(300 * time.Millisecond) // the close is waiting for the release to land
	if err := cfsrclone.CloseAll(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(drive, ".cfs", "lease.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"released":true`)) {
		t.Fatalf("CloseAll returned while the volume was still closing, lease not released: %s", b)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
