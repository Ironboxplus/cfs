package cfsrclone_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	cfsrclone "github.com/Ironboxplus/cfs/frontend/rclone"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/object"
)

// Two front ends of one process (a storage driver and a mount) must end up on
// the same Volume instance, and the volume closes with the last of them.
func TestSharedVolumeIsOneInstance(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	opt, err := cfsrclone.OptionsFrom(configmap.Simple{"state_dir": t.TempDir(), "seal_quiet": "10ms", "seal_max": "100ms"})
	if err != nil {
		t.Fatal(err)
	}
	if opt.Transfers != 8 || opt.MetaInflight != 1 {
		t.Fatalf("defaults not applied: transfers=%d meta_inflight=%d", opt.Transfers, opt.MetaInflight)
	}
	under := func() (fs.Fs, error) { return fs.NewFs(ctx, drive) }
	a, err := cfsrclone.Acquire(ctx, "test-shared", under, opt)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	b, err := cfsrclone.Acquire(ctx, "test-shared", func() (fs.Fs, error) { calls++; return under() }, opt)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || a.Volume() != b.Volume() {
		t.Fatalf("second acquire opened a second volume (underlying opened %d times)", calls)
	}
	c := cfsrclone.Lookup("test-shared")
	if c == nil || c.Volume() != a.Volume() {
		t.Fatal("lookup must find the open volume")
	}
	if cfsrclone.Lookup("nothing") != nil {
		t.Fatal("lookup of an unknown key must be nil")
	}
	// A write through the rclone Fs is seen through the Volume of the driver side.
	f, err := a.NewFs(ctx, "cfs", "")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("shared between two front ends")
	if _, err := f.Put(ctx, bytes.NewReader(data), object.NewStaticObjectInfo("x/y.txt", time.Now(), int64(len(data)), true, nil, nil)); err != nil {
		t.Fatal(err)
	}
	rc, err := b.Volume().Open(ctx, "x/y.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q", got)
	}
	if err := f.(fs.Shutdowner).Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// References: a, b, c and the Fs. The volume stays until the last one goes.
	b.Release(ctx)
	a.Release(ctx)
	if x := cfsrclone.Lookup("test-shared"); x == nil {
		t.Fatal("volume closed while a reference is left")
	} else {
		x.Release(ctx)
	}
	if err := c.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if cfsrclone.Lookup("test-shared") != nil {
		t.Fatal("volume still registered after the last release")
	}
}
