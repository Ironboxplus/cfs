package core_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
)

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

// readOnlyWorks lists and reads the volume, and finds every change refused.
func readOnlyWorks(t *testing.T, v *core.Volume, small, big []byte) {
	t.Helper()
	ctx := context.Background()
	if !v.ReadOnly() {
		t.Fatal("the volume opened with ReadOnly is not read-only")
	}
	ents, err := v.List(ctx, "lib")
	if err != nil || len(ents) != 2 {
		t.Fatalf("list: %v %v", ents, err)
	}
	if got, err := readAllVol(v, "lib/small.txt"); err != nil || !bytes.Equal(got, small) {
		t.Fatalf("the packed file: %v", err)
	}
	if got, err := readAllVol(v, "lib/big.bin"); err != nil || !bytes.Equal(got, big) {
		t.Fatalf("the big file: %v", err)
	}
	refused := map[string]error{
		"put small": func() error { _, err := v.Put(ctx, "lib/n.txt", strings.NewReader("no"), 2, time.Now()); return err }(),
		"put big": func() error {
			_, err := v.Put(ctx, "lib/n.bin", bytes.NewReader(big), int64(len(big)), time.Now())
			return err
		}(),
		"remove":     v.Remove(ctx, "lib/small.txt"),
		"move":       v.Move(ctx, "lib/small.txt", "lib/moved.txt"),
		"mkdir":      v.Mkdir(ctx, "newdir"),
		"rmdir":      v.Rmdir(ctx, "lib"),
		"dirmove":    v.DirMove(ctx, "lib", "lib2"),
		"removeall":  v.RemoveAll(ctx, "lib"),
		"setmodtime": v.SetModTime(ctx, "lib/small.txt", time.Now()),
		"checkpoint": v.Checkpoint(ctx),
		"compact":    func() error { _, err := v.Compact(ctx, 0.1); return err }(),
		"gc":         func() error { _, err := v.GC(ctx); return err }(),
	}
	for what, err := range refused {
		if !errors.Is(err, core.ErrReadOnly) {
			t.Errorf("%s on a read-only volume: %v, want ErrReadOnly", what, err)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Errorf("flush of a read-only volume: %v", err)
	}
}

// A volume whose writer closed cleanly (lease released) opened read-only: it
// reads, refuses changes, writes nothing to the drive, and does not stand in
// the way of a writer that comes later.
func TestLeaseReadOnlyOpenOfAReleasedVolumeWritesNothing(t *testing.T) {
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

	ro, err := openAt(t, drive, t.TempDir(), func(c *core.Config) { bigTweak(c); c.ReadOnly = true })
	if err != nil {
		t.Fatal(err)
	}
	readOnlyWorks(t, ro, small, big)
	time.Sleep(1500 * time.Millisecond) // longer than a heartbeat would take (TTL 3s, every third)
	sameDrive(t, "while open read-only", before, driveBytes(t, drive))

	// the writer that comes later takes the lease and writes
	w2, err := openAt(t, drive, t.TempDir(), bigTweak)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(ctx)
	if w2.ReadOnly() {
		t.Fatal("a writer opened after a read-only mount is read-only")
	}
	more := []byte("written after the read-only mount")
	if _, err := w2.Put(ctx, "lib/later.txt", bytes.NewReader(more), int64(len(more)), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := w2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ro.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := readAllVol(ro, "lib/later.txt"); err != nil || !bytes.Equal(got, more) {
		t.Fatalf("the read-only volume does not see the later write: %v", err)
	}
	if err := ro.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Put(ctx, "lib/after-close.txt", strings.NewReader("ok"), 2, time.Now()); err != nil {
		t.Fatalf("the writer failed after the read-only volume closed: %v", err)
	}
}

// A writer is heartbeating and writing: the read-only mount leaves its lease
// and its files alone, and the writer is never fenced by it.
func TestLeaseReadOnlyOpenBesideALiveWriterLeavesItsLease(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	long := func(c *core.Config) { bigTweak(c); c.LeaseTTL = time.Hour } // no heartbeat write during the test
	w, err := openAt(t, drive, t.TempDir(), long)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	small, big := writeLibrary(t, w)
	before := driveBytes(t, drive)

	ro, err := openAt(t, drive, t.TempDir(), func(c *core.Config) { long(c); c.ReadOnly = true })
	if err != nil {
		t.Fatal(err)
	}
	readOnlyWorks(t, ro, small, big)
	sameDrive(t, "beside a live writer", before, driveBytes(t, drive))
	if err := ro.Close(ctx); err != nil {
		t.Fatal(err)
	}
	sameDrive(t, "after the read-only close", before, driveBytes(t, drive))

	more := []byte("the writer carries on")
	if _, err := w.Put(ctx, "lib/more.txt", bytes.NewReader(more), int64(len(more)), time.Now()); err != nil {
		t.Fatalf("the live writer was disturbed: %v", err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("flush of the live writer: %v", err)
	}
}

// With no readable lease the epoch comes from what is on the drive, and the
// drive is still not written.
func TestLeaseReadOnlyOpenWithoutALeaseWritesNothing(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	w, err := openAt(t, drive, t.TempDir(), bigTweak)
	if err != nil {
		t.Fatal(err)
	}
	small, big := writeLibrary(t, w)
	epoch := w.Epoch()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(drive, core.CtlDir, "lease.json")); err != nil {
		t.Fatal(err)
	}
	before := driveBytes(t, drive)
	ro, err := openAt(t, drive, t.TempDir(), func(c *core.Config) { bigTweak(c); c.ReadOnly = true })
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close(ctx)
	if ro.Epoch() != epoch {
		t.Errorf("epoch %d, want the drive's %d", ro.Epoch(), epoch)
	}
	readOnlyWorks(t, ro, small, big)
	sameDrive(t, "without a lease", before, driveBytes(t, drive))
}
