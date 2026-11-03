package core_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

// A move of a large file that was journalled and not yet done on the drive when
// the process died is completed by the next Open. Whatever it listed on the way
// (the recovery looks at both folders) must not stay in the listing cache: the
// volume has to list the file at the destination only, and read it there.
// (CI crash test TestCrashPoints/staged/pack_uploaded:4 found this: the listing
// still had the source while reading it said not found.)
func TestFinishedMoveUpdatesTheListing(t *testing.T) {
	drive, state := t.TempDir(), t.TempDir()
	open := func() *core.Volume {
		st, _ := testutil.Stacked(testutil.LocalStore(t, drive), aspectOpts())
		cfg := testutil.FastConfig(state)
		cfg.Threshold = 64 << 10
		v, err := core.Open(context.Background(), st, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	ctx := context.Background()
	data := bytes.Repeat([]byte("0123456789abcdef"), 8<<10) // 128 KiB: a large file, 1:1 on the drive

	v := open()
	if e, err := v.Put(ctx, "a/src.bin", bytes.NewReader(data), int64(len(data)), time.Now()); err != nil || e.Packed {
		t.Fatalf("put: %+v %v", e, err)
	}
	if err := v.SetMvIntent("a/src.bin", "b/dst.bin"); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(ctx); err != nil { // the "crash": the intent is journalled, the drive not touched
		t.Fatal(err)
	}

	v = open() // recovery completes the move
	defer v.Close(ctx)
	names := func(dir string) []string {
		ents, err := v.List(ctx, dir)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("list %q: %v", dir, err)
		}
		var out []string
		for _, e := range ents {
			out = append(out, e.Path)
		}
		return out
	}
	if got := names("a"); len(got) != 0 {
		t.Errorf("the source is still listed after the move was completed: %v", got)
	}
	if got := names("b"); len(got) != 1 || got[0] != "b/dst.bin" {
		t.Errorf("the destination is not listed: %v", got)
	}
	rc, err := v.Open(ctx, "b/dst.bin", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(b, data) {
		t.Errorf("destination content differs (%d bytes)", len(b))
	}
	if _, err := v.Open(ctx, "a/src.bin", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("reading the source: %v, want not found", err)
	}
}

// The recovery of a shadowed large file (stray) removes it from the drive after
// an interrupted move in the same folder has already listed that folder. This is
// a pin, not a fail-first test: List puts the index rows over the drive's entry
// of the same name, so a stale cache entry for a shadowed file is not visible
// through List, Stat or Open (it could only mislead bigAt, whose callers
// tolerate "not found" from the removal). The cache is still kept right.
func TestRecoveryOfStrayAfterMoveInSameFolder(t *testing.T) {
	drive, state := t.TempDir(), t.TempDir()
	open := func() *core.Volume {
		st, _ := testutil.Stacked(testutil.LocalStore(t, drive), aspectOpts())
		cfg := testutil.FastConfig(state)
		cfg.Threshold = 64 << 10
		v, err := core.Open(context.Background(), st, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	ctx := context.Background()
	big := bytes.Repeat([]byte("0123456789abcdef"), 8<<10)
	small := []byte("small file")

	v := open()
	if _, err := v.Put(ctx, "a/src.bin", bytes.NewReader(big), int64(len(big)), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Put(ctx, "b/s.txt", bytes.NewReader(small), int64(len(small)), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// the large file the packed one shadows, as a crash after the commit leaves it
	if err := os.MkdirAll(filepath.Join(drive, "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(drive, "b", "s.txt"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := v.SetStray("b/s.txt"); err != nil {
		t.Fatal(err)
	}
	if err := v.SetMvIntent("a/src.bin", "b/dst.bin"); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(ctx); err != nil {
		t.Fatal(err)
	}

	v = open()
	defer v.Close(ctx)
	if _, err := os.Stat(filepath.Join(drive, "b", "s.txt")); !os.IsNotExist(err) {
		t.Errorf("the shadowed large file is still on the drive: %v", err)
	}
	ents, err := v.List(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]core.Entry{}
	for _, e := range ents {
		got[e.Path] = e
	}
	if len(got) != 2 || !got["b/s.txt"].Packed || got["b/dst.bin"].Path == "" {
		t.Errorf("listing of b: %+v", ents)
	}
	rc, err := v.Open(ctx, "b/s.txt", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(b, small) {
		t.Errorf("packed file content: %q", b)
	}
}
