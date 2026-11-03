package core_test

import (
	"bytes"
	"context"
	"errors"
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

// A commit that died while its log segment was being written leaves a torn file
// on the drive (rclone's local backend writes the final name in place). The
// recovery discards it and takes the metadata rows that rode in it back for
// another upload. It used to delete those rows as "already on the drive" in the
// next step, so a removal that had been acknowledged never reached the log: a
// rebuild from the drive would bring the file back, and fsck said "in the log
// but not in the index". (Found on Windows by TestCrashPoints/staged/
// pack_uploaded:4, where a killed process is more likely to leave a torn file.)
func TestTornSegmentOfInterruptedCommitKeepsItsRows(t *testing.T) {
	drive, state := t.TempDir(), t.TempDir()
	open := func() *core.Volume {
		st, _ := testutil.Stacked(testutil.LocalStore(t, drive), aspectOpts())
		cfg := testutil.FastConfig(state)
		cfg.Threshold = 64 << 10
		cfg.CheckpointEvery = -1 // a checkpoint would carry the index, and hide the log
		v, err := core.Open(context.Background(), st, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	ctx := context.Background()
	data := []byte("small file")

	v := open()
	if _, err := v.Put(ctx, "x/f.txt", bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil { // the put is on the drive's log
		t.Fatal(err)
	}
	if err := v.Remove(ctx, "x/f.txt"); err != nil { // acknowledged, queued for the log
		t.Fatal(err)
	}
	name, err := v.SealMetaInflight() // the commit that was uploading when the process died
	if err != nil || name == "" {
		t.Fatalf("seal: %q %v", name, err)
	}
	// the torn segment: a gzip header and nothing else
	torn := []byte{0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0xff}
	if err := os.WriteFile(filepath.Join(drive, ".cfs", "log", name), torn, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(ctx); err != nil {
		t.Fatal(err)
	}

	v = open()
	defer v.Close(ctx)
	if err := v.Flush(ctx); err != nil { // uploads whatever is still queued
		t.Fatal(err)
	}
	rep, err := v.Fsck(ctx, core.FsckOpts{Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range rep.Problems {
		t.Errorf("fsck: %s", p)
	}
	if _, err := v.Stat(ctx, "x/f.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the removed file: %v, want not found", err)
	}
}

// The same for a log segment that holds only metadata (no pack): the process
// died while it was uploading. The recovery used to take the torn file for a
// segment that had reached the drive, drop the rows sealed under it, and leave
// the file: a rebuild from the drive could not replay it (fsck: log_unreadable).
func TestTornMetadataSegmentIsUploadedAgain(t *testing.T) {
	drive, state := t.TempDir(), t.TempDir()
	open := func() *core.Volume {
		st, _ := testutil.Stacked(testutil.LocalStore(t, drive), aspectOpts())
		cfg := testutil.FastConfig(state)
		cfg.Threshold = 64 << 10
		cfg.CheckpointEvery = -1
		v, err := core.Open(context.Background(), st, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	ctx := context.Background()
	data := []byte("small file")

	v := open()
	if _, err := v.Put(ctx, "x/f.txt", bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := v.Remove(ctx, "x/f.txt"); err != nil {
		t.Fatal(err)
	}
	name, err := v.SealMeta() // sealed, and the upload died half way
	if err != nil || name == "" {
		t.Fatalf("seal: %q %v", name, err)
	}
	torn := []byte{0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0xff}
	if err := os.WriteFile(filepath.Join(drive, ".cfs", "log", name), torn, 0o644); err != nil {
		t.Fatal(err)
	}
	v.Fence() // the process is gone: no flush at close
	if err := v.Close(ctx); err != nil {
		t.Fatal(err)
	}

	v = open()
	defer v.Close(ctx)
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	rep, err := v.Fsck(ctx, core.FsckOpts{Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range rep.Problems {
		t.Errorf("fsck: %s", p)
	}
}

// readFault fails every read of a log segment with a transient error, as a
// drive that is rate-limited or unreachable does.
type readFault struct {
	store.Store
	on atomic.Bool
}

func (f *readFault) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	if f.on.Load() && strings.Contains(p, ".cfs/log/") {
		return nil, errors.New("transient: 429 too many requests")
	}
	return f.Store.Open(ctx, p, off, n)
}

// A log segment that cannot be read for a reason that is not the segment's (the
// drive said "slow down", the network dropped) must be left alone: Open fails
// and the next Open looks again. Deleting it would leave the local index as the
// only copy of the rows in it.
func TestUnreadableSegmentForAnotherReasonIsKept(t *testing.T) {
	for _, inflight := range []bool{false, true} {
		name := "metadata-only"
		if inflight {
			name = "pack-commit"
		}
		t.Run(name, func(t *testing.T) {
			drive, state := t.TempDir(), t.TempDir()
			ft := &readFault{Store: testutil.LocalStore(t, drive)}
			open := func() (*core.Volume, error) {
				cfg := testutil.FastConfig(state)
				cfg.Threshold = 64 << 10
				cfg.CheckpointEvery = -1
				return core.Open(context.Background(), ft, cfg)
			}
			ctx := context.Background()
			data := []byte("small file")

			v, err := open()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := v.Put(ctx, "x/f.txt", bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
				t.Fatal(err)
			}
			if err := v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := v.Remove(ctx, "x/f.txt"); err != nil {
				t.Fatal(err)
			}
			seal := v.SealMeta
			if inflight {
				seal = v.SealMetaInflight
			}
			seg, err := seal()
			if err != nil || seg == "" {
				t.Fatalf("seal: %q %v", seg, err)
			}
			// a segment file the process died writing (what is in it does not matter:
			// the reads below never get to its bytes)
			segPath := filepath.Join(drive, ".cfs", "log", seg)
			if err := os.WriteFile(segPath, []byte{0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0xff}, 0o644); err != nil {
				t.Fatal(err)
			}
			v.Fence()
			if err := v.Close(ctx); err != nil {
				t.Fatal(err)
			}

			ft.on.Store(true)
			if v, err := open(); err == nil {
				v.Close(ctx)
				t.Fatal("Open succeeded although a log segment could not be read")
			}
			if _, err := os.Stat(segPath); err != nil {
				t.Fatalf("the segment was deleted after a transient read error: %v", err)
			}

			ft.on.Store(false) // the drive answers again
			v, err = open()
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close(ctx)
			if err := v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			rep, err := v.Fsck(ctx, core.FsckOpts{Deep: true})
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range rep.Problems {
				t.Errorf("fsck: %s", p)
			}
		})
	}
}
