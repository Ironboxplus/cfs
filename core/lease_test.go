package core_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
)

func openAt(t *testing.T, drive, state string, tweak func(*core.Config)) (*core.Volume, error) {
	t.Helper()
	base := testutil.LocalStore(t, drive)
	st, _ := testutil.Stacked(base, aspectOpts())
	cfg := testutil.FastConfig(state)
	cfg.LeaseTTL = 3 * time.Second
	if tweak != nil {
		tweak(&cfg)
	}
	return core.Open(context.Background(), st, cfg)
}

func TestSecondWriterIsReadOnly(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	a, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)
	if a.ReadOnly() || a.Epoch() == 0 {
		t.Fatalf("first writer: readonly=%v epoch=%d", a.ReadOnly(), a.Epoch())
	}
	data := []byte("from the writer")
	if _, err := a.Put(ctx, "x/a.txt", bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
		t.Fatal(err)
	}
	b, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(ctx)
	if !b.ReadOnly() {
		t.Fatal("second instance must be read-only while the first holds the lease")
	}
	if _, err := b.Put(ctx, "x/b.txt", strings.NewReader("no"), 2, time.Now()); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("put on read-only volume: %v", err)
	}
	if err := b.Remove(ctx, "x/a.txt"); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("remove on read-only volume: %v", err)
	}
	got, err := readAllVol(b, "x/a.txt")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read-only volume can't read the writer's data: %v", err)
	}
	// It follows the writer.
	more := []byte("written later")
	a.Put(ctx, "x/later.txt", bytes.NewReader(more), int64(len(more)), time.Now())
	if err := b.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = readAllVol(b, "x/later.txt")
	if err != nil || !bytes.Equal(got, more) {
		t.Fatalf("read-only volume didn't catch up: %v", err)
	}
}

func TestTakeoverAfterCleanCloseAndAfterDeath(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	a, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	epochA := a.Epoch()
	a.Put(ctx, "f.txt", strings.NewReader("one"), 3, time.Now())
	if err := a.Close(ctx); err != nil { // releases the lease
		t.Fatal(err)
	}
	b, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if b.ReadOnly() || b.Epoch() != epochA+1 {
		t.Fatalf("after a clean close the next writer takes over: readonly=%v epoch %d (was %d)", b.ReadOnly(), b.Epoch(), epochA)
	}
	if got, _ := readAllVol(b, "f.txt"); string(got) != "one" {
		t.Fatalf("takeover lost data: %q", got)
	}
	b.Put(ctx, "g.txt", strings.NewReader("two"), 3, time.Now())
	b.Close(ctx)

	// A writer that died without releasing: its lease goes stale and is taken over.
	state := filepath.Join(t.TempDir(), "state")
	events := filepath.Join(t.TempDir(), "events")
	if code := runWriter(t, []string{"-remote", drive, "-state", state, "-events", events, "-workers", "1", "-ops", "100000", "-seed", "5"}, "", 1500*time.Millisecond); code == 0 {
		t.Fatal("writer should have been killed")
	}
	c, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !c.ReadOnly() {
		t.Fatal("a fresh lease of a dead writer must still be respected")
	}
	c.Close(ctx)
	time.Sleep(3500 * time.Millisecond) // LeaseTTL
	d, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	if d.ReadOnly() {
		t.Fatal("a stale lease must be taken over")
	}
	if d.Epoch() <= epochA+1 {
		t.Fatalf("epoch must increase on takeover: %d", d.Epoch())
	}
	if _, err := d.Put(ctx, "after.txt", strings.NewReader("ok"), 2, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// An old writer that wakes up after being replaced must not write.
func TestFencedWriterStops(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	a, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)
	if _, err := a.Put(ctx, "ok.txt", strings.NewReader("fine"), 4, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Another writer takes over: all it has to do is publish a higher epoch.
	lease, _ := json.Marshal(map[string]any{"writer": "someone-else", "epoch": a.Epoch() + 1, "heartbeat_ms": time.Now().UnixMilli()})
	if err := os.WriteFile(filepath.Join(drive, ".cfs", "lease.json"), lease, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = a.Put(ctx, "late.txt", strings.NewReader("too late"), 8, time.Now())
	if !errors.Is(err, core.ErrFenced) {
		t.Fatalf("fenced writer's put: %v", err)
	}
	if _, serr := os.Stat(filepath.Join(drive, ".cfs", "packs")); serr == nil {
		ents, _ := os.ReadDir(filepath.Join(drive, ".cfs", "packs"))
		for _, e := range ents {
			if strings.Contains(e.Name(), "late") {
				t.Fatal("fenced writer wrote a pack")
			}
		}
	}
	if err := a.Remove(ctx, "ok.txt"); !errors.Is(err, core.ErrFenced) {
		t.Fatalf("fenced writer's remove: %v", err)
	}
}

// Two segments with one sequence number mean two writers were active: report it.
func TestEpochConflictIsReported(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	a, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	a.Put(ctx, "a.txt", strings.NewReader("a"), 1, time.Now())
	a.Close(ctx)
	segs, _ := filepath.Glob(filepath.Join(drive, ".cfs", "log", "*.jsonl.gz"))
	if len(segs) == 0 {
		t.Fatal("no segment")
	}
	// Forge the same sequence number under another epoch.
	base := filepath.Base(segs[0]) // 000001-0000000001.jsonl.gz
	forged := "000009" + base[6:]
	b, _ := os.ReadFile(segs[0])
	os.WriteFile(filepath.Join(drive, ".cfs", "log", forged), b, 0o644)
	_, err = openAt(t, drive, t.TempDir(), nil)
	if !errors.Is(err, core.ErrEpochConflict) {
		t.Fatalf("expected an epoch conflict, got %v", err)
	}
}

// With the log and every checkpoint gone, the packs' own manifests rebuild the index.
func TestRebuildFromPacksAlone(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	a, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]byte{}
	for i := 0; i < 30; i++ {
		p := "dir" + string(rune('a'+i%3)) + "/file" + string(rune('A'+i)) + ".txt"
		want[p] = randBytes(50 + i*20)
		if _, err := a.Put(ctx, p, bytes.NewReader(want[p]), int64(len(want[p])), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	// Overwrite one: the newest pack must win.
	want["dira/fileA.txt"] = []byte("the newer version")
	a.Put(ctx, "dira/fileA.txt", bytes.NewReader(want["dira/fileA.txt"]), int64(len(want["dira/fileA.txt"])), time.Now())
	a.Close(ctx)
	os.RemoveAll(filepath.Join(drive, ".cfs", "log"))
	os.RemoveAll(filepath.Join(drive, ".cfs", "ckpt"))

	b, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(ctx)
	for p, d := range want {
		got, err := readAllVol(b, p)
		if err != nil || !bytes.Equal(got, d) {
			t.Fatalf("%s after pack-only rebuild: %v", p, err)
		}
	}
	// The volume is usable again and logs from here on.
	if _, err := b.Put(ctx, "new.txt", strings.NewReader("new"), 3, time.Now()); err != nil {
		t.Fatal(err)
	}
}
