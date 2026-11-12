package core_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

// The lease is a directory of files, each written once under a name of its own
// (restic's locks): the drive has no atomic replace, so a file rewritten in
// place can be read half written (JOURNAL 2026-11-12, cfs 1 and 2: 304
// "unreadable lease" on 115, each one taken for no lease at all).

// leaseFiles is the names in the lease directory of the drive, sorted.
func leaseFiles(t *testing.T, drive string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(drive, ".cfs", "lease"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// leaseName is what a lease file's name says: <epoch>-<heartbeat ms>-<held or
// released>-<writer>.json.
type leaseName struct {
	epoch     uint64
	heartbeat int64
	state     string
	writer    string
}

func parseLeaseFile(t *testing.T, name string) leaseName {
	t.Helper()
	base, ok := strings.CutSuffix(name, ".json")
	parts := strings.SplitN(base, "-", 4)
	if !ok || len(parts) != 4 {
		t.Fatalf("lease file name %q", name)
	}
	epoch, err1 := strconv.ParseUint(parts[0], 10, 64)
	hb, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		t.Fatalf("lease file name %q", name)
	}
	return leaseName{epoch, hb, parts[2], parts[3]}
}

// garbledLease is a drive that hands out the first bytes of every lease file,
// as 115 did while one was being replaced.
type garbledLease struct{ store.Store }

func (g garbledLease) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	if strings.HasPrefix(p, core.CtlDir+"/lease") {
		return io.NopCloser(strings.NewReader(`{"writer":"`)), nil
	}
	return g.Store.Open(ctx, p, off, n)
}

func openGarbled(t *testing.T, drive string) *core.Volume {
	t.Helper()
	st, _ := testutil.Stacked(garbledLease{testutil.LocalStore(t, drive)}, aspectOpts())
	cfg := testutil.FastConfig(t.TempDir())
	cfg.LeaseTTL = 3 * time.Second
	v, err := core.Open(context.Background(), st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// A live writer's lease that can't be read is still held: the second volume
// stays a reader and the writer is not fenced. Once the writer is gone (its
// lease past the TTL) the second volume takes over.
func TestLeaseThatCantBeReadIsNotTakenOver(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	a, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Put(ctx, "a.txt", strings.NewReader("from a"), 6, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	b := openGarbled(t, drive)
	defer b.Close(ctx)
	if _, err := b.Put(ctx, "b.txt", strings.NewReader("no"), 2, time.Now()); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("B's put while A holds the lease, B reading it garbled: %v, want ErrReadOnly", err)
	}
	if err := a.ReadCheckLease(ctx); err != nil {
		t.Fatalf("A after B's refused put: %v", err)
	}

	a.Fence() // A dies: no release
	a.Close(ctx)
	time.Sleep(3500 * time.Millisecond) // LeaseTTL
	if _, err := b.Put(ctx, "b.txt", strings.NewReader("from b"), 6, time.Now()); err != nil {
		t.Fatalf("B's put after A's lease went stale: %v", err)
	}
	if b.ReadOnly() || b.Epoch() <= a.Epoch() {
		t.Fatalf("takeover: readonly=%v epoch %d (A had %d)", b.ReadOnly(), b.Epoch(), a.Epoch())
	}
}

// Every write of the lease is a new file, and the writer removes the one before:
// heartbeats leave at most two files of the writer, the newest of them its
// lease; Close leaves one, released.
func TestLeaseHeartbeatsLeaveOneFileEach(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, err := openAt(t, drive, t.TempDir(), func(c *core.Config) { c.LeaseTTL = 300 * time.Millisecond })
	if err != nil {
		t.Fatal(err)
	}
	if err := v.TakeLease(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond) // a dozen heartbeats (every TTL/3)
	files := leaseFiles(t, drive)
	if len(files) == 0 || len(files) > 2 {
		t.Fatalf("after the heartbeats the lease directory holds %v", files)
	}
	newest := parseLeaseFile(t, files[len(files)-1])
	if newest.epoch != v.Epoch() || newest.state != "held" {
		t.Fatalf("newest lease file %+v, want epoch %d held", newest, v.Epoch())
	}
	if age := time.Since(time.UnixMilli(newest.heartbeat)); age > 300*time.Millisecond {
		t.Fatalf("newest heartbeat is %v old", age)
	}
	if err := v.Close(ctx); err != nil {
		t.Fatal(err)
	}
	files = leaseFiles(t, drive)
	if len(files) != 1 || parseLeaseFile(t, files[0]).state != "released" {
		t.Fatalf("after Close the lease directory holds %v", files)
	}
}

// writeLegacy writes .cfs/lease.json, where versions before the lease directory
// kept the lease, last modified at mtime.
func writeLegacy(t *testing.T, drive, body string, mtime time.Time) {
	t.Helper()
	p := filepath.Join(drive, ".cfs", "lease.json")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

// A volume last written by an older version: its lease.json is respected while
// live, taken over once stale, and removed by the new writer.
func TestLeaseLegacyIsRespectedThenRemoved(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	now := time.Now()
	writeLegacy(t, drive, `{"writer":"old-version","epoch":7,"heartbeat_ms":`+strconv.FormatInt(now.UnixMilli(), 10)+`}`, now)
	v, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close(ctx)
	if _, err := v.Put(ctx, "x.txt", strings.NewReader("no"), 2, time.Now()); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("put while an older version holds the lease: %v, want ErrReadOnly", err)
	}

	old := now.Add(-time.Minute)
	writeLegacy(t, drive, `{"writer":"old-version","epoch":7,"heartbeat_ms":`+strconv.FormatInt(old.UnixMilli(), 10)+`}`, old)
	if _, err := v.Put(ctx, "x.txt", strings.NewReader("ok"), 2, time.Now()); err != nil {
		t.Fatalf("put after the older version's lease went stale: %v", err)
	}
	if v.Epoch() <= 7 {
		t.Fatalf("epoch %d after taking over epoch 7", v.Epoch())
	}
	if _, err := os.Stat(filepath.Join(drive, ".cfs", "lease.json")); !os.IsNotExist(err) {
		t.Fatalf("lease.json after the new writer took the lease: %v", err)
	}
	if files := leaseFiles(t, drive); len(files) != 1 || parseLeaseFile(t, files[0]).epoch != v.Epoch() {
		t.Fatalf("lease directory %v, want the writer's lease (epoch %d)", files, v.Epoch())
	}
}

// A lease.json that can't be read counts as held from its modification time:
// a reader while that is within the TTL, taken over after.
func TestLeaseLegacyUnreadableIsHeldUntilStale(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	writeLegacy(t, drive, `{"writer":"old-ver`, time.Now())
	v, err := openAt(t, drive, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close(ctx)
	if _, err := v.Put(ctx, "x.txt", strings.NewReader("no"), 2, time.Now()); !errors.Is(err, core.ErrReadOnly) {
		t.Fatalf("put while an unreadable lease.json is fresh: %v, want ErrReadOnly", err)
	}
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(filepath.Join(drive, ".cfs", "lease.json"), old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Put(ctx, "x.txt", strings.NewReader("ok"), 2, time.Now()); err != nil {
		t.Fatalf("put once the unreadable lease.json is stale: %v", err)
	}
	if _, err := os.Stat(filepath.Join(drive, ".cfs", "lease.json")); !os.IsNotExist(err) {
		t.Fatalf("lease.json after the takeover: %v", err)
	}
}
