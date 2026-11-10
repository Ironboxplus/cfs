package core_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
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

// opCounter counts the calls a volume makes to the drive, by kind of object.
type opCounter struct {
	store.Store
	segReads, ckptReads, lists, packPuts, packReads atomic.Int64
}

func (c *opCounter) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	switch {
	case strings.HasPrefix(p, core.CtlDir+"/log/"):
		c.segReads.Add(1)
	case strings.HasPrefix(p, core.CtlDir+"/ckpt/"):
		c.ckptReads.Add(1)
	case strings.HasPrefix(p, core.CtlDir+"/packs/"):
		c.packReads.Add(1)
	}
	return c.Store.Open(ctx, p, off, n)
}

func (c *opCounter) List(ctx context.Context, dir string) ([]store.Entry, error) {
	c.lists.Add(1)
	return c.Store.List(ctx, dir)
}

func (c *opCounter) Put(ctx context.Context, p string, src store.Source, o store.PutOpts) error {
	if strings.HasPrefix(p, core.CtlDir+"/packs/") {
		c.packPuts.Add(1)
	}
	return c.Store.Put(ctx, p, src, o)
}

func (c *opCounter) String() string {
	return fmt.Sprintf("checkpoint reads=%d, segment reads=%d, listings=%d", c.ckptReads.Load(), c.segReads.Load(), c.lists.Load())
}

func openCounting(t *testing.T, drive, state string, tweak func(*core.Config)) (*core.Volume, *opCounter) {
	t.Helper()
	if os.Getenv("CFS_TEST_ACK") == "durable" {
		t.Skip("batched logging is the staged mode")
	}
	oc := &opCounter{Store: testutil.LocalStore(t, drive)}
	st, _ := testutil.Stacked(oc, aspectOpts())
	cfg := testutil.FastConfig(state)
	if tweak != nil {
		tweak(&cfg)
	}
	v, err := core.Open(context.Background(), st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return v, oc
}

func driveCount(t *testing.T, drive, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(drive, ".cfs", dir))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if !e.IsDir() {
			n++
		}
	}
	return n
}

func putMany(t *testing.T, v *core.Volume, prefix string, n int) map[string][]byte {
	t.Helper()
	want := map[string][]byte{}
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("%s/d%d/f%d.txt", prefix, i%20, i)
		data := []byte(fmt.Sprintf("content of %s %d, a few bytes to pack", p, i))
		if _, err := v.Put(context.Background(), p, bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
			t.Fatal(err)
		}
		want[p] = data
	}
	return want
}

func stagedEmpty(v *core.Volume) bool { n, _ := v.Staged(); return n == 0 }

func checkFiles(t *testing.T, v *core.Volume, want map[string][]byte) {
	t.Helper()
	for p, d := range want {
		if got, err := readAllVol(v, p); err != nil || !bytes.Equal(got, d) {
			t.Fatalf("%s: %v", p, err)
		}
	}
}

// A backup of 300 packs makes one log segment, not 300: the packs are on the
// drive, and the segment that makes them visible to the other volumes is written
// when something asks for the log to be complete (here Flush).
func TestLogBatchSegmentsDoNotFollowThePacks(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) {
		c.PackMaxFiles = 2
		c.LogBatchAge = time.Hour
	})
	defer v.Close(ctx)
	other, _ := openCounting(t, drive, t.TempDir(), nil)
	defer other.Close(ctx)

	want := putMany(t, v, "bk", 600)
	waitFor(t, "every file is in a pack", 60*time.Second, func() bool { return stagedEmpty(v) })
	packs, segs := driveCount(t, drive, "packs"), driveCount(t, drive, "log")
	t.Logf("before Flush: %d packs, %d segments", packs, segs)
	if packs < 300 {
		t.Fatalf("%d packs, the test wants at least 300", packs)
	}
	if segs != 0 {
		t.Fatalf("%d segments before anything asked for the log", segs)
	}
	checkFiles(t, v, want) // the writer reads its own files meanwhile
	if err := other.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if ents, _ := other.List(ctx, "bk/d0"); len(ents) != 0 {
		t.Fatalf("another volume sees %d files before the segment is written", len(ents))
	}

	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if segs = driveCount(t, drive, "log"); segs != 1 {
		t.Fatalf("%d segments after Flush for %d packs, want 1", segs, packs)
	}
	if err := other.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	checkFiles(t, other, want)
}

// With the batch age as the only trigger the segments follow the time: three
// bursts a little further apart than the age make three segments.
func TestLogBatchSegmentsFollowTheTime(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) {
		c.PackMaxFiles = 5
		c.LogBatchAge = 400 * time.Millisecond
		c.CheckpointEvery = -1
	})
	defer v.Close(ctx)
	want := map[string][]byte{}
	for burst := 0; burst < 3; burst++ {
		for p, d := range putMany(t, v, fmt.Sprintf("b%d", burst), 100) {
			want[p] = d
		}
		waitFor(t, "the burst is in packs", 30*time.Second, func() bool { return stagedEmpty(v) })
		time.Sleep(1200 * time.Millisecond)
	}
	packs, segs := driveCount(t, drive, "packs"), driveCount(t, drive, "log")
	t.Logf("%d packs, %d segments", packs, segs)
	if segs < 3 || segs > 12 || packs < 50 {
		t.Fatalf("%d segments for %d packs in three bursts, want about 3", segs, packs)
	}
	checkFiles(t, v, want)
}

// The member count is the other trigger, and checkpoints come with the segments:
// one after 10 of them.
func TestLogBatchMemberCountAndCheckpointEveryTenSegments(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) {
		c.PackMaxFiles = 10
		c.LogBatchAge = time.Hour
		c.LogBatchMembers = 100
	})
	defer v.Close(ctx)
	want := putMany(t, v, "m", 3000)
	waitFor(t, "every file is in a pack", 120*time.Second, func() bool { return stagedEmpty(v) })
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the checkpoints are written", 30*time.Second, func() bool { return driveCount(t, drive, "ckpt") >= 1 })
	packs, segs, ckpts := driveCount(t, drive, "packs"), driveCount(t, drive, "log"), driveCount(t, drive, "ckpt")
	t.Logf("%d files in %d packs: %d segments, %d checkpoints", len(want), packs, segs, ckpts)
	if segs > packs/3 || segs < 3 {
		t.Errorf("%d segments for %d packs and a batch of 100 members", segs, packs)
	}
	if ckpts < 1 || ckpts > segs/10+2 {
		t.Errorf("%d checkpoints for %d segments, want about one per 10", ckpts, segs)
	}
	checkFiles(t, v, want)
}

// Segments that add up to the size limit bring a checkpoint before there are 10.
func TestLogBatchCheckpointOnTheSizeOfTheSegments(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) {
		c.PackMaxFiles = 5
		c.LogBatchAge = time.Hour
		c.LogBatchMembers = 200
		c.CheckpointEvery = 1000
		c.CheckpointBytes = 3000
	})
	defer v.Close(ctx)
	putMany(t, v, "s", 1200)
	waitFor(t, "every file is in a pack", 60*time.Second, func() bool { return stagedEmpty(v) })
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a checkpoint for the size of the segments", 30*time.Second, func() bool { return driveCount(t, drive, "ckpt") >= 1 })
	if segs := driveCount(t, drive, "log"); segs >= 10 {
		t.Errorf("%d segments: the size limit should have brought a checkpoint before 10", segs)
	}
}

// What a volume that opens afterwards reads of the drive: one checkpoint and
// fewer than 10 segments besides the listings.
func TestLogBatchNewReaderReadsOneCheckpointAndAFewSegments(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) {
		c.PackMaxFiles = 10
		c.LogBatchAge = time.Hour
		c.LogBatchMembers = 100
		c.CheckpointEvery = 10
	})
	defer v.Close(ctx)
	want := putMany(t, v, "r", 2500)
	waitFor(t, "every file is in a pack", 120*time.Second, func() bool { return stagedEmpty(v) })
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a checkpoint", 30*time.Second, func() bool { return driveCount(t, drive, "ckpt") >= 1 })
	time.Sleep(500 * time.Millisecond) // one that is being written
	segs, ckpts := driveCount(t, drive, "log"), driveCount(t, drive, "ckpt")

	r, oc := openCounting(t, drive, t.TempDir(), nil)
	defer r.Close(ctx)
	checkFiles(t, r, want)
	t.Logf("drive: %d segments, %d checkpoints; the new reader made: %s", segs, ckpts, oc)
	if oc.ckptReads.Load() > 1 {
		t.Errorf("%d checkpoint reads, want at most 1", oc.ckptReads.Load())
	}
	if oc.segReads.Load() > 10 {
		t.Errorf("%d segment reads, want at most 10", oc.segReads.Load())
	}
	if oc.packReads.Load() > 0 && oc.segReads.Load() == 0 {
		t.Logf("%d pack reads (file contents)", oc.packReads.Load())
	}
}

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, p)
		dst := filepath.Join(to, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		if strings.HasSuffix(p, "-shm") { // rebuilt from the log of the database when it is opened
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The packs are on the drive and the segment is not written when the process
// dies (the state directory and the drive as they are at that moment are
// copied): the next start writes the segment, uploads no pack again, and every
// file is there.
func TestLogBatchCrashAfterThePacksBeforeTheSegment(t *testing.T) {
	ctx := context.Background()
	drive, state := t.TempDir(), t.TempDir()
	v, _ := openCounting(t, drive, state, func(c *core.Config) {
		c.PackMaxFiles = 5
		c.LogBatchAge = time.Hour
	})
	defer v.Close(ctx)
	want := putMany(t, v, "c", 200)
	waitFor(t, "every file is in a pack", 60*time.Second, func() bool { return stagedEmpty(v) })
	time.Sleep(300 * time.Millisecond)
	if n := driveCount(t, drive, "log"); n != 0 {
		t.Fatalf("%d segments, the test wants none", n)
	}
	packsBefore := driveCount(t, drive, "packs")
	drive2, state2 := t.TempDir(), t.TempDir()
	copyDir(t, drive, drive2)
	copyDir(t, state, state2)

	r, oc := openCounting(t, drive2, state2, func(c *core.Config) { c.LogBatchAge = time.Hour })
	defer r.Close(ctx)
	if n := driveCount(t, drive2, "log"); n != 1 {
		t.Fatalf("%d segments after the restart, want the one the crash had not written", n)
	}
	if oc.packPuts.Load() != 0 {
		t.Errorf("%d packs uploaded again", oc.packPuts.Load())
	}
	if n := driveCount(t, drive2, "packs"); n != packsBefore {
		t.Errorf("%d packs after the restart, %d before", n, packsBefore)
	}
	if n, _ := r.Staged(); n != 0 {
		t.Errorf("%d files staged again", n)
	}
	checkFiles(t, r, want)
	// a volume with nothing local finds them through the segment
	fresh, _ := openCounting(t, drive2, t.TempDir(), nil)
	defer fresh.Close(ctx)
	checkFiles(t, fresh, want)
}

// Rounds of writes, with checkpoints every three segments. Once a checkpoint has
// been on the drive for the safety time, the segments it covers and the
// checkpoints before it are deleted by the writer; a volume that was following
// the log from far behind starts from the newest checkpoint and has everything.
func TestLogBatchCleanupAfterTheSafetyTimeAndAFollowerFarBehind(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) {
		c.LogBatchAge = 20 * time.Millisecond
		c.CheckpointEvery = 3
		c.CleanupSafety = 600 * time.Millisecond
	})
	defer v.Close(ctx)
	want := map[string][]byte{}
	round := func(i int) {
		for p, d := range putMany(t, v, fmt.Sprintf("round%d", i), 10) {
			want[p] = d
		}
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	round(0)
	follower, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) { c.DirCacheTTL = time.Hour })
	defer follower.Close(ctx)
	for i := 1; i <= 8; i++ {
		round(i)
		time.Sleep(300 * time.Millisecond)
	}
	waitFor(t, "an old checkpoint to be old enough and the log to be cleaned", 60*time.Second, func() bool {
		round(100 + int(time.Now().UnixNano()%1000))
		time.Sleep(300 * time.Millisecond)
		return driveCount(t, drive, "ckpt") <= 3 && driveCount(t, drive, "log") < 9
	})
	t.Logf("drive after the cleanup: %d segments, %d checkpoints", driveCount(t, drive, "log"), driveCount(t, drive, "ckpt"))
	if err := follower.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	checkFiles(t, follower, want)
	fresh, _ := openCounting(t, drive, t.TempDir(), nil)
	defer fresh.Close(ctx)
	checkFiles(t, fresh, want)
}

// A volume that has only segments (no checkpoint yet) gets one from MaintainLog,
// and the next volume to open reads that checkpoint and no segment at all.
func TestLogMaintainMakesTheNextOpenCheap(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	w, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) { c.CheckpointEvery = -1 })
	want := map[string][]byte{}
	for i := 0; i < 15; i++ {
		for p, d := range putMany(t, w, fmt.Sprintf("seg%d", i), 5) {
			want[p] = d
		}
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if driveCount(t, drive, "ckpt") != 0 || driveCount(t, drive, "log") < 15 {
		t.Fatalf("setup: %d checkpoints, %d segments", driveCount(t, drive, "ckpt"), driveCount(t, drive, "log"))
	}
	slow, slowOps := openCounting(t, drive, t.TempDir(), nil)
	t.Logf("before the maintenance a new volume reads: %s", slowOps)
	slow.Close(ctx)

	m, _ := openCounting(t, drive, t.TempDir(), nil)
	rep, err := m.MaintainLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("maintain: %+v", rep)
	if rep.CheckpointSeq == 0 || driveCount(t, drive, "ckpt") != 1 {
		t.Fatalf("report %+v, %d checkpoints on the drive", rep, driveCount(t, drive, "ckpt"))
	}
	if l, ok := readLeaseFile(t, drive); !ok || !l.Released || !m.ReadOnly() {
		t.Errorf("the lease after the maintenance: %+v, reader %v", l, m.ReadOnly())
	}
	m.Close(ctx)

	r, oc := openCounting(t, drive, t.TempDir(), nil)
	defer r.Close(ctx)
	checkFiles(t, r, want)
	t.Logf("after the maintenance a new volume reads: %s", oc)
	if oc.segReads.Load() != 0 || oc.ckptReads.Load() != 1 {
		t.Errorf("a new volume read %d segments and %d checkpoints, want 0 and 1", oc.segReads.Load(), oc.ckptReads.Load())
	}
}
