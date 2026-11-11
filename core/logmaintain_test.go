package core_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
)

func writeRounds(t *testing.T, v *core.Volume, want map[string][]byte, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		for p, d := range putMany(t, v, fmt.Sprintf("%s%d", prefix, i), 5) {
			want[p] = d
		}
		if err := v.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

// MaintainLog writes a checkpoint only when a segment came after the newest one
// on the drive.
func TestLogMaintainWritesNoCheckpointWithoutNewSegments(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, _ := openCounting(t, drive, t.TempDir(), func(c *core.Config) { c.CheckpointEvery = -1 })
	defer v.Close(ctx)
	want := map[string][]byte{}
	writeRounds(t, v, want, "a", 6)

	first, err := v.MaintainLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !first.CheckpointWritten || driveCount(t, drive, "ckpt") != 1 {
		t.Fatalf("first maintain: %+v, %d checkpoints", first, driveCount(t, drive, "ckpt"))
	}
	for i := 0; i < 2; i++ {
		again, err := v.MaintainLog(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if again.CheckpointWritten || again.CheckpointSeq != first.CheckpointSeq || driveCount(t, drive, "ckpt") != 1 {
			t.Fatalf("maintain #%d with nothing new: %+v, %d checkpoints (first %+v)", i+2, again, driveCount(t, drive, "ckpt"), first)
		}
	}
	writeRounds(t, v, want, "b", 2)
	next, err := v.MaintainLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !next.CheckpointWritten || next.CheckpointSeq <= first.CheckpointSeq || driveCount(t, drive, "ckpt") != 2 {
		t.Fatalf("maintain after new writes: %+v, %d checkpoints", next, driveCount(t, drive, "ckpt"))
	}
	t.Logf("first %+v, with nothing new: no checkpoint (1 on the drive), after new writes %+v (2 on the drive)", first, next)
}

// A checkpoint upload that fails does not stop the cleanup: the checkpoint that
// is already on the drive and older than the safety time covers the segments. A
// checkpoint superseded by a newer one is deleted once that is old enough, and a
// fresh volume then opens with one checkpoint and reads every file.
func TestLogMaintainCleansWhenTheCheckpointUploadFails(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	v, oc := openCounting(t, drive, t.TempDir(), func(c *core.Config) {
		c.CheckpointEvery = -1
		c.CleanupSafety = 300 * time.Millisecond
	})
	defer v.Close(ctx)
	want := map[string][]byte{}
	writeRounds(t, v, want, "a", 6)
	first, err := v.MaintainLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segsAfterFirst := driveCount(t, drive, "log")
	time.Sleep(500 * time.Millisecond) // the checkpoint is now older than the margin

	writeRounds(t, v, want, "b", 3)
	segsBefore := driveCount(t, drive, "log")
	oc.failCkptPut.Store(true)
	failed, err := v.MaintainLog(ctx)
	if err == nil {
		t.Fatal("maintain did not report the failed checkpoint upload")
	}
	if failed.CheckpointWritten || failed.SegmentsDeleted == 0 {
		t.Fatalf("failed maintain: %+v", failed)
	}
	segsAfter := driveCount(t, drive, "log")
	if segsAfter != segsBefore-failed.SegmentsDeleted || segsAfter >= segsBefore {
		t.Fatalf("segments %d before, %d after, report %+v", segsBefore, segsAfter, failed)
	}
	if driveCount(t, drive, "ckpt") != 1 {
		t.Fatalf("%d checkpoints after the failed upload", driveCount(t, drive, "ckpt"))
	}
	t.Logf("first checkpoint at %d with %d segments; upload refused: %d segments before, %d after (%d deleted), error %v",
		first.CheckpointSeq, segsAfterFirst, segsBefore, segsAfter, failed.SegmentsDeleted, err)

	oc.failCkptPut.Store(false)
	second, err := v.MaintainLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !second.CheckpointWritten || driveCount(t, drive, "ckpt") != 2 {
		t.Fatalf("maintain after the drive recovered: %+v, %d checkpoints", second, driveCount(t, drive, "ckpt"))
	}
	time.Sleep(500 * time.Millisecond)
	last, err := v.MaintainLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if last.CheckpointWritten || last.CheckpointsDeleted != 1 || last.DeletesFailed != 0 {
		t.Fatalf("last maintain: %+v", last)
	}
	if c, s := driveCount(t, drive, "ckpt"), driveCount(t, drive, "log"); c != 1 || s != 0 {
		t.Fatalf("drive has %d checkpoints and %d segments, want 1 and 0", c, s)
	}
	t.Logf("last maintain: %+v; drive: 1 checkpoint, 0 segments", last)

	fresh, fc := openCounting(t, drive, t.TempDir(), nil)
	defer fresh.Close(ctx)
	checkFiles(t, fresh, want)
	if fc.ckptReads.Load() != 1 || fc.segReads.Load() != 0 {
		t.Errorf("a fresh volume read %d checkpoints and %d segments, want 1 and 0", fc.ckptReads.Load(), fc.segReads.Load())
	}
	rep, err := fresh.Fsck(ctx, core.FsckOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		t.Fatalf("fsck: %v", rep.Problems)
	}
}
