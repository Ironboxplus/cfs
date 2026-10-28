package cfsrclone

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/atexit"
)

var exitOnce sync.Once

// registerExit makes the process flush and close every volume when rclone
// exits normally or on a signal (not on a crash, which is the point of the
// crash tests).
func registerExit() {
	exitOnce.Do(func() {
		atexit.Register(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			fs.Logf(nil, "cfs: exit: closing the volumes")
			err := CloseAll(ctx)
			fs.Logf(nil, "cfs: exit: volumes closed: %v", err)
		})
	})
}

// startStats writes the meter of ref as JSON to the file named by the
// CFS_STATS_FILE environment variable every 300ms, for black-box tests that
// can only reach the process from outside.
func startStats(ref *volRef) {
	p := os.Getenv("CFS_STATS_FILE")
	if p == "" {
		return
	}
	write := func() {
		files, bytes := ref.v.Staged()
		b, err := json.Marshal(struct {
			aspect.Stats
			// Acknowledged small files that are not on the drive yet.
			StagedFiles int
			StagedBytes int64
			// Acknowledged staged files found missing or corrupt at open: data loss.
			StagedDamaged int64
		}{ref.meter.Snapshot(), files, bytes, ref.v.StagedDamaged()})
		if err != nil {
			return
		}
		tmp := p + ".tmp"
		if os.WriteFile(tmp, b, 0o644) == nil {
			os.Rename(tmp, p)
		}
	}
	go func() {
		t := time.NewTicker(300 * time.Millisecond)
		defer t.Stop()
		for range t.C {
			write()
		}
	}()
}
