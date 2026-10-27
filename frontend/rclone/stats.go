package cfsrclone

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

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
			_ = CloseAll(ctx)
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
		b, err := json.Marshal(ref.meter.Snapshot())
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
