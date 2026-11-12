// Command crashwriter is the workload process of the crash tests. It opens a
// volume directly (no VFS in between), runs several workers that each own a
// private part of the namespace and perform a deterministic random sequence of
// Put/Remove/Move, and logs every operation to an event file, fsynced, before
// ("try") and after ("ack") it: an "ack" means the volume returned success.
// The test kills it (CFS_CRASHPOINT or at a random time) and checks the volume
// against the events.
package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	_ "github.com/Ironboxplus/cfs/sqlitedrv"
	"github.com/Ironboxplus/cfs/store"
	"github.com/Ironboxplus/cfs/store/rclonestore"
	_ "github.com/rclone/rclone/backend/local"
	_ "github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/fs"
)

// Event is one line of the event file.
type Event struct {
	W    int    `json:"w"`
	Ev   string `json:"ev"` // try | ack | err
	Op   string `json:"op"` // put | rm | mv
	Path string `json:"p"`
	To   string `json:"to,omitempty"`
	SHA1 string `json:"sha1,omitempty"`
	Size int    `json:"sz,omitempty"`
}

func main() {
	var (
		remote    = flag.String("remote", "", "rclone remote string of the drive")
		state     = flag.String("state", "", "state dir")
		events    = flag.String("events", "", "event file")
		workers   = flag.Int("workers", 4, "workers")
		ops       = flag.Int("ops", 100, "operations per worker")
		seed      = flag.Uint64("seed", 1, "seed")
		threshold = flag.Int64("threshold", 256<<10, "threshold")
		transfers = flag.Int("transfers", 8, "transfers")
		meta      = flag.Int("meta", 1, "meta inflight")
		pause     = flag.Duration("pause", 0, "pause between operations")
		ckpt      = flag.Int("ckpt", 0, "checkpoint every N segments (0: default)")
		ack       = flag.String("ack", "", "ack mode: staged or durable (default: the volume's)")
		packFiles = flag.Int("packfiles", 0, "seal a pack at this many files (0: default); small values make many packs")
	)
	flag.Parse()
	ctx := context.Background()
	f, err := fs.NewFs(ctx, *remote)
	if err != nil {
		fmt.Fprintln(os.Stderr, "drive:", err)
		os.Exit(2)
	}
	var st store.Store = rclonestore.New(f)
	st, _ = aspect.Stack(st, aspect.Options{Gate: aspect.GateOpts{MetaInflight: *meta, Transfers: *transfers},
		Retry: aspect.RetryOpts{Base: 20 * time.Millisecond, Max: 200 * time.Millisecond}})
	cfg := testutil.FastConfig(*state)
	cfg.Threshold = *threshold
	cfg.Pipeline = *transfers
	if *ckpt != 0 {
		cfg.CheckpointEvery = *ckpt
	}
	if *ack != "" {
		cfg.Ack = core.AckMode(*ack)
	}
	if *packFiles > 0 {
		cfg.PackMaxFiles = *packFiles
	}
	v, err := core.Open(ctx, st, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	ef, err := os.OpenFile(*events, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "events:", err)
		os.Exit(2)
	}
	var mu sync.Mutex
	emit := func(e Event) {
		b, _ := json.Marshal(e)
		mu.Lock()
		ef.Write(append(b, '\n'))
		ef.Sync()
		mu.Unlock()
	}
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, v, w, *seed, *ops, *threshold, *pause, emit)
		}()
	}
	wg.Wait()
	// A change that no pack is open to carry: the log of it is a segment of its
	// own, which the crash point meta_log_uploaded waits for. Everything staged
	// is committed first, or (staged ack) the commit of a pack still to come
	// carries the change in its own segment and that point is never reached.
	if err := v.Flush(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "flush:", err)
	}
	if err := v.Mkdir(ctx, "zz-final"); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir:", err)
	}
	if err := v.Close(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "close:", err)
		os.Exit(3)
	}
	ef.Close()
}

func sum(b []byte) string { s := sha1.Sum(b); return hex.EncodeToString(s[:]) }

// Content is deterministic from (seed, worker, op index, size) so the test can
// regenerate it.
func Content(seed uint64, w, i, size int) []byte {
	rng := rand.New(rand.NewPCG(seed^uint64(w)<<32, uint64(i)))
	b := make([]byte, size)
	for j := range b {
		b[j] = byte(rng.Uint32())
	}
	return b
}

func worker(ctx context.Context, v *core.Volume, w int, seed uint64, ops int, threshold int64, pause time.Duration, emit func(Event)) {
	rng := rand.New(rand.NewPCG(seed, uint64(w)+1000))
	live := map[string]bool{}
	name := func() string { return fmt.Sprintf("w%d/d%d/f%d", w, rng.IntN(4), rng.IntN(24)) }
	pick := func() string {
		if len(live) == 0 {
			return ""
		}
		k := rng.IntN(len(live))
		for p := range live {
			if k == 0 {
				return p
			}
			k--
		}
		return ""
	}
	for i := 0; i < ops; i++ {
		if pause > 0 {
			time.Sleep(pause)
		}
		switch r := rng.IntN(100); {
		case r < 62 || len(live) == 0: // put
			p := name()
			size := []int{0, 10, 500, 3000, 40000}[rng.IntN(5)]
			if rng.IntN(8) == 0 {
				size = int(threshold) + rng.IntN(1000)
			}
			data := Content(seed, w, i, size)
			ev := Event{W: w, Op: "put", Path: p, SHA1: sum(data), Size: size}
			ev.Ev = "try"
			emit(ev)
			if _, err := v.Put(ctx, p, bytes.NewReader(data), int64(size), time.Now()); err != nil {
				ev.Ev = "err"
				emit(ev)
				fmt.Fprintln(os.Stderr, "put:", err)
				continue
			}
			ev.Ev = "ack"
			emit(ev)
			live[p] = true
		case r < 82: // remove
			p := pick()
			ev := Event{W: w, Op: "rm", Path: p, Ev: "try"}
			emit(ev)
			if err := v.Remove(ctx, p); err != nil {
				ev.Ev = "err"
				emit(ev)
				fmt.Fprintln(os.Stderr, "remove:", err)
				continue
			}
			ev.Ev = "ack"
			emit(ev)
			delete(live, p)
		default: // move
			p, to := pick(), name()
			if p == to {
				continue
			}
			ev := Event{W: w, Op: "mv", Path: p, To: to, Ev: "try"}
			emit(ev)
			if err := v.Move(ctx, p, to); err != nil {
				ev.Ev = "err"
				emit(ev)
				fmt.Fprintln(os.Stderr, "move:", err)
				continue
			}
			ev.Ev = "ack"
			emit(ev)
			delete(live, p)
			live[to] = true
		}
	}
}
