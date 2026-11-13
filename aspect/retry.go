package aspect

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"time"

	"github.com/Ironboxplus/cfs/store"
)

// RetryOpts control Retry.
type RetryOpts struct {
	Attempts int           // total tries per call, default 4
	Base     time.Duration // first back-off, default 500ms; doubles each time
	Max      time.Duration // back-off cap, default 10s
}

func (o *RetryOpts) fill() {
	if o.Attempts <= 0 {
		o.Attempts = 4
	}
	if o.Base <= 0 {
		o.Base = 500 * time.Millisecond
	}
	if o.Max <= 0 {
		o.Max = 10 * time.Second
	}
}

type retry struct {
	store.Store
	o RetryOpts
}

// Retry repeats calls that failed for a reason that may go away (network,
// 429/5xx, timeouts) and gives up at once on definite answers (not found,
// exists, not empty, ...) and on cancellation. It also makes the mutating calls
// safe to repeat: a Put that errored after the object really landed is
// recognised by size/hash, and a Move/Remove/Rmdir that errored after taking
// effect counts as done. The slot of the gate below is free while it backs off.
func Retry(s store.Store, o RetryOpts) store.Store {
	o.fill()
	return &retry{Store: s, o: o}
}

func definite(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrExists) ||
		errors.Is(err, store.ErrNotEmpty) || errors.Is(err, store.ErrIsDir) ||
		errors.Is(err, store.ErrNotDir) || errors.Is(err, store.ErrNotSupported) || errors.Is(err, store.ErrChanged) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (r *retry) wait(ctx context.Context, attempt int) error {
	d := r.o.Base << attempt
	if d > r.o.Max || d <= 0 {
		d = r.o.Max
	}
	d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// do runs f up to Attempts times. onErr may turn a failed attempt into success.
func (r *retry) do(ctx context.Context, f func() error, onErr func(error) bool) error {
	var err error
	for i := 0; i < r.o.Attempts; i++ {
		if i > 0 {
			if werr := r.wait(ctx, i-1); werr != nil {
				return err
			}
		}
		err = f()
		if err == nil {
			return nil
		}
		if onErr != nil && onErr(err) {
			return nil
		}
		if definite(err) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

func (r *retry) List(ctx context.Context, dir string) (out []store.Entry, err error) {
	err = r.do(ctx, func() (e error) { out, e = r.Store.List(ctx, dir); return }, nil)
	return
}

func (r *retry) Stat(ctx context.Context, p string) (out store.Entry, err error) {
	err = r.do(ctx, func() (e error) { out, e = r.Store.Stat(ctx, p); return }, nil)
	return
}

func (r *retry) Open(ctx context.Context, p string, off, n int64) (out io.ReadCloser, err error) {
	err = r.do(ctx, func() (e error) { out, e = r.Store.Open(ctx, p, off, n); return }, nil)
	return
}

func (r *retry) OpenStream(ctx context.Context, p string, off, n int64, want store.Entry) (out io.ReadCloser, err error) {
	err = r.do(ctx, func() (e error) { out, e = r.Store.OpenStream(ctx, p, off, n, want); return }, nil)
	return
}

func (r *retry) Hash(ctx context.Context, p string) (out string, err error) {
	err = r.do(ctx, func() (e error) { out, e = r.Store.Hash(ctx, p); return }, nil)
	return
}

func (r *retry) Mkdir(ctx context.Context, dir string) error {
	return r.do(ctx, func() error { return r.Store.Mkdir(ctx, dir) }, func(e error) bool { return errors.Is(e, store.ErrExists) })
}

func (r *retry) SetModTime(ctx context.Context, p string, t time.Time) error {
	return r.do(ctx, func() error { return r.Store.SetModTime(ctx, p, t) }, nil)
}

func (r *retry) Remove(ctx context.Context, p string) error {
	attempt := 0
	return r.do(ctx, func() error { attempt++; return r.Store.Remove(ctx, p) },
		func(e error) bool { return attempt > 1 && errors.Is(e, store.ErrNotFound) })
}

func (r *retry) Purge(ctx context.Context, dir string) error {
	attempt := 0
	return r.do(ctx, func() error { attempt++; return r.Store.Purge(ctx, dir) },
		func(e error) bool { return attempt > 1 && errors.Is(e, store.ErrNotFound) })
}

func (r *retry) Rmdir(ctx context.Context, dir string) error {
	attempt := 0
	return r.do(ctx, func() error { attempt++; return r.Store.Rmdir(ctx, dir) },
		func(e error) bool { return attempt > 1 && errors.Is(e, store.ErrNotFound) })
}

func (r *retry) Move(ctx context.Context, src, dst string) error {
	attempt := 0
	return r.do(ctx, func() error { attempt++; return r.Store.Move(ctx, src, dst) },
		func(e error) bool {
			if attempt > 1 && errors.Is(e, store.ErrNotFound) {
				_, serr := r.Store.Stat(ctx, dst)
				return serr == nil
			}
			return false
		})
}

func (r *retry) DirMove(ctx context.Context, src, dst string) error {
	attempt := 0
	return r.do(ctx, func() error { attempt++; return r.Store.DirMove(ctx, src, dst) },
		func(e error) bool {
			if attempt > 1 {
				if _, serr := r.Store.Stat(ctx, dst); serr == nil {
					if _, serr := r.Store.Stat(ctx, src); errors.Is(serr, store.ErrNotFound) {
						return true
					}
				}
			}
			return false
		})
}

func (r *retry) Put(ctx context.Context, p string, src store.Source, o store.PutOpts) error {
	attempts := r.o.Attempts
	if !src.Replayable() {
		attempts = 1
	}
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			if werr := r.wait(ctx, i-1); werr != nil {
				return err
			}
		}
		err = r.Store.Put(ctx, p, src, o)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		// The upload may have landed even though the call failed. Accept an
		// object of the right size when its hash matches, or when the name is
		// never reused for other content (Unique).
		if o.Size >= 0 && !errors.Is(err, store.ErrNotSupported) {
			e, serr := r.Store.Stat(ctx, p)
			if serr == nil && !e.IsDir && e.Size == o.Size {
				h := e.SHA1
				if h == "" && o.SHA1 != "" {
					h, _ = r.Store.Hash(ctx, p)
				}
				if (h != "" && h == o.SHA1) || (h == "" && o.Unique) {
					return nil
				}
			}
		}
		if definite(err) {
			return err
		}
	}
	return err
}
