// Package aspect holds the cross-cutting concerns of cfs as Store decorators.
// Nothing here is known to core: in-flight limits (gate), retry, call metering
// and log redaction all wrap a store.Store.
package aspect

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/Ironboxplus/cfs/store"
)

// Class is a concurrency class. Metadata calls and data transfers have
// separate in-flight limits: a drive rate-limits listing very differently from
// moving bytes.
type Class int

const (
	Meta     Class = iota // List/Stat/Remove/Move/DirMove/Mkdir/Rmdir/SetModTime/Hash
	Transfer              // Put and ranged Open
)

func (c Class) String() string {
	if c == Meta {
		return "meta"
	}
	return "transfer"
}

// GateOpts are the in-flight limits. API request rate (QPS) is the business of
// the layer below (OpenList limit_rate); the gate only bounds concurrency.
type GateOpts struct {
	MetaInflight int // default 1
	Transfers    int // default 8
}

func (o *GateOpts) fill() {
	if o.MetaInflight <= 0 {
		o.MetaInflight = 1
	}
	if o.Transfers <= 0 {
		o.Transfers = 8
	}
}

type gate struct {
	store.Store
	sem [2]chan struct{}
}

// Gate limits the calls reaching s: at most MetaInflight metadata calls and at
// most Transfers data transfers at a time. A transfer holds its slot from the
// start of Put or Open until the Put returns or the reader is closed (or hits
// EOF), so callers must read ranges in bounded chunks and close promptly.
func Gate(s store.Store, o GateOpts) store.Store {
	o.fill()
	return &gate{Store: s, sem: [2]chan struct{}{
		make(chan struct{}, o.MetaInflight), make(chan struct{}, o.Transfers),
	}}
}

func (g *gate) acquire(ctx context.Context, c Class) error {
	select {
	case g.sem[c] <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gate) release(c Class) { <-g.sem[c] }

func (g *gate) List(ctx context.Context, dir string) ([]store.Entry, error) {
	if err := g.acquire(ctx, Meta); err != nil {
		return nil, err
	}
	defer g.release(Meta)
	return g.Store.List(ctx, dir)
}

func (g *gate) Stat(ctx context.Context, p string) (store.Entry, error) {
	if err := g.acquire(ctx, Meta); err != nil {
		return store.Entry{}, err
	}
	defer g.release(Meta)
	return g.Store.Stat(ctx, p)
}

func (g *gate) Remove(ctx context.Context, p string) error {
	if err := g.acquire(ctx, Meta); err != nil {
		return err
	}
	defer g.release(Meta)
	return g.Store.Remove(ctx, p)
}

func (g *gate) Move(ctx context.Context, src, dst string) error {
	if err := g.acquire(ctx, Meta); err != nil {
		return err
	}
	defer g.release(Meta)
	return g.Store.Move(ctx, src, dst)
}

func (g *gate) DirMove(ctx context.Context, src, dst string) error {
	if err := g.acquire(ctx, Meta); err != nil {
		return err
	}
	defer g.release(Meta)
	return g.Store.DirMove(ctx, src, dst)
}

func (g *gate) Mkdir(ctx context.Context, dir string) error {
	if err := g.acquire(ctx, Meta); err != nil {
		return err
	}
	defer g.release(Meta)
	return g.Store.Mkdir(ctx, dir)
}

func (g *gate) Rmdir(ctx context.Context, dir string) error {
	if err := g.acquire(ctx, Meta); err != nil {
		return err
	}
	defer g.release(Meta)
	return g.Store.Rmdir(ctx, dir)
}

func (g *gate) SetModTime(ctx context.Context, p string, t time.Time) error {
	if err := g.acquire(ctx, Meta); err != nil {
		return err
	}
	defer g.release(Meta)
	return g.Store.SetModTime(ctx, p, t)
}

func (g *gate) Hash(ctx context.Context, p string) (string, error) {
	if err := g.acquire(ctx, Meta); err != nil {
		return "", err
	}
	defer g.release(Meta)
	return g.Store.Hash(ctx, p)
}

func (g *gate) Put(ctx context.Context, p string, src store.Source, o store.PutOpts) error {
	if err := g.acquire(ctx, Transfer); err != nil {
		return err
	}
	defer g.release(Transfer)
	return g.Store.Put(ctx, p, src, o)
}

func (g *gate) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	if err := g.acquire(ctx, Transfer); err != nil {
		return nil, err
	}
	rc, err := g.Store.Open(ctx, p, off, n)
	if err != nil {
		g.release(Transfer)
		return nil, err
	}
	return &slotReader{rc: rc, release: func() { g.release(Transfer) }}, nil
}

// slotReader returns its transfer slot at EOF or Close, whichever comes first.
type slotReader struct {
	rc      io.ReadCloser
	release func()
	once    sync.Once
}

func (r *slotReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if err != nil {
		r.once.Do(r.release)
	}
	return n, err
}

func (r *slotReader) Close() error {
	err := r.rc.Close()
	r.once.Do(r.release)
	return err
}
