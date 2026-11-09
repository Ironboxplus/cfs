// Package rclonestore adapts any rclone fs.Fs (a local directory, WebDAV,
// OpenList's rclonefs, ...) to the store.Store interface.
package rclonestore

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Ironboxplus/cfs/store"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
)

const objTTL = 30 * time.Second

// Store implements store.Store on an rclone Fs.
type Store struct {
	f fs.Fs

	mu   sync.Mutex
	objs map[string]cachedObj // objects seen by List/Stat/Put, so a ranged Open needs no lookup
}

type cachedObj struct {
	o  fs.Object
	at time.Time
}

var _ store.Store = (*Store)(nil)

// New wraps f. Paths given to the Store are relative to f's root.
func New(f fs.Fs) *Store { return &Store{f: f, objs: map[string]cachedObj{}} }

// Fs returns the wrapped rclone Fs.
func (s *Store) Fs() fs.Fs { return s.f }

func (s *Store) Features() store.Features {
	return store.Features{
		SHA1:             s.f.Hashes().Contains(hash.SHA1),
		ModTimePrecision: s.f.Precision(),
	}
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrorObjectNotFound), errors.Is(err, fs.ErrorDirNotFound):
		return store.ErrNotFound
	case errors.Is(err, fs.ErrorIsDir):
		return store.ErrIsDir
	case errors.Is(err, fs.ErrorIsFile):
		return store.ErrNotDir
	case errors.Is(err, fs.ErrorDirectoryNotEmpty):
		return store.ErrNotEmpty
	case errors.Is(err, fs.ErrorDirExists):
		return store.ErrExists
	case errors.Is(err, fs.ErrorCantSetModTime), errors.Is(err, fs.ErrorCantDirMove), errors.Is(err, fs.ErrorCantMove):
		return store.ErrNotSupported
	}
	return err
}

func (s *Store) remember(o fs.Object) {
	s.mu.Lock()
	if len(s.objs) > 4096 {
		s.objs = map[string]cachedObj{}
	}
	s.objs[o.Remote()] = cachedObj{o, time.Now()}
	s.mu.Unlock()
}

func (s *Store) forget(p string) {
	s.mu.Lock()
	delete(s.objs, p)
	s.mu.Unlock()
}

func (s *Store) forgetTree(p string) {
	s.mu.Lock()
	for k := range s.objs {
		if k == p || strings.HasPrefix(k, p+"/") {
			delete(s.objs, k)
		}
	}
	s.mu.Unlock()
}

func (s *Store) cached(p string) fs.Object {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.objs[p]
	if !ok || time.Since(c.at) > objTTL {
		delete(s.objs, p)
		return nil
	}
	return c.o
}

func (s *Store) object(ctx context.Context, p string) (fs.Object, error) {
	if o := s.cached(p); o != nil {
		return o, nil
	}
	o, err := s.f.NewObject(ctx, p)
	if err != nil {
		return nil, mapErr(err)
	}
	s.remember(o)
	return o, nil
}

func entryOf(o fs.Object, ctx context.Context) store.Entry {
	return store.Entry{Path: o.Remote(), Size: o.Size(), ModTime: o.ModTime(ctx)}
}

func (s *Store) List(ctx context.Context, dir string) ([]store.Entry, error) {
	ents, err := s.f.List(ctx, dir)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]store.Entry, 0, len(ents))
	for _, e := range ents {
		switch v := e.(type) {
		case fs.Object:
			s.remember(v)
			out = append(out, entryOf(v, ctx))
		case fs.Directory:
			out = append(out, store.Entry{Path: v.Remote(), IsDir: true, ModTime: v.ModTime(ctx)})
		}
	}
	return out, nil
}

func (s *Store) Stat(ctx context.Context, p string) (store.Entry, error) {
	if p == "" {
		return store.Entry{IsDir: true}, nil
	}
	s.forget(p)
	o, err := s.f.NewObject(ctx, p)
	if err == nil {
		s.remember(o)
		return entryOf(o, ctx), nil
	}
	if !errors.Is(err, fs.ErrorObjectNotFound) && !errors.Is(err, fs.ErrorIsDir) {
		return store.Entry{}, mapErr(err)
	}
	// Maybe a directory.
	if _, lerr := s.f.List(ctx, p); lerr != nil {
		if errors.Is(lerr, fs.ErrorDirNotFound) || errors.Is(lerr, fs.ErrorIsFile) {
			return store.Entry{}, store.ErrNotFound
		}
		return store.Entry{}, mapErr(lerr)
	}
	return store.Entry{Path: p, IsDir: true}, nil
}

func (s *Store) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	o, err := s.object(ctx, p)
	if err != nil {
		return nil, err
	}
	var opts []fs.OpenOption
	if off > 0 || n >= 0 {
		end := int64(-1)
		if n >= 0 {
			end = off + n - 1
		}
		if n == 0 {
			return io.NopCloser(strings.NewReader("")), nil
		}
		opts = append(opts, &fs.RangeOption{Start: off, End: end})
	}
	// operations.Open reads through ReOpen: a read that breaks off in the middle is
	// reopened at the byte where it stopped (at most LowLevelRetries times), the way
	// rclone copies do.
	rc, err := operations.Open(ctx, o, opts...)
	if err != nil {
		s.forget(p)
		return nil, mapErr(err)
	}
	return rc, nil
}

// OpenStream is Open: nothing here holds a slot for the body, and a break in the
// middle is repaired by operations.Open either way.
func (s *Store) OpenStream(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	return s.Open(ctx, p, off, n)
}

func (s *Store) Put(ctx context.Context, p string, src store.Source, o store.PutOpts) error {
	rc, err := src.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	mt := o.ModTime
	if mt.IsZero() {
		mt = time.Now()
	}
	var hashes map[hash.Type]string
	if o.SHA1 != "" {
		hashes = map[hash.Type]string{hash.SHA1: o.SHA1}
	}
	var info fs.ObjectInfo = object.NewStaticObjectInfo(p, mt, o.Size, true, hashes, s.f)
	if o.Ranged != nil && o.Size >= 0 {
		info = &rangeObject{f: s.f, remote: p, size: o.Size, modTime: mt, sha1: o.SHA1, r: o.Ranged}
	}
	s.forget(p)
	var res fs.Object
	if old, err := s.f.NewObject(ctx, p); err == nil {
		err = old.Update(ctx, rc, info)
		if err != nil {
			return mapErr(err)
		}
		res = old
	} else if errors.Is(err, fs.ErrorObjectNotFound) {
		res, err = s.f.Put(ctx, rc, info)
		if err != nil {
			return mapErr(err)
		}
	} else {
		return mapErr(err)
	}
	if res != nil {
		s.remember(res)
	}
	return nil
}

func (s *Store) Remove(ctx context.Context, p string) error {
	o, err := s.object(ctx, p)
	if err != nil {
		return err
	}
	s.forget(p)
	return mapErr(o.Remove(ctx))
}

func (s *Store) Move(ctx context.Context, src, dst string) error {
	o, err := s.object(ctx, src)
	if err != nil {
		return err
	}
	s.forget(src)
	s.forget(dst)
	_, err = operations.Move(ctx, s.f, nil, dst, o)
	return mapErr(err)
}

func (s *Store) DirMove(ctx context.Context, src, dst string) error {
	s.forgetTree(src)
	s.forgetTree(dst)
	return mapErr(operations.DirMove(ctx, s.f, src, dst))
}

func (s *Store) Purge(ctx context.Context, dir string) error {
	s.forgetTree(dir)
	if err := operations.Purge(ctx, s.f, dir); err != nil {
		return mapErr(err)
	}
	return nil
}

func (s *Store) Mkdir(ctx context.Context, dir string) error {
	return mapErr(s.f.Mkdir(ctx, dir))
}

func (s *Store) Rmdir(ctx context.Context, dir string) error {
	return mapErr(s.f.Rmdir(ctx, dir))
}

func (s *Store) SetModTime(ctx context.Context, p string, t time.Time) error {
	o, err := s.object(ctx, p)
	if err != nil {
		return err
	}
	return mapErr(o.SetModTime(ctx, t))
}

func (s *Store) Hash(ctx context.Context, p string) (string, error) {
	if !s.f.Hashes().Contains(hash.SHA1) {
		return "", nil
	}
	o, err := s.object(ctx, p)
	if err != nil {
		return "", err
	}
	h, err := o.Hash(ctx, hash.SHA1)
	if errors.Is(err, hash.ErrUnsupported) {
		return "", nil
	}
	return h, mapErr(err)
}

var _ = path.Join
