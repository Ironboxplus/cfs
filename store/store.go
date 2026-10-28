// Package store is the object-level interface cfs needs from the drive below
// it. Paths are slash separated, relative to the store root, without a leading
// slash; "" is the root. cfs never assumes more than this: no atomic rename
// over an existing name, no conditional writes, no listing consistency.
package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"time"
)

var (
	ErrNotFound     = errors.New("cfs/store: not found")
	ErrExists       = errors.New("cfs/store: already exists")
	ErrNotEmpty     = errors.New("cfs/store: directory not empty")
	ErrIsDir        = errors.New("cfs/store: is a directory")
	ErrNotDir       = errors.New("cfs/store: not a directory")
	ErrNotSupported = errors.New("cfs/store: not supported")
)

// Entry describes a file or directory.
type Entry struct {
	Path    string // full path relative to the store root
	IsDir   bool
	Size    int64
	ModTime time.Time
	SHA1    string // lower-case hex, only if the listing already carried it
}

// Name is the last path element.
func (e Entry) Name() string {
	for i := len(e.Path) - 1; i >= 0; i-- {
		if e.Path[i] == '/' {
			return e.Path[i+1:]
		}
	}
	return e.Path
}

// Features are the store's static capabilities.
type Features struct {
	// SHA1 reports that Hash can return a SHA-1 for stored files.
	SHA1 bool
	// ModTimePrecision is the resolution of stored modification times; a
	// negative value means they can't be set.
	ModTimePrecision time.Duration
}

// Source is the content of a Put. Retries need to read it again, which only a
// Replayable source allows.
type Source interface {
	Open() (io.ReadCloser, error)
	Replayable() bool
}

// PutOpts describe the object being written.
type PutOpts struct {
	Size    int64 // -1 if unknown
	ModTime time.Time
	SHA1    string // optional; lets a retry recognise an upload that did land
	// Unique says the name is never reused for different content (packs, log
	// segments), so an object of the right size is the upload that landed.
	Unique bool
}

// Store is implemented by rclonestore and wrapped by the aspect decorators.
type Store interface {
	// List returns the direct children of dir; ErrNotFound if dir is absent.
	List(ctx context.Context, dir string) ([]Entry, error)
	// Stat returns a file or directory; ErrNotFound if absent.
	Stat(ctx context.Context, p string) (Entry, error)
	// Open reads n bytes from off (n < 0: to the end). The caller must Close it.
	Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error)
	// OpenStream is Open for a stream whose consumer sets the pace (a large file
	// forwarded to a client that may pause for as long as it likes). Open is for
	// bounded reads that are read and closed at once, and a decorator may keep a
	// shared slot for them until the body is closed; a stream must not hold a shared
	// resource while it is open, so a decorator may only limit the call itself.
	OpenStream(ctx context.Context, p string, off, n int64) (io.ReadCloser, error)
	// Put writes (or overwrites) a file, creating missing parent directories.
	Put(ctx context.Context, p string, src Source, o PutOpts) error
	Remove(ctx context.Context, p string) error
	// Move renames a file; the destination's parent exists, the destination
	// itself is removed first by the caller.
	Move(ctx context.Context, src, dst string) error
	// DirMove renames a directory with its whole subtree to a name that
	// doesn't exist yet.
	DirMove(ctx context.Context, src, dst string) error
	// Purge removes a directory with everything in it, in as few calls as the
	// drive allows. ErrNotFound if it is absent.
	Purge(ctx context.Context, dir string) error
	Mkdir(ctx context.Context, dir string) error
	Rmdir(ctx context.Context, dir string) error
	SetModTime(ctx context.Context, p string, t time.Time) error
	// Hash returns the SHA-1 of a stored file, "" if the store has none.
	Hash(ctx context.Context, p string) (string, error)
	Features() Features
}

type onceSource struct {
	r    io.Reader
	used bool
}

// Once wraps a one-shot stream; it can't be retried.
func Once(r io.Reader) Source { return &onceSource{r: r} }

func (s *onceSource) Open() (io.ReadCloser, error) {
	if s.used {
		return nil, errors.New("cfs/store: one-shot source already consumed")
	}
	s.used = true
	if rc, ok := s.r.(io.ReadCloser); ok {
		return rc, nil
	}
	return io.NopCloser(s.r), nil
}
func (s *onceSource) Replayable() bool { return false }

type bytesSource []byte

// Bytes is a replayable in-memory source.
func Bytes(b []byte) Source { return bytesSource(b) }

func (s bytesSource) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s)), nil
}
func (s bytesSource) Replayable() bool { return true }

type fileSource string

// File is a replayable source reading a local file.
func File(path string) Source { return fileSource(path) }

func (s fileSource) Open() (io.ReadCloser, error) { return os.Open(string(s)) }
func (s fileSource) Replayable() bool             { return true }

type seekSource struct {
	rs    io.ReadSeeker
	start int64
}

// Seekable wraps a seekable stream as a replayable source: every Open starts
// again at the position the stream had when it was wrapped.
func Seekable(rs io.ReadSeeker) (Source, error) {
	start, err := rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return &seekSource{rs, start}, nil
}

func (s *seekSource) Open() (io.ReadCloser, error) {
	if _, err := s.rs.Seek(s.start, io.SeekStart); err != nil {
		return nil, err
	}
	return io.NopCloser(s.rs), nil
}
func (s *seekSource) Replayable() bool { return true }
