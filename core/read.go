package core

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Ironboxplus/cfs/store"
	"golang.org/x/sync/semaphore"
)

// Open returns a reader for n bytes of the file at p starting at off (n < 0:
// to the end).
//
// A large file is the body of one ranged request for [off, off+n), handed to the
// caller as it is, the way OpenList forwards a range of a 115 file
// (internal/stream GetRangeReaderFromLink): nothing is read ahead or buffered
// here, and breaks in the middle of the body are repaired by the store below
// (store/rclonestore reads through rclone's operations.Open, which reopens at the
// byte where it stopped). The store holds no slot for the body (OpenStream): a client
// that pauses cannot starve the bounded reads.
//
// A packed or staged file is a member of bounded size (below Threshold): it is
// read, decoded and verified as a whole, as restic and JuiceFS read their blocks.
// What such reads hold in memory is capped by Config.ReadBuffer, and concurrent
// reads of one member share one download.
func (v *Volume) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	v.follow(ctx)
	p, err := clean(p)
	if err != nil {
		return nil, err
	}
	if reserved(p) {
		return nil, store.ErrNotFound
	}
	if off < 0 {
		off = 0
	}
	for try := 0; ; try++ {
		r, ok, err := v.ix.get(p)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		release, err := v.reserveRead(ctx, r)
		if err != nil {
			return nil, err
		}
		data, err := v.memberBytes(ctx, r)
		if err != nil {
			release()
			// The staged file went to its pack between the lookup and the read (on
			// Windows a file being deleted can't be opened, which is not ErrNotExist):
			// look the name up again.
			if r.Pack == "" && !errors.Is(err, ErrCorrupt) && try < 5 {
				time.Sleep(time.Duration(try) * 2 * time.Millisecond)
				continue
			}
			return nil, err
		}
		return &heldReader{ReadCloser: sliceReader(data, off, n), release: release}, nil
	}
	e, err := v.Stat(ctx, p)
	if err != nil {
		return nil, err
	}
	if e.IsDir {
		return nil, store.ErrIsDir
	}
	end := e.Size
	if n >= 0 && off+n < end {
		end = off + n
	}
	if off >= end {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return v.st.OpenStream(ctx, p, off, end-off)
}

func sliceReader(data []byte, off, n int64) io.ReadCloser {
	if off > int64(len(data)) {
		off = int64(len(data))
	}
	data = data[off:]
	if n >= 0 && n < int64(len(data)) {
		data = data[:n]
	}
	return io.NopCloser(bytes.NewReader(data))
}

// heldReader gives back the read budget its member took when it is closed.
type heldReader struct {
	io.ReadCloser
	release func()
}

func (h *heldReader) Close() error {
	err := h.ReadCloser.Close()
	h.release()
	return err
}

// reserveRead takes the memory a read of r holds (the compressed bytes and the
// decoded ones, which live until the reader is closed) from the global budget,
// waiting while the budget is spent. A member larger than the whole budget takes
// all of it, so it can still be read. The returned function gives it back.
func (v *Volume) reserveRead(ctx context.Context, r fileRow) (func(), error) {
	w := r.Size
	if r.Pack != "" {
		w += r.CSize
	}
	if w <= 0 {
		return func() {}, nil
	}
	w = min(w, v.cfg.ReadBuffer)
	if err := v.rb.Acquire(ctx, w); err != nil {
		return nil, err
	}
	used := v.rbUsed.Add(w)
	for {
		peak := v.rbPeak.Load()
		if used <= peak || v.rbPeak.CompareAndSwap(peak, used) {
			break
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			v.rbUsed.Add(-w)
			v.rb.Release(w)
		})
	}, nil
}

// ReadBufferStats reports the bytes that reads of packed and staged files hold
// now, and the most they have held at once since the volume was opened.
func (v *Volume) ReadBufferStats() (inUse, peak int64) { return v.rbUsed.Load(), v.rbPeak.Load() }

func newReadBuffer(limit int64) *semaphore.Weighted { return semaphore.NewWeighted(limit) }

// once runs fn for key unless a call for the same key is already running, in
// which case it waits for that one and shares its result (JuiceFS does this for
// the loads of one block, group.Execute). If the call it joined was cancelled
// while ours is not, ours runs again.
func (v *Volume) once(ctx context.Context, key string, fn func() (any, error)) (any, error) {
	for try := 0; ; try++ {
		res, err, _ := v.sf.Do(key, fn)
		if err != nil && try < 2 && ctx.Err() == nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			continue
		}
		return res, err
	}
}

// memberBytes reads a packed file's content, verifying size and SHA-1.
func (v *Volume) memberBytes(ctx context.Context, r fileRow) ([]byte, error) {
	if r.Size == 0 {
		return nil, nil
	}
	if r.Pack == "" {
		return v.readStaged(r)
	}
	key := fmt.Sprintf("%s@%d", r.Pack, r.Off)
	if d, ok := v.mc.get(key); ok {
		return d, nil
	}
	res, err := v.once(ctx, key, func() (any, error) {
		if d, ok := v.mc.get(key); ok {
			return d, nil
		}
		raw, err := v.packRange(ctx, r.Pack, r.Off, r.CSize)
		if err != nil {
			return nil, fmt.Errorf("cfs: read pack %s for %s: %w", r.Pack, r.Path, err)
		}
		if int64(len(raw)) != r.CSize {
			return nil, fmt.Errorf("cfs: pack %s: got %d bytes for %s, expected %d: %w", r.Pack, len(raw), r.Path, r.CSize, ErrCorrupt)
		}
		data := bytes.Clone(raw) // raw may alias a shared read window
		if r.Meth == 8 {
			data, err = io.ReadAll(flate.NewReader(bytes.NewReader(raw)))
			if err != nil {
				return nil, fmt.Errorf("cfs: pack %s: member %s: %v: %w", r.Pack, r.Path, err, ErrCorrupt)
			}
		}
		sum := sha1.Sum(data)
		if int64(len(data)) != r.Size || hex.EncodeToString(sum[:]) != r.SHA1 {
			return nil, fmt.Errorf("cfs: pack %s: member %s: %w", r.Pack, r.Path, ErrCorrupt)
		}
		v.mc.put(key, data)
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return res.([]byte), nil
}

func (v *Volume) packBytes(pack string) int64 {
	if x, ok := v.packSz.Load(pack); ok {
		return x.(int64)
	}
	var n int64
	if v.ix.db.QueryRow("SELECT bytes FROM packs WHERE name=?", pack).Scan(&n) == nil && n > 0 {
		v.packSz.Store(pack, n)
	}
	return n
}

// packRange returns n bytes of a pack at off. Small ranges are served from the
// aligned window around them, fetched once per window (concurrent readers of
// one window share the fetch).
func (v *Volume) packRange(ctx context.Context, pack string, off, n int64) ([]byte, error) {
	read := func(from, count int64) ([]byte, error) {
		rc, err := v.st.Open(ctx, CtlDir+"/packs/"+pack, from, count)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}
	w := v.cfg.ReadWindow
	if w > 0 && n <= w/2 && off/w == (off+n-1)/w {
		idx := off / w
		start := idx * w
		key := fmt.Sprintf("%s#%d", pack, idx)
		buf, ok := v.wc.get(key)
		if !ok {
			res, err := v.once(ctx, key, func() (any, error) {
				if b, ok := v.wc.get(key); ok {
					return b, nil
				}
				count := w
				if size := v.packBytes(pack); size > 0 && start+count > size {
					count = size - start
				}
				b, err := read(start, count)
				if err != nil {
					return nil, err
				}
				v.wc.put(key, b)
				return b, nil
			})
			if err != nil {
				return nil, err
			}
			buf = res.([]byte)
		}
		if off-start+n > int64(len(buf)) {
			return nil, fmt.Errorf("short read: pack window has %d bytes, member ends at %d: %w", len(buf), off-start+n, io.ErrUnexpectedEOF)
		}
		return buf[off-start : off-start+n], nil
	}
	return read(off, n)
}
