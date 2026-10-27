package core

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/Ironboxplus/cfs/store"
)

// Open returns a reader for n bytes of the file at p starting at off (n < 0:
// to the end). A packed file is read as one verified member; a large file is
// read in sequential ranged requests of Config.ReadChunk, each fully read and
// closed before the next, so the drive's transfer slot is never held while the
// consumer is slow.
func (v *Volume) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
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
	r, ok, err := v.ix.get(p)
	if err != nil {
		return nil, err
	}
	if ok {
		data, err := v.memberBytes(ctx, r)
		if err != nil {
			return nil, err
		}
		return sliceReader(data, off, n), nil
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
	return &chunkReader{v: v, ctx: ctx, p: p, pos: off, end: end}, nil
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

type chunkReader struct {
	v        *Volume
	ctx      context.Context
	p        string
	pos, end int64
	buf      []byte
	bufOff   int64
}

func (c *chunkReader) Read(b []byte) (int, error) {
	if c.pos >= c.end {
		return 0, io.EOF
	}
	if c.pos < c.bufOff || c.pos >= c.bufOff+int64(len(c.buf)) {
		n := min(c.v.cfg.ReadChunk, c.end-c.pos)
		rc, err := c.v.st.Open(c.ctx, c.p, c.pos, n)
		if err != nil {
			return 0, err
		}
		buf, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return 0, err
		}
		if int64(len(buf)) < n {
			return 0, fmt.Errorf("cfs: %s: drive returned %d bytes for a %d byte range at %d: %w", c.p, len(buf), n, c.pos, io.ErrUnexpectedEOF)
		}
		c.buf, c.bufOff = buf[:n], c.pos
	}
	k := copy(b, c.buf[c.pos-c.bufOff:])
	c.pos += int64(k)
	return k, nil
}

func (c *chunkReader) Close() error { c.buf = nil; return nil }

// memberBytes reads a packed file's content, verifying size and SHA-1.
func (v *Volume) memberBytes(ctx context.Context, r fileRow) ([]byte, error) {
	if r.Size == 0 {
		return nil, nil
	}
	key := fmt.Sprintf("%s@%d", r.Pack, r.Off)
	if d, ok := v.mc.get(key); ok {
		return d, nil
	}
	rc, err := v.st.Open(ctx, CtlDir+"/packs/"+r.Pack, r.Off, r.CSize)
	if err != nil {
		return nil, fmt.Errorf("cfs: read pack %s for %s: %w", r.Pack, r.Path, err)
	}
	raw, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, fmt.Errorf("cfs: read pack %s for %s: %w", r.Pack, r.Path, err)
	}
	if int64(len(raw)) != r.CSize {
		return nil, fmt.Errorf("cfs: pack %s: got %d bytes for %s, expected %d: %w", r.Pack, len(raw), r.Path, r.CSize, ErrCorrupt)
	}
	data := raw
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
}
