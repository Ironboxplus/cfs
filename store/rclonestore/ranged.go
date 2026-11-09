package rclonestore

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/Ironboxplus/cfs/store"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
)

// rangeObject is what a drive gets as the source of a Put when the bytes can be
// read by range. It is an fs.Object, so that a drive that range-reads its source
// (fs.UnWrapObjectInfo(src).Open with a RangeOption: OpenList's rclonefs does, to
// hash and upload a file without copying it first) finds it, and it knows the
// SHA-1 if the caller did. A drive that doesn't care reads the stream as ever.
type rangeObject struct {
	f       fs.Info
	remote  string
	size    int64
	modTime time.Time
	sha1    string
	r       store.RangeOpener
}

var _ fs.Object = (*rangeObject)(nil)

func (o *rangeObject) Fs() fs.Info                       { return o.f }
func (o *rangeObject) String() string                    { return o.remote }
func (o *rangeObject) Remote() string                    { return o.remote }
func (o *rangeObject) Size() int64                       { return o.size }
func (o *rangeObject) ModTime(context.Context) time.Time { return o.modTime }
func (o *rangeObject) Storable() bool                    { return true }

func (o *rangeObject) Hash(_ context.Context, ty hash.Type) (string, error) {
	if ty != hash.SHA1 {
		return "", hash.ErrUnsupported
	}
	return o.sha1, nil
}

func (o *rangeObject) Open(_ context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	off, n := int64(0), int64(-1)
	for _, opt := range options {
		switch x := opt.(type) {
		case *fs.SeekOption:
			off = x.Offset
		case *fs.RangeOption:
			off, n = x.Decode(o.size)
		}
	}
	if n == 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	return o.r.OpenRange(off, n)
}

func (o *rangeObject) SetModTime(context.Context, time.Time) error { return fs.ErrorCantSetModTime }

func (o *rangeObject) Update(context.Context, io.Reader, fs.ObjectInfo, ...fs.OpenOption) error {
	return fs.ErrorNotImplemented
}

func (o *rangeObject) Remove(context.Context) error {
	return errors.New("cfs: the source of a Put can't be removed")
}
