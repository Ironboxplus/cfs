package core

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"time"
)

// ReadCheckLease and CheckLease expose the commit path's lease checks to the
// tests of package core_test.
func (v *Volume) ReadCheckLease(ctx context.Context) error { return v.readCheckLease(ctx) }
func (v *Volume) CheckLease(ctx context.Context) error     { return v.checkLease(ctx) }

// PackAdd puts a small file straight into the open pack, the way the staged
// packer does, and returns a function that waits for its commit.
func (v *Volume) PackAdd(ctx context.Context, p string, data []byte, mtime time.Time) (func() error, error) {
	sum := sha1.Sum(data)
	m := &member{path: p, size: int64(len(data)), mtime: mtime.UnixNano(), sha1: hex.EncodeToString(sum[:])}
	stored, meth, crc := v.encodeMember(p, data)
	m.meth = meth
	b, err := v.packAdd(ctx, m, stored, crc)
	if err != nil {
		return nil, err
	}
	return func() error { <-b.done; return b.err }, nil
}

// SealCur closes the open pack for new members and starts its commit.
func (v *Volume) SealCur() {
	v.batchMu.Lock()
	defer v.batchMu.Unlock()
	if v.cur != nil {
		v.sealLocked(v.cur)
	}
}

// SetMvIntent journals a move without doing it: the state a crash leaves
// between the journal write and the move on the drive.
func (v *Volume) SetMvIntent(src, dst string) error { return v.setMvIntent(src, dst) }
