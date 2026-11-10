// Package metalog is the durable, drive-side log of namespace changes that
// makes a cfs volume recoverable from the drive alone. MetaLog is an
// interface so that a different implementation (git, for several writers and
// history) can replace the jsonl segments later; only the jsonl one exists.
package metalog

import (
	"context"
	"fmt"
	"time"
)

// Ops of a Record.
const (
	OpPut    = "put"    // a small file lives in Pack at Off..Off+CSize
	OpDel    = "del"    // small file Path removed
	OpMv     = "mv"     // small file Path renamed to To
	OpDmv    = "dmv"    // directory Path (and all small files below) renamed to To
	OpTouch  = "touch"  // modification time of small file Path set
	OpRmTree = "rmtree" // every small file and every directory entry below directory Path (and Path itself) removed
	OpMkdir  = "mkdir"  // directory Path (and its ancestors) exists, even with nothing in it; MTime is its creation time
	OpRmdir  = "rmdir"  // empty directory Path removed
)

// Record is one namespace change. Only small files (those kept in packs) and
// the directories of the namespace (which need not exist on the drive) are
// logged: large files are the drive's own objects at their own paths.
type Record struct {
	Op    string `json:"op"`
	Path  string `json:"p"`
	To    string `json:"to,omitempty"`
	Size  int64  `json:"sz,omitempty"`
	MTime int64  `json:"mt,omitempty"` // unix nanoseconds
	SHA1  string `json:"sha1,omitempty"`
	Pack  string `json:"pack,omitempty"`
	Off   int64  `json:"off,omitempty"`  // offset of the member's data in the pack
	CSize int64  `json:"csz,omitempty"`  // stored (compressed) size
	Meth  uint16 `json:"meth,omitempty"` // zip method: 0 stored, 8 deflate
	// Expect makes a put conditional ("pack@off" the path must still point to):
	// compaction moves data without ever overriding a newer write.
	Expect string `json:"exp,omitempty"`
}

// Segment is one atomic group of records. Segments are applied in Seq order.
// Pack, when set, is the pack uploaded before the segment that its put records
// point into.
//
// A segment may carry several commits at once (the log is written once for a
// batch of packs): Seq is then the highest sequence number among them, Pack
// and its facts belong to that commit, and Extra lists the others. The
// records are in the order of the commits.
type Segment struct {
	Epoch uint64
	Seq   uint64
	Pack  string
	// Pack facts, so a rebuild knows the pack without opening it.
	PackBytes int64
	PackSHA1  string
	PackN     int
	Extra     []PackFact
	Records   []Record
}

// PackFact is a pack of a segment besides the one named by Segment.Pack, with
// the sequence number (and epoch) of the commit that uploaded it.
type PackFact struct {
	Epoch uint64 `json:"epoch"`
	Seq   uint64 `json:"seq"`
	Name  string `json:"name"`
	Bytes int64  `json:"bytes"`
	SHA1  string `json:"sha1,omitempty"`
	N     int    `json:"n"`
	// R is how many of the segment's records belong to this commit, so that a
	// volume that already has the commit (from a checkpoint) can skip them.
	R int `json:"r,omitempty"`
}

// Ref names a stored segment or checkpoint.
type Ref struct {
	Epoch, Seq uint64
	Name       string
	Size       int64
	ModTime    time.Time
}

func (r Ref) String() string { return fmt.Sprintf("%d-%d(%s)", r.Epoch, r.Seq, r.Name) }

// MetaLog stores segments and checkpoints.
type MetaLog interface {
	// AppendSegment returns once the segment is durable on the drive and has
	// been verified there; it returns the size of the segment's file.
	AppendSegment(ctx context.Context, seg Segment) (int64, error)
	ListSegments(ctx context.Context) ([]Ref, error)
	ReadSegment(ctx context.Context, ref Ref) (Segment, error)
	DeleteSegment(ctx context.Context, ref Ref) error

	// PutCheckpoint stores the local snapshot file dbPath as the state after
	// segment seq (of epoch).
	PutCheckpoint(ctx context.Context, epoch, seq uint64, dbPath string) error
	ListCheckpoints(ctx context.Context) ([]Ref, error)
	// FetchCheckpoint writes the snapshot to dstPath.
	FetchCheckpoint(ctx context.Context, ref Ref, dstPath string) error
	DeleteCheckpoint(ctx context.Context, ref Ref) error
}
