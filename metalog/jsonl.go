package metalog

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"

	"github.com/Ironboxplus/cfs/store"
)

// Directory names below the volume's control directory.
const (
	LogDir  = "log"
	CkptDir = "ckpt"
)

type header struct {
	V     int    `json:"v"`
	Epoch uint64 `json:"epoch"`
	Seq   uint64 `json:"seq"`
	Pack  string `json:"pack,omitempty"`
	N     int    `json:"n"`
	PB    int64  `json:"pack_bytes,omitempty"`
	PS    string `json:"pack_sha1,omitempty"`
	PN    int    `json:"pack_n,omitempty"`
}

// Jsonl keeps segments as "<epoch>-<seq>.jsonl.gz" (a header line, then one
// record per line) and checkpoints as "<epoch>-<seq>.db.gz" under
// <ctl>/log and <ctl>/ckpt of the store.
type Jsonl struct {
	St  store.Store
	Ctl string // control directory, e.g. ".cfs"
}

var _ MetaLog = (*Jsonl)(nil)

var (
	segRE  = regexp.MustCompile(`^(\d+)-(\d+)\.jsonl\.gz$`)
	ckptRE = regexp.MustCompile(`^(\d+)-(\d+)\.db\.gz$`)
)

// SegmentName and CheckpointName are the deterministic object names.
func SegmentName(epoch, seq uint64) string { return fmt.Sprintf("%06d-%010d.jsonl.gz", epoch, seq) }
func CheckpointName(epoch, seq uint64) string {
	return fmt.Sprintf("%06d-%010d.db.gz", epoch, seq)
}

func (j *Jsonl) path(dir, name string) string { return j.Ctl + "/" + dir + "/" + name }

func (j *Jsonl) AppendSegment(ctx context.Context, seg Segment) error {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	enc := json.NewEncoder(zw)
	if err := enc.Encode(header{V: 1, Epoch: seg.Epoch, Seq: seg.Seq, Pack: seg.Pack, N: len(seg.Records), PB: seg.PackBytes, PS: seg.PackSHA1, PN: seg.PackN}); err != nil {
		return err
	}
	for i := range seg.Records {
		if err := enc.Encode(&seg.Records[i]); err != nil {
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return err
	}
	data := buf.Bytes()
	p := j.path(LogDir, SegmentName(seg.Epoch, seg.Seq))
	if err := j.St.Put(ctx, p, store.Bytes(data), store.PutOpts{Size: int64(len(data)), Unique: true}); err != nil {
		return fmt.Errorf("metalog: upload segment: %w", err)
	}
	e, err := j.St.Stat(ctx, p)
	if err != nil {
		return fmt.Errorf("metalog: verify segment: %w", err)
	}
	if e.Size != int64(len(data)) {
		return fmt.Errorf("metalog: segment %s is %d bytes on the drive, wrote %d", p, e.Size, len(data))
	}
	return nil
}

func (j *Jsonl) list(ctx context.Context, dir string, re *regexp.Regexp) ([]Ref, error) {
	ents, err := j.St.List(ctx, j.Ctl+"/"+dir)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Ref
	for _, e := range ents {
		if e.IsDir {
			continue
		}
		m := re.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		ep, _ := strconv.ParseUint(m[1], 10, 64)
		sq, _ := strconv.ParseUint(m[2], 10, 64)
		out = append(out, Ref{Epoch: ep, Seq: sq, Name: e.Name(), Size: e.Size, ModTime: e.ModTime})
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Seq != out[b].Seq {
			return out[a].Seq < out[b].Seq
		}
		return out[a].Epoch < out[b].Epoch
	})
	return out, nil
}

func (j *Jsonl) ListSegments(ctx context.Context) ([]Ref, error) {
	return j.list(ctx, LogDir, segRE)
}

func (j *Jsonl) ListCheckpoints(ctx context.Context) ([]Ref, error) {
	return j.list(ctx, CkptDir, ckptRE)
}

func readAll(ctx context.Context, st store.Store, p string) ([]byte, error) {
	rc, err := st.Open(ctx, p, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (j *Jsonl) ReadSegment(ctx context.Context, ref Ref) (Segment, error) {
	raw, err := readAll(ctx, j.St, j.path(LogDir, ref.Name))
	if err != nil {
		return Segment{}, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return Segment{}, fmt.Errorf("metalog: segment %s: %w", ref.Name, err)
	}
	sc := bufio.NewReaderSize(zr, 1<<20)
	line, err := sc.ReadBytes('\n')
	if err != nil {
		return Segment{}, fmt.Errorf("metalog: segment %s: header: %w", ref.Name, err)
	}
	var h header
	if err := json.Unmarshal(line, &h); err != nil {
		return Segment{}, fmt.Errorf("metalog: segment %s: header: %w", ref.Name, err)
	}
	if h.Epoch != ref.Epoch || h.Seq != ref.Seq {
		return Segment{}, fmt.Errorf("metalog: segment %s claims epoch %d seq %d", ref.Name, h.Epoch, h.Seq)
	}
	seg := Segment{Epoch: h.Epoch, Seq: h.Seq, Pack: h.Pack, PackBytes: h.PB, PackSHA1: h.PS, PackN: h.PN}
	for {
		line, err := sc.ReadBytes('\n')
		if len(line) > 0 {
			var r Record
			if jerr := json.Unmarshal(line, &r); jerr != nil {
				return Segment{}, fmt.Errorf("metalog: segment %s: %w", ref.Name, jerr)
			}
			seg.Records = append(seg.Records, r)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return Segment{}, fmt.Errorf("metalog: segment %s: %w", ref.Name, err)
		}
	}
	if len(seg.Records) != h.N {
		return Segment{}, fmt.Errorf("metalog: segment %s has %d records, header says %d", ref.Name, len(seg.Records), h.N)
	}
	return seg, nil
}

func (j *Jsonl) DeleteSegment(ctx context.Context, ref Ref) error {
	return j.St.Remove(ctx, j.path(LogDir, ref.Name))
}

func (j *Jsonl) PutCheckpoint(ctx context.Context, epoch, seq uint64, dbPath string) error {
	tmp, err := os.CreateTemp("", "cfs-ckpt-*.gz")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	src, err := os.Open(dbPath)
	if err != nil {
		tmp.Close()
		return err
	}
	zw := gzip.NewWriter(tmp)
	_, err = io.Copy(zw, src)
	src.Close()
	if err == nil {
		err = zw.Close()
	}
	if err == nil {
		err = tmp.Close()
	} else {
		tmp.Close()
	}
	if err != nil {
		return err
	}
	st, err := os.Stat(tmp.Name())
	if err != nil {
		return err
	}
	p := j.path(CkptDir, CheckpointName(epoch, seq))
	if err := j.St.Put(ctx, p, store.File(tmp.Name()), store.PutOpts{Size: st.Size(), Unique: true}); err != nil {
		return fmt.Errorf("metalog: upload checkpoint: %w", err)
	}
	e, err := j.St.Stat(ctx, p)
	if err != nil {
		return fmt.Errorf("metalog: verify checkpoint: %w", err)
	}
	if e.Size != st.Size() {
		return fmt.Errorf("metalog: checkpoint %s is %d bytes on the drive, wrote %d", p, e.Size, st.Size())
	}
	return nil
}

func (j *Jsonl) FetchCheckpoint(ctx context.Context, ref Ref, dstPath string) error {
	rc, err := j.St.Open(ctx, j.path(CkptDir, ref.Name), 0, -1)
	if err != nil {
		return err
	}
	defer rc.Close()
	zr, err := gzip.NewReader(rc)
	if err != nil {
		return fmt.Errorf("metalog: checkpoint %s: %w", ref.Name, err)
	}
	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, zr); err != nil {
		out.Close()
		os.Remove(dstPath)
		return fmt.Errorf("metalog: checkpoint %s: %w", ref.Name, err)
	}
	return out.Close()
}

func (j *Jsonl) DeleteCheckpoint(ctx context.Context, ref Ref) error {
	return j.St.Remove(ctx, j.path(CkptDir, ref.Name))
}
