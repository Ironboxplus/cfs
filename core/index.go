package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"unicode/utf8"

	"github.com/Ironboxplus/cfs/metalog"
	_ "modernc.org/sqlite"
)

// index is the local SQLite view of the small files kept in packs. It is a
// cache of the drive-side log: it can always be rebuilt from the drive. It also
// holds what has to survive a crash before it reaches the drive (pending).
type index struct {
	db   *sql.DB
	path string
}

const schema = `
CREATE TABLE IF NOT EXISTS files(
  path  TEXT PRIMARY KEY,
  dir   TEXT NOT NULL,
  name  TEXT NOT NULL,
  size  INTEGER NOT NULL,
  mtime INTEGER NOT NULL,
  sha1  TEXT NOT NULL,
  pack  TEXT NOT NULL,
  off   INTEGER NOT NULL,
  csize INTEGER NOT NULL,
  meth  INTEGER NOT NULL,
  seq   INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS files_dir ON files(dir);
CREATE INDEX IF NOT EXISTS files_pack ON files(pack);
CREATE TABLE IF NOT EXISTS packs(
  name    TEXT PRIMARY KEY,
  epoch   INTEGER NOT NULL,
  seq     INTEGER NOT NULL,
  bytes   INTEGER NOT NULL,
  sha1    TEXT NOT NULL,
  members INTEGER NOT NULL,
  dead_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pending(
  id  INTEGER PRIMARY KEY AUTOINCREMENT,
  op  TEXT NOT NULL,
  seg TEXT,
  at  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS segs(name TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS inflight(name TEXT PRIMARY KEY);
`

func openIndex(dir string) (*index, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, "index.db")
	return openIndexFile(p)
}

func openIndexFile(p string) (*index, error) {
	dsn := "file:" + filepath.ToSlash(p) + "?_txlock=immediate&_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("cfs: open index %s: %w", p, err)
	}
	return &index{db: db, path: p}, nil
}

func (ix *index) close() error { return ix.db.Close() }

func (ix *index) tx(f func(*sql.Tx) error) error {
	tx, err := ix.db.Begin()
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

type fileRow struct {
	Path, Dir, Name string
	Size, MTime     int64
	SHA1, Pack      string
	Off, CSize      int64
	Meth            uint16
	Seq             uint64
}

const fileCols = "path,dir,name,size,mtime,sha1,pack,off,csize,meth,seq"

type scanner interface{ Scan(...any) error }

func scanFile(s scanner) (fileRow, error) {
	var r fileRow
	var meth int
	var seq int64
	err := s.Scan(&r.Path, &r.Dir, &r.Name, &r.Size, &r.MTime, &r.SHA1, &r.Pack, &r.Off, &r.CSize, &meth, &seq)
	r.Meth, r.Seq = uint16(meth), uint64(seq)
	return r, err
}

type queryer interface {
	QueryRow(string, ...any) *sql.Row
	Query(string, ...any) (*sql.Rows, error)
	Exec(string, ...any) (sql.Result, error)
}

func getFile(q queryer, p string) (fileRow, bool, error) {
	r, err := scanFile(q.QueryRow("SELECT "+fileCols+" FROM files WHERE path=?", p))
	if err == sql.ErrNoRows {
		return fileRow{}, false, nil
	}
	return r, err == nil, err
}

func (ix *index) get(p string) (fileRow, bool, error) { return getFile(ix.db, p) }

func (ix *index) listDir(dir string) ([]fileRow, error) {
	rows, err := ix.db.Query("SELECT "+fileCols+" FROM files WHERE dir=?", dir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fileRow
	for rows.Next() {
		r, err := scanFile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// underRange is the key range of everything below directory p.
func underRange(p string) (lo, hi string) { return p + "/", p + "0" }

func (ix *index) listUnder(p string) ([]fileRow, error) {
	lo, hi := underRange(p)
	rows, err := ix.db.Query("SELECT "+fileCols+" FROM files WHERE path>=? AND path<?", lo, hi)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fileRow
	for rows.Next() {
		r, err := scanFile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (ix *index) anyUnder(p string) (bool, error) {
	lo, hi := underRange(p)
	var one int
	err := ix.db.QueryRow("SELECT 1 FROM files WHERE path>=? AND path<? LIMIT 1", lo, hi).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (ix *index) all(f func(fileRow) error) error {
	rows, err := ix.db.Query("SELECT " + fileCols + " FROM files ORDER BY path")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		r, err := scanFile(rows)
		if err != nil {
			return err
		}
		if err := f(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (ix *index) kvGet(k string) (string, bool) {
	var v string
	if err := ix.db.QueryRow("SELECT v FROM kv WHERE k=?", k).Scan(&v); err != nil {
		return "", false
	}
	return v, true
}

func kvSet(q queryer, k, v string) error {
	_, err := q.Exec("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", k, v)
	return err
}

func (ix *index) kvSet(k, v string) error { return kvSet(ix.db, k, v) }

func (ix *index) kvUint(k string) uint64 {
	v, ok := ix.kvGet(k)
	if !ok {
		return 0
	}
	n, _ := strconv.ParseUint(v, 10, 64)
	return n
}

// allocSeq reserves the next sequence number durably before anything named by
// it is written.
func (ix *index) allocSeq() (uint64, error) {
	var n uint64
	err := ix.tx(func(tx *sql.Tx) error {
		var s string
		err := tx.QueryRow("SELECT v FROM kv WHERE k='next_seq'").Scan(&s)
		n = 1
		if err == nil {
			n, _ = strconv.ParseUint(s, 10, 64)
		} else if err != sql.ErrNoRows {
			return err
		}
		return kvSet(tx, "next_seq", strconv.FormatUint(n+1, 10))
	})
	return n, err
}

// raiseSeq makes sure the next allocated number is above n.
func (ix *index) raiseSeq(n uint64) error {
	return ix.tx(func(tx *sql.Tx) error {
		var s string
		cur := uint64(1)
		if err := tx.QueryRow("SELECT v FROM kv WHERE k='next_seq'").Scan(&s); err == nil {
			cur, _ = strconv.ParseUint(s, 10, 64)
		}
		if n+1 > cur {
			return kvSet(tx, "next_seq", strconv.FormatUint(n+1, 10))
		}
		return nil
	})
}

func splitPath(p string) (dir, name string) {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i], p[i+1:]
		}
	}
	return "", p
}

// applyRecord applies one logged change to the index. It is used both live and
// when replaying the drive's log; replay order is Seq order, and every case is
// written so that applying a record twice, or after a later one, is harmless.
func applyRecord(q queryer, r metalog.Record, seq uint64) error {
	switch r.Op {
	case metalog.OpPut:
		cur, ok, err := getFile(q, r.Path)
		if err != nil {
			return err
		}
		if r.Expect != "" && (!ok || fmt.Sprintf("%s@%d", cur.Pack, cur.Off) != r.Expect) {
			return nil
		}
		if r.Expect == "" && ok && cur.Seq > seq {
			return nil
		}
		dir, name := splitPath(r.Path)
		_, err = q.Exec("INSERT OR REPLACE INTO files("+fileCols+") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
			r.Path, dir, name, r.Size, r.MTime, r.SHA1, r.Pack, r.Off, r.CSize, int(r.Meth), int64(seq))
		return err
	case metalog.OpDel:
		if r.Expect != "" {
			cur, ok, err := getFile(q, r.Path)
			if err != nil || !ok || fmt.Sprintf("%s@%d", cur.Pack, cur.Off) != r.Expect {
				return err
			}
		}
		_, err := q.Exec("DELETE FROM files WHERE path=?", r.Path)
		return err
	case metalog.OpMv:
		cur, ok, err := getFile(q, r.Path)
		if err != nil || !ok {
			return err
		}
		if _, err := q.Exec("DELETE FROM files WHERE path=?", r.To); err != nil {
			return err
		}
		dir, name := splitPath(r.To)
		_, err = q.Exec("UPDATE files SET path=?, dir=?, name=? WHERE path=?", r.To, dir, name, cur.Path)
		return err
	case metalog.OpDmv:
		lo, hi := underRange(r.To)
		if _, err := q.Exec("DELETE FROM files WHERE path>=? AND path<?", lo, hi); err != nil {
			return err
		}
		n := utf8.RuneCountInString(r.Path) + 1
		lo, hi = underRange(r.Path)
		_, err := q.Exec("UPDATE files SET path=?||substr(path,?), dir=CASE WHEN dir=? THEN ? ELSE ?||substr(dir,?) END WHERE path>=? AND path<?",
			r.To, n, r.Path, r.To, r.To, n, lo, hi)
		return err
	case metalog.OpRmTree:
		lo, hi := underRange(r.Path)
		_, err := q.Exec("DELETE FROM files WHERE path>=? AND path<?", lo, hi)
		return err
	case metalog.OpTouch:
		_, err := q.Exec("UPDATE files SET mtime=? WHERE path=?", r.MTime, r.Path)
		return err
	}
	return fmt.Errorf("cfs: unknown log record op %q", r.Op)
}

func addPending(q queryer, r metalog.Record) error {
	b, _ := json.Marshal(r)
	_, err := q.Exec("INSERT INTO pending(op,at) VALUES(?,strftime('%s','now'))", string(b))
	return err
}

type pendingRow struct {
	ID  int64
	Seg string
	Rec metalog.Record
}

func readPending(q queryer, where string, args ...any) ([]pendingRow, error) {
	rows, err := q.Query("SELECT id,COALESCE(seg,''),op FROM pending "+where+" ORDER BY id", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pendingRow
	for rows.Next() {
		var p pendingRow
		var op string
		if err := rows.Scan(&p.ID, &p.Seg, &op); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(op), &p.Rec); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
