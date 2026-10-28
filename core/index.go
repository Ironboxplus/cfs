package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Ironboxplus/cfs/metalog"
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
CREATE TABLE IF NOT EXISTS staged(
  path  TEXT PRIMARY KEY,
  dir   TEXT NOT NULL,
  name  TEXT NOT NULL,
  id    INTEGER NOT NULL,
  size  INTEGER NOT NULL,
  mtime INTEGER NOT NULL,
  sha1  TEXT NOT NULL,
  stray INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS staged_dir ON staged(dir);
-- dirs: the directories of the namespace. Every ancestor of a packed file is in
-- here (applyRecord adds them), and so is every directory made by Mkdir; they
-- exist whether or not the drive has a folder of that name. ddirs: the folders
-- that really exist on the drive (only large files need one): a local cache,
-- rebuilt by walking the drive, never logged.
CREATE TABLE IF NOT EXISTS dirs(
  path  TEXT PRIMARY KEY,
  dir   TEXT NOT NULL,
  mtime INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS dirs_dir ON dirs(dir);
CREATE TABLE IF NOT EXISTS ddirs(path TEXT PRIMARY KEY) WITHOUT ROWID;
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

// getCommitted returns the row of a file as the drive's log has it.
func (ix *index) getCommitted(p string) (fileRow, bool, error) { return getFile(ix.db, p) }

// A staged file is a small file that was acknowledged but isn't in a pack on
// the drive yet. The namespace readers see is the committed rows overlaid with
// the staged ones; a staged row has an empty Pack and the stage id in Off.
func stagedRow(s scanner) (fileRow, error) {
	var r fileRow
	var stray int
	err := s.Scan(&r.Path, &r.Dir, &r.Name, &r.Off, &r.Size, &r.MTime, &r.SHA1, &stray)
	r.CSize = r.Size
	return r, err
}

const stagedCols = "path,dir,name,id,size,mtime,sha1,stray"

func getStaged(q queryer, p string) (fileRow, bool, error) {
	r, err := stagedRow(q.QueryRow("SELECT "+stagedCols+" FROM staged WHERE path=?", p))
	if err == sql.ErrNoRows {
		return fileRow{}, false, nil
	}
	return r, err == nil, err
}

// get returns the file at p as readers see it: staged data first.
func (ix *index) get(p string) (fileRow, bool, error) {
	if r, ok, err := getStaged(ix.db, p); err != nil || ok {
		return r, ok, err
	}
	return getFile(ix.db, p)
}

func (ix *index) listDirCommitted(dir string) ([]fileRow, error) {
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

// listDir lists the small files of dir as readers see them.
func (ix *index) listDir(dir string) ([]fileRow, error) {
	// Staged rows first: a commit moves a file from staged to committed in one
	// transaction, so a file not staged by the time the staged rows were read is
	// committed by the time the committed rows are. The other order could miss
	// both and show the previous version.
	srows, err := ix.db.Query("SELECT "+stagedCols+" FROM staged WHERE dir=?", dir)
	if err != nil {
		return nil, err
	}
	var staged []fileRow
	for srows.Next() {
		r, err := stagedRow(srows)
		if err != nil {
			srows.Close()
			return nil, err
		}
		staged = append(staged, r)
	}
	serr := srows.Err()
	srows.Close()
	if serr != nil {
		return nil, serr
	}
	out, err := ix.listDirCommitted(dir)
	if err != nil {
		return nil, err
	}
	if len(staged) == 0 {
		return out, nil
	}
	hide := make(map[string]bool, len(staged))
	for _, r := range staged {
		hide[r.Name] = true
	}
	kept := out[:0]
	for _, r := range out {
		if !hide[r.Name] {
			kept = append(kept, r)
		}
	}
	return append(kept, staged...), nil
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
	if err == nil {
		return true, nil
	}
	if err != sql.ErrNoRows {
		return false, err
	}
	err = ix.db.QueryRow("SELECT 1 FROM staged WHERE path>=? AND path<? LIMIT 1", lo, hi).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// stagedDirs returns the names of the subdirectories of dir that hold staged
// files: a directory a staged file lives in is only created on the drive when
// its pack is committed, but readers must see it from the moment of the Put.
func (ix *index) stagedDirs(dir string) ([]string, error) {
	var rows *sql.Rows
	var err error
	if dir == "" {
		rows, err = ix.db.Query("SELECT DISTINCT dir FROM staged WHERE dir<>''")
	} else {
		lo, hi := underRange(dir)
		rows, err = ix.db.Query("SELECT DISTINCT dir FROM staged WHERE dir>=? AND dir<?", lo, hi)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		if dir != "" {
			d = d[len(dir)+1:]
		}
		name, _, _ := strings.Cut(d, "/")
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out, rows.Err()
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

// sealPending reserves the next sequence number and seals every queued
// metadata row under it, in one transaction. Doing both at once is what keeps
// the log in the order the operations were issued: a segment never carries a
// row that is older than a row of a segment with a lower number, whichever of
// several concurrent committers got there first. With inflight, the segment
// is also marked as being committed by this process. With onlyIfAny, nothing
// is reserved when no row is queued (seq 0).
func (ix *index) sealPending(epoch uint64, inflight, onlyIfAny bool) (seq uint64, rows []pendingRow, err error) {
	err = ix.tx(func(tx *sql.Tx) error {
		var err error
		seq, rows = 0, nil
		if rows, err = readPending(tx, "WHERE seg IS NULL"); err != nil {
			return err
		}
		if onlyIfAny && len(rows) == 0 {
			return nil
		}
		var s string
		seq = 1
		switch err := tx.QueryRow("SELECT v FROM kv WHERE k='next_seq'").Scan(&s); {
		case err == nil:
			seq, _ = strconv.ParseUint(s, 10, 64)
		case err != sql.ErrNoRows:
			return err
		}
		if err := kvSet(tx, "next_seq", strconv.FormatUint(seq+1, 10)); err != nil {
			return err
		}
		key := segKeyOf(epoch, seq)
		if len(rows) > 0 {
			if _, err := tx.Exec("UPDATE pending SET seg=? WHERE seg IS NULL", key); err != nil {
				return err
			}
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO segs(name) VALUES(?)", key); err != nil {
			return err
		}
		if inflight {
			_, err = tx.Exec("INSERT OR IGNORE INTO inflight(name) VALUES(?)", key)
		}
		return err
	})
	return
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
		if err != nil {
			return err
		}
		return ensureDirs(q, dir, r.MTime)
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
		if err != nil {
			return err
		}
		return ensureDirs(q, dir, 0)
	case metalog.OpDmv:
		lo, hi := underRange(r.To)
		if _, err := q.Exec("DELETE FROM files WHERE path>=? AND path<?", lo, hi); err != nil {
			return err
		}
		if _, err := q.Exec("DELETE FROM dirs WHERE path=? OR (path>=? AND path<?)", r.To, lo, hi); err != nil {
			return err
		}
		n := utf8.RuneCountInString(r.Path) + 1
		lo, hi = underRange(r.Path)
		if _, err := q.Exec("UPDATE files SET path=?||substr(path,?), dir=CASE WHEN dir=? THEN ? ELSE ?||substr(dir,?) END WHERE path>=? AND path<?",
			r.To, n, r.Path, r.To, r.To, n, lo, hi); err != nil {
			return err
		}
		if _, err := q.Exec("UPDATE dirs SET path=?||substr(path,?), dir=CASE WHEN dir=? THEN ? ELSE ?||substr(dir,?) END WHERE path>=? AND path<?",
			r.To, n, r.Path, r.To, r.To, n, lo, hi); err != nil {
			return err
		}
		if _, err := q.Exec("UPDATE dirs SET path=?, dir=? WHERE path=?", r.To, parentOf(r.To), r.Path); err != nil {
			return err
		}
		return ensureDirs(q, r.To, 0)
	case metalog.OpRmTree:
		lo, hi := underRange(r.Path)
		if _, err := q.Exec("DELETE FROM files WHERE path>=? AND path<?", lo, hi); err != nil {
			return err
		}
		_, err := q.Exec("DELETE FROM dirs WHERE path=? OR (path>=? AND path<?)", r.Path, lo, hi)
		return err
	case metalog.OpMkdir:
		return ensureDirs(q, r.Path, r.MTime)
	case metalog.OpRmdir:
		_, err := q.Exec("DELETE FROM dirs WHERE path=?", r.Path)
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

// anyStagedUnder reports whether a staged file lives below directory p.
func (ix *index) anyStagedUnder(p string) (bool, error) {
	lo, hi := underRange(p)
	var one int
	err := ix.db.QueryRow("SELECT 1 FROM staged WHERE path>=? AND path<? LIMIT 1", lo, hi).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// ensureDirs records dir and all its ancestors as directories of the namespace.
// A row that exists is left alone.
func ensureDirs(q queryer, dir string, mtime int64) error {
	for d := dir; d != ""; d = parentOf(d) {
		if _, err := q.Exec("INSERT OR IGNORE INTO dirs(path,dir,mtime) VALUES(?,?,?)", d, parentOf(d), mtime); err != nil {
			return err
		}
	}
	return nil
}

// dirMTime returns the creation time of a directory that has a row of its own.
func (ix *index) dirMTime(p string) (mtime int64, ok bool, err error) {
	err = ix.db.QueryRow("SELECT mtime FROM dirs WHERE path=?", p).Scan(&mtime)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	return mtime, err == nil, err
}

// hasDDir: the drive has a folder p, as far as the local record knows.
func (ix *index) hasDDir(p string) bool {
	var one int
	return ix.db.QueryRow("SELECT 1 FROM ddirs WHERE path=?", p).Scan(&one) == nil
}

// isDir reports whether p is a directory the index knows of: one with a row of
// its own, or a folder of the drive. Directories that only hold staged files
// are not included (see anyStagedUnder).
func (ix *index) isDir(p string) (bool, error) {
	var one int
	err := ix.db.QueryRow("SELECT 1 FROM dirs WHERE path=? UNION ALL SELECT 1 FROM ddirs WHERE path=? LIMIT 1", p, p).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// anyDirsUnder: a directory row lies strictly below p.
func (ix *index) anyDirsUnder(p string) (bool, error) {
	lo, hi := underRange(p)
	var one int
	err := ix.db.QueryRow("SELECT 1 FROM dirs WHERE path>=? AND path<? LIMIT 1", lo, hi).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// childDirs returns the directories of the namespace that sit directly in dir.
func (ix *index) childDirs(dir string) ([]dirRow, error) {
	rows, err := ix.db.Query("SELECT path,mtime FROM dirs WHERE dir=?", dir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dirRow
	for rows.Next() {
		var r dirRow
		if err := rows.Scan(&r.Path, &r.MTime); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type dirRow struct {
	Path  string
	MTime int64
}

// addDDirs records folders that exist on the drive.
func (ix *index) addDDirs(paths ...string) error {
	if len(paths) == 0 {
		return nil
	}
	return ix.tx(func(tx *sql.Tx) error {
		for _, p := range paths {
			if _, err := tx.Exec("INSERT OR IGNORE INTO ddirs(path) VALUES(?)", p); err != nil {
				return err
			}
		}
		return nil
	})
}

// dropDDirs forgets the folder p and everything below it.
func dropDDirs(q queryer, p string) error {
	lo, hi := underRange(p)
	_, err := q.Exec("DELETE FROM ddirs WHERE path=? OR (path>=? AND path<?)", p, lo, hi)
	return err
}

// moveDDirs renames the folder src and everything below it to dst, dropping
// whatever was recorded at dst before.
func moveDDirs(q queryer, src, dst string) error {
	if err := dropDDirs(q, dst); err != nil {
		return err
	}
	lo, hi := underRange(src)
	n := utf8.RuneCountInString(src) + 1
	if _, err := q.Exec("UPDATE ddirs SET path=?||substr(path,?) WHERE path>=? AND path<?", dst, n, lo, hi); err != nil {
		return err
	}
	_, err := q.Exec("UPDATE ddirs SET path=? WHERE path=?", dst, src)
	return err
}

// anyDDirUnder: the drive has a folder at p or below it.
func (ix *index) anyDDirUnder(p string) (bool, error) {
	lo, hi := underRange(p)
	var one int
	err := ix.db.QueryRow("SELECT 1 FROM ddirs WHERE path=? OR (path>=? AND path<?) LIMIT 1", p, lo, hi).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}
