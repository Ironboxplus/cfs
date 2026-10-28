package testutil

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DAV is a minimal WebDAV client (the external interface the black-box tests
// drive). It shares nothing with the server implementation.
type DAV struct {
	Base string
	HC   *http.Client
	// User and Pass, when set, are sent as HTTP basic auth.
	User, Pass string
}

// NewDAV returns a client for the server at base (no trailing slash).
func NewDAV(base string) *DAV {
	return &DAV{Base: strings.TrimRight(base, "/"), HC: &http.Client{
		Timeout:   10 * time.Minute,
		Transport: &http.Transport{MaxIdleConnsPerHost: 64, DisableCompression: true},
	}}
}

func (d *DAV) url(p string) string {
	segs := strings.Split(strings.Trim(p, "/"), "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return d.Base + "/" + strings.Join(segs, "/")
}

func (d *DAV) do(method, p string, body io.Reader, size int64, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequest(method, d.url(p), body)
	if err != nil {
		return nil, err
	}
	if size >= 0 && body != nil {
		req.ContentLength = size
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if d.User != "" {
		req.SetBasicAuth(d.User, d.Pass)
	}
	return d.HC.Do(req)
}

func check(resp *http.Response, what string, ok ...int) error {
	for _, c := range ok {
		if resp.StatusCode == c {
			return nil
		}
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
	return fmt.Errorf("%s: HTTP %d %s", what, resp.StatusCode, strings.TrimSpace(string(b)))
}

// Put uploads a file; size must be the exact length of r.
func (d *DAV) Put(p string, r io.Reader, size int64) error {
	resp, err := d.do("PUT", p, r, size, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return check(resp, "PUT "+p, 200, 201, 204)
}

// PutBytes uploads b.
func (d *DAV) PutBytes(p string, b []byte) error {
	return d.Put(p, bytes.NewReader(b), int64(len(b)))
}

// Get downloads a file completely.
func (d *DAV) Get(p string) ([]byte, error) {
	resp, err := d.do("GET", p, nil, -1, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := check(resp, "GET "+p, 200); err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

// GetRange downloads n bytes at off.
func (d *DAV) GetRange(p string, off, n int64) ([]byte, error) {
	resp, err := d.do("GET", p, nil, -1, map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", off, off+n-1)})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := check(resp, "GET range "+p, 206, 200); err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

// Sum downloads a file and returns its SHA-256 and size without keeping it.
func (d *DAV) Sum(p string) (string, int64, error) {
	resp, err := d.do("GET", p, nil, -1, nil)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	if err := check(resp, "GET "+p, 200); err != nil {
		return "", 0, err
	}
	h := sha256.New()
	n, err := io.Copy(h, resp.Body)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// Delete removes a file or a directory tree.
func (d *DAV) Delete(p string) error {
	resp, err := d.do("DELETE", p, nil, -1, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return check(resp, "DELETE "+p, 200, 204)
}

// Mkcol creates a directory (parents must exist).
func (d *DAV) Mkcol(p string) error {
	resp, err := d.do("MKCOL", p, nil, -1, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return check(resp, "MKCOL "+p, 200, 201, 405)
}

// MkcolAll creates a directory and its parents.
func (d *DAV) MkcolAll(p string) error {
	p = strings.Trim(p, "/")
	if p == "" || p == "." {
		return nil
	}
	if err := d.MkcolAll(path.Dir(p)); err != nil {
		return err
	}
	return d.Mkcol(p)
}

// Move renames src to dst.
func (d *DAV) Move(src, dst string) error {
	resp, err := d.do("MOVE", src, nil, -1, map[string]string{"Destination": d.url(dst), "Overwrite": "T"})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return check(resp, "MOVE "+src+" -> "+dst, 200, 201, 204)
}

// Item is one entry of a listing.
type Item struct {
	Name  string
	IsDir bool
	Size  int64
}

type msResp struct {
	Href     string `xml:"href"`
	Propstat []struct {
		Prop struct {
			Len  string `xml:"getcontentlength"`
			Type struct {
				Collection *struct{} `xml:"collection"`
			} `xml:"resourcetype"`
		} `xml:"prop"`
	} `xml:"propstat"`
}

// List returns the children of a directory.
func (d *DAV) List(p string) ([]Item, error) {
	resp, err := d.do("PROPFIND", p, nil, -1, map[string]string{"Depth": "1"})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := check(resp, "PROPFIND "+p, 207); err != nil {
		return nil, err
	}
	var ms struct {
		Resp []msResp `xml:"response"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, err
	}
	self := strings.Trim(path.Join("/", p), "/")
	var out []Item
	for _, r := range ms.Resp {
		h, err := url.PathUnescape(r.Href)
		if err != nil {
			return nil, err
		}
		if u, err := url.Parse(d.Base); err == nil {
			h = strings.TrimPrefix(h, u.Path)
		}
		h = strings.Trim(h, "/")
		if h == self {
			continue
		}
		it := Item{Name: path.Base(h)}
		for _, ps := range r.Propstat {
			if ps.Prop.Type.Collection != nil {
				it.IsDir = true
			}
			if ps.Prop.Len != "" {
				it.Size, _ = strconv.ParseInt(ps.Prop.Len, 10, 64)
			}
		}
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Walk lists a tree: path (relative, slash separated) -> size for files, and
// the set of directories.
func (d *DAV) Walk(root string) (files map[string]int64, dirs map[string]bool, err error) {
	files, dirs = map[string]int64{}, map[string]bool{}
	var rec func(rel string) error
	rec = func(rel string) error {
		items, err := d.List(path.Join(root, rel))
		if err != nil {
			return err
		}
		for _, it := range items {
			p := path.Join(rel, it.Name)
			if it.IsDir {
				dirs[p] = true
				if err := rec(p); err != nil {
					return err
				}
			} else {
				files[p] = it.Size
			}
		}
		return nil
	}
	err = rec("")
	return
}
