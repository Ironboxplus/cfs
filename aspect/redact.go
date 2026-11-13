package aspect

import (
	"context"
	"io"
	"regexp"
	"time"

	"github.com/Ironboxplus/cfs/store"
)

var (
	secretKV    = regexp.MustCompile(`(?i)\b(access_?token|refresh_?token|token|password|passwd|pass|secret|api_?key|sign|signature|authorization|x-amz-signature)(["']?\s*[=:]\s*["']?)[^&\s"',;]+`)
	bearer      = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]+`)
	urlUserinfo = regexp.MustCompile(`(?i)(://)[^/\s:@]+:[^/\s@]*@`)
)

// Scrub masks credentials in text destined for logs or error messages:
// URL userinfo, token/password/signature query parameters, bearer headers.
func Scrub(s string) string {
	s = urlUserinfo.ReplaceAllString(s, "${1}***@")
	s = bearer.ReplaceAllString(s, "${1} ***")
	s = secretKV.ReplaceAllString(s, "${1}${2}***")
	return s
}

type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

func scrubErr(err error) error {
	if err == nil {
		return nil
	}
	m := err.Error()
	if s := Scrub(m); s != m {
		return &redactedError{msg: s, err: err}
	}
	return err
}

type redact struct{ store.Store }

// Redact makes every error leaving the store free of credentials, so they can
// be logged as they are. errors.Is/As still see the original error.
func Redact(s store.Store) store.Store { return redact{s} }

func (r redact) List(ctx context.Context, dir string) ([]store.Entry, error) {
	v, err := r.Store.List(ctx, dir)
	return v, scrubErr(err)
}
func (r redact) Stat(ctx context.Context, p string) (store.Entry, error) {
	v, err := r.Store.Stat(ctx, p)
	return v, scrubErr(err)
}
func (r redact) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	v, err := r.Store.Open(ctx, p, off, n)
	return v, scrubErr(err)
}
func (r redact) OpenStream(ctx context.Context, p string, off, n int64, want store.Entry) (io.ReadCloser, error) {
	v, err := r.Store.OpenStream(ctx, p, off, n, want)
	return v, scrubErr(err)
}
func (r redact) Put(ctx context.Context, p string, src store.Source, o store.PutOpts) error {
	return scrubErr(r.Store.Put(ctx, p, src, o))
}
func (r redact) Remove(ctx context.Context, p string) error {
	return scrubErr(r.Store.Remove(ctx, p))
}
func (r redact) Move(ctx context.Context, a, b string) error {
	return scrubErr(r.Store.Move(ctx, a, b))
}
func (r redact) DirMove(ctx context.Context, a, b string) error {
	return scrubErr(r.Store.DirMove(ctx, a, b))
}
func (r redact) Purge(ctx context.Context, d string) error { return scrubErr(r.Store.Purge(ctx, d)) }
func (r redact) Mkdir(ctx context.Context, d string) error { return scrubErr(r.Store.Mkdir(ctx, d)) }
func (r redact) Rmdir(ctx context.Context, d string) error { return scrubErr(r.Store.Rmdir(ctx, d)) }
func (r redact) SetModTime(ctx context.Context, p string, t time.Time) error {
	return scrubErr(r.Store.SetModTime(ctx, p, t))
}
func (r redact) Hash(ctx context.Context, p string) (string, error) {
	v, err := r.Store.Hash(ctx, p)
	return v, scrubErr(err)
}
