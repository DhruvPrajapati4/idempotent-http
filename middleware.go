// Package idempotent provides HTTP middleware that makes mutating endpoints
// safe to retry. A client tags a logical operation with an Idempotency-Key
// header; the middleware runs the wrapped handler at most once per key and
// gives every retry a coherent response.
//
// It is in-memory and single-node. The Store interface is the seam where a
// durable, multi-node backend would slot in.
package idempotent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
)

// HeaderKey is the request header carrying the idempotency key.
const HeaderKey = "Idempotency-Key"

// HeaderReplayed is set on responses served from a stored result rather than a
// fresh execution.
const HeaderReplayed = "Idempotent-Replayed"

// failureFloor is the status at or above which a result is treated as
// indeterminate: not cached, so a retry re-executes.
const failureFloor = 500

// New wraps handlers so that requests sharing an Idempotency-Key execute the
// handler at most once.
func New(store Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return &middleware{store: store, next: next}
	}
}

type middleware struct {
	store Store
	next  http.Handler
}

func (m *middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get(HeaderKey)
	if key == "" {
		http.Error(w, HeaderKey+" header is required", http.StatusBadRequest)
		return
	}

	// Buffer the body: we need it to fingerprint the request, and again to hand
	// to the handler.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "could not read request body", http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	fp := fingerprint(r.Method, r.URL, body)

	rec, leader := m.store.Claim(key, fp)
	if !leader {
		m.serveExisting(w, rec, fp)
		return
	}
	m.serveLeader(w, r, key)
}

// serveExisting handles a key that is already known: a genuine retry replays,
// an in-flight duplicate is told to back off, a mismatched request is rejected.
func (m *middleware) serveExisting(w http.ResponseWriter, rec Record, fp string) {
	if rec.Fingerprint != fp {
		http.Error(w, "Idempotency-Key reused for a different request", http.StatusUnprocessableEntity)
		return
	}
	switch rec.State {
	case StateInFlight:
		http.Error(w, "a request with this Idempotency-Key is already in progress", http.StatusConflict)
	case StateCompleted:
		replay(w, rec.Response)
	}
}

// serveLeader runs the handler exactly once for this key and records the
// outcome. A 5xx or a panic is treated as indeterminate and releases the key so
// a later retry can re-execute.
func (m *middleware) serveLeader(w http.ResponseWriter, r *http.Request, key string) {
	rr := &recorder{ResponseWriter: w}

	defer func() {
		if p := recover(); p != nil {
			m.store.Discard(key)
			panic(p)
		}
	}()

	m.next.ServeHTTP(rr, r)

	if res := rr.result(); res.Status >= failureFloor {
		m.store.Discard(key)
	} else {
		m.store.Complete(key, res)
	}
}

func replay(w http.ResponseWriter, resp Response) {
	h := w.Header()
	for k, vs := range resp.Header {
		h[k] = vs
	}
	h.Set(HeaderReplayed, "true")
	w.WriteHeader(resp.Status)
	w.Write(resp.Body)
}

// fingerprint identifies the request a key belongs to, so the same key used for
// a different request can be rejected. Method, path and query select the
// operation; the body distinguishes its arguments. NUL separators keep the
// fields unambiguous.
func fingerprint(method string, u *url.URL, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(u.Path))
	h.Write([]byte{0})
	h.Write([]byte(u.RawQuery))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}
