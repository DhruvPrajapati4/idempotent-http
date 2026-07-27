package idempotent

import (
	"net/http"
	"sync"
	"time"
)

// State describes where a key is in its lifecycle.
type State int

const (
	// StateInFlight means a request holding this key is currently executing.
	StateInFlight State = iota
	// StateCompleted means a result has been recorded and can be replayed.
	StateCompleted
)

// Response is a captured handler result, held so retries can replay it.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Record is a snapshot of a key's stored state, returned by Claim to a caller
// that did not win leadership.
type Record struct {
	State       State
	Fingerprint string
	Response    Response // populated only when State == StateCompleted
}

// Store holds idempotency records. The single operation that must be atomic is
// Claim: "return the existing record, or reserve this key if none exists." That
// atomicity is what guarantees at-most-once execution. In a multi-node store
// this becomes a conditional insert (e.g. SETNX); here it is a mutex.
type Store interface {
	// Claim reserves key for a new execution and reports leader == true, or
	// returns the existing record with leader == false.
	Claim(key, fingerprint string) (rec Record, leader bool)
	// Complete records the final response for an in-flight key.
	Complete(key string, resp Response)
	// Discard drops an in-flight reservation so the key can be re-executed.
	Discard(key string)
}

type entry struct {
	state       State
	fingerprint string
	resp        Response
	expiresAt   time.Time // TTL deadline; meaningful only once completed
}

// MemoryStore is an in-memory, single-node Store. Completed entries live until
// their TTL elapses; a background sweeper reclaims those that are never looked
// up again.
type MemoryStore struct {
	mu  sync.Mutex // guards m
	m   map[string]*entry
	ttl time.Duration

	stopOnce sync.Once
	stop     chan struct{}
}

// NewMemoryStore returns a store whose completed entries live for ttl. If
// sweepEvery > 0 a goroutine prunes expired entries on that interval; call
// Close to stop it.
func NewMemoryStore(ttl, sweepEvery time.Duration) *MemoryStore {
	s := &MemoryStore{
		m:    make(map[string]*entry),
		ttl:  ttl,
		stop: make(chan struct{}),
	}
	if sweepEvery > 0 {
		go s.sweepLoop(sweepEvery)
	}
	return s
}

func (s *MemoryStore) Claim(key, fingerprint string) (Record, bool) {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// A key is a live follower target if it is still in flight, or completed
	// and not yet expired. Anything else (absent, or expired) is claimable.
	if e, ok := s.m[key]; ok && (e.state == StateInFlight || now.Before(e.expiresAt)) {
		return Record{State: e.state, Fingerprint: e.fingerprint, Response: e.resp}, false
	}

	s.m[key] = &entry{state: StateInFlight, fingerprint: fingerprint}
	return Record{}, true
}

func (s *MemoryStore) Complete(key string, resp Response) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only the in-flight reservation we made may be completed. If it is gone
	// (discarded, or already superseded) there is nothing to record.
	e, ok := s.m[key]
	if !ok || e.state != StateInFlight {
		return
	}
	e.state = StateCompleted
	e.resp = resp
	e.expiresAt = time.Now().Add(s.ttl)
}

func (s *MemoryStore) Discard(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[key]; ok && e.state == StateInFlight {
		delete(s.m, key)
	}
}

// Close stops the background sweeper. It is safe to call more than once.
func (s *MemoryStore) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
}

func (s *MemoryStore) sweepLoop(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.sweep()
		}
	}
}

func (s *MemoryStore) sweep() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.m {
		// Never evict an in-flight entry; a leader is relying on it.
		if e.state == StateCompleted && now.After(e.expiresAt) {
			delete(s.m, k)
		}
	}
}
