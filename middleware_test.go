package idempotent

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// size reports how many entries the store holds. Test-only.
func (s *MemoryStore) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

// echoHandler counts its executions and echoes the request body with a fixed
// header, so tests can tell a fresh execution from a replay.
type echoHandler struct {
	calls  atomic.Int64
	status int
}

func (h *echoHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.calls.Add(1)
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("X-Custom", "from-handler")
	if h.status != 0 {
		w.WriteHeader(h.status)
	}
	w.Write(body)
}

func newReq(method, target, key, body string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if key != "" {
		r.Header.Set(HeaderKey, key)
	}
	return r
}

// wrap builds the middleware around next with a non-sweeping store.
func wrap(next http.Handler) (http.Handler, *MemoryStore) {
	store := NewMemoryStore(time.Hour, 0)
	return New(store)(next), store
}

func TestMissingKeyRejected(t *testing.T) {
	h := &echoHandler{}
	mw, _ := wrap(h)

	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, newReq("POST", "/pay", "", "amount=10"))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if got := h.calls.Load(); got != 0 {
		t.Fatalf("handler ran %d times, want 0", got)
	}
}

func TestSingleExecution(t *testing.T) {
	h := &echoHandler{}
	mw, _ := wrap(h)

	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, newReq("POST", "/pay", "k1", "amount=10"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "amount=10" {
		t.Fatalf("body = %q, want echoed request", rec.Body.String())
	}
	if h.calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", h.calls.Load())
	}
}

func TestDuplicateReplayed(t *testing.T) {
	h := &echoHandler{}
	mw, _ := wrap(h)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, newReq("POST", "/pay", "k1", "amount=10"))

	second := httptest.NewRecorder()
	mw.ServeHTTP(second, newReq("POST", "/pay", "k1", "amount=10"))

	if h.calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", h.calls.Load())
	}
	if second.Code != http.StatusOK || second.Body.String() != "amount=10" {
		t.Fatalf("replay = %d %q, want 200 echoed", second.Code, second.Body.String())
	}
	if second.Header().Get(HeaderReplayed) != "true" {
		t.Fatalf("replay missing %s header", HeaderReplayed)
	}
	if second.Header().Get("X-Custom") != "from-handler" {
		t.Fatalf("replay did not preserve handler headers")
	}
	if first.Header().Get(HeaderReplayed) != "" {
		t.Fatalf("first (fresh) response should not be marked replayed")
	}
}

func TestSameKeyDifferentBodyRejected(t *testing.T) {
	h := &echoHandler{}
	mw, _ := wrap(h)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, newReq("POST", "/pay", "k1", "amount=10"))

	second := httptest.NewRecorder()
	mw.ServeHTTP(second, newReq("POST", "/pay", "k1", "amount=999"))

	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", second.Code)
	}
	if h.calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", h.calls.Load())
	}
}

func TestSameKeyDifferentRouteRejected(t *testing.T) {
	h := &echoHandler{}
	mw, _ := wrap(h)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, newReq("POST", "/pay", "k1", "amount=10"))

	second := httptest.NewRecorder()
	mw.ServeHTTP(second, newReq("POST", "/refund", "k1", "amount=10"))

	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", second.Code)
	}
}

func TestInFlightRetryGets409(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(started)
		<-release
		w.Write([]byte("done"))
	})
	mw, _ := wrap(h)

	leaderDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, newReq("POST", "/pay", "k1", "amount=10"))
		leaderDone <- rec
	}()

	<-started // leader is inside the handler

	conflict := httptest.NewRecorder()
	mw.ServeHTTP(conflict, newReq("POST", "/pay", "k1", "amount=10"))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("in-flight retry status = %d, want 409", conflict.Code)
	}

	close(release)
	leader := <-leaderDone
	if leader.Code != http.StatusOK || leader.Body.String() != "done" {
		t.Fatalf("leader = %d %q, want 200 done", leader.Code, leader.Body.String())
	}

	// After completion, a retry replays.
	replayRec := httptest.NewRecorder()
	mw.ServeHTTP(replayRec, newReq("POST", "/pay", "k1", "amount=10"))
	if replayRec.Code != http.StatusOK || replayRec.Header().Get(HeaderReplayed) != "true" {
		t.Fatalf("post-completion retry = %d replayed=%q, want 200 true",
			replayRec.Code, replayRec.Header().Get(HeaderReplayed))
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
}

func TestServerErrorReExecutes(t *testing.T) {
	h := &echoHandler{status: http.StatusInternalServerError}
	mw, _ := wrap(h)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, newReq("POST", "/pay", "k1", "amount=10"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first status = %d, want 500", first.Code)
	}

	// Handler now succeeds; the same key must re-execute, not replay the 500.
	h.status = 0
	second := httptest.NewRecorder()
	mw.ServeHTTP(second, newReq("POST", "/pay", "k1", "amount=10"))
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, want 200 (re-executed)", second.Code)
	}
	if second.Header().Get(HeaderReplayed) != "" {
		t.Fatalf("500 should not have been cached/replayed")
	}
	if h.calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", h.calls.Load())
	}
}

func TestClientErrorCached(t *testing.T) {
	h := &echoHandler{status: http.StatusBadRequest}
	mw, _ := wrap(h)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, newReq("POST", "/pay", "k1", "amount=10"))

	second := httptest.NewRecorder()
	mw.ServeHTTP(second, newReq("POST", "/pay", "k1", "amount=10"))

	if second.Code != http.StatusBadRequest {
		t.Fatalf("second status = %d, want 400 (cached)", second.Code)
	}
	if second.Header().Get(HeaderReplayed) != "true" {
		t.Fatalf("4xx should have been cached and replayed")
	}
	if h.calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", h.calls.Load())
	}
}

func TestPanicReleasesKey(t *testing.T) {
	var calls atomic.Int64
	panicOnce := true
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if panicOnce {
			panicOnce = false
			panic("boom")
		}
		w.Write([]byte("ok"))
	})
	mw, _ := wrap(h)

	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic to propagate")
			}
		}()
		mw.ServeHTTP(httptest.NewRecorder(), newReq("POST", "/pay", "k1", "x"))
	}()

	// Key must have been released: the retry re-executes and succeeds.
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, newReq("POST", "/pay", "k1", "x"))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("retry after panic = %d %q, want 200 ok", rec.Code, rec.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", calls.Load())
	}
}

func TestCompletedEntryExpiresLazily(t *testing.T) {
	h := &echoHandler{}
	store := NewMemoryStore(40*time.Millisecond, 0) // lazy expiry only
	mw := New(store)(h)

	mw.ServeHTTP(httptest.NewRecorder(), newReq("POST", "/pay", "k1", "x"))

	time.Sleep(70 * time.Millisecond)

	// Past the TTL the key behaves as new and re-executes.
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, newReq("POST", "/pay", "k1", "x"))
	if rec.Header().Get(HeaderReplayed) != "" {
		t.Fatalf("expired entry should not replay")
	}
	if h.calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", h.calls.Load())
	}
}

func TestSweeperReclaimsMemory(t *testing.T) {
	h := &echoHandler{}
	store := NewMemoryStore(30*time.Millisecond, 10*time.Millisecond)
	defer store.Close()
	mw := New(store)(h)

	mw.ServeHTTP(httptest.NewRecorder(), newReq("POST", "/pay", "k1", "x"))
	if store.size() != 1 {
		t.Fatalf("size = %d, want 1", store.size())
	}

	deadline := time.Now().Add(time.Second)
	for store.size() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if store.size() != 0 {
		t.Fatalf("sweeper did not reclaim entry, size = %d", store.size())
	}
}

// TestConcurrentSameKeyExecutesOnce is the core at-most-once guarantee under a
// stampede. Run with -race.
func TestConcurrentSameKeyExecutesOnce(t *testing.T) {
	var calls atomic.Int64
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(5 * time.Millisecond) // widen the in-flight window
		w.Write([]byte("ok"))
	})
	mw, _ := wrap(h)

	const n = 64
	var wg sync.WaitGroup
	recs := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs[i] = httptest.NewRecorder()
			mw.ServeHTTP(recs[i], newReq("POST", "/pay", "k1", "amount=10"))
		}(i)
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", calls.Load())
	}

	// Every response is either the single fresh 200, a 409 (raced the leader),
	// or a replayed 200. Exactly one 200 must be un-replayed: the leader's.
	fresh, conflicts, replays := 0, 0, 0
	for _, rec := range recs {
		switch {
		case rec.Code == http.StatusConflict:
			conflicts++
		case rec.Code == http.StatusOK && rec.Header().Get(HeaderReplayed) == "true":
			replays++
		case rec.Code == http.StatusOK:
			fresh++
		default:
			t.Fatalf("unexpected response %d", rec.Code)
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh executions = %d, want 1 (conflicts=%d replays=%d)", fresh, conflicts, replays)
	}
	if fresh+conflicts+replays != n {
		t.Fatalf("accounted %d responses, want %d", fresh+conflicts+replays, n)
	}
}

// TestConcurrentDifferentKeysAllRun shows distinct keys are not serialized by
// the middleware: all execute concurrently.
func TestConcurrentDifferentKeysAllRun(t *testing.T) {
	var calls atomic.Int64
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write([]byte("ok"))
	})
	mw, _ := wrap(h)

	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			mw.ServeHTTP(rec, newReq("POST", "/pay", fmt.Sprintf("key-%d", i), "amount=10"))
		}(i)
	}
	wg.Wait()

	if calls.Load() != n {
		t.Fatalf("handler ran %d times, want %d", calls.Load(), n)
	}
}

// TestStoreClaimSingleLeaderUnderStampede exercises the store primitive on its
// own: under a stampede on one key, Claim must hand leadership to exactly one
// caller. This is the atomic guarantee the whole middleware rests on, tested
// here without the middleware in the way. Run with -race.
func TestStoreClaimSingleLeaderUnderStampede(t *testing.T) {
	store := NewMemoryStore(time.Hour, 0)

	const n = 200
	var leaders atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, leader := store.Claim("k1", "fp"); leader {
				leaders.Add(1)
			}
		}()
	}
	wg.Wait()

	if leaders.Load() != 1 {
		t.Fatalf("Claim granted leadership %d times, want exactly 1", leaders.Load())
	}
}

// TestConcurrentManyKeysEachExecuteOnce runs a stampede across many distinct
// keys at once, each key hit by several concurrent duplicates. Every key must
// execute exactly once no matter how the duplicates interleave. Run with -race.
func TestConcurrentManyKeysEachExecuteOnce(t *testing.T) {
	var perKey sync.Map // key -> *atomic.Int64
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := perKey.LoadOrStore(r.Header.Get(HeaderKey), &atomic.Int64{})
		c.(*atomic.Int64).Add(1)
		time.Sleep(2 * time.Millisecond) // widen the in-flight window
		w.Write([]byte("ok"))
	})
	mw, _ := wrap(h)

	const keys = 32
	const dupes = 8
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		for d := 0; d < dupes; d++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				rec := httptest.NewRecorder()
				mw.ServeHTTP(rec, newReq("POST", "/pay", fmt.Sprintf("key-%d", k), "amount=10"))
			}(k)
		}
	}
	wg.Wait()

	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%d", k)
		c, ok := perKey.Load(key)
		if !ok {
			t.Fatalf("%s never executed", key)
		}
		if got := c.(*atomic.Int64).Load(); got != 1 {
			t.Fatalf("%s executed %d times, want exactly 1", key, got)
		}
	}
}

// TestCompleteAfterDiscardIsNoOp asserts a completion cannot resurrect a key
// that was already discarded (as a 5xx or panic releases it). The state guard
// in Complete keeps a late writer from re-creating the entry.
func TestCompleteAfterDiscardIsNoOp(t *testing.T) {
	store := NewMemoryStore(time.Hour, 0)

	if _, leader := store.Claim("k1", "fp"); !leader {
		t.Fatalf("first Claim should win leadership")
	}
	store.Discard("k1")
	store.Complete("k1", Response{Status: http.StatusOK})

	if store.size() != 0 {
		t.Fatalf("Complete resurrected a discarded key, size = %d", store.size())
	}
	if _, leader := store.Claim("k1", "fp"); !leader {
		t.Fatalf("a discarded key should be claimable again")
	}
}

// TestDiscardLeavesCompletedEntry asserts Discard releases only an in-flight
// reservation; it must not delete an already-completed result that a retry
// still needs to replay.
func TestDiscardLeavesCompletedEntry(t *testing.T) {
	store := NewMemoryStore(time.Hour, 0)

	store.Claim("k1", "fp")
	store.Complete("k1", Response{Status: http.StatusCreated})
	store.Discard("k1") // ignored: the entry is completed, not in-flight

	rec, leader := store.Claim("k1", "fp")
	if leader {
		t.Fatalf("completed entry was wrongly discarded; key was re-claimed")
	}
	if rec.State != StateCompleted || rec.Response.Status != http.StatusCreated {
		t.Fatalf("completed record lost after Discard: %+v", rec)
	}
}
