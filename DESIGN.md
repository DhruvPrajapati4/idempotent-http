# Design Notes: Idempotency-Key Middleware

An in-memory, single-node idempotency layer for HTTP POST handlers. A client attaches an
`Idempotency-Key` header; the middleware guarantees the wrapped handler runs at most once per
key, and that retries receive a coherent response no matter how they interleave.

## Shape

`Middleware(store, opts...) func(http.Handler) http.Handler`, built over a small `Store`
interface with one in-memory implementation. Per request:

1. Read the `Idempotency-Key` header.
2. Buffer the request body (needed twice: to fingerprint, and to hand to the handler).
3. Atomically look up / claim the key in the store. The entry is in one of three states:
   **absent**, **in-flight**, or **completed**.
4. Branch on state.
5. Capture the handler's output through a `http.ResponseWriter` wrapper (status, headers, body)
   so a completed result can be replayed byte-for-byte.

The `Store` interface is the single seam where the multi-node story lives: the in-process
"claim if absent" becomes an atomic `SETNX`/conditional-insert in a shared store. That is the
only structural change the concurrency model requires.

## Decisions

### Concurrency (the backbone, `-race` clean)

**Decision.** A single `map[string]*entry` guarded by one mutex. The lock is held only for the
"check-whether-present-and-claim-if-absent" step, then released before the handler runs. Exactly
one goroutine can find the key absent and insert it; every other goroutine is forced to find it
present. All access to the map and to an entry's mutable fields (`state`, `resp`) happens under
the same mutex, which gives the happens-before edge the race detector requires.

**Why not hold the lock across the handler.** The lock protects the map, not the work. Holding it
during a handler (which may block on I/O) would serialize every request for every key, collapsing
throughput. So: lock, claim, unlock, run, lock, record, unlock.

**Rejected alternative.** `sync.Map` with `LoadOrStore` gives atomic claim for free, but makes
TTL sweeping and ordered eviction awkward to express. A plain map plus one mutex is easier to
reason about and to defend end to end.

### A retry arrives while the first execution is still in flight

**Decision.** Return `409 Conflict` immediately. The in-flight entry is left in place; the leader
keeps running.

**Why.** Concurrent duplicates cluster during retry storms, exactly when the server is already
stressed. Returning 409 sheds that load in microseconds. `409` is also honest: the outcome is not
yet known, and an idempotent client already retries with backoff, which is the correct place to
decide when to look again. It keeps the concurrency machinery minimal (no waiting goroutines, no
wait-timeouts), which matters against the `-race` bar. Cost: the caller pays one extra round trip
after the leader finishes.

**Rejected alternative.** Block the retry on a `done` channel and replay the leader's result when
it lands. Better single-call UX, but it pins a goroutine and connection per waiting retry for an
unbounded time and adds timeout/cancellation machinery. Would be added behind an option only if a
synchronous "wait for the in-flight result" guarantee were actually required.

### Same key, different request

**Decision.** Store a fingerprint (hash of method + path + query + body) alongside the entry. On a
key hit, matching fingerprint means a legitimate retry; a mismatch returns `422 Unprocessable
Entity`.

**Why.** A key reused for a different request is a client bug or a collision. Without the check,
the middleware would silently replay the wrong response, which defeats its purpose.

**Rejected alternative.** Trust the key and skip the fingerprint. Cheaper and less memory, but
turns a client mistake into a silent correctness hole.

### Which failures replay, which re-execute

**Decision.** A completed response with status `< 500` (including 4xx) is cached and replayed. A
response `>= 500`, a handler panic, or a connection dropped mid-flight is treated as
indeterminate: the entry is removed so a retry re-executes.

**Why.** 5xx means the side effect's fate is unknown, which is the whole reason idempotent retries
exist; caching it would lock in a transient failure permanently. 4xx is a deterministic verdict on
the request itself, so replaying it is correct and cheap.

**Rejected alternatives.** Cache everything including 5xx (locks in transient failures, retries can
never recover). Cache only 2xx (4xx would re-execute needlessly, and a side effect performed before
a 4xx could double).

### Bounded memory

**Decision.** Completed entries carry a long TTL (e.g. 24h) governing how long a result is
remembered for replay; it must be at least the client's retry window. Expiry is both lazy (an
expired entry is treated as absent on lookup, so the key behaves as new) and swept by a background
ticker that reclaims memory for keys never looked up again. In-flight entries are not TTL'd: they
are cleared only by completion, by an explicit discard on a 5xx, or by `defer`/`recover` on a panic.
The count of in-flight entries is therefore naturally bounded by the number of requests in progress.

**Why.** Lazy expiry keeps the hot path correct at zero background cost; the sweeper reclaims memory
that lazy expiry alone would never revisit. With TTL `T` and arrival rate `R` unique keys/sec,
steady-state memory is about `R x T` entries: bounded given a bounded arrival rate.

**Known limitation.** A handler that hangs forever (alive process, stuck goroutine) holds its key at
`409` until the process restarts, which clears the in-memory map. The tempting fix is a lease that
lets another request take over an expired in-flight key, but a lease also fires on a merely *slow*
handler, which would both double-execute and let the slow leader's late completion clobber the
takeover's result. Doing that safely needs fencing tokens (each leader holds a token; completion is
rejected if the token no longer matches). That machinery belongs with the durable multi-node store,
not a single-node cache whose map dies with the process anyway, so it is deliberately not built.

**Rejected alternatives.** An unbounded map leaks. TTL alone is not bounded against an adversary who
mints unlimited unique keys inside one window; a hard max-entries cap with soonest-to-expire
eviction would close that, and is named here as the next step but deliberately not built, since the
brief's threat model does not require it.

### Requests without a key

**Decision.** Reject with `400 Bad Request`.

**Why.** The reason to wrap an endpoint in this middleware is retry safety. Silently letting an
unkeyed mutation through would defeat that guarantee and hide the omission from the caller.

**Rejected alternative.** Pass through unprotected (treat the key as optional, Stripe's model).
Reasonable, but for a middleware whose single job is safety, failing loud is the more defensible
default.

## Out of scope (deliberately)

Persistence, multi-node coordination, pluggable hashing or eviction policies, metrics, and a real
external store. The `Store` interface stays as the seam for those; only the in-memory
implementation ships. An unfinished deliberate design beats a finished indiscriminate one.
