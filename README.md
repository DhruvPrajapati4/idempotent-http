# idempotent-http

HTTP middleware that makes mutating endpoints safe to retry. A client tags a
logical operation with an `Idempotency-Key` header; the middleware runs the
wrapped handler **at most once** per key and gives every retry a coherent
response, no matter how the retries interleave.

In-memory, single node, standard library only.

## Usage

```go
import idempotent "github.com/DhruvPrajapati4/idempotent-http"

store := idempotent.NewMemoryStore(24*time.Hour, time.Minute) // TTL, sweep interval
defer store.Close()

mw := idempotent.New(store)
http.Handle("/charge", mw(chargeHandler))
```

## Semantics

| Situation | Response |
|---|---|
| First request with a key | handler runs; result stored |
| Retry, same key + same request | stored result replayed (`Idempotent-Replayed: true`), handler not run |
| Retry arrives while the first is still running | `409 Conflict` |
| Same key, different request (method, path, query or body) | `422 Unprocessable Entity` |
| Handler returned `< 500` (incl. 4xx) | cached and replayed |
| Handler returned `>= 500`, or panicked | not cached; a retry re-executes |
| No `Idempotency-Key` header | `400 Bad Request` |
| Completed entry older than the TTL | treated as new; re-executes |

The reasoning behind each choice, and the alternatives rejected, is in
[DESIGN.md](DESIGN.md).

## Run

```sh
go test -race ./...   # tests prove the semantics above
go run ./example      # self-driving demonstration of every scenario
```

## Scope

Persistence and multi-node coordination are out of scope. The `Store` interface
is the single seam where a durable, shared backend would slot in: the in-process
"claim if absent" becomes a conditional insert (e.g. Redis `SETNX`). See DESIGN.md
for the known single-node limitation (a permanently hung handler) and what a
durable store changes.
