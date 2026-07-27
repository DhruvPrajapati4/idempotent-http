package idempotent

import (
	"bytes"
	"net/http"
)

// recorder tees a handler's output: it writes through to the real client (the
// leader still gets a live response) while capturing status, headers and body
// so the result can be stored and replayed to later retries.
type recorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	body        bytes.Buffer
}

func (r *recorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		// Mirror net/http: a bare Write implies 200.
		r.WriteHeader(http.StatusOK)
	}
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

// result snapshots what the handler produced. status defaults to 200 to match
// net/http's behaviour when a handler returns without writing anything.
func (r *recorder) result() Response {
	status := r.status
	if status == 0 {
		status = http.StatusOK
	}
	return Response{
		Status: status,
		Header: r.ResponseWriter.Header().Clone(),
		Body:   append([]byte(nil), r.body.Bytes()...),
	}
}
