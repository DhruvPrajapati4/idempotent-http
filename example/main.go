// Command example demonstrates the idempotency middleware end to end. It starts
// an in-process HTTP server whose handler simulates a slow, side-effecting
// "charge" endpoint, then drives it through each scenario the library handles
// and prints the outcome.
//
//	go run ./example
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	idempotent "github.com/DhruvPrajapati4/idempotent-http"
)

func main() {
	var charges atomic.Int64

	// The protected endpoint: a mutating operation we do not want to run twice
	// for one logical request. It is deliberately slow to widen the in-flight
	// window.
	charge := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		n := charges.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "charge #%d created", n)
	})

	store := idempotent.NewMemoryStore(1*time.Hour, 1*time.Minute)
	defer store.Close()

	srv := httptest.NewServer(idempotent.New(store)(charge))
	defer srv.Close()

	fmt.Println("server:", srv.URL)
	fmt.Println()

	// 1. First request with a key: the handler runs.
	step("first request (key=abc)")
	post(srv.URL, "abc", "amount=1000")

	// 2. Retry with the same key and body: replayed, handler does NOT run.
	step("retry, same key + body (key=abc)")
	post(srv.URL, "abc", "amount=1000")

	// 3. Same key, different body: rejected as a conflict.
	step("same key, DIFFERENT body (key=abc)")
	post(srv.URL, "abc", "amount=9999")

	// 4. Two concurrent requests with a fresh key: one leads, one gets 409.
	step("two concurrent requests (key=xyz)")
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); post(srv.URL, "xyz", "amount=500") }()
	}
	wg.Wait()

	// 5. Missing key: rejected.
	step("no Idempotency-Key header")
	post(srv.URL, "", "amount=1")

	fmt.Println()
	fmt.Printf("handler executed %d time(s) across all requests above\n", charges.Load())
}

func step(title string) {
	fmt.Println("=>", title)
}

func post(url, key, body string) {
	req, _ := http.NewRequest(http.MethodPost, url+"/charge", strings.NewReader(body))
	if key != "" {
		req.Header.Set(idempotent.HeaderKey, key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("   error:", err)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)

	replayed := ""
	if resp.Header.Get(idempotent.HeaderReplayed) == "true" {
		replayed = "  [replayed]"
	}
	fmt.Printf("   %d %s  %q%s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(b), replayed)
}
