package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBenchmarkCountConcurrencyAndBody(t *testing.T) {
	var calls, active, peak atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != `{"test":true}` || r.Method != http.MethodPost || r.Header.Get("X-Test") != "yes" {
			t.Error("request method, body, or header was not preserved")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		time.Sleep(time.Millisecond)
		io.WriteString(w, "ok")
	}))
	defer server.Close()
	cfg := config{url: server.URL, count: 137, concurrency: 8, timeout: time.Second, method: http.MethodPost, data: `{"test":true}`, headers: headers{"X-Test: yes"}}
	result, err := benchmark(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != cfg.count || result.completed != cfg.count || result.succeeded != cfg.count || result.errors != 0 {
		t.Fatalf("calls=%d result=%+v", calls.Load(), result)
	}
	if peak.Load() <= 1 || peak.Load() > int64(cfg.concurrency) {
		t.Fatalf("peak concurrency = %d, want 2..%d", peak.Load(), cfg.concurrency)
	}
	if result.bytes != 2*cfg.count || result.statuses[200] != cfg.count {
		t.Fatalf("unexpected response statistics: %+v", result)
	}
}

func TestBenchmarkDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/target" {
			redirected.Add(1)
			return
		}
		http.Redirect(w, r, "/target", http.StatusFound)
	}))
	defer server.Close()
	result, err := benchmark(context.Background(), config{url: server.URL, count: 10, concurrency: 3, timeout: time.Second, method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if redirected.Load() != 0 || result.statuses[302] != 10 || result.succeeded != 0 || result.errors != 0 {
		t.Fatalf("redirected=%d result=%+v", redirected.Load(), result)
	}
}

func TestTimeoutCoversResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	result, err := benchmark(context.Background(), config{url: server.URL, count: 4, concurrency: 2, timeout: 50 * time.Millisecond, method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if result.completed != 4 || result.errors != 4 || result.succeeded != 0 || result.firstErr == nil {
		t.Fatalf("unexpected timeout statistics: %+v", result)
	}
}

func TestCancellationStopsScheduling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cancel()
		<-r.Context().Done()
	}))
	defer server.Close()
	result, err := benchmark(ctx, config{url: server.URL, count: 1000, concurrency: 4, timeout: time.Second, method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if result.completed < 1 || result.completed > 4 || result.errors != result.completed {
		t.Fatalf("unexpected cancellation statistics: %+v", result)
	}
}

func TestCLIClampsConcurrencyAndReportsFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	var out, errOut bytes.Buffer
	code := runCLI([]string{"-url", server.URL, "-count", "3", "-concurrency", "100"}, &out, &errOut)
	if code != 1 || !strings.Contains(out.String(), "concurrency 3") || !strings.Contains(out.String(), "Completed: 3/3") || !strings.Contains(out.String(), "HTTP 429: 3") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, &out, &errOut)
	}
}

func TestCLIRejectsInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"-url", "ftp://example.com"},
		{"-url", "https://example.com", "-count", "0"},
		{"-url", "https://example.com", "-concurrency", "-1"},
		{"-url", "https://example.com", "-timeout", "0s"},
		{"-url", "https://example.com", "unexpected"},
	} {
		if code := runCLI(args, io.Discard, io.Discard); code != 2 {
			t.Errorf("args=%v exit=%d, want 2", args, code)
		}
	}
}
