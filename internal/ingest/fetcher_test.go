package ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPFetcherSetsUserAgent(t *testing.T) {
	const userAgent = "sheffield-live-test/1.0"
	seen := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.UserAgent()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fetcher, err := NewHTTPFetcher(time.Second, userAgent)
	if err != nil {
		t.Fatalf("new fetcher: %v", err)
	}

	if _, err := fetcher.Fetch(context.Background(), server.URL); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if got := <-seen; got != userAgent {
		t.Fatalf("user-agent = %q, want %q", got, userAgent)
	}
}

func TestHTTPFetcherBoundedBodySetsTruncated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("abcdef"))
	}))
	defer server.Close()

	fetcher, err := NewHTTPFetcher(time.Second, "sheffield-live-test/1.0")
	if err != nil {
		t.Fatalf("new fetcher: %v", err)
	}
	fetcher.maxBodyBytes = 3

	result, err := fetcher.Fetch(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !result.Truncated {
		t.Fatal("truncated = false, want true")
	}
	if got, want := string(result.Body), "abc"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestHTTPFetcherReturnsNon2xxBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("short and stout"))
	}))
	defer server.Close()

	fetcher, err := NewHTTPFetcher(time.Second, "sheffield-live-test/1.0")
	if err != nil {
		t.Fatalf("new fetcher: %v", err)
	}

	result, err := fetcher.Fetch(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if got, want := result.StatusCode, http.StatusTeapot; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
	if got, want := string(result.Body), "short and stout"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestHTTPFetcherRedirectPreservesUserAgent(t *testing.T) {
	const userAgent = "sheffield-live-test/1.0"
	seen := make(chan string, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.UserAgent()
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirect.Close()

	fetcher, err := NewHTTPFetcher(time.Second, userAgent)
	if err != nil {
		t.Fatalf("new fetcher: %v", err)
	}

	result, err := fetcher.Fetch(context.Background(), redirect.URL)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if got := <-seen; got != userAgent {
		t.Fatalf("redirected user-agent = %q, want %q", got, userAgent)
	}
	if !strings.HasPrefix(result.FinalURL, target.URL) {
		t.Fatalf("final URL = %q, want prefix %q", result.FinalURL, target.URL)
	}
}
