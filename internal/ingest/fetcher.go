package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const DefaultMaxBodyBytes int64 = 2 * 1024 * 1024

type Fetcher interface {
	Fetch(ctx context.Context, url string) (FetchResult, error)
}

type FetchResult struct {
	URL           string
	FinalURL      string
	Status        string
	StatusCode    int
	ContentType   string
	ContentLength int64
	Body          []byte
	Truncated     bool
	CapturedAt    time.Time
}

type HTTPFetcher struct {
	client       *http.Client
	userAgent    string
	maxBodyBytes int64
}

func NewHTTPFetcher(timeout time.Duration, userAgent string) (*HTTPFetcher, error) {
	if timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}
	if userAgent == "" {
		return nil, errors.New("user-agent must be set")
	}

	fetcher := &HTTPFetcher{
		userAgent:    userAgent,
		maxBodyBytes: DefaultMaxBodyBytes,
	}
	fetcher.client = &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			req.Header.Set("User-Agent", userAgent)
			return nil
		},
	}
	return fetcher, nil
}

func (f *HTTPFetcher) Fetch(ctx context.Context, rawURL string) (FetchResult, error) {
	if f == nil {
		return FetchResult{}, errors.New("fetcher is nil")
	}
	if f.client == nil {
		return FetchResult{}, errors.New("fetcher client is nil")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return FetchResult{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", f.userAgent)

	resp, err := f.client.Do(req)
	if err != nil {
		return FetchResult{URL: rawURL, CapturedAt: time.Now().UTC()}, fmt.Errorf("fetch %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	body, truncated, err := readBounded(resp.Body, f.maxBodyBytes)
	if err != nil {
		return FetchResult{URL: rawURL, FinalURL: resp.Request.URL.String(), Status: resp.Status, StatusCode: resp.StatusCode, CapturedAt: time.Now().UTC()}, fmt.Errorf("read %s body: %w", rawURL, err)
	}

	return FetchResult{
		URL:           rawURL,
		FinalURL:      resp.Request.URL.String(),
		Status:        resp.Status,
		StatusCode:    resp.StatusCode,
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: resp.ContentLength,
		Body:          body,
		Truncated:     truncated,
		CapturedAt:    time.Now().UTC(),
	}, nil
}

func readBounded(r io.Reader, maxBytes int64) ([]byte, bool, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBodyBytes
	}

	body, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) <= maxBytes {
		return body, false, nil
	}
	return body[:maxBytes], true, nil
}

func statusIsOK(statusCode int) bool {
	return statusCode >= 200 && statusCode <= 299
}
