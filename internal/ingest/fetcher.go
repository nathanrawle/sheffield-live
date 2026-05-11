package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"
)

const DefaultMaxBodyBytes int64 = 2 * 1024 * 1024
const DefaultMaxImageBytes int64 = 5 * 1024 * 1024

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
	return NewHTTPFetcherWithMaxBodyBytes(timeout, userAgent, DefaultMaxBodyBytes)
}

func NewHTTPFetcherWithMaxBodyBytes(timeout time.Duration, userAgent string, maxBodyBytes int64) (*HTTPFetcher, error) {
	if timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}
	if userAgent == "" {
		return nil, errors.New("user-agent must be set")
	}
	if maxBodyBytes <= 0 {
		maxBodyBytes = DefaultMaxBodyBytes
	}

	fetcher := &HTTPFetcher{
		userAgent:    userAgent,
		maxBodyBytes: maxBodyBytes,
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
	return f.fetch(ctx, rawURL, f.client)
}

func (f *HTTPFetcher) FetchWithURLValidator(ctx context.Context, rawURL string, validate func(string) error) (FetchResult, error) {
	if f == nil {
		return FetchResult{}, errors.New("fetcher is nil")
	}
	if f.client == nil {
		return FetchResult{}, errors.New("fetcher client is nil")
	}
	if validate != nil {
		if err := validate(rawURL); err != nil {
			return FetchResult{URL: rawURL, CapturedAt: time.Now().UTC()}, err
		}
	}
	client := *f.client
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if validate != nil {
			if err := validate(req.URL.String()); err != nil {
				return err
			}
		}
		req.Header.Set("User-Agent", f.userAgent)
		return nil
	}
	client.Transport = restrictedRemoteTransport(f.client.Transport)
	return f.fetch(ctx, rawURL, &client)
}

func (f *HTTPFetcher) fetch(ctx context.Context, rawURL string, client *http.Client) (FetchResult, error) {
	if f == nil {
		return FetchResult{}, errors.New("fetcher is nil")
	}
	if client == nil {
		return FetchResult{}, errors.New("fetcher client is nil")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return FetchResult{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", f.userAgent)

	resp, err := client.Do(req)
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

func restrictedRemoteTransport(base http.RoundTripper) http.RoundTripper {
	var transport *http.Transport
	if base == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	} else if t, ok := base.(*http.Transport); ok {
		transport = t.Clone()
	} else {
		return base
	}
	transport.Proxy = nil
	transport.DialContext = publicRemoteDialContext
	return transport
}

func publicRemoteDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if err := validateRemoteHost(host); err != nil {
		return nil, err
	}
	var dialer net.Dialer
	if ip := net.ParseIP(host); ip != nil {
		return dialer.DialContext(ctx, network, address)
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("resolve %s: no addresses", host)
	}
	for _, addr := range addrs {
		parsed, ok := netipAddr(addr.IP)
		if !ok {
			return nil, fmt.Errorf("resolve %s: invalid address %q", host, addr.IP.String())
		}
		if err := validateRemoteAddr(parsed); err != nil {
			return nil, fmt.Errorf("resolve %s: %w", host, err)
		}
	}
	var lastErr error
	for _, addr := range addrs {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(addr.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func netipAddr(ip net.IP) (netip.Addr, bool) {
	if ip4 := ip.To4(); ip4 != nil {
		var addr [4]byte
		copy(addr[:], ip4)
		return netip.AddrFrom4(addr), true
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return netip.Addr{}, false
	}
	var addr [16]byte
	copy(addr[:], ip16)
	return netip.AddrFrom16(addr), true
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
