package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/store"
)

func TestRoutes(t *testing.T) {
	server, err := NewServer(store.NewSeedStore())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	tests := []struct {
		name string
		path string
		code int
		body string
	}{
		{name: "home", path: "/", code: http.StatusOK, body: "Sheffield live music"},
		{name: "events", path: "/events", code: http.StatusOK, body: "Upcoming shows"},
		{name: "event detail", path: "/events/matinee-noise-at-the-leadmill", code: http.StatusOK, body: "The Leadmill live music listings"},
		{name: "venues", path: "/venues", code: http.StatusOK, body: "Sheffield rooms"},
		{name: "venue detail", path: "/venues/leadmill", code: http.StatusOK, body: "Leadmill"},
		{name: "static css", path: "/static/site.css", code: http.StatusOK, body: "color-scheme"},
		{name: "healthz", path: "/healthz", code: http.StatusOK, body: "ok"},
		{name: "missing", path: "/events/missing", code: http.StatusNotFound, body: "404 page not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rr := httptest.NewRecorder()

			server.ServeHTTP(rr, req)

			if rr.Code != tc.code {
				t.Fatalf("status = %d, want %d", rr.Code, tc.code)
			}
			if !strings.Contains(rr.Body.String(), tc.body) {
				t.Fatalf("body missing %q in %q", tc.body, rr.Body.String())
			}
		})
	}
}

func TestNewServerRejectsMissingEventVenue(t *testing.T) {
	st := store.NewStore(nil, []domain.Event{
		{
			Slug:        "missing-venue",
			Name:        "Missing Venue",
			VenueSlug:   "not-a-venue",
			Start:       time.Date(2026, time.May, 8, 18, 0, 0, 0, time.UTC),
			End:         time.Date(2026, time.May, 8, 20, 0, 0, 0, time.UTC),
			SourceName:  "test",
			SourceURL:   "https://example.test",
			LastChecked: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC),
		},
	})

	if _, err := NewServer(st); err == nil {
		t.Fatal("expected missing venue validation error")
	}
}
