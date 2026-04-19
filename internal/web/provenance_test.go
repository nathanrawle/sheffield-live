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

func TestSeedPagesShowOriginLabels(t *testing.T) {
	server := mustServer(t, store.NewSeedStore())

	for _, path := range []string{
		"/events",
		"/venues",
		"/events/matinee-noise-at-the-leadmill",
		"/venues/leadmill",
	} {
		body := renderPath(t, server, path)
		if !strings.Contains(body, "Seed data") {
			t.Fatalf("%s missing seed badge in %q", path, body)
		}
	}
}

func TestLiveFixtureDataRendersWithoutLabel(t *testing.T) {
	server := mustServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "live-venue",
			Name:          "Live Venue",
			Address:       "1 Live Street, Sheffield",
			Neighbourhood: "Centre",
			Description:   "Live venue",
			Website:       "https://example.com/live",
			Origin:        domain.OriginLive,
		}},
		[]domain.Event{{
			Slug:        "live-event",
			Name:        "Live Event",
			VenueSlug:   "live-venue",
			Start:       time.Date(2026, time.May, 8, 18, 0, 0, 0, time.UTC),
			End:         time.Date(2026, time.May, 8, 20, 0, 0, 0, time.UTC),
			SourceName:  "Live source",
			SourceURL:   "https://example.com/live",
			LastChecked: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC),
			Origin:      domain.OriginLive,
		}},
	))

	for _, path := range []string{
		"/events",
		"/venues",
		"/events/live-event",
		"/venues/live-venue",
	} {
		assertNoOriginLabels(t, renderPath(t, server, path))
	}
}

func TestMixedPagesBadgeItemByItem(t *testing.T) {
	server := mustServer(t, store.NewStore(
		[]domain.Venue{
			{
				Slug:          "seed-venue",
				Name:          "Seed Venue",
				Address:       "1 Seed Street, Sheffield",
				Neighbourhood: "Centre",
				Description:   "Seed venue",
				Website:       "https://example.com/seed",
				Origin:        domain.OriginSeed,
			},
			{
				Slug:          "test-venue",
				Name:          "Test Venue",
				Address:       "2 Test Street, Sheffield",
				Neighbourhood: "Centre",
				Description:   "Test venue",
				Website:       "https://example.com/test",
				Origin:        domain.OriginTest,
			},
			{
				Slug:          "dev-venue",
				Name:          "Dev Venue",
				Address:       "3 Dev Street, Sheffield",
				Neighbourhood: "Centre",
				Description:   "Dev venue",
				Website:       "https://example.com/dev",
				Origin:        domain.OriginDev,
			},
			{
				Slug:          "live-venue",
				Name:          "Live Venue",
				Address:       "4 Live Street, Sheffield",
				Neighbourhood: "Centre",
				Description:   "Live venue",
				Website:       "https://example.com/live",
				Origin:        domain.OriginLive,
			},
		},
		[]domain.Event{
			{
				Slug:        "seed-event",
				Name:        "Seed Event",
				VenueSlug:   "seed-venue",
				Start:       time.Date(2026, time.May, 8, 18, 0, 0, 0, time.UTC),
				End:         time.Date(2026, time.May, 8, 20, 0, 0, 0, time.UTC),
				SourceName:  "Seed source",
				SourceURL:   "https://example.com/seed",
				LastChecked: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC),
				Origin:      domain.OriginSeed,
			},
			{
				Slug:        "test-event",
				Name:        "Test Event",
				VenueSlug:   "test-venue",
				Start:       time.Date(2026, time.May, 9, 18, 0, 0, 0, time.UTC),
				End:         time.Date(2026, time.May, 9, 20, 0, 0, 0, time.UTC),
				SourceName:  "Test source",
				SourceURL:   "https://example.com/test",
				LastChecked: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC),
				Origin:      domain.OriginTest,
			},
			{
				Slug:        "dev-event",
				Name:        "Dev Event",
				VenueSlug:   "dev-venue",
				Start:       time.Date(2026, time.May, 10, 18, 0, 0, 0, time.UTC),
				End:         time.Date(2026, time.May, 10, 20, 0, 0, 0, time.UTC),
				SourceName:  "Dev source",
				SourceURL:   "https://example.com/dev",
				LastChecked: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC),
				Origin:      domain.OriginDev,
			},
			{
				Slug:        "live-event",
				Name:        "Live Event",
				VenueSlug:   "live-venue",
				Start:       time.Date(2026, time.May, 11, 18, 0, 0, 0, time.UTC),
				End:         time.Date(2026, time.May, 11, 20, 0, 0, 0, time.UTC),
				SourceName:  "Live source",
				SourceURL:   "https://example.com/live",
				LastChecked: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC),
				Origin:      domain.OriginLive,
			},
		},
	))

	eventBody := renderPath(t, server, "/events")
	assertLabelCount(t, eventBody, "Seed data", 1)
	assertLabelCount(t, eventBody, "Test data", 1)
	assertLabelCount(t, eventBody, "Development data", 1)
	assertLabelCount(t, eventBody, "live data", 0)

	venueBody := renderPath(t, server, "/venues")
	assertLabelCount(t, venueBody, "Seed data", 1)
	assertLabelCount(t, venueBody, "Test data", 1)
	assertLabelCount(t, venueBody, "Development data", 1)
	assertLabelCount(t, venueBody, "live data", 0)
}

func TestMissingOriginRendersNoBadge(t *testing.T) {
	server := mustServer(t, store.NewStore(
		[]domain.Venue{{
			Slug:          "missing-origin-venue",
			Name:          "Missing Origin Venue",
			Address:       "1 Missing Street, Sheffield",
			Neighbourhood: "Centre",
			Description:   "Venue without provenance",
			Website:       "https://example.com/missing",
		}},
		[]domain.Event{{
			Slug:        "missing-origin-event",
			Name:        "Missing Origin Event",
			VenueSlug:   "missing-origin-venue",
			Start:       time.Date(2026, time.May, 12, 18, 0, 0, 0, time.UTC),
			End:         time.Date(2026, time.May, 12, 20, 0, 0, 0, time.UTC),
			SourceName:  "Missing source",
			SourceURL:   "https://example.com/missing",
			LastChecked: time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC),
		}},
	))

	for _, path := range []string{
		"/events",
		"/venues",
		"/events/missing-origin-event",
		"/venues/missing-origin-venue",
	} {
		assertNoOriginLabels(t, renderPath(t, server, path))
	}
}

func renderPath(t *testing.T, server http.Handler, path string) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()

	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("%s status = %d, want 200", path, rr.Code)
	}
	return rr.Body.String()
}

func mustServer(t *testing.T, st *store.Store) *Server {
	t.Helper()

	server, err := NewServer(st)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	server.SetClockForTesting(func() time.Time {
		return fixtureLocalTime(2026, time.April, 19, 10, 0)
	})
	return server
}

func assertNoOriginLabels(t *testing.T, body string) {
	t.Helper()

	for _, label := range []string{"Seed data", "Test data", "Development data"} {
		if strings.Contains(body, label) {
			t.Fatalf("unexpected label %q in %q", label, body)
		}
	}
}

func assertLabelCount(t *testing.T, body, label string, want int) {
	t.Helper()

	if got := strings.Count(body, label); got != want {
		t.Fatalf("count(%q) = %d, want %d in %q", label, got, want, body)
	}
}
