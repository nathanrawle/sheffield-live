package store

import (
	"testing"
	"time"

	"sheffield-live/internal/domain"
)

func TestSeedStoreLookups(t *testing.T) {
	st := NewSeedStore()

	if got := len(st.Venues()); got != 3 {
		t.Fatalf("venues = %d, want 3", got)
	}
	if got := len(st.Events()); got != 4 {
		t.Fatalf("events = %d, want 4", got)
	}

	if _, ok := st.VenueBySlug("leadmill"); !ok {
		t.Fatal("expected leadmill venue")
	}
	if _, ok := st.EventBySlug("matinee-noise-at-the-leadmill"); !ok {
		t.Fatal("expected matinee-noise-at-the-leadmill event")
	}

	events := st.EventsForVenue("leadmill")
	if got := len(events); got != 2 {
		t.Fatalf("leadmill events = %d, want 2", got)
	}
}

func TestSeedStoreMarksAllFixturesSeed(t *testing.T) {
	st := NewSeedStore()

	for _, venue := range st.Venues() {
		if venue.Origin != domain.OriginSeed {
			t.Fatalf("venue %q origin = %q, want seed", venue.Slug, venue.Origin)
		}
	}
	for _, event := range st.Events() {
		if event.Origin != domain.OriginSeed {
			t.Fatalf("event %q origin = %q, want seed", event.Slug, event.Origin)
		}
	}
}

func TestEventsAreSortedByStartThenSlug(t *testing.T) {
	start := time.Date(2026, time.May, 8, 18, 0, 0, 0, time.UTC)
	st := NewStore([]domain.Venue{
		{Slug: "venue", Name: "Venue"},
	}, []domain.Event{
		{Slug: "b", VenueSlug: "venue", Start: start.Add(time.Hour)},
		{Slug: "c", VenueSlug: "venue", Start: start},
		{Slug: "a", VenueSlug: "venue", Start: start},
	})

	events := st.Events()
	got := []string{events[0].Slug, events[1].Slug, events[2].Slug}
	want := []string{"a", "c", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event order = %v, want %v", got, want)
		}
	}
}

func TestValidateRejectsMissingVenueReference(t *testing.T) {
	st := NewStore(nil, []domain.Event{
		{Slug: "event", VenueSlug: "missing"},
	})

	if err := st.Validate(); err == nil {
		t.Fatal("expected missing venue validation error")
	}
}

func TestSeedEventsStoreUTC(t *testing.T) {
	st := NewSeedStore()
	event, ok := st.EventBySlug("matinee-noise-at-the-leadmill")
	if !ok {
		t.Fatal("expected event")
	}
	if event.Start.Location() != time.UTC {
		t.Fatalf("start location = %v, want UTC", event.Start.Location())
	}
	if event.LastChecked.Location() != time.UTC {
		t.Fatalf("last checked location = %v, want UTC", event.LastChecked.Location())
	}
}
