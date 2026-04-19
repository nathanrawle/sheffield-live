package store

import (
	"fmt"
	"sort"
	"time"

	"sheffield-live/internal/domain"
)

type Store struct {
	venues []domain.Venue
	events []domain.Event
}

func NewStore(venues []domain.Venue, events []domain.Event) *Store {
	venueCopy := make([]domain.Venue, len(venues))
	copy(venueCopy, venues)

	eventCopy := make([]domain.Event, len(events))
	copy(eventCopy, events)
	sortEvents(eventCopy)

	return &Store{
		venues: venueCopy,
		events: eventCopy,
	}
}

func NewSeedStore() *Store {
	venues := []domain.Venue{
		{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "A long-running Sheffield venue for touring bands and club nights.",
			Website:       "https://leadmill.co.uk/events/live-music/",
			Origin:        domain.OriginSeed,
		},
		{
			Slug:          "yellow-arch",
			Name:          "Yellow Arch Studios",
			Address:       "30-36 Burton Road, Sheffield",
			Neighbourhood: "Neepsend",
			Description:   "A studio and venue space for live shows, club nights, and independent promoters.",
			Website:       "https://www.yellowarch.com/events/",
			Origin:        domain.OriginSeed,
		},
		{
			Slug:          "sidney-and-matilda",
			Name:          "Sidney & Matilda",
			Address:       "Rivelin Works, 46 Sidney Street, Sheffield",
			Neighbourhood: "Cultural Industries Quarter",
			Description:   "A venue and gallery space with mixed bills, DJs, and late-night shows.",
			Website:       "https://www.sidneyandmatilda.com/",
			Origin:        domain.OriginSeed,
		},
	}

	checked := localTime(2026, time.April, 19, 10, 0)
	events := []domain.Event{
		{
			Slug:        "matinee-noise-at-the-leadmill",
			Name:        "Matinee Noise",
			VenueSlug:   "leadmill",
			Start:       localTime(2026, time.May, 8, 19, 30),
			End:         localTime(2026, time.May, 8, 23, 0),
			Genre:       "Indie / alt",
			Status:      "Listed",
			Description: "A Friday bill with local support and a touring headliner.",
			SourceName:  "The Leadmill live music listings",
			SourceURL:   "https://leadmill.co.uk/events/live-music/",
			LastChecked: checked,
			Origin:      domain.OriginSeed,
		},
		{
			Slug:        "neepsend-afterhours",
			Name:        "Abbeydale Afterhours",
			VenueSlug:   "yellow-arch",
			Start:       localTime(2026, time.May, 14, 20, 0),
			End:         localTime(2026, time.May, 14, 23, 30),
			Genre:       "Jazz / soul",
			Status:      "Listed",
			Description: "A midweek set with keys, brass, and soul.",
			SourceName:  "Yellow Arch what's on",
			SourceURL:   "https://www.yellowarch.com/events/",
			LastChecked: checked,
			Origin:      domain.OriginSeed,
		},
		{
			Slug:        "courtyard-wildcards",
			Name:        "Courtyard Wildcards",
			VenueSlug:   "sidney-and-matilda",
			Start:       localTime(2026, time.May, 22, 18, 45),
			End:         localTime(2026, time.May, 22, 22, 45),
			Genre:       "Punk / garage",
			Status:      "Listed",
			Description: "A garage and punk double bill.",
			SourceName:  "Sidney & Matilda listings",
			SourceURL:   "https://www.sidneyandmatilda.com/",
			LastChecked: checked,
			Origin:      domain.OriginSeed,
		},
		{
			Slug:        "leadmill-late-room",
			Name:        "Late Room",
			VenueSlug:   "leadmill",
			Start:       localTime(2026, time.June, 5, 22, 0),
			End:         localTime(2026, time.June, 6, 1, 0),
			Genre:       "DJ / club",
			Status:      "Listed",
			Description: "A late-room dance set.",
			SourceName:  "The Leadmill live music listings",
			SourceURL:   "https://leadmill.co.uk/events/live-music/",
			LastChecked: checked,
			Origin:      domain.OriginSeed,
		},
	}

	return NewStore(venues, events)
}

func (s *Store) Venues() []domain.Venue {
	out := make([]domain.Venue, len(s.venues))
	copy(out, s.venues)
	return out
}

func (s *Store) Events() []domain.Event {
	out := make([]domain.Event, len(s.events))
	copy(out, s.events)
	return out
}

func (s *Store) VenueBySlug(slug string) (domain.Venue, bool) {
	for _, venue := range s.venues {
		if venue.Slug == slug {
			return venue, true
		}
	}
	return domain.Venue{}, false
}

func (s *Store) EventBySlug(slug string) (domain.Event, bool) {
	for _, event := range s.events {
		if event.Slug == slug {
			return event, true
		}
	}
	return domain.Event{}, false
}

func (s *Store) EventsForVenue(venueSlug string) []domain.Event {
	var out []domain.Event
	for _, event := range s.events {
		if event.VenueSlug == venueSlug {
			out = append(out, event)
		}
	}
	return out
}

func (s *Store) Validate() error {
	for _, event := range s.events {
		if _, ok := s.VenueBySlug(event.VenueSlug); !ok {
			return fmt.Errorf("event %q references missing venue %q", event.Slug, event.VenueSlug)
		}
	}
	return nil
}

func sortEvents(events []domain.Event) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Start.Equal(events[j].Start) {
			return events[i].Slug < events[j].Slug
		}
		return events[i].Start.Before(events[j].Start)
	})
}

func localTime(year int, month time.Month, day, hour, minute int) time.Time {
	return time.Date(year, month, day, hour, minute, 0, 0, sheffieldLocation()).UTC()
}

func sheffieldLocation() *time.Location {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.FixedZone("Europe/London", 0)
	}
	return loc
}
