package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"sheffield-live/internal/domain"
)

type Store struct {
	venues []domain.Venue
	events []domain.Event
}

type CatalogStore interface {
	ListVenues(ctx context.Context) ([]domain.Venue, error)
	ListEvents(ctx context.Context) ([]domain.Event, error)
	LoadVenueBySlug(ctx context.Context, slug string) (domain.Venue, bool, error)
	LoadEventBySlug(ctx context.Context, slug string) (domain.Event, bool, error)
	ListEventsForVenue(ctx context.Context, venueSlug string) ([]domain.Event, error)
	Validate(ctx context.Context) error
	Ready(ctx context.Context) error
}

var _ CatalogStore = (*Store)(nil)

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
			Slug:          "cafe-no-9",
			Name:          "Cafe No. 9",
			Address:       "9 Nether Edge Road, Sheffield",
			Neighbourhood: "Nether Edge",
			Description:   "An intimate Sheffield listening room hosting acoustic, folk, jazz, and songwriter bills.",
			Website:       "https://www.cafe9sheffield.co.uk/",
			CoverageKind:  domain.CoverageKindVenue,
			Origin:        domain.OriginSeed,
		},
		{
			Slug:          "corporation",
			Name:          "Corporation",
			Address:       "2 Milton Street, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "A Sheffield live venue and club space hosting touring acts, tribute nights, and alternative club events.",
			Website:       "https://www.corporation.org.uk/live/",
			CoverageKind:  domain.CoverageKindVenue,
			Origin:        domain.OriginSeed,
		},
		{
			Slug:          "leadmill",
			Name:          "The Leadmill",
			Address:       "6 Leadmill Road, Sheffield",
			Neighbourhood: "City Centre",
			Description:   "A long-running Sheffield venue for touring bands and club nights.",
			Website:       "https://leadmill.co.uk/events/live-music/",
			CoverageKind:  domain.CoverageKindVenue,
			Origin:        domain.OriginSeed,
		},
		{
			Slug:          "lescar",
			Name:          "The Lescar",
			Address:       "303 Sharrow Vale Road, Sheffield",
			Neighbourhood: "Sharrow Vale",
			Description:   "A Sheffield pub venue that hosts weekly jazz nights and occasional live music events.",
			Website:       "http://www.jazzatthelescar.com/index.html",
			CoverageKind:  domain.CoverageKindProgram,
			CoverageNote:  "Current coverage follows the Jazz at The Lescar programme rather than every event at The Lescar.",
			Origin:        domain.OriginSeed,
		},
		{
			Slug:          "greystones",
			Name:          "The Greystones",
			Address:       "Greystones Road, Sheffield",
			Neighbourhood: "Ecclesall",
			Description:   "A Sheffield pub and live room hosting touring folk, jazz, roots, and singer-songwriter shows.",
			Website:       "https://www.mygreystones.co.uk/events/",
			CoverageKind:  domain.CoverageKindVenue,
			Origin:        domain.OriginSeed,
		},
		{
			Slug:          "yellow-arch",
			Name:          "Yellow Arch Studios",
			Address:       "30-36 Burton Road, Sheffield",
			Neighbourhood: "Neepsend",
			Description:   "A studio and venue space for live shows, club nights, and independent promoters.",
			Website:       "https://www.yellowarch.com/events/",
			CoverageKind:  domain.CoverageKindVenue,
			Origin:        domain.OriginSeed,
		},
		{
			Slug:          "sidney-and-matilda",
			Name:          "Sidney & Matilda",
			Address:       "Rivelin Works, 46 Sidney Street, Sheffield",
			Neighbourhood: "Cultural Industries Quarter",
			Description:   "A venue and gallery space with mixed bills, DJs, and late-night shows.",
			Website:       "https://www.sidneyandmatilda.com/",
			CoverageKind:  domain.CoverageKindVenue,
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
	venues, _ := s.ListVenues(context.Background())
	return venues
}

func (s *Store) ListVenues(context.Context) ([]domain.Venue, error) {
	out := make([]domain.Venue, len(s.venues))
	copy(out, s.venues)
	return out, nil
}

func (s *Store) Events() []domain.Event {
	events, _ := s.ListEvents(context.Background())
	return events
}

func (s *Store) ListEvents(context.Context) ([]domain.Event, error) {
	out := make([]domain.Event, len(s.events))
	copy(out, s.events)
	return out, nil
}

func (s *Store) VenueBySlug(slug string) (domain.Venue, bool) {
	venue, ok, _ := s.LoadVenueBySlug(context.Background(), slug)
	return venue, ok
}

func (s *Store) LoadVenueBySlug(_ context.Context, slug string) (domain.Venue, bool, error) {
	for _, venue := range s.venues {
		if venue.Slug == slug {
			return venue, true, nil
		}
	}
	return domain.Venue{}, false, nil
}

func (s *Store) EventBySlug(slug string) (domain.Event, bool) {
	event, ok, _ := s.LoadEventBySlug(context.Background(), slug)
	return event, ok
}

func (s *Store) LoadEventBySlug(_ context.Context, slug string) (domain.Event, bool, error) {
	for _, event := range s.events {
		if event.Slug == slug {
			return event, true, nil
		}
	}
	return domain.Event{}, false, nil
}

func (s *Store) EventsForVenue(venueSlug string) []domain.Event {
	events, _ := s.ListEventsForVenue(context.Background(), venueSlug)
	return events
}

func (s *Store) ListEventsForVenue(_ context.Context, venueSlug string) ([]domain.Event, error) {
	var out []domain.Event
	for _, event := range s.events {
		if event.VenueSlug == venueSlug {
			out = append(out, event)
		}
	}
	return out, nil
}

func (s *Store) Validate(_ context.Context) error {
	for _, event := range s.events {
		if _, ok, _ := s.LoadVenueBySlug(context.Background(), event.VenueSlug); !ok {
			return fmt.Errorf("event %q references missing venue %q", event.Slug, event.VenueSlug)
		}
		if err := event.ValidateCanonical(); err != nil {
			return fmt.Errorf("event %q %w", event.Slug, err)
		}
	}
	return nil
}

func (s *Store) Ready(context.Context) error {
	return nil
}

func VenueCoverageKind(venue domain.Venue) domain.CoverageKind {
	if strings.TrimSpace(string(venue.CoverageKind)) == "" {
		return domain.CoverageKindVenue
	}
	return venue.CoverageKind
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
