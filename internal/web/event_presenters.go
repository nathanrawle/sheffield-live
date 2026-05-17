package web

import (
	"fmt"
	"html/template"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
)

type eventCardLocationMode int

const (
	eventCardLocationFull eventCardLocationMode = iota
	eventCardLocationVenuePage
)

type eventPresenter struct {
	venueNames map[string]string
	venueAreas map[string]string
	loc        *time.Location
}

type EventCardView struct {
	Classes      string
	Slug         string
	Title        string
	TimeLabel    string
	DateTimeAttr string
	StatusLabel  string
	Genre        string
	OriginLabel  string
	Image        EventImageView
	Location     EventLocationView
}

type EventDetailView struct {
	HeaderClass        string
	Title              string
	OriginLabel        string
	Image              EventImageView
	DescriptionHTML    template.HTML
	OfficialListingURL string
	CalendarURL        string
	SourceCheckedLabel string
	WhenDateLabel      string
	WhenTimeLabel      string
	Meta               string
	Location           EventLocationView
}

type EventImageView struct {
	URL        string
	Alt        string
	FocusStyle template.CSS
	Portrait   bool
}

type EventLocationView struct {
	Room     string
	Venue    string
	VenueURL string
	Area     string
	Address  string
}

func newEventPresenter(venueNames, venueAreas map[string]string, loc *time.Location) eventPresenter {
	return eventPresenter{
		venueNames: venueNames,
		venueAreas: venueAreas,
		loc:        loc,
	}
}

func (p eventPresenter) Card(event domain.Event, mode eventCardLocationMode) EventCardView {
	image := eventImageView(event)
	status := eventStatusLabel(event)
	classes := []string{"event-card"}
	if mode == eventCardLocationVenuePage {
		classes = append(classes, "venue-event-card")
	}
	if image.URL != "" {
		classes = append(classes, "has-image")
	} else {
		classes = append(classes, "missing-image")
	}
	if status != "" {
		classes = append(classes, "has-status")
	}

	return EventCardView{
		Classes:      strings.Join(classes, " "),
		Slug:         event.Slug,
		Title:        publicEventTitle(event, p.venueNames),
		TimeLabel:    event.Start.In(p.loc).Format("15:04"),
		DateTimeAttr: event.Start.In(p.loc).Format("2006-01-02T15:04:05-07:00"),
		StatusLabel:  status,
		Genre:        strings.TrimSpace(event.Genre),
		OriginLabel:  originLabel(event.Origin),
		Image:        image,
		Location:     p.Location(event, mode),
	}
}

func (p eventPresenter) Cards(events []domain.Event, mode eventCardLocationMode) []EventCardView {
	cards := make([]EventCardView, 0, len(events))
	for _, event := range events {
		cards = append(cards, p.Card(event, mode))
	}
	return cards
}

func (p eventPresenter) Detail(event domain.Event, venue domain.Venue) EventDetailView {
	image := eventImageView(event)
	headerClass := "event-detail-head"
	if image.URL != "" {
		if image.Portrait {
			headerClass += " portrait-image"
		} else {
			headerClass += " hero-image"
		}
	}

	return EventDetailView{
		HeaderClass:        headerClass,
		Title:              publicEventTitle(event, map[string]string{venue.Slug: venue.Name}),
		OriginLabel:        originLabel(event.Origin),
		Image:              image,
		DescriptionHTML:    descriptionHTML(event.Description),
		OfficialListingURL: strings.TrimSpace(event.OfficialListingURL),
		CalendarURL:        strings.TrimSpace(event.CalendarURL),
		SourceCheckedLabel: sourceCheckedLabel(event, p.loc),
		WhenDateLabel:      event.Start.In(p.loc).Format("Monday, 2 January 2006"),
		WhenTimeLabel:      eventTimeLabel(event, p.loc),
		Meta:               publicEventDetailMeta(event),
		Location: EventLocationView{
			Room:     eventRoomText(event),
			Venue:    strings.TrimSpace(venue.Name),
			VenueURL: "/venues/" + venue.Slug,
			Area:     strings.TrimSpace(venue.Neighbourhood),
			Address:  formatVenueAddress(venue.Name, venue.Address),
		},
	}
}

func (p eventPresenter) Location(event domain.Event, mode eventCardLocationMode) EventLocationView {
	location := EventLocationView{
		Room: strings.TrimSpace(eventRoomText(event)),
	}
	if mode == eventCardLocationVenuePage {
		return location
	}

	location.Venue = strings.TrimSpace(publicVenueName(p.venueNames, event.VenueSlug))
	if p.venueAreas != nil {
		location.Area = strings.TrimSpace(p.venueAreas[event.VenueSlug])
	}
	return location
}

func (l EventLocationView) HasSummary() bool {
	return l.Room != "" || l.Venue != "" || l.Area != ""
}

func (l EventLocationView) HasPrimary() bool {
	return l.Room != "" || l.Venue != ""
}

func (l EventLocationView) Classes() string {
	classes := []string{"event-location"}
	if l.Room != "" {
		classes = append(classes, "has-room")
	}
	if l.Venue != "" {
		classes = append(classes, "has-venue")
	}
	if l.Area != "" {
		classes = append(classes, "has-area")
	}
	return strings.Join(classes, " ")
}

func eventImageView(event domain.Event) EventImageView {
	if strings.TrimSpace(event.ImageURL) == "" {
		return EventImageView{}
	}
	return EventImageView{
		URL:        strings.TrimSpace(event.ImageURL),
		Alt:        eventImageAlt(event),
		FocusStyle: imageFocusStyle(event.ImageFocusX, event.ImageFocusY),
		Portrait:   eventImagePortrait(event),
	}
}

func eventImageAlt(event domain.Event) string {
	if alt := strings.TrimSpace(event.ImageAlt); alt != "" {
		return alt
	}
	return event.Name
}

func eventImagePortrait(event domain.Event) bool {
	return event.ImageWidth > 0 && event.ImageHeight > event.ImageWidth
}

func imageFocusStyle(x, y int) template.CSS {
	focus := ingest.ImageFocus{
		X: ingest.NormalizeExplicitImageFocusValue(x),
		Y: ingest.NormalizeExplicitImageFocusValue(y),
	}
	return template.CSS(fmt.Sprintf("--image-focus-x: %d%%; --image-focus-y: %d%%;", focus.X, focus.Y))
}

func originLabel(origin domain.Origin) string {
	switch origin {
	case domain.OriginTest:
		return "Test data"
	case domain.OriginDev:
		return "Development data"
	default:
		return ""
	}
}

func eventTimeLabel(event domain.Event, loc *time.Location) string {
	if event.HasEnd() {
		return fmt.Sprintf("%s to %s", event.Start.In(loc).Format("15:04"), event.End.In(loc).Format("15:04"))
	}
	return fmt.Sprintf("Starts at %s", event.Start.In(loc).Format("15:04"))
}

func sourceCheckedLabel(event domain.Event, loc *time.Location) string {
	sourceName := strings.TrimSpace(event.SourceName)
	if sourceName == "" {
		return ""
	}
	if event.LastChecked.IsZero() {
		return fmt.Sprintf("Checked from %s.", sourceName)
	}
	return fmt.Sprintf("Checked from %s on %s.", sourceName, event.LastChecked.In(loc).Format("2 Jan 2006"))
}
