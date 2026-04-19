package web

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

type Server struct {
	store         store.ReadOnlyStore
	localLocation *time.Location
	clock         func() time.Time
	layout        *template.Template
	pages         map[string]*template.Template
	fileServer    http.Handler
}

type PageData struct {
	SiteName        string
	PageTitle       string
	MetaDescription string
	Active          string
	Content         template.HTML
	Now             time.Time
	Events          []domain.Event
	EventGroups     []EventGroup
	EventFilters    EventFilters
	Event           domain.Event
	Venues          []domain.Venue
	Venue           domain.Venue
	FeaturedEvent   domain.Event
	TodayEvents     []domain.Event
	ThisWeekEvents  []domain.Event
	VenueEvents     []domain.Event
}

type EventGroup struct {
	Date   time.Time
	Events []domain.Event
}

type EventFilters struct {
	Window string
	Venue  string
}

func NewServer(st store.ReadOnlyStore) (*Server, error) {
	if err := st.Validate(); err != nil {
		return nil, fmt.Errorf("validate store: %w", err)
	}

	localLocation, err := time.LoadLocation("Europe/London")
	if err != nil {
		localLocation = time.FixedZone("Europe/London", 0)
	}

	funcs := template.FuncMap{
		"dateLong":  func(t time.Time) string { return t.In(localLocation).Format("Monday, 2 January 2006") },
		"dateShort": func(t time.Time) string { return t.In(localLocation).Format("2 Jan 2006") },
		"originLabel": func(origin domain.Origin) string {
			switch origin {
			case domain.OriginSeed:
				return "Seed data"
			case domain.OriginTest:
				return "Test data"
			case domain.OriginDev:
				return "Development data"
			default:
				return ""
			}
		},
		"timeShort": func(t time.Time) string { return t.In(localLocation).Format("15:04") },
		"venueName": func(slug string) string {
			venue, ok := st.VenueBySlug(slug)
			if !ok {
				return slug
			}
			return venue.Name
		},
		"year": func(t time.Time) string { return t.In(localLocation).Format("2006") },
	}

	layout, err := template.New("layout.html").Funcs(funcs).ParseFS(templateFS, "templates/layout.html")
	if err != nil {
		return nil, fmt.Errorf("parse layout: %w", err)
	}

	pageFiles := []string{
		"templates/home.html",
		"templates/events.html",
		"templates/event_detail.html",
		"templates/venues.html",
		"templates/venue_detail.html",
	}
	pages := make(map[string]*template.Template, len(pageFiles))
	for _, file := range pageFiles {
		t, err := template.New(filepath.Base(file)).Funcs(funcs).ParseFS(templateFS, file)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", file, err)
		}
		pages[file] = t
	}

	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("static fs: %w", err)
	}

	return &Server{
		store:         st,
		localLocation: localLocation,
		clock:         func() time.Time { return time.Now().UTC() },
		layout:        layout,
		pages:         pages,
		fileServer:    http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))),
	}, nil
}

func (s *Server) SetClockForTesting(clock func() time.Time) {
	s.clock = clock
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cleaned := path.Clean(r.URL.Path)
	switch {
	case cleaned == "/":
		s.handleHome(w, r)
	case cleaned == "/events":
		s.handleEvents(w, r)
	case cleaned == "/venues":
		s.handleVenues(w, r)
	case cleaned == "/healthz":
		s.handleHealthz(w, r)
	case strings.HasPrefix(cleaned, "/events/"):
		s.handleEventDetail(w, r, strings.TrimPrefix(cleaned, "/events/"))
	case strings.HasPrefix(cleaned, "/venues/"):
		s.handleVenueDetail(w, r, strings.TrimPrefix(cleaned, "/venues/"))
	case cleaned == "/static" || strings.HasPrefix(cleaned, "/static/"):
		s.fileServer.ServeHTTP(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	events := sortEventsForDisplay(upcomingEvents(s.store.Events(), now, s.localLocation))
	todayEvents := filterEventsByWindow(events, now, s.localLocation, "today")
	thisWeekEvents := filterEventsByWindow(events, now, s.localLocation, "week")
	thisWeekEvents = excludeLocalDate(thisWeekEvents, localDayStart(now, s.localLocation), s.localLocation)
	if len(events) > 3 {
		events = events[:3]
	}
	data := PageData{
		SiteName:        "Sheffield Live",
		PageTitle:       "Sheffield live music",
		MetaDescription: "Upcoming live music in Sheffield, grouped by date and linked back to venue sources.",
		Active:          "home",
		Now:             now,
		Venues:          s.store.Venues(),
		Events:          events,
		FeaturedEvent:   firstEvent(events),
		TodayEvents:     todayEvents,
		ThisWeekEvents:  thisWeekEvents,
	}
	s.renderPage(w, "templates/home.html", data)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	filters := parseEventFilters(r, s.store)
	events := filterEventsByVenue(s.store.Events(), filters.Venue)
	events = filterEventsByWindow(events, now, s.localLocation, filters.Window)
	events = sortEventsForDisplay(events)
	data := PageData{
		SiteName:        "Sheffield Live",
		PageTitle:       "Events",
		MetaDescription: "Browse Sheffield live music by date window and venue.",
		Active:          "events",
		Now:             now,
		Events:          events,
		EventGroups:     groupEventsByLocalDate(events, s.localLocation),
		EventFilters:    filters,
		Venues:          s.store.Venues(),
	}
	s.renderPage(w, "templates/events.html", data)
}

func (s *Server) handleEventDetail(w http.ResponseWriter, r *http.Request, slug string) {
	event, ok := s.store.EventBySlug(slug)
	if !ok {
		http.NotFound(w, r)
		return
	}
	venue, ok := s.store.VenueBySlug(event.VenueSlug)
	if !ok {
		http.Error(w, "event venue not found", http.StatusInternalServerError)
		return
	}
	data := PageData{
		SiteName:        "Sheffield Live",
		PageTitle:       event.Name,
		MetaDescription: event.Description,
		Active:          "events",
		Now:             s.now(),
		Event:           event,
		Venue:           venue,
	}
	s.renderPage(w, "templates/event_detail.html", data)
}

func (s *Server) handleVenues(w http.ResponseWriter, r *http.Request) {
	data := PageData{
		SiteName:        "Sheffield Live",
		PageTitle:       "Venues",
		MetaDescription: "Sheffield venues with upcoming live music listings.",
		Active:          "venues",
		Now:             s.now(),
		Venues:          s.store.Venues(),
	}
	s.renderPage(w, "templates/venues.html", data)
}

func (s *Server) handleVenueDetail(w http.ResponseWriter, r *http.Request, slug string) {
	venue, ok := s.store.VenueBySlug(slug)
	if !ok {
		http.NotFound(w, r)
		return
	}
	now := s.now()
	data := PageData{
		SiteName:        "Sheffield Live",
		PageTitle:       venue.Name,
		MetaDescription: venue.Description,
		Active:          "venues",
		Now:             now,
		Venue:           venue,
		VenueEvents:     sortEventsForDisplay(upcomingEvents(s.store.EventsForVenue(slug), now, s.localLocation)),
	}
	s.renderPage(w, "templates/venue_detail.html", data)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) renderPage(w http.ResponseWriter, pageKey string, data PageData) {
	page, ok := s.pages[pageKey]
	if !ok {
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}

	var pageBuf bytes.Buffer
	if err := page.ExecuteTemplate(&pageBuf, filepath.Base(pageKey), data); err != nil {
		http.Error(w, "render page", http.StatusInternalServerError)
		return
	}

	data.Content = template.HTML(pageBuf.String())

	var layoutBuf bytes.Buffer
	if err := s.layout.ExecuteTemplate(&layoutBuf, "layout.html", data); err != nil {
		http.Error(w, "render layout", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(layoutBuf.Bytes())
}

func firstEvent(events []domain.Event) domain.Event {
	if len(events) == 0 {
		return domain.Event{}
	}
	return events[0]
}

func (s *Server) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock().UTC()
}

func parseEventFilters(r *http.Request, st store.ReadOnlyStore) EventFilters {
	window := r.URL.Query().Get("window")
	switch window {
	case "today", "week", "all":
	default:
		window = "all"
	}
	venue := r.URL.Query().Get("venue")
	if venue != "" {
		if _, ok := st.VenueBySlug(venue); !ok {
			venue = ""
		}
	}
	return EventFilters{
		Window: window,
		Venue:  venue,
	}
}

func filterEventsByVenue(events []domain.Event, venueSlug string) []domain.Event {
	if venueSlug == "" {
		return events
	}
	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		if event.VenueSlug == venueSlug {
			out = append(out, event)
		}
	}
	return out
}

func sortEventsForDisplay(events []domain.Event) []domain.Event {
	out := make([]domain.Event, len(events))
	copy(out, events)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start.Equal(out[j].Start) {
			return out[i].Slug < out[j].Slug
		}
		return out[i].Start.Before(out[j].Start)
	})
	return out
}

func filterEventsByWindow(events []domain.Event, now time.Time, loc *time.Location, window string) []domain.Event {
	if window == "all" {
		return upcomingEvents(events, now, loc)
	}

	today := localDayStart(now, loc)
	end := today.AddDate(0, 0, 1)
	if window == "week" {
		end = today.AddDate(0, 0, 7)
	}

	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		start := event.Start.In(loc)
		if !start.Before(today) && start.Before(end) {
			out = append(out, event)
		}
	}
	return out
}

func upcomingEvents(events []domain.Event, now time.Time, loc *time.Location) []domain.Event {
	today := localDayStart(now, loc)
	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		if !event.Start.In(loc).Before(today) {
			out = append(out, event)
		}
	}
	return out
}

func excludeLocalDate(events []domain.Event, date time.Time, loc *time.Location) []domain.Event {
	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		if !sameLocalDate(event.Start, date, loc) {
			out = append(out, event)
		}
	}
	return out
}

func groupEventsByLocalDate(events []domain.Event, loc *time.Location) []EventGroup {
	var groups []EventGroup
	for _, event := range events {
		date := localDayStart(event.Start, loc)
		if len(groups) == 0 || !sameLocalDate(groups[len(groups)-1].Date, date, loc) {
			groups = append(groups, EventGroup{Date: date})
		}
		groups[len(groups)-1].Events = append(groups[len(groups)-1].Events, event)
	}
	return groups
}

func localDayStart(t time.Time, loc *time.Location) time.Time {
	local := t.In(loc)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
}

func sameLocalDate(a, b time.Time, loc *time.Location) bool {
	ay, am, ad := a.In(loc).Date()
	by, bm, bd := b.In(loc).Date()
	return ay == by && am == bm && ad == bd
}
