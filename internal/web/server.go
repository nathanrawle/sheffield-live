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
	store         *store.Store
	localLocation *time.Location
	layout        *template.Template
	pages         map[string]*template.Template
	fileServer    http.Handler
}

type PageData struct {
	SiteName      string
	PageTitle     string
	Active        string
	Content       template.HTML
	Now           time.Time
	Events        []domain.Event
	Event         domain.Event
	Venues        []domain.Venue
	Venue         domain.Venue
	FeaturedEvent domain.Event
	VenueEvents   []domain.Event
}

func NewServer(st *store.Store) (*Server, error) {
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
		layout:        layout,
		pages:         pages,
		fileServer:    http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))),
	}, nil
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
	events := s.store.Events()
	if len(events) > 3 {
		events = events[:3]
	}
	data := PageData{
		SiteName:      "Sheffield Live",
		PageTitle:     "Sheffield live music",
		Active:        "home",
		Now:           time.Now().UTC(),
		Venues:        s.store.Venues(),
		Events:        events,
		FeaturedEvent: firstEvent(events),
	}
	s.renderPage(w, "templates/home.html", data)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	data := PageData{
		SiteName:  "Sheffield Live",
		PageTitle: "Events",
		Active:    "events",
		Now:       time.Now().UTC(),
		Events:    s.store.Events(),
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
		SiteName:  "Sheffield Live",
		PageTitle: event.Name,
		Active:    "events",
		Now:       time.Now().UTC(),
		Event:     event,
		Venue:     venue,
	}
	s.renderPage(w, "templates/event_detail.html", data)
}

func (s *Server) handleVenues(w http.ResponseWriter, r *http.Request) {
	data := PageData{
		SiteName:  "Sheffield Live",
		PageTitle: "Venues",
		Active:    "venues",
		Now:       time.Now().UTC(),
		Venues:    s.store.Venues(),
	}
	s.renderPage(w, "templates/venues.html", data)
}

func (s *Server) handleVenueDetail(w http.ResponseWriter, r *http.Request, slug string) {
	venue, ok := s.store.VenueBySlug(slug)
	if !ok {
		http.NotFound(w, r)
		return
	}
	data := PageData{
		SiteName:    "Sheffield Live",
		PageTitle:   venue.Name,
		Active:      "venues",
		Now:         time.Now().UTC(),
		Venue:       venue,
		VenueEvents: s.store.EventsForVenue(slug),
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
