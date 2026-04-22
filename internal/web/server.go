package web

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	"sheffield-live/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

type Server struct {
	store                     store.ReadOnlyStore
	reviewStore               ReviewStore
	importRunStore            ingest.ImportRunStore
	replayStore               ingest.ReplayStore
	importRunReviewGroupStore ImportRunReviewGroupStore
	localLocation             *time.Location
	clock                     func() time.Time
	layout                    *template.Template
	pages                     map[string]*template.Template
	fileServer                http.Handler
}

type ReviewStore interface {
	ListOpenReviewGroups(ctx context.Context) ([]review.GroupSummary, error)
	ListClosedReviewGroups(ctx context.Context, limit int) ([]review.GroupSummary, error)
	LoadReviewGroup(ctx context.Context, id int64) (review.Group, bool, error)
	SaveReviewDraftChoices(ctx context.Context, groupID int64, choices []review.DraftChoiceInput) error
	ResolveReviewGroup(ctx context.Context, groupID int64, choices []review.DraftChoiceInput) error
	UpdateReviewGroupStatus(ctx context.Context, groupID int64, status string) error
}

const adminReviewHistoryLimit = 50

type ImportRunReviewGroupStore interface {
	ListReviewGroupsForImportRun(ctx context.Context, importRunID int64) ([]review.GroupSummary, error)
}

type PageData struct {
	SiteName           string
	PageTitle          string
	MetaDescription    string
	Active             string
	Content            template.HTML
	Now                time.Time
	Events             []domain.Event
	EventGroups        []EventGroup
	EventFilters       EventFilters
	Event              domain.Event
	Venues             []domain.Venue
	Venue              domain.Venue
	FeaturedEvent      domain.Event
	TodayEvents        []domain.Event
	ThisWeekEvents     []domain.Event
	VenueEvents        []domain.Event
	ReviewGroups       []review.GroupSummary
	ReviewDetail       ReviewDetail
	ImportRuns         []ingest.ImportRunSummary
	ImportRunDetail    ImportRunDetail
	LatestImport       *ingest.ImportRunSummary
	HasImportHistory   bool
	HasImportRunDetail bool
	HasReviewStorage   bool
	Flash              string
}

type EventGroup struct {
	Date   time.Time
	Events []domain.Event
}

type EventFilters struct {
	Window string
	Venue  string
}

type ReviewDetail struct {
	Group                review.Group
	IsDuplicate          bool
	IsSingleton          bool
	OriginImportRunID    int64
	CanonicalSummaryRows []ReviewCanonicalSummaryRow
	Rows                 []ReviewFieldRow
	Preview              []ReviewPreviewRow
	SingleCandidateRows  []ReviewSingleCandidateRow
}

type ReviewFieldRow struct {
	Field review.Field
	Label string
	Cells []ReviewChoiceCell
}

type ReviewChoiceCell struct {
	CandidateID int64
	Value       string
	Checked     bool
	Provenance  string
}

type ReviewPreviewRow struct {
	Label     string
	Value     string
	Candidate string
}

type ReviewCanonicalSummaryRow struct {
	Label     string
	Value     string
	Candidate string
	Selected  bool
}

type ReviewSingleCandidateRow struct {
	Label string
	Value string
}

type ImportRunDetail struct {
	ID            int64
	Status        string
	StartedAt     time.Time
	FinishedAt    *time.Time
	Notes         string
	SnapshotCount int
	ReviewGroups  []review.GroupSummary
	Snapshots     []ImportRunSnapshotRow
}

type ImportRunSnapshotRow struct {
	ID                int64
	SourceName        string
	SourceURL         string
	CapturedAt        time.Time
	MetadataAvailable bool
	DecodeState       string
	URL               string
	FinalURL          string
	Status            string
	StatusCode        int
	StatusDisplay     string
	ContentType       string
	ContentLength     int64
	BodyBytes         int
	CapturedAtText    string
	SHA256            string
	Truncated         bool
}

type adminSnapshotEnvelope struct {
	Version   int                            `json:"version"`
	Metadata  ingest.SnapshotContentMetadata `json:"metadata"`
	SHA256    string                         `json:"sha256"`
	Truncated bool                           `json:"truncated"`
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
		"dateShortPtr": func(t *time.Time) string {
			if t == nil {
				return ""
			}
			return t.In(localLocation).Format("2 Jan 2006")
		},
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
		"blankValue": func(value string) string {
			if strings.TrimSpace(value) == "" {
				return "(blank)"
			}
			return value
		},
		"snapshotCountLabel": func(count int) string {
			if count == 1 {
				return "1 snapshot"
			}
			return fmt.Sprintf("%d snapshots", count)
		},
		"candidateCountLabel": func(count int) string {
			if count == 1 {
				return "New listing review - 1 candidate"
			}
			return fmt.Sprintf("Duplicate review - %d candidates", count)
		},
		"timeShort": func(t time.Time) string { return t.In(localLocation).Format("15:04") },
		"timeShortPtr": func(t *time.Time) string {
			if t == nil {
				return ""
			}
			return t.In(localLocation).Format("15:04")
		},
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
		"templates/admin_review.html",
		"templates/admin_review_history.html",
		"templates/admin_import_runs.html",
		"templates/admin_import_run_detail.html",
		"templates/admin_review_detail.html",
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
		store:                     st,
		reviewStore:               reviewStoreFor(st),
		importRunStore:            importRunStoreFor(st),
		replayStore:               replayStoreFor(st),
		importRunReviewGroupStore: importRunReviewGroupStoreFor(st),
		localLocation:             localLocation,
		clock:                     func() time.Time { return time.Now().UTC() },
		layout:                    layout,
		pages:                     pages,
		fileServer:                http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))),
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
	case cleaned == "/admin/review":
		s.handleAdminReview(w, r)
	case cleaned == "/admin/review/history":
		s.handleAdminReviewHistory(w, r)
	case r.URL.Path == "/admin/import-runs":
		s.handleAdminImportRuns(w, r)
	case strings.HasPrefix(r.URL.Path, "/admin/import-runs/"):
		s.handleAdminImportRunDetail(w, r)
	case strings.HasPrefix(cleaned, "/admin/review/"):
		s.handleAdminReviewDetail(w, r, strings.TrimPrefix(cleaned, "/admin/review/"))
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

func (s *Server) handleAdminReview(w http.ResponseWriter, r *http.Request) {
	if s.reviewStore == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	groups, err := s.reviewStore.ListOpenReviewGroups(r.Context())
	if err != nil {
		http.Error(w, "load review groups", http.StatusInternalServerError)
		return
	}
	flash := ""
	switch {
	case r.URL.Query().Get("saved") == "1":
		flash = "Draft saved."
	case r.URL.Query().Get("resolved") == "1":
		flash = "Marked resolved."
	case r.URL.Query().Get("accepted") == "1":
		flash = "Accepted new listing."
	case r.URL.Query().Get("rejected") == "1":
		flash = "Rejected."
	}
	data := PageData{
		SiteName:           "Sheffield Live",
		PageTitle:          "Review",
		MetaDescription:    "Review open staged event candidates.",
		Active:             "admin-review",
		Now:                s.now(),
		ReviewGroups:       groups,
		HasImportHistory:   s.importRunStore != nil,
		HasImportRunDetail: s.replayStore != nil,
		HasReviewStorage:   s.reviewStore != nil,
		Flash:              flash,
	}
	if s.importRunStore != nil {
		latest, err := s.importRunStore.LatestSuccessfulImport(r.Context())
		if err != nil {
			http.Error(w, "load latest import run", http.StatusInternalServerError)
			return
		}
		data.LatestImport = latest
	}
	s.renderPage(w, "templates/admin_review.html", data)
}

func (s *Server) handleAdminReviewHistory(w http.ResponseWriter, r *http.Request) {
	if s.reviewStore == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	groups, err := s.reviewStore.ListClosedReviewGroups(r.Context(), adminReviewHistoryLimit)
	if err != nil {
		http.Error(w, "load review history", http.StatusInternalServerError)
		return
	}
	data := PageData{
		SiteName:           "Sheffield Live",
		PageTitle:          "Review history",
		MetaDescription:    "Read-only history of resolved and rejected review groups.",
		Active:             "admin-review",
		Now:                s.now(),
		ReviewGroups:       groups,
		HasImportHistory:   s.importRunStore != nil,
		HasImportRunDetail: s.replayStore != nil,
		HasReviewStorage:   s.reviewStore != nil,
	}
	s.renderPage(w, "templates/admin_review_history.html", data)
}

func (s *Server) handleAdminImportRuns(w http.ResponseWriter, r *http.Request) {
	if s.importRunStore == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	importRuns, err := s.importRunStore.ListImportRuns(r.Context(), 20)
	if err != nil {
		http.Error(w, "load import runs", http.StatusInternalServerError)
		return
	}
	data := PageData{
		SiteName:           "Sheffield Live",
		PageTitle:          "Import history",
		MetaDescription:    "Read-only history of import runs and snapshot counts.",
		Now:                s.now(),
		ImportRuns:         importRuns,
		HasImportRunDetail: s.replayStore != nil,
		HasReviewStorage:   s.reviewStore != nil,
	}
	s.renderPage(w, "templates/admin_import_runs.html", data)
}

func (s *Server) handleAdminImportRunDetail(w http.ResponseWriter, r *http.Request) {
	if s.replayStore == nil {
		http.NotFound(w, r)
		return
	}
	runID, ok := parseStrictPositiveIDPath(r.URL.Path, "/admin/import-runs/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	run, err := s.replayStore.LoadImportRun(r.Context(), runID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "load import run", http.StatusInternalServerError)
		return
	}
	detail := buildImportRunDetail(run)
	if s.importRunReviewGroupStore != nil {
		groups, err := s.importRunReviewGroupStore.ListReviewGroupsForImportRun(r.Context(), run.ID)
		if err != nil {
			http.Error(w, "load import run review groups", http.StatusInternalServerError)
			return
		}
		detail.ReviewGroups = groups
	}

	data := PageData{
		SiteName:           "Sheffield Live",
		PageTitle:          fmt.Sprintf("Import run #%d", run.ID),
		MetaDescription:    "Read-only import run snapshot metadata.",
		Now:                s.now(),
		ImportRunDetail:    detail,
		HasImportHistory:   s.importRunStore != nil,
		HasImportRunDetail: s.replayStore != nil,
		HasReviewStorage:   s.reviewStore != nil,
	}
	s.renderPage(w, "templates/admin_import_run_detail.html", data)
}

func (s *Server) handleAdminReviewDetail(w http.ResponseWriter, r *http.Request, rawGroupID string) {
	if s.reviewStore == nil {
		http.NotFound(w, r)
		return
	}
	groupID, err := strconv.ParseInt(strings.TrimSpace(rawGroupID), 10, 64)
	if err != nil || groupID <= 0 {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		flash := ""
		if r.URL.Query().Get("saved") == "1" {
			flash = "Draft saved."
		}
		s.renderAdminReviewDetail(w, r, groupID, flash)
	case http.MethodPost:
		s.postAdminReviewDecision(w, r, groupID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) postAdminReviewDecision(w http.ResponseWriter, r *http.Request, groupID int64) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "parse form", http.StatusBadRequest)
		return
	}

	group, ok, err := s.reviewStore.LoadReviewGroup(r.Context(), groupID)
	if err != nil {
		http.Error(w, "load review group", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	if group.Status != review.StatusOpen {
		http.Error(w, "review group is closed", http.StatusConflict)
		return
	}

	action := strings.TrimSpace(r.FormValue("action"))
	switch action {
	case "", "save":
		if !reviewGroupIsDuplicate(group) {
			http.Error(w, "new listing reviews do not accept draft choices", http.StatusBadRequest)
			return
		}
		if err := s.saveAdminReviewDraft(r.Context(), groupID, group, r.Form); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, fmt.Sprintf("/admin/review/%d?saved=1", groupID), http.StatusSeeOther)
	case review.StatusResolved:
		if !reviewGroupIsDuplicate(group) {
			http.Error(w, "new listing reviews must be accepted without field choices", http.StatusBadRequest)
			return
		}
		choices, err := reviewChoicesFromForm(group, r.Form, true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.reviewStore.ResolveReviewGroup(r.Context(), groupID, choices); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/admin/review?resolved=1", http.StatusSeeOther)
	case "accept":
		if err := acceptChoicesFromForm(r.Form); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		choices, err := singletonReviewChoices(group)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.reviewStore.ResolveReviewGroup(r.Context(), groupID, choices); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/admin/review?accepted=1", http.StatusSeeOther)
	case review.StatusRejected:
		if err := rejectChoicesFromForm(r.Form); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.reviewStore.UpdateReviewGroupStatus(r.Context(), groupID, review.StatusRejected); err != nil {
			http.Error(w, "update review status", http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/admin/review?rejected=1", http.StatusSeeOther)
	default:
		http.Error(w, "invalid review action", http.StatusBadRequest)
		return
	}
}

func (s *Server) saveAdminReviewDraft(ctx context.Context, groupID int64, group review.Group, form url.Values) error {
	choices, err := reviewChoicesFromForm(group, form, false)
	if err != nil {
		return err
	}
	if len(choices) == 0 {
		return fmt.Errorf("at least one review choice is required")
	}
	if err := s.reviewStore.SaveReviewDraftChoices(ctx, groupID, choices); err != nil {
		return fmt.Errorf("save review draft: %w", err)
	}
	return nil
}

func reviewChoicesFromForm(group review.Group, form url.Values, requireAll bool) ([]review.DraftChoiceInput, error) {
	choices := make([]review.DraftChoiceInput, 0, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		rawCandidateID := strings.TrimSpace(form.Get("choice_" + string(field)))
		if rawCandidateID == "" {
			if requireAll {
				return nil, fmt.Errorf("all review fields must be selected before resolving")
			}
			continue
		}
		candidateID, err := strconv.ParseInt(rawCandidateID, 10, 64)
		if err != nil || candidateID <= 0 {
			return nil, fmt.Errorf("invalid candidate choice")
		}
		if !groupCandidateExists(group.Candidates, candidateID) {
			return nil, fmt.Errorf("review candidate %d not found in group %d", candidateID, group.ID)
		}
		choices = append(choices, review.DraftChoiceInput{
			Field:       field,
			CandidateID: candidateID,
		})
	}
	return choices, nil
}

func rejectChoicesFromForm(form url.Values) error {
	return rejectReviewChoiceFields(form, "rejecting a review group")
}

func acceptChoicesFromForm(form url.Values) error {
	return rejectReviewChoiceFields(form, "accepting a new listing")
}

func rejectReviewChoiceFields(form url.Values, action string) error {
	for key := range form {
		if strings.HasPrefix(key, "choice_") {
			return fmt.Errorf("%s does not accept field choices", action)
		}
	}
	return nil
}

func singletonReviewChoices(group review.Group) ([]review.DraftChoiceInput, error) {
	if !reviewGroupIsSingleton(group) {
		return nil, fmt.Errorf("accepting a new listing requires exactly one candidate")
	}
	candidateID := group.Candidates[0].ID
	choices := make([]review.DraftChoiceInput, 0, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		choices = append(choices, review.DraftChoiceInput{
			Field:       field,
			CandidateID: candidateID,
		})
	}
	return choices, nil
}

func reviewGroupIsDuplicate(group review.Group) bool {
	return len(group.Candidates) >= 2
}

func reviewGroupIsSingleton(group review.Group) bool {
	return len(group.Candidates) == 1
}

func groupCandidateExists(candidates []review.Candidate, candidateID int64) bool {
	for _, candidate := range candidates {
		if candidate.ID == candidateID {
			return true
		}
	}
	return false
}

func (s *Server) renderAdminReviewDetail(w http.ResponseWriter, r *http.Request, groupID int64, flash string) {
	group, ok, err := s.reviewStore.LoadReviewGroup(r.Context(), groupID)
	if err != nil {
		http.Error(w, "load review group", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	data := PageData{
		SiteName:           "Sheffield Live",
		PageTitle:          group.Title,
		MetaDescription:    "Review staged event candidates.",
		Active:             "admin-review",
		Now:                s.now(),
		HasImportHistory:   s.importRunStore != nil,
		HasImportRunDetail: s.replayStore != nil,
		HasReviewStorage:   s.reviewStore != nil,
		Flash:              flash,
	}
	detail := buildReviewDetail(group)
	if s.replayStore != nil {
		detail.OriginImportRunID, _ = review.ParseOriginImportRunID(group.Notes)
	}
	data.ReviewDetail = detail
	s.renderPage(w, "templates/admin_review_detail.html", data)
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

func reviewStoreFor(st store.ReadOnlyStore) ReviewStore {
	reviewStore, ok := st.(ReviewStore)
	if !ok {
		return nil
	}
	return reviewStore
}

func importRunStoreFor(st store.ReadOnlyStore) ingest.ImportRunStore {
	importRunStore, ok := st.(ingest.ImportRunStore)
	if !ok {
		return nil
	}
	return importRunStore
}

func replayStoreFor(st store.ReadOnlyStore) ingest.ReplayStore {
	replayStore, ok := st.(ingest.ReplayStore)
	if !ok {
		return nil
	}
	return replayStore
}

func importRunReviewGroupStoreFor(st store.ReadOnlyStore) ImportRunReviewGroupStore {
	reviewGroupStore, ok := st.(ImportRunReviewGroupStore)
	if !ok {
		return nil
	}
	return reviewGroupStore
}

func parseStrictPositiveIDPath(rawPath, prefix string) (int64, bool) {
	if !strings.HasPrefix(rawPath, prefix) {
		return 0, false
	}
	rawID := strings.TrimPrefix(rawPath, prefix)
	if rawID == "" || strings.Contains(rawID, "/") {
		return 0, false
	}
	for _, r := range rawID {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func buildImportRunDetail(run ingest.ReplayRun) ImportRunDetail {
	var finishedAt *time.Time
	if run.FinishedAt != nil {
		finished := *run.FinishedAt
		finishedAt = &finished
	}
	detail := ImportRunDetail{
		ID:            run.ID,
		Status:        run.Status,
		StartedAt:     run.StartedAt,
		FinishedAt:    finishedAt,
		Notes:         run.Notes,
		SnapshotCount: len(run.Snapshots),
		Snapshots:     make([]ImportRunSnapshotRow, 0, len(run.Snapshots)),
	}
	for _, snapshot := range run.Snapshots {
		row := ImportRunSnapshotRow{
			ID:          snapshot.ID,
			SourceName:  snapshot.SourceName,
			SourceURL:   snapshot.SourceURL,
			CapturedAt:  snapshot.CapturedAt,
			DecodeState: "Metadata unavailable",
		}
		var envelope adminSnapshotEnvelope
		if err := json.Unmarshal([]byte(snapshot.Payload), &envelope); err == nil && envelope.Version == 1 {
			row.MetadataAvailable = true
			row.DecodeState = "Metadata available"
			row.URL = envelope.Metadata.URL
			row.FinalURL = envelope.Metadata.FinalURL
			row.Status = envelope.Metadata.Status
			row.StatusCode = envelope.Metadata.StatusCode
			row.StatusDisplay = httpStatusDisplay(envelope.Metadata.Status, envelope.Metadata.StatusCode)
			row.ContentType = envelope.Metadata.ContentType
			row.ContentLength = envelope.Metadata.ContentLength
			row.BodyBytes = envelope.Metadata.BodyBytes
			row.CapturedAtText = envelope.Metadata.CapturedAt
			row.SHA256 = envelope.SHA256
			row.Truncated = envelope.Truncated
		}
		detail.Snapshots = append(detail.Snapshots, row)
	}
	return detail
}

func httpStatusDisplay(status string, statusCode int) string {
	if trimmed := strings.TrimSpace(status); trimmed != "" {
		return trimmed
	}
	if statusCode != 0 {
		return strconv.Itoa(statusCode)
	}
	return ""
}

func buildReviewDetail(group review.Group) ReviewDetail {
	detail := ReviewDetail{
		Group:       group,
		IsDuplicate: reviewGroupIsDuplicate(group),
		IsSingleton: reviewGroupIsSingleton(group),
	}
	for _, field := range review.CanonicalFields {
		row := ReviewFieldRow{
			Field: field,
			Label: field.Label(),
		}
		choice, hasChoice := group.DraftChoices[field]
		if detail.IsDuplicate {
			candidate := ""
			if hasChoice {
				candidate = reviewCandidateLabel(group.Candidates, choice.CandidateID)
			}
			detail.CanonicalSummaryRows = append(detail.CanonicalSummaryRows, ReviewCanonicalSummaryRow{
				Label:     field.Label(),
				Value:     choice.Value,
				Candidate: candidate,
				Selected:  hasChoice,
			})
		}
		for _, candidate := range group.Candidates {
			row.Cells = append(row.Cells, ReviewChoiceCell{
				CandidateID: candidate.ID,
				Value:       review.CandidateValue(candidate, field),
				Checked:     hasChoice && choice.CandidateID == candidate.ID,
				Provenance:  candidate.Provenance,
			})
		}
		if detail.IsDuplicate {
			detail.Rows = append(detail.Rows, row)
		}
		if detail.IsSingleton && len(group.Candidates) == 1 {
			detail.SingleCandidateRows = append(detail.SingleCandidateRows, ReviewSingleCandidateRow{
				Label: field.Label(),
				Value: review.CandidateValue(group.Candidates[0], field),
			})
		}
		if hasChoice {
			detail.Preview = append(detail.Preview, ReviewPreviewRow{
				Label:     field.Label(),
				Value:     choice.Value,
				Candidate: reviewCandidateLabel(group.Candidates, choice.CandidateID),
			})
		}
	}
	return detail
}

func reviewCandidateLabel(candidates []review.Candidate, id int64) string {
	for _, candidate := range candidates {
		if candidate.ID == id {
			if candidate.ExternalID != "" {
				return fmt.Sprintf("Candidate %d (%s)", candidate.Position, candidate.ExternalID)
			}
			return fmt.Sprintf("Candidate %d", candidate.Position)
		}
	}
	return "Unknown candidate"
}
