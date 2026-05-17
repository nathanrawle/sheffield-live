package web

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/genre"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/logging"
	"sheffield-live/internal/review"
	"sheffield-live/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

type Server struct {
	catalog                   store.CatalogStore
	reviewStore               ReviewStore
	importRunStore            ingest.ImportRunStore
	replayStore               ingest.ReplayStore
	importRunReviewGroupStore ImportRunReviewGroupStore
	secondarySourceStore      EventSecondarySourceInfoStore
	eventGenreStore           EventGenreStore
	genreConfigStore          GenreConfigurationStore
	readyChecker              ReadyChecker
	localLocation             *time.Location
	clock                     func() time.Time
	layout                    *template.Template
	pages                     map[string]*template.Template
	fileServer                http.Handler
	mediaServer               http.Handler
	mediaURLPrefix            string
	logger                    *slog.Logger
	adminAuth                 *adminAuthenticator
}

type ReviewStore interface {
	ListOpenReviewGroups(ctx context.Context) ([]review.GroupSummary, error)
	ListClosedReviewGroups(ctx context.Context, limit int) ([]review.GroupSummary, error)
	LoadReviewGroup(ctx context.Context, id int64) (review.Group, bool, error)
	SaveReviewDraftChoices(ctx context.Context, groupID int64, choices []review.DraftChoiceInput) error
	ResolveReviewGroup(ctx context.Context, groupID int64, choices []review.DraftChoiceInput) error
	UpdateReviewGroupStatus(ctx context.Context, groupID int64, status string) error
}

type VenueAdminStore interface {
	ValidateVenue(ctx context.Context, slug string) error
	UpdateProvisionalVenue(ctx context.Context, input store.VenueUpdateInput) error
}

type RoomAdminStore interface {
	ListVenueRooms(ctx context.Context) ([]domain.VenueRoom, error)
	ListVenueRoomsForVenue(ctx context.Context, venueSlug string) ([]domain.VenueRoom, error)
	LoadVenueRoomBySlug(ctx context.Context, venueSlug, roomSlug string) (domain.VenueRoom, bool, error)
	ValidateVenueRoom(ctx context.Context, venueSlug, roomSlug string) error
	UpdateProvisionalVenueRoom(ctx context.Context, input store.RoomUpdateInput) error
}

const adminReviewHistoryLimit = 50

type ImportRunReviewGroupStore interface {
	ListReviewGroupsForImportRun(ctx context.Context, importRunID int64) ([]review.GroupSummary, error)
}

type EventSecondarySourceInfoStore interface {
	EventSecondarySourceInfoByEventSlug(ctx context.Context, slug string) ([]store.EventSecondarySourceInfo, error)
}

type EventGenreStore interface {
	EventGenresByEventSlug(ctx context.Context, slug string) ([]genre.Match, error)
}

type GenreConfigurationStore interface {
	ListGenreRules(ctx context.Context) ([]genre.Rule, error)
	SaveGenreRule(ctx context.Context, input genre.RuleInput) error
	DeleteGenreRule(ctx context.Context, id int64) error
	RecomputeEventGenres(ctx context.Context) error
}

type ReadyChecker interface {
	Ready(ctx context.Context) error
}

type ServerDeps struct {
	Catalog                   store.CatalogStore
	ReviewStore               ReviewStore
	ImportRunStore            ingest.ImportRunStore
	ReplayStore               ingest.ReplayStore
	ImportRunReviewGroupStore ImportRunReviewGroupStore
	EventSecondarySourceStore EventSecondarySourceInfoStore
	EventGenreStore           EventGenreStore
	GenreConfigurationStore   GenreConfigurationStore
	ReadyChecker              ReadyChecker
	MediaRoot                 string
	MediaURLPrefix            string
	AdminAuth                 AdminAuthConfig
	Logger                    *slog.Logger
}

type PageData struct {
	SiteName                 string
	PageTitle                string
	MetaDescription          string
	Active                   string
	Content                  template.HTML
	Now                      time.Time
	Events                   []domain.Event
	EventGroups              []EventGroup
	EventSections            []EventSection
	EventFilters             EventFilters
	EventFiltersApplied      bool
	EventDetail              EventDetailView
	VenueNames               map[string]string
	VenueAreas               map[string]string
	Areas                    []string
	Event                    domain.Event
	EventSecondarySources    []store.EventSecondarySourceInfo
	EventGenres              []genre.Match
	Venues                   []domain.Venue
	Venue                    domain.Venue
	VenueEvents              []domain.Event
	VenueTimelineSections    []VenueTimelineSection
	ReviewGroups             []review.GroupSummary
	ReviewHistoryRows        []ReviewHistoryRow
	ReviewDetail             ReviewDetail
	ProvisionalVenues        []ProvisionalVenueRow
	ProvisionalRooms         []ProvisionalRoomRow
	Room                     domain.VenueRoom
	RoomEvents               []domain.Event
	ImportRunRows            []ImportRunRow
	ImportRunDetail          ImportRunDetail
	GenreRules               []genre.Rule
	LatestImport             *ingest.ImportRunSummary
	HasImportHistory         bool
	HasImportRunDetail       bool
	HasImportRunReviewGroups bool
	HasReviewStorage         bool
	HasVenueAdmin            bool
	HasVenueAdminWrites      bool
	HasRoomAdmin             bool
	HasGenreConfiguration    bool
	AdminAuthenticated       bool
	CSRFToken                string
	LoginNext                string
	LoginError               string
	Flash                    string
}

type EventGroup struct {
	Date   time.Time
	Events []EventCardView
}

type EventSection struct {
	ID     string
	Title  string
	Date   time.Time
	Events []EventCardView
}

type VenueTimelineSection struct {
	Dates  []time.Time
	Events []EventCardView
}

type EventFilters struct {
	Window string
	Venue  string
	Area   string
}

type ReviewDetail struct {
	Group                review.Group
	IsDuplicate          bool
	IsSingleton          bool
	CanonicalSummaryRows []ReviewCanonicalSummaryRow
	Rows                 []ReviewFieldRow
	Preview              []ReviewPreviewRow
	SingleCandidateRows  []ReviewSingleCandidateRow
}

type ProvisionalVenueRow struct {
	Venue              domain.Venue
	UpcomingEventCount int
	NextEvent          *domain.Event
}

type ProvisionalRoomRow struct {
	Room               domain.VenueRoom
	Venue              domain.Venue
	UpcomingEventCount int
	NextEvent          *domain.Event
}

type ReviewHistoryRow struct {
	review.GroupSummary
}

type ReviewFieldRow struct {
	Field review.Field
	Label string
	Cells []ReviewChoiceCell
}

type ReviewChoiceCell struct {
	CandidateID       int64
	Value             string
	Checked           bool
	Consensus         bool
	SelectedConsensus bool
	Provenance        string
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
	Defaulted bool
}

type ReviewSingleCandidateRow struct {
	Label string
	Value string
}

type ImportRunRow struct {
	ingest.ImportRunSummary
	ReviewGroupStatusSummary string
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

func NewServer(deps ServerDeps) (*Server, error) {
	if deps.Catalog == nil {
		return nil, errors.New("catalog store is required")
	}
	if err := deps.Catalog.Validate(context.Background()); err != nil {
		return nil, fmt.Errorf("validate catalog store: %w", err)
	}

	localLocation, err := time.LoadLocation("Europe/London")
	if err != nil {
		localLocation = time.FixedZone("Europe/London", 0)
	}

	funcs := template.FuncMap{
		"dateLong":  func(t time.Time) string { return t.In(localLocation).Format("Monday, 2 January 2006") },
		"dateShort": func(t time.Time) string { return t.In(localLocation).Format("2 Jan 2006") },
		"dateDayMonth": func(t time.Time) string {
			return t.In(localLocation).Format("2 Jan")
		},
		"dateSectionTitle": func(date, now time.Time) string {
			return dateSectionTitle(date, now, localLocation)
		},
		"venueTimelineTitle": func(section VenueTimelineSection, now time.Time) string {
			return venueTimelineTitle(section, now, localLocation)
		},
		"venueTimelineMeta": func(section VenueTimelineSection) string {
			return venueTimelineMeta(section, localLocation)
		},
		"venueTimelineToneClass": venueTimelineToneClass,
		"dateShortPtr": func(t *time.Time) string {
			if t == nil {
				return ""
			}
			return t.In(localLocation).Format("2 Jan 2006")
		},
		"originLabel": originLabel,
		"blankValue": func(value string) string {
			if strings.TrimSpace(value) == "" {
				return "(blank)"
			}
			return value
		},
		"multilineAddress": formatMultilineAddress,
		"displayAddress": func(name, value string) string {
			return formatVenueAddress(name, value)
		},
		"venueCardMeta": venueCardMeta,
		"eventTitle": func(event domain.Event, venueNames map[string]string) string {
			return publicEventTitle(event, venueNames)
		},
		"eventCardMeta": func(event domain.Event, venueNames map[string]string) string {
			return publicEventCardMeta(event, venueNames)
		},
		"eventDetailMeta": publicEventDetailMeta,
		"candidateDisplayLabel": func(candidate review.Candidate) string {
			if candidate.IsCanonicalSnapshot() {
				return "Live canonical snapshot"
			}
			return fmt.Sprintf("Candidate %d", candidate.Position)
		},
		"choiceCellClass": func(cell ReviewChoiceCell) string {
			classes := []string{"choice-cell"}
			if cell.Checked {
				classes = append(classes, "selected-choice")
			}
			if cell.Consensus {
				classes = append(classes, "consensus")
			}
			if cell.SelectedConsensus {
				classes = append(classes, "selected-consensus")
			}
			return strings.Join(classes, " ")
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
		"dateTimeAttr": func(t time.Time) string {
			return t.In(localLocation).Format("2006-01-02T15:04:05-07:00")
		},
		"timeShortPtr": func(t *time.Time) string {
			if t == nil {
				return ""
			}
			return t.In(localLocation).Format("15:04")
		},
		"venueName": publicVenueName,
		"venueArea": func(venueAreas map[string]string, slug string) string {
			if venueAreas == nil {
				return ""
			}
			return strings.TrimSpace(venueAreas[slug])
		},
		"eventRoomText":    eventRoomText,
		"eventStatusLabel": eventStatusLabel,
		"showCount":        showCount,
		"originText": func(origin domain.Origin) string {
			return string(origin)
		},
		"eventImageAlt":      eventImageAlt,
		"eventImagePortrait": eventImagePortrait,
		"imageFocusStyle":    imageFocusStyle,
		"year":               func(t time.Time) string { return t.In(localLocation).Format("2006") },
		"joinStrings":        func(values []string, sep string) string { return strings.Join(values, sep) },
		"genreNames":         func(values []genre.Match, sep string) string { return strings.Join(genre.Names(values), sep) },
		"descriptionHTML":    descriptionHTML,
	}

	layout, err := template.New("layout.html").Funcs(funcs).ParseFS(templateFS, "templates/layout.html")
	if err != nil {
		return nil, fmt.Errorf("parse layout: %w", err)
	}

	pageFiles := []string{
		"templates/events.html",
		"templates/event_detail.html",
		"templates/venues.html",
		"templates/venue_detail.html",
		"templates/admin_login.html",
		"templates/admin.html",
		"templates/admin_review.html",
		"templates/admin_review_history.html",
		"templates/admin_venues.html",
		"templates/admin_venue_detail.html",
		"templates/admin_rooms.html",
		"templates/admin_room_detail.html",
		"templates/admin_configuration.html",
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
	mediaURLPrefix := normalizeMediaURLPrefix(deps.MediaURLPrefix)
	var mediaServer http.Handler
	if strings.TrimSpace(deps.MediaRoot) != "" {
		mediaServer = http.StripPrefix(mediaURLPrefix+"/", http.FileServer(fileSystemRejectingDirectories{base: http.Dir(deps.MediaRoot)}))
	}
	adminAuth, err := newAdminAuthenticator(deps.AdminAuth)
	if err != nil {
		return nil, err
	}

	return &Server{
		catalog:                   deps.Catalog,
		reviewStore:               deps.ReviewStore,
		importRunStore:            deps.ImportRunStore,
		replayStore:               deps.ReplayStore,
		importRunReviewGroupStore: deps.ImportRunReviewGroupStore,
		secondarySourceStore:      deps.EventSecondarySourceStore,
		eventGenreStore:           deps.EventGenreStore,
		genreConfigStore:          deps.GenreConfigurationStore,
		readyChecker:              deps.ReadyChecker,
		localLocation:             localLocation,
		clock:                     func() time.Time { return time.Now().UTC() },
		layout:                    layout,
		pages:                     pages,
		fileServer:                http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))),
		mediaServer:               mediaServer,
		mediaURLPrefix:            mediaURLPrefix,
		logger:                    logging.EnsureLogger(deps.Logger),
		adminAuth:                 adminAuth,
	}, nil
}

func normalizeMediaURLPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "/media"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	prefix = strings.TrimRight(prefix, "/")
	if prefix == "" {
		return "/media"
	}
	return prefix
}

type fileSystemRejectingDirectories struct {
	base http.FileSystem
}

func (fsys fileSystemRejectingDirectories) Open(name string) (http.File, error) {
	file, err := fsys.base.Open(name)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.IsDir() {
		_ = file.Close()
		return nil, fs.ErrNotExist
	}
	return file, nil
}

func formatMultilineAddress(value string) string {
	return formatVenueAddress("", value)
}

func venueCardMeta(name, neighbourhood, address string) string {
	if neighbourhood = strings.TrimSpace(neighbourhood); neighbourhood != "" {
		return neighbourhood
	}

	line, _, _ := strings.Cut(formatVenueAddress(name, address), "\n")
	return strings.TrimSuffix(strings.TrimSpace(line), ",")
}

func publicEventTitle(event domain.Event, venueNames map[string]string) string {
	title := normalizePublicText(event.Name)
	if title == "" {
		return strings.TrimSpace(event.Name)
	}
	if trimmed := trimLeadingTitlePunctuation(title); trimmed != "" {
		title = trimmed
	}
	if venueName := publicVenueName(venueNames, event.VenueSlug); strings.TrimSpace(venueName) != "" {
		title = stripPublicEventVenueSuffix(title, venueName)
	}
	return titleCasePublicShoutingTitle(title)
}

func publicEventCardMeta(event domain.Event, venueNames map[string]string) string {
	parts := []string{}
	if venueName := publicEventVenueName(event, venueNames); venueName != "" {
		parts = append(parts, venueName)
	}
	if genre := normalizePublicText(event.Genre); genre != "" {
		parts = append(parts, genre)
	}
	if status := publicEventStatus(event.Status); status != "" {
		parts = append(parts, status)
	}
	return strings.Join(parts, " · ")
}

func publicEventVenueName(event domain.Event, venueNames map[string]string) string {
	venueName := normalizePublicText(publicVenueName(venueNames, event.VenueSlug))
	if venueName == "" {
		return ""
	}
	if room := normalizePublicText(eventRoomText(event)); room != "" {
		return fmt.Sprintf("%s (%s)", venueName, room)
	}
	return venueName
}

func publicEventDetailMeta(event domain.Event) string {
	parts := []string{}
	if genre := normalizePublicText(event.Genre); genre != "" {
		parts = append(parts, genre)
	}
	if status := publicEventStatus(event.Status); status != "" {
		parts = append(parts, status)
	}
	return strings.Join(parts, " · ")
}

func publicVenueName(venueNames map[string]string, slug string) string {
	if venueNames == nil {
		return slug
	}
	if name := strings.TrimSpace(venueNames[slug]); name != "" {
		return name
	}
	return slug
}

func normalizePublicText(value string) string {
	value = decodePublicHTMLEntities(strings.TrimSpace(value))
	return strings.Join(strings.Fields(value), " ")
}

func decodePublicHTMLEntities(value string) string {
	for i := 0; i < 4; i++ {
		decoded := html.UnescapeString(value)
		if decoded == value {
			break
		}
		value = decoded
	}
	return value
}

func trimLeadingTitlePunctuation(value string) string {
	trimmed := strings.TrimLeftFunc(value, func(r rune) bool {
		switch r {
		case '|', '-', '–', '—', ':', '•', '·':
			return true
		default:
			return unicode.IsSpace(r)
		}
	})
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return value
	}
	return trimmed
}

func stripPublicEventVenueSuffix(title, venueName string) string {
	if stripped := stripTrailingParentheticalVenue(title, venueName); stripped != title {
		title = stripped
	}
	if stripped := stripTrailingLiveAtVenue(title, venueName); stripped != title {
		title = stripped
	}
	if stripped := stripTrailingDelimitedVenue(title, venueName); stripped != title {
		title = stripped
	}
	return title
}

func stripTrailingParentheticalVenue(title, venueName string) string {
	if !strings.HasSuffix(title, ")") {
		return title
	}
	start := strings.LastIndex(title, "(")
	if start < 0 {
		return title
	}
	suffix := strings.TrimSpace(title[start+1 : len(title)-1])
	if !publicVenueSuffixMatches(suffix, venueName) {
		return title
	}
	return nonEmptyTitlePrefix(title, strings.TrimSpace(title[:start]))
}

func stripTrailingLiveAtVenue(title, venueName string) string {
	lower := strings.ToLower(title)
	marker := " live at "
	idx := strings.LastIndex(lower, marker)
	if idx < 0 {
		return title
	}
	suffix := strings.TrimSpace(title[idx+len(marker):])
	if !publicVenueSuffixMatches(suffix, venueName) {
		return title
	}
	return nonEmptyTitlePrefix(title, strings.TrimSpace(title[:idx]))
}

func stripTrailingDelimitedVenue(title, venueName string) string {
	delimiters := []string{" - ", " – ", " — ", " | ", ", "}
	for _, delimiter := range delimiters {
		for searchEnd := len(title); searchEnd > 0; {
			idx := strings.LastIndex(title[:searchEnd], delimiter)
			if idx < 0 {
				break
			}
			suffix := strings.TrimSpace(title[idx+len(delimiter):])
			if publicVenueSuffixMatches(suffix, venueName) {
				return nonEmptyTitlePrefix(title, strings.TrimSpace(title[:idx]))
			}
			searchEnd = idx
		}
	}
	return title
}

func publicVenueSuffixMatches(suffix, venueName string) bool {
	suffix = strings.TrimSpace(suffix)
	venueName = strings.TrimSpace(venueName)
	if suffix == "" || venueName == "" {
		return false
	}
	if samePublicVenueName(suffix, venueName) {
		return true
	}
	if head, _, ok := strings.Cut(suffix, ","); ok && samePublicVenueName(head, venueName) {
		return true
	}
	return false
}

func samePublicVenueName(left, right string) bool {
	leftKey := normalizedDisplayAddressNameKey(left)
	rightKey := normalizedDisplayAddressNameKey(right)
	return leftKey != "" && leftKey == rightKey
}

func nonEmptyTitlePrefix(original, prefix string) string {
	if prefix == "" {
		return original
	}
	return prefix
}

func titleCasePublicShoutingTitle(value string) string {
	if !publicTitleLooksShouting(value) {
		return value
	}
	words := strings.Fields(value)
	for i, word := range words {
		words[i] = titleCasePublicTitleWord(word)
	}
	return strings.Join(words, " ")
}

func publicTitleLooksShouting(value string) bool {
	if len(strings.Fields(value)) < 2 {
		return false
	}
	letters := 0
	for _, r := range value {
		if !unicode.IsLetter(r) {
			continue
		}
		letters++
		if unicode.IsLower(r) {
			return false
		}
	}
	return letters >= 4
}

func titleCasePublicTitleWord(word string) string {
	if preservePublicUppercaseWord(word) {
		return word
	}
	runes := []rune(strings.ToLower(word))
	for i, r := range runes {
		if unicode.IsLetter(r) {
			runes[i] = unicode.ToUpper(r)
			break
		}
	}
	return string(runes)
}

func preservePublicUppercaseWord(word string) bool {
	key := publicTitleWordKey(word)
	switch key {
	case "ABBA", "DJ", "DJS", "EP", "EU", "LP", "MC", "MCS", "UK", "US", "USA":
		return true
	}
	for _, r := range word {
		if unicode.IsDigit(r) || r == '.' {
			return true
		}
	}
	return false
}

func publicTitleWordKey(word string) string {
	var b strings.Builder
	for _, r := range word {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToUpper(r))
		}
	}
	return b.String()
}

func publicEventStatus(status string) string {
	status = normalizePublicText(status)
	switch {
	case status == "":
		return ""
	case strings.EqualFold(status, "Listed"), strings.EqualFold(status, "CONFIRMED"):
		return ""
	default:
		return titleCasePublicStatus(status)
	}
}

func titleCasePublicStatus(status string) string {
	letters := 0
	for _, r := range status {
		if !unicode.IsLetter(r) {
			continue
		}
		letters++
		if unicode.IsLower(r) {
			return status
		}
	}
	if letters == 0 {
		return status
	}
	return titleCasePublicTitleWord(status)
}

func formatVenueAddress(name, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	replacer := strings.NewReplacer(
		`\n`, "\n",
		`\N`, "\n",
		`\,`, ",",
		`\;`, ";",
		`\\`, `\`,
	)
	value = replacer.Replace(value)

	lines := strings.Split(value, "\n")
	parts := make([]string, 0, len(lines))
	for _, line := range lines {
		for _, part := range strings.Split(line, ",") {
			part = strings.Join(strings.Fields(strings.TrimSpace(part)), " ")
			if part != "" {
				parts = append(parts, part)
			}
		}
	}
	if sameDisplayAddressLine(parts, name) {
		parts = parts[1:]
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ",\n")
}

func sameDisplayAddressLine(parts []string, name string) bool {
	if len(parts) == 0 {
		return false
	}
	return normalizedDisplayAddressNameKey(parts[0]) != "" && normalizedDisplayAddressNameKey(parts[0]) == normalizedDisplayAddressNameKey(name)
}

func normalizedDisplayAddressNameKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.ToLower(value)
	value = strings.ReplaceAll(value, "&", " and ")
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "the ") {
		value = strings.TrimSpace(strings.TrimPrefix(value, "the "))
	}

	var b strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (s *Server) SetClockForTesting(clock func() time.Time) {
	s.clock = clock
}

func (s *Server) hasVenueAdmin() bool {
	return s.reviewStore != nil || s.importRunStore != nil || s.replayStore != nil || s.venueAdminStore() != nil
}

func (s *Server) venueAdminStore() VenueAdminStore {
	if store, ok := s.catalog.(VenueAdminStore); ok {
		return store
	}
	return nil
}

func (s *Server) canWriteVenueAdmin() bool {
	return s.venueAdminStore() != nil
}

func (s *Server) roomAdminStore() RoomAdminStore {
	if store, ok := s.catalog.(RoomAdminStore); ok {
		return store
	}
	return nil
}

func (s *Server) hasRoomAdmin() bool {
	return s.roomAdminStore() != nil
}

func (s *Server) hasGenreConfiguration() bool {
	return s.genreConfigStore != nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	recorder := &responseLogWriter{ResponseWriter: w, status: http.StatusOK}
	s.routeHTTP(recorder, r)

	if shouldLogRequest(r.URL.Path, recorder.status) {
		s.log().Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"bytes", recorder.bytes,
			"duration", time.Since(start),
			"remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)
	}
}

func (s *Server) routeHTTP(w http.ResponseWriter, r *http.Request) {
	cleaned := path.Clean(r.URL.Path)
	if cleaned == "/admin/login" {
		s.handleAdminLogin(w, r)
		return
	}
	if cleaned == "/admin/logout" {
		var ok bool
		r, ok = s.requireAdmin(w, r)
		if !ok {
			return
		}
		s.handleAdminLogout(w, r)
		return
	}
	if isAdminRequestPath(cleaned) {
		var ok bool
		r, ok = s.requireAdmin(w, r)
		if !ok {
			return
		}
	}
	switch {
	case cleaned == "/":
		s.handleEvents(w, r)
	case cleaned == "/events":
		s.handleEvents(w, r)
	case cleaned == "/venues":
		s.handleVenues(w, r)
	case cleaned == "/healthz":
		s.handleHealthz(w, r)
	case cleaned == "/readyz":
		s.handleReadyz(w, r)
	case cleaned == "/admin":
		s.handleAdmin(w, r)
	case cleaned == "/admin/review":
		s.handleAdminReview(w, r)
	case cleaned == "/admin/review/history":
		s.handleAdminReviewHistory(w, r)
	case cleaned == "/admin/venues":
		s.handleAdminVenues(w, r)
	case cleaned == "/admin/rooms":
		s.handleAdminRooms(w, r)
	case cleaned == "/admin/configuration":
		s.handleAdminConfiguration(w, r)
	case r.URL.Path == "/admin/import-runs":
		s.handleAdminImportRuns(w, r)
	case strings.HasPrefix(r.URL.Path, "/admin/import-runs/"):
		s.handleAdminImportRunDetail(w, r)
	case strings.HasPrefix(cleaned, "/admin/venues/"):
		s.handleAdminVenueDetail(w, r, strings.TrimPrefix(cleaned, "/admin/venues/"))
	case strings.HasPrefix(cleaned, "/admin/rooms/"):
		s.handleAdminRoomDetail(w, r, strings.TrimPrefix(cleaned, "/admin/rooms/"))
	case strings.HasPrefix(cleaned, "/admin/review/"):
		s.handleAdminReviewDetail(w, r, strings.TrimPrefix(cleaned, "/admin/review/"))
	case strings.HasPrefix(cleaned, "/events/"):
		s.handleEventDetail(w, r, strings.TrimPrefix(cleaned, "/events/"))
	case strings.HasPrefix(cleaned, "/venues/"):
		s.handleVenueDetail(w, r, strings.TrimPrefix(cleaned, "/venues/"))
	case cleaned == "/static" || strings.HasPrefix(cleaned, "/static/"):
		s.fileServer.ServeHTTP(w, r)
	case s.mediaServer != nil && (cleaned == s.mediaURLPrefix || strings.HasPrefix(cleaned, s.mediaURLPrefix+"/")):
		if cleaned == s.mediaURLPrefix {
			http.NotFound(w, r)
			return
		}
		s.mediaServer.ServeHTTP(w, r)
	default:
		http.NotFound(w, r)
	}
}

type responseLogWriter struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (w *responseLogWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseLogWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseLogWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(body)
	w.bytes += n
	return n, err
}

func shouldLogRequest(requestPath string, status int) bool {
	cleaned := path.Clean(requestPath)
	if (cleaned == "/healthz" || cleaned == "/readyz") && status < http.StatusInternalServerError {
		return false
	}
	return true
}

func (s *Server) log() *slog.Logger {
	if s == nil {
		return logging.NopLogger()
	}
	return logging.EnsureLogger(s.logger)
}

func (s *Server) logRequestError(r *http.Request, message string, err error, attrs ...any) {
	if err == nil {
		return
	}
	fields := []any{
		"method", r.Method,
		"path", r.URL.Path,
		"error", err,
	}
	fields = append(fields, attrs...)
	s.log().Error(message, fields...)
}

func (s *Server) handleAdminLogin(w http.ResponseWriter, r *http.Request) {
	if !s.adminAuth.enabled() {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.renderAdminLogin(w, sanitizeAdminNextPath(r.URL.Query().Get("next")), "")
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, "parse form", http.StatusBadRequest)
			return
		}
		next := sanitizeAdminNextPath(r.PostForm.Get("next"))
		failureKey := adminFailureKey(r)
		now := s.now()
		if s.adminAuth.failures.locked(failureKey, now) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusTooManyRequests)
			s.renderAdminLogin(w, next, "Sign in is temporarily unavailable. Try again later.")
			return
		}
		if !s.adminAuth.authenticate(r.PostForm.Get("password")) {
			s.adminAuth.failures.recordFailure(failureKey, now)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			s.renderAdminLogin(w, next, "Sign in failed.")
			return
		}
		s.adminAuth.failures.clear(failureKey)
		if err := s.adminAuth.startSession(w, now); err != nil {
			s.logRequestError(r, "start admin session", err)
			http.Error(w, "start admin session", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, next, http.StatusSeeOther)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) renderAdminLogin(w http.ResponseWriter, next, errorText string) {
	data := PageData{
		SiteName:        "Sheffield Live",
		PageTitle:       "Admin login",
		MetaDescription: "Admin login for Sheffield Live.",
		Now:             s.now(),
		LoginNext:       sanitizeAdminNextPath(next),
		LoginError:      errorText,
	}
	s.renderPage(w, "templates/admin_login.html", data)
}

func (s *Server) handleAdminLogout(w http.ResponseWriter, r *http.Request) {
	if !s.adminAuth.enabled() {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "parse form", http.StatusBadRequest)
		return
	}
	if !s.requireAdminCSRF(w, r) {
		return
	}
	s.adminAuth.endSession(w, r)
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	if !s.hasVenueAdmin() {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	data := PageData{
		SiteName:            "Sheffield Live",
		PageTitle:           "Admin",
		MetaDescription:     "Admin tools for Sheffield Live.",
		Active:              "admin",
		Now:                 s.now(),
		HasImportHistory:    s.importRunStore != nil,
		HasImportRunDetail:  s.replayStore != nil,
		HasReviewStorage:    s.reviewStore != nil,
		HasVenueAdmin:       s.hasVenueAdmin(),
		HasVenueAdminWrites: s.canWriteVenueAdmin(),
		HasRoomAdmin:        s.hasRoomAdmin(),
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin.html", data)
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
		s.logRequestError(r, "load review groups", err)
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
		SiteName:              "Sheffield Live",
		PageTitle:             "Review",
		MetaDescription:       "Review open staged event candidates.",
		Active:                "admin-review",
		Now:                   s.now(),
		ReviewGroups:          groups,
		HasImportHistory:      s.importRunStore != nil,
		HasImportRunDetail:    s.replayStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
		Flash:                 flash,
	}
	if s.importRunStore != nil {
		latest, err := s.importRunStore.LatestSuccessfulImport(r.Context())
		if err != nil {
			s.logRequestError(r, "load latest import run", err)
			http.Error(w, "load latest import run", http.StatusInternalServerError)
			return
		}
		data.LatestImport = latest
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_review.html", data)
}

func (s *Server) handleAdminVenues(w http.ResponseWriter, r *http.Request) {
	if s.catalog == nil || !s.hasVenueAdmin() {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	venues, err := s.catalog.ListVenues(r.Context())
	if err != nil {
		s.logRequestError(r, "load venues", err)
		http.Error(w, "load venues", http.StatusInternalServerError)
		return
	}
	events, err := s.catalog.ListEvents(r.Context())
	if err != nil {
		s.logRequestError(r, "load events", err)
		http.Error(w, "load events", http.StatusInternalServerError)
		return
	}
	now := s.now()
	flash := ""
	if r.URL.Query().Get("validated") == "1" {
		flash = "Venue validated."
	}
	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             "Provisional venues",
		MetaDescription:       "Queue of provisional venue rows awaiting validation.",
		Now:                   now,
		HasImportHistory:      s.importRunStore != nil,
		HasImportRunDetail:    s.replayStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasRoomAdmin:          s.hasRoomAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
		Flash:                 flash,
		ProvisionalVenues: buildProvisionalVenueRows(
			venues,
			events,
			now,
			s.localLocation,
		),
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_venues.html", data)
}

func (s *Server) handleAdminRooms(w http.ResponseWriter, r *http.Request) {
	roomStore := s.roomAdminStore()
	if s.catalog == nil || roomStore == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	venues, err := s.catalog.ListVenues(r.Context())
	if err != nil {
		s.logRequestError(r, "load venues", err)
		http.Error(w, "load venues", http.StatusInternalServerError)
		return
	}
	rooms, err := roomStore.ListVenueRooms(r.Context())
	if err != nil {
		s.logRequestError(r, "load rooms", err)
		http.Error(w, "load rooms", http.StatusInternalServerError)
		return
	}
	events, err := s.catalog.ListEvents(r.Context())
	if err != nil {
		s.logRequestError(r, "load events", err)
		http.Error(w, "load events", http.StatusInternalServerError)
		return
	}
	now := s.now()
	flash := ""
	if r.URL.Query().Get("validated") == "1" {
		flash = "Room validated."
	}
	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             "Provisional rooms",
		MetaDescription:       "Queue of provisional venue rooms awaiting validation.",
		Now:                   now,
		HasImportHistory:      s.importRunStore != nil,
		HasImportRunDetail:    s.replayStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasRoomAdmin:          s.hasRoomAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
		Flash:                 flash,
		ProvisionalRooms: buildProvisionalRoomRows(
			venues,
			rooms,
			events,
			now,
			s.localLocation,
		),
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_rooms.html", data)
}

func (s *Server) handleAdminConfiguration(w http.ResponseWriter, r *http.Request) {
	if s.genreConfigStore == nil {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
	case http.MethodPost:
		s.postAdminConfiguration(w, r)
		return
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rules, err := s.genreConfigStore.ListGenreRules(r.Context())
	if err != nil {
		s.logRequestError(r, "load genre rules", err)
		http.Error(w, "load genre rules", http.StatusInternalServerError)
		return
	}
	flash := ""
	switch {
	case r.URL.Query().Get("saved") == "1":
		flash = "Genre rule saved."
	case r.URL.Query().Get("deleted") == "1":
		flash = "Genre rule deleted."
	case r.URL.Query().Get("recomputed") == "1":
		flash = "Event genres recomputed."
	}
	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             "Configuration",
		MetaDescription:       "Admin configuration for genre inference.",
		Active:                "admin-configuration",
		Now:                   s.now(),
		GenreRules:            rules,
		HasImportHistory:      s.importRunStore != nil,
		HasImportRunDetail:    s.replayStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasRoomAdmin:          s.hasRoomAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
		Flash:                 flash,
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_configuration.html", data)
}

func (s *Server) postAdminConfiguration(w http.ResponseWriter, r *http.Request) {
	if s.genreConfigStore == nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "parse form", http.StatusBadRequest)
		return
	}
	if !s.requireAdminCSRF(w, r) {
		return
	}
	action := strings.TrimSpace(r.FormValue("action"))
	switch action {
	case "save":
		input, err := genreRuleInputFromForm(r.Form)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.genreConfigStore.SaveGenreRule(r.Context(), input); err != nil {
			s.logRequestError(r, "save genre rule", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/admin/configuration?saved=1", http.StatusSeeOther)
	case "delete":
		id, err := strconv.ParseInt(strings.TrimSpace(r.FormValue("id")), 10, 64)
		if err != nil || id <= 0 {
			http.Error(w, "genre rule ID is required", http.StatusBadRequest)
			return
		}
		if err := s.genreConfigStore.DeleteGenreRule(r.Context(), id); err != nil {
			s.logRequestError(r, "delete genre rule", err, "genre_rule_id", id)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/admin/configuration?deleted=1", http.StatusSeeOther)
	case "recompute":
		if err := s.genreConfigStore.RecomputeEventGenres(r.Context()); err != nil {
			s.logRequestError(r, "recompute event genres", err)
			http.Error(w, "recompute event genres", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/admin/configuration?recomputed=1", http.StatusSeeOther)
	default:
		http.Error(w, "invalid configuration action", http.StatusBadRequest)
	}
}

func genreRuleInputFromForm(form url.Values) (genre.RuleInput, error) {
	var input genre.RuleInput
	if rawID := strings.TrimSpace(form.Get("id")); rawID != "" {
		id, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil || id <= 0 {
			return genre.RuleInput{}, fmt.Errorf("invalid genre rule ID")
		}
		input.ID = id
	}
	input.Key = strings.TrimSpace(form.Get("key"))
	input.Name = strings.TrimSpace(form.Get("name"))
	input.MatchType = strings.TrimSpace(form.Get("match_type"))
	if input.MatchType == "" {
		input.MatchType = genre.MatchTypePlain
	}
	input.Pattern = strings.TrimSpace(form.Get("pattern"))
	input.Enabled = form.Get("enabled") == "1"
	if rawSortOrder := strings.TrimSpace(form.Get("sort_order")); rawSortOrder != "" {
		sortOrder, err := strconv.Atoi(rawSortOrder)
		if err != nil {
			return genre.RuleInput{}, fmt.Errorf("invalid sort order")
		}
		input.SortOrder = sortOrder
	}
	return input, nil
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
		s.logRequestError(r, "load review history", err)
		http.Error(w, "load review history", http.StatusInternalServerError)
		return
	}
	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             "Review history",
		MetaDescription:       "Read-only history of resolved and rejected review groups.",
		Active:                "admin-review",
		Now:                   s.now(),
		ReviewHistoryRows:     buildReviewHistoryRows(groups),
		HasImportHistory:      s.importRunStore != nil,
		HasImportRunDetail:    s.replayStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_review_history.html", data)
}

func (s *Server) handleAdminVenueDetail(w http.ResponseWriter, r *http.Request, slug string) {
	if s.catalog == nil || !s.hasVenueAdmin() {
		http.NotFound(w, r)
		return
	}
	slug = strings.TrimSpace(slug)
	if slug == "" || strings.Contains(slug, "/") {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
	case http.MethodPost:
		s.postAdminVenueDecision(w, r, slug)
		return
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	venue, ok, err := s.catalog.LoadVenueBySlug(r.Context(), slug)
	if err != nil {
		s.logRequestError(r, "load venue", err, "venue_slug", slug)
		http.Error(w, "load venue", http.StatusInternalServerError)
		return
	}
	if !ok || venue.ValidationState != domain.ValidationStateProvisional {
		http.NotFound(w, r)
		return
	}

	events, err := s.catalog.ListEventsForVenue(r.Context(), slug)
	if err != nil {
		s.logRequestError(r, "load venue events", err, "venue_slug", slug)
		http.Error(w, "load venue events", http.StatusInternalServerError)
		return
	}
	pageTitle := strings.TrimSpace(venue.Name)
	if pageTitle == "" {
		pageTitle = strings.TrimSpace(venue.Slug)
	}
	if pageTitle == "" {
		pageTitle = "Provisional venue"
	}
	flash := ""
	if r.URL.Query().Get("saved") == "1" {
		flash = "Venue saved."
	}
	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             pageTitle,
		MetaDescription:       venue.Description,
		Now:                   s.now(),
		Venue:                 venue,
		VenueNames:            map[string]string{venue.Slug: venue.Name},
		VenueAreas:            map[string]string{venue.Slug: venue.Neighbourhood},
		VenueEvents:           sortEventsForDisplay(upcomingEvents(events, s.now(), s.localLocation)),
		HasImportHistory:      s.importRunStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
		Flash:                 flash,
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_venue_detail.html", data)
}

func (s *Server) handleAdminRoomDetail(w http.ResponseWriter, r *http.Request, raw string) {
	roomStore := s.roomAdminStore()
	if s.catalog == nil || roomStore == nil {
		http.NotFound(w, r)
		return
	}
	venueSlug, roomSlug, ok := parseAdminRoomPath(raw)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
	case http.MethodPost:
		s.postAdminRoomDecision(w, r, venueSlug, roomSlug)
		return
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	room, ok, err := roomStore.LoadVenueRoomBySlug(r.Context(), venueSlug, roomSlug)
	if err != nil {
		s.logRequestError(r, "load room", err, "venue_slug", venueSlug, "room_slug", roomSlug)
		http.Error(w, "load room", http.StatusInternalServerError)
		return
	}
	if !ok || room.ValidationState != domain.ValidationStateProvisional {
		http.NotFound(w, r)
		return
	}
	venue, ok, err := s.catalog.LoadVenueBySlug(r.Context(), venueSlug)
	if err != nil {
		s.logRequestError(r, "load room venue", err, "venue_slug", venueSlug)
		http.Error(w, "load venue", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	events, err := s.catalog.ListEventsForVenue(r.Context(), venueSlug)
	if err != nil {
		s.logRequestError(r, "load room events", err, "venue_slug", venueSlug, "room_slug", roomSlug)
		http.Error(w, "load room events", http.StatusInternalServerError)
		return
	}
	pageTitle := strings.TrimSpace(room.Name)
	if pageTitle == "" {
		pageTitle = strings.TrimSpace(room.Slug)
	}
	if pageTitle == "" {
		pageTitle = "Provisional room"
	}
	flash := ""
	if r.URL.Query().Get("saved") == "1" {
		flash = "Room saved."
	}
	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             pageTitle,
		MetaDescription:       "Provisional room awaiting validation.",
		Now:                   s.now(),
		Venue:                 venue,
		Room:                  room,
		RoomEvents:            sortEventsForDisplay(upcomingEvents(filterEventsByRoom(events, room.Slug), s.now(), s.localLocation)),
		HasImportHistory:      s.importRunStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasRoomAdmin:          s.hasRoomAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
		Flash:                 flash,
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_room_detail.html", data)
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
		s.logRequestError(r, "load import runs", err)
		http.Error(w, "load import runs", http.StatusInternalServerError)
		return
	}
	importRunRows, err := buildImportRunRows(r.Context(), importRuns, s.importRunReviewGroupStore)
	if err != nil {
		s.logRequestError(r, "load import run review groups", err)
		http.Error(w, "load import run review groups", http.StatusInternalServerError)
		return
	}
	data := PageData{
		SiteName:                 "Sheffield Live",
		PageTitle:                "Import history",
		MetaDescription:          "Read-only history of import runs and snapshot counts.",
		Now:                      s.now(),
		ImportRunRows:            importRunRows,
		HasImportRunDetail:       s.replayStore != nil,
		HasImportRunReviewGroups: s.importRunReviewGroupStore != nil,
		HasReviewStorage:         s.reviewStore != nil,
		HasVenueAdmin:            s.hasVenueAdmin(),
		HasVenueAdminWrites:      s.canWriteVenueAdmin(),
		HasGenreConfiguration:    s.hasGenreConfiguration(),
	}
	s.populateAdminAuthData(r, &data)
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
		s.logRequestError(r, "load import run", err, "import_run_id", runID)
		http.Error(w, "load import run", http.StatusInternalServerError)
		return
	}
	detail := buildImportRunDetail(run)
	if s.importRunReviewGroupStore != nil {
		groups, err := s.importRunReviewGroupStore.ListReviewGroupsForImportRun(r.Context(), run.ID)
		if err != nil {
			s.logRequestError(r, "load import run review groups", err, "import_run_id", run.ID)
			http.Error(w, "load import run review groups", http.StatusInternalServerError)
			return
		}
		detail.ReviewGroups = groups
	}

	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             fmt.Sprintf("Import run #%d", run.ID),
		MetaDescription:       "Read-only import run snapshot metadata.",
		Now:                   s.now(),
		ImportRunDetail:       detail,
		HasImportHistory:      s.importRunStore != nil,
		HasImportRunDetail:    s.replayStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
	}
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_import_run_detail.html", data)
}

func (s *Server) postAdminVenueDecision(w http.ResponseWriter, r *http.Request, slug string) {
	adminStore := s.venueAdminStore()
	if adminStore == nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "parse form", http.StatusBadRequest)
		return
	}
	if !s.requireAdminCSRF(w, r) {
		return
	}
	action := strings.TrimSpace(r.FormValue("action"))
	venue, ok, err := s.catalog.LoadVenueBySlug(r.Context(), slug)
	if err != nil {
		s.logRequestError(r, "load venue", err, "venue_slug", slug)
		http.Error(w, "load venue", http.StatusInternalServerError)
		return
	}
	if !ok || venue.ValidationState != domain.ValidationStateProvisional {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "validate":
		if err := adminStore.ValidateVenue(r.Context(), slug); err != nil {
			lower := strings.ToLower(err.Error())
			if strings.Contains(lower, "not found") || strings.Contains(lower, "not provisional") {
				http.NotFound(w, r)
				return
			}
			s.logRequestError(r, "validate venue", err, "venue_slug", slug)
			http.Error(w, "validate venue", http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/admin/venues?validated=1", http.StatusSeeOther)
	case "save":
		input, err := provisionalVenueUpdateFromForm(slug, r.Form)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := adminStore.UpdateProvisionalVenue(r.Context(), input); err != nil {
			lower := strings.ToLower(err.Error())
			if strings.Contains(lower, "not found") || strings.Contains(lower, "not provisional") {
				http.NotFound(w, r)
				return
			}
			s.logRequestError(r, "update provisional venue", err, "venue_slug", slug)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, fmt.Sprintf("/admin/venues/%s?saved=1", slug), http.StatusSeeOther)
	default:
		http.Error(w, "invalid venue action", http.StatusBadRequest)
	}
}

func (s *Server) postAdminRoomDecision(w http.ResponseWriter, r *http.Request, venueSlug, roomSlug string) {
	roomStore := s.roomAdminStore()
	if roomStore == nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "parse form", http.StatusBadRequest)
		return
	}
	if !s.requireAdminCSRF(w, r) {
		return
	}
	action := strings.TrimSpace(r.FormValue("action"))
	room, ok, err := roomStore.LoadVenueRoomBySlug(r.Context(), venueSlug, roomSlug)
	if err != nil {
		s.logRequestError(r, "load room", err, "venue_slug", venueSlug, "room_slug", roomSlug)
		http.Error(w, "load room", http.StatusInternalServerError)
		return
	}
	if !ok || room.ValidationState != domain.ValidationStateProvisional {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "validate":
		if err := roomStore.ValidateVenueRoom(r.Context(), venueSlug, roomSlug); err != nil {
			lower := strings.ToLower(err.Error())
			if strings.Contains(lower, "not found") || strings.Contains(lower, "not provisional") {
				http.NotFound(w, r)
				return
			}
			s.logRequestError(r, "validate room", err, "venue_slug", venueSlug, "room_slug", roomSlug)
			http.Error(w, "validate room", http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/admin/rooms?validated=1", http.StatusSeeOther)
	case "save":
		input, err := provisionalRoomUpdateFromForm(venueSlug, roomSlug, r.Form)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := roomStore.UpdateProvisionalVenueRoom(r.Context(), input); err != nil {
			lower := strings.ToLower(err.Error())
			if strings.Contains(lower, "not found") || strings.Contains(lower, "not provisional") {
				http.NotFound(w, r)
				return
			}
			s.logRequestError(r, "update provisional room", err, "venue_slug", venueSlug, "room_slug", roomSlug)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, fmt.Sprintf("/admin/rooms/%s/%s?saved=1", venueSlug, roomSlug), http.StatusSeeOther)
	default:
		http.Error(w, "invalid room action", http.StatusBadRequest)
	}
}

func provisionalRoomUpdateFromForm(venueSlug, roomSlug string, form url.Values) (store.RoomUpdateInput, error) {
	input := store.RoomUpdateInput{
		VenueSlug: strings.TrimSpace(venueSlug),
		Slug:      strings.TrimSpace(roomSlug),
		Name:      strings.TrimSpace(form.Get("name")),
	}
	if rawSortOrder := strings.TrimSpace(form.Get("sort_order")); rawSortOrder != "" {
		sortOrder, err := strconv.Atoi(rawSortOrder)
		if err != nil {
			return store.RoomUpdateInput{}, fmt.Errorf("invalid sort order")
		}
		input.SortOrder = sortOrder
	}
	if input.VenueSlug == "" {
		return store.RoomUpdateInput{}, fmt.Errorf("room venue slug is required")
	}
	if input.Slug == "" {
		return store.RoomUpdateInput{}, fmt.Errorf("room slug is required")
	}
	if input.Name == "" {
		return store.RoomUpdateInput{}, fmt.Errorf("room name is required")
	}
	return input, nil
}

func provisionalVenueUpdateFromForm(slug string, form url.Values) (store.VenueUpdateInput, error) {
	input := store.VenueUpdateInput{
		Slug:          strings.TrimSpace(slug),
		Name:          strings.TrimSpace(form.Get("name")),
		Address:       strings.TrimSpace(form.Get("address")),
		Neighbourhood: strings.TrimSpace(form.Get("neighbourhood")),
		Description:   strings.TrimSpace(form.Get("description")),
		Website:       strings.TrimSpace(form.Get("website")),
		CoverageNote:  strings.TrimSpace(form.Get("coverage_note")),
	}
	switch strings.TrimSpace(form.Get("coverage_kind")) {
	case "", string(domain.CoverageKindVenue):
		input.CoverageKind = domain.CoverageKindVenue
	case string(domain.CoverageKindProgram):
		input.CoverageKind = domain.CoverageKindProgram
	default:
		return store.VenueUpdateInput{}, fmt.Errorf("invalid coverage kind")
	}
	if input.Slug == "" {
		return store.VenueUpdateInput{}, fmt.Errorf("venue slug is required")
	}
	return input, nil
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
	if !s.requireAdminCSRF(w, r) {
		return
	}

	group, ok, err := s.reviewStore.LoadReviewGroup(r.Context(), groupID)
	if err != nil {
		s.logRequestError(r, "load review group", err, "review_group_id", groupID)
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
			s.logRequestError(r, "save review draft", err, "review_group_id", groupID)
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
			s.logRequestError(r, "resolve review group", err, "review_group_id", groupID)
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
			s.logRequestError(r, "accept review group", err, "review_group_id", groupID)
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
			s.logRequestError(r, "reject review group", err, "review_group_id", groupID)
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
	return group.StagedCandidateCount >= 2 || reviewGroupHasCanonicalSnapshot(group)
}

func reviewGroupIsSingleton(group review.Group) bool {
	return group.StagedCandidateCount == 1 && !reviewGroupHasCanonicalSnapshot(group)
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
		s.logRequestError(r, "load review group", err, "review_group_id", groupID)
		http.Error(w, "load review group", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             group.Title,
		MetaDescription:       "Review staged event candidates.",
		Active:                "admin-review",
		Now:                   s.now(),
		HasImportHistory:      s.importRunStore != nil,
		HasImportRunDetail:    s.replayStore != nil,
		HasReviewStorage:      s.reviewStore != nil,
		HasVenueAdmin:         s.hasVenueAdmin(),
		HasVenueAdminWrites:   s.canWriteVenueAdmin(),
		HasGenreConfiguration: s.hasGenreConfiguration(),
		Flash:                 flash,
	}
	data.ReviewDetail = buildReviewDetail(group)
	s.populateAdminAuthData(r, &data)
	s.renderPage(w, "templates/admin_review_detail.html", data)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	venues, err := s.catalog.ListVenues(r.Context())
	if err != nil {
		s.logRequestError(r, "load venues", err)
		http.Error(w, "load venues", http.StatusInternalServerError)
		return
	}
	events, err := s.catalog.ListEvents(r.Context())
	if err != nil {
		s.logRequestError(r, "load events", err)
		http.Error(w, "load events", http.StatusInternalServerError)
		return
	}
	filters := parseEventFilters(r, venues)
	venueNames := venueNameMap(venues)
	venueAreas := venueAreaMap(venues)
	presenter := newEventPresenter(venueNames, venueAreas, s.localLocation)
	filtered := hasEventFilterQuery(r)
	events = filterEventsByVenue(events, filters.Venue)
	events = filterEventsByArea(events, venues, filters.Area)
	if filtered {
		events = filterEventsByWindow(events, now, s.localLocation, filters.Window)
	}
	events = sortEventsForDisplay(events)
	data := PageData{
		SiteName:            "Sheffield Live",
		PageTitle:           "Events",
		MetaDescription:     "Browse Sheffield live music by date and venue.",
		Active:              "events",
		Now:                 now,
		Events:              events,
		EventFilters:        filters,
		EventFiltersApplied: filtered,
		VenueNames:          venueNames,
		VenueAreas:          venueAreas,
		Areas:               venueAreasList(venues),
		Venues:              venues,
	}
	if filtered {
		data.EventGroups = groupEventsByDisplayDate(events, now, s.localLocation, presenter)
	} else {
		data.EventSections = buildEventBoardSections(events, now, s.localLocation, presenter)
	}
	s.renderPage(w, "templates/events.html", data)
}

func (s *Server) handleEventDetail(w http.ResponseWriter, r *http.Request, slug string) {
	event, ok, err := s.catalog.LoadEventBySlug(r.Context(), slug)
	if err != nil {
		s.logRequestError(r, "load event", err, "event_slug", slug)
		http.Error(w, "load event", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	venue, ok, err := s.catalog.LoadVenueBySlug(r.Context(), event.VenueSlug)
	if err != nil {
		s.logRequestError(r, "load event venue", err, "event_slug", slug, "venue_slug", event.VenueSlug)
		http.Error(w, "load event venue", http.StatusInternalServerError)
		return
	}
	if !ok {
		s.log().Error("event venue not found", "event_slug", slug, "venue_slug", event.VenueSlug)
		http.Error(w, "event venue not found", http.StatusInternalServerError)
		return
	}
	event = eventWithPublicLinks(event, venue)
	venueNames := map[string]string{venue.Slug: venue.Name}
	var secondarySources []store.EventSecondarySourceInfo
	if s.secondarySourceStore != nil {
		loaded, err := s.secondarySourceStore.EventSecondarySourceInfoByEventSlug(r.Context(), slug)
		if err != nil {
			s.logRequestError(r, "load event secondary source info", err, "event_slug", slug)
			http.Error(w, "event secondary source info not available", http.StatusInternalServerError)
			return
		}
		secondarySources = loaded
	}
	var eventGenres []genre.Match
	if s.eventGenreStore != nil {
		loaded, err := s.eventGenreStore.EventGenresByEventSlug(r.Context(), slug)
		if err != nil {
			s.logRequestError(r, "load event genres", err, "event_slug", slug)
			http.Error(w, "event genres not available", http.StatusInternalServerError)
			return
		}
		eventGenres = loaded
	}
	data := PageData{
		SiteName:              "Sheffield Live",
		PageTitle:             publicEventTitle(event, venueNames),
		MetaDescription:       event.Description,
		Active:                "events",
		Now:                   s.now(),
		Event:                 event,
		EventDetail:           newEventPresenter(venueNames, map[string]string{venue.Slug: venue.Neighbourhood}, s.localLocation).Detail(event, venue),
		VenueNames:            venueNames,
		EventSecondarySources: secondarySources,
		EventGenres:           eventGenres,
		Venue:                 venue,
	}
	s.renderPage(w, "templates/event_detail.html", data)
}

func eventWithPublicLinks(event domain.Event, venue domain.Venue) domain.Event {
	sourceURL := strings.TrimSpace(event.SourceURL)
	officialListingURL := strings.TrimSpace(event.OfficialListingURL)
	calendarURL := ""
	for _, candidate := range []string{event.CalendarURL, officialListingURL, sourceURL} {
		candidate = strings.TrimSpace(candidate)
		if ingest.IsCalendarURL(candidate) {
			calendarURL = candidate
			break
		}
	}

	if officialListingURL == "" || ingest.IsCalendarURL(officialListingURL) {
		switch venueWebsite := strings.TrimSpace(venue.Website); {
		case sourceURL != "" && !ingest.IsCalendarURL(sourceURL):
			officialListingURL = sourceURL
		case venueWebsite != "" && !ingest.IsCalendarURL(venueWebsite):
			officialListingURL = venueWebsite
		default:
			officialListingURL = ""
		}
	}

	event.OfficialListingURL = officialListingURL
	event.CalendarURL = calendarURL
	return event
}

func (s *Server) handleVenues(w http.ResponseWriter, r *http.Request) {
	venues, err := s.catalog.ListVenues(r.Context())
	if err != nil {
		s.logRequestError(r, "load venues", err)
		http.Error(w, "load venues", http.StatusInternalServerError)
		return
	}
	data := PageData{
		SiteName:        "Sheffield Live",
		PageTitle:       "Venues",
		MetaDescription: "Sheffield venues with upcoming live music listings.",
		Active:          "venues",
		Now:             s.now(),
		Venues:          venues,
	}
	s.renderPage(w, "templates/venues.html", data)
}

func (s *Server) handleVenueDetail(w http.ResponseWriter, r *http.Request, slug string) {
	venue, ok, err := s.catalog.LoadVenueBySlug(r.Context(), slug)
	if err != nil {
		s.logRequestError(r, "load venue", err, "venue_slug", slug)
		http.Error(w, "load venue", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	now := s.now()
	events, err := s.catalog.ListEventsForVenue(r.Context(), slug)
	if err != nil {
		s.logRequestError(r, "load venue events", err, "venue_slug", slug)
		http.Error(w, "load venue events", http.StatusInternalServerError)
		return
	}
	venueEvents := sortEventsForDisplay(upcomingEvents(events, now, s.localLocation))
	venueNames := map[string]string{venue.Slug: venue.Name}
	venueAreas := map[string]string{venue.Slug: venue.Neighbourhood}
	presenter := newEventPresenter(venueNames, venueAreas, s.localLocation)
	data := PageData{
		SiteName:        "Sheffield Live",
		PageTitle:       venue.Name,
		MetaDescription: venue.Description,
		Active:          "venues",
		Now:             now,
		Venue:           venue,
		VenueNames:      venueNames,
		VenueAreas:      venueAreas,
		VenueEvents:     venueEvents,
		VenueTimelineSections: buildVenueTimelineSections(
			venueEvents,
			now,
			s.localLocation,
			presenter,
		),
	}
	s.renderPage(w, "templates/venue_detail.html", data)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.readyChecker != nil {
		if err := s.readyChecker.Ready(r.Context()); err != nil {
			s.logRequestError(r, "readiness check failed", err)
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) renderPage(w http.ResponseWriter, pageKey string, data PageData) {
	page, ok := s.pages[pageKey]
	if !ok {
		s.log().Error("template not found", "page", pageKey)
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}

	var pageBuf bytes.Buffer
	if err := page.ExecuteTemplate(&pageBuf, filepath.Base(pageKey), data); err != nil {
		s.log().Error("render page", "page", pageKey, "error", err)
		http.Error(w, "render page", http.StatusInternalServerError)
		return
	}

	data.Content = template.HTML(pageBuf.String())

	var layoutBuf bytes.Buffer
	if err := s.layout.ExecuteTemplate(&layoutBuf, "layout.html", data); err != nil {
		s.log().Error("render layout", "page", pageKey, "error", err)
		http.Error(w, "render layout", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(layoutBuf.Bytes())
}

func (s *Server) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock().UTC()
}

func parseEventFilters(r *http.Request, venues []domain.Venue) EventFilters {
	validWindows := map[string]struct{}{
		"today":   {},
		"tonight": {},
		"week":    {},
		"weekend": {},
		"all":     {},
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if _, ok := validWindows[window]; !ok {
		window = "all"
	}

	validVenues := make(map[string]struct{}, len(venues))
	validAreas := make(map[string]struct{}, len(venues))
	for _, venue := range venues {
		validVenues[venue.Slug] = struct{}{}
		if area := strings.TrimSpace(venue.Neighbourhood); area != "" {
			validAreas[area] = struct{}{}
		}
	}

	venue := strings.TrimSpace(r.URL.Query().Get("venue"))
	if venue != "" {
		if _, ok := validVenues[venue]; !ok {
			venue = ""
		}
	}

	area := strings.TrimSpace(r.URL.Query().Get("area"))
	if area != "" {
		if _, ok := validAreas[area]; !ok {
			area = ""
		}
	}

	return EventFilters{Window: window, Venue: venue, Area: area}
}

func hasEventFilterQuery(r *http.Request) bool {
	query := r.URL.Query()
	_, hasWindow := query["window"]
	_, hasVenue := query["venue"]
	_, hasArea := query["area"]
	return hasWindow || hasVenue || hasArea
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

func filterEventsByRoom(events []domain.Event, roomSlug string) []domain.Event {
	roomSlug = strings.TrimSpace(roomSlug)
	if roomSlug == "" {
		return events
	}
	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		for _, room := range event.Rooms {
			if room.Slug == roomSlug {
				out = append(out, event)
				break
			}
		}
	}
	return out
}

func filterEventsByArea(events []domain.Event, venues []domain.Venue, area string) []domain.Event {
	area = strings.TrimSpace(area)
	if area == "" {
		return events
	}
	areasBySlug := make(map[string]string, len(venues))
	for _, venue := range venues {
		areasBySlug[venue.Slug] = strings.TrimSpace(venue.Neighbourhood)
	}
	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		if areasBySlug[event.VenueSlug] == area {
			out = append(out, event)
		}
	}
	return out
}

func parseAdminRoomPath(raw string) (string, string, bool) {
	raw = strings.Trim(strings.TrimSpace(raw), "/")
	venueSlug, roomSlug, ok := strings.Cut(raw, "/")
	if !ok {
		return "", "", false
	}
	venueSlug = strings.TrimSpace(venueSlug)
	roomSlug = strings.TrimSpace(roomSlug)
	if venueSlug == "" || roomSlug == "" || strings.Contains(roomSlug, "/") {
		return "", "", false
	}
	return venueSlug, roomSlug, true
}

func eventRoomText(event domain.Event) string {
	if len(event.Rooms) == 0 {
		return strings.TrimSpace(event.RoomText)
	}
	names := make([]string, 0, len(event.Rooms))
	for _, room := range event.Rooms {
		name := strings.TrimSpace(room.Name)
		if name == "" {
			name = strings.TrimSpace(room.Slug)
		}
		if name != "" {
			names = append(names, name)
		}
	}
	return strings.Join(names, " + ")
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
	windowStart := now.In(loc)
	end := today.AddDate(0, 0, 1)
	switch window {
	case "today":
	case "tonight":
	case "week":
		end = today.AddDate(0, 0, 7)
	case "weekend":
		windowStart, end = weekendWindow(now, loc)
	}

	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		if eventFallsInCurrentOrLocalRange(event, windowStart, end, now, loc) {
			out = append(out, event)
		}
	}
	return out
}

func upcomingEvents(events []domain.Event, now time.Time, loc *time.Location) []domain.Event {
	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		if eventIsCurrentOrUpcoming(event, now, loc) {
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

func buildEventBoardSections(events []domain.Event, now time.Time, loc *time.Location, presenter eventPresenter) []EventSection {
	today := localDayStart(now, loc)
	sections := []EventSection{}
	if todayEvents := eventsCurrentOrStartingInLocalRange(events, now.In(loc), today.AddDate(0, 0, 1), now, loc); len(todayEvents) > 0 {
		sections = append(sections, EventSection{
			ID:     "tonight",
			Title:  "Tonight",
			Date:   today,
			Events: presenter.Cards(todayEvents, eventCardLocationFull),
		})
	}

	for offset := 1; offset <= 8; offset++ {
		date := today.AddDate(0, 0, offset)
		title := date.Format("Monday")
		id := "day-" + date.Format("2006-01-02")
		if offset == 1 {
			title = "Tomorrow"
			id = "tomorrow"
		}
		dayEvents := eventsInLocalRange(events, date, date.AddDate(0, 0, 1), loc)
		if len(dayEvents) == 0 {
			continue
		}
		sections = append(sections, EventSection{
			ID:     id,
			Title:  title,
			Date:   date,
			Events: presenter.Cards(dayEvents, eventCardLocationFull),
		})
	}
	return sections
}

func buildVenueTimelineSections(events []domain.Event, now time.Time, loc *time.Location, presenter eventPresenter) []VenueTimelineSection {
	today := localDayStart(now, loc)
	windowEnd := today.AddDate(0, 0, 8)
	eventsByDate := make(map[time.Time][]domain.Event)
	for _, event := range events {
		date := localDayStart(event.Start, loc)
		if date.Before(today) && eventIsCurrentOrUpcoming(event, now, loc) {
			date = today
		}
		eventsByDate[date] = append(eventsByDate[date], event)
	}

	sections := []VenueTimelineSection{}
	for offset := 0; offset <= 8; offset++ {
		date := today.AddDate(0, 0, offset)
		dayEvents := eventsByDate[date]
		if len(dayEvents) == 0 {
			continue
		}
		sections = append(sections, VenueTimelineSection{
			Dates:  []time.Time{date},
			Events: presenter.Cards(dayEvents, eventCardLocationVenuePage),
		})
		delete(eventsByDate, date)
	}

	laterDates := make([]time.Time, 0, len(eventsByDate))
	for date := range eventsByDate {
		if date.After(windowEnd) {
			laterDates = append(laterDates, date)
		}
	}
	sort.Slice(laterDates, func(i, j int) bool {
		return laterDates[i].Before(laterDates[j])
	})
	for _, date := range laterDates {
		sections = append(sections, VenueTimelineSection{
			Dates:  []time.Time{date},
			Events: presenter.Cards(eventsByDate[date], eventCardLocationVenuePage),
		})
	}

	return sections
}

func eventsInLocalRange(events []domain.Event, start, end time.Time, loc *time.Location) []domain.Event {
	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		if eventFallsInLocalRange(event, start, end, loc) {
			out = append(out, event)
		}
	}
	return out
}

func eventsCurrentOrStartingInLocalRange(events []domain.Event, start, end, now time.Time, loc *time.Location) []domain.Event {
	out := make([]domain.Event, 0, len(events))
	for _, event := range events {
		if eventFallsInCurrentOrLocalRange(event, start, end, now, loc) {
			out = append(out, event)
		}
	}
	return out
}

func eventFallsInCurrentOrLocalRange(event domain.Event, start, end, now time.Time, loc *time.Location) bool {
	if eventFallsInLocalRange(event, start, end, loc) {
		return true
	}
	nowLocal := now.In(loc)
	if nowLocal.Before(start) || !nowLocal.Before(end) {
		return false
	}
	return eventIsOngoingAt(event, now, loc)
}

func eventFallsInLocalRange(event domain.Event, start, end time.Time, loc *time.Location) bool {
	eventStart := event.Start.In(loc)
	if !eventStart.Before(start) && eventStart.Before(end) {
		return true
	}
	if eventStart.Before(start) && sameLocalDate(eventStart, start, loc) {
		eventEnd, hasEnd := eventDisplayEnd(event, loc)
		return hasEnd && eventEnd.After(start)
	}
	return false
}

func eventIsOngoingAt(event domain.Event, at time.Time, loc *time.Location) bool {
	eventStart := event.Start.In(loc)
	atLocal := at.In(loc)
	if !eventStart.Before(atLocal) {
		return false
	}
	eventEnd, hasEnd := eventDisplayEnd(event, loc)
	return hasEnd && eventEnd.After(atLocal)
}

func eventIsCurrentOrUpcoming(event domain.Event, now time.Time, loc *time.Location) bool {
	eventStart := event.Start.In(loc)
	if !eventStart.Before(now.In(loc)) {
		return true
	}
	eventEnd, hasEnd := eventDisplayEnd(event, loc)
	return hasEnd && eventEnd.After(now.In(loc))
}

func eventDisplayEnd(event domain.Event, loc *time.Location) (time.Time, bool) {
	eventStart := event.Start.In(loc)
	if event.End.IsZero() {
		return localDayStart(eventStart, loc).AddDate(0, 0, 1), true
	}
	eventEnd := event.End.In(loc)
	if eventEnd.Before(eventStart) {
		return eventStart, false
	}
	return eventEnd, true
}

func groupEventsByDisplayDate(events []domain.Event, now time.Time, loc *time.Location, presenter eventPresenter) []EventGroup {
	var groups []EventGroup
	for _, event := range events {
		date := eventDisplayDate(event, now, loc)
		if len(groups) == 0 || !sameLocalDate(groups[len(groups)-1].Date, date, loc) {
			groups = append(groups, EventGroup{Date: date})
		}
		groups[len(groups)-1].Events = append(groups[len(groups)-1].Events, presenter.Card(event, eventCardLocationFull))
	}
	return groups
}

func eventDisplayDate(event domain.Event, now time.Time, loc *time.Location) time.Time {
	date := localDayStart(event.Start, loc)
	today := localDayStart(now, loc)
	if date.Before(today) && eventIsOngoingAt(event, now, loc) {
		return today
	}
	return date
}

func dateSectionTitle(date, now time.Time, loc *time.Location) string {
	day := localDayStart(date, loc)
	today := localDayStart(now, loc)
	switch {
	case sameLocalDate(day, today, loc):
		return "Tonight"
	case sameLocalDate(day, today.AddDate(0, 0, 1), loc):
		return "Tomorrow"
	default:
		return day.Format("Monday")
	}
}

func venueTimelineTitle(section VenueTimelineSection, now time.Time, loc *time.Location) string {
	titles := make([]string, 0, len(section.Dates))
	for _, date := range section.Dates {
		titles = append(titles, venueTimelineDayTitle(date, now, loc))
	}
	return strings.Join(titles, " · ")
}

func venueTimelineDayTitle(date, now time.Time, loc *time.Location) string {
	day := localDayStart(date, loc)
	today := localDayStart(now, loc)
	switch {
	case sameLocalDate(day, today, loc):
		return "Today"
	case sameLocalDate(day, today.AddDate(0, 0, 1), loc):
		return "Tomorrow"
	default:
		return day.Format("Monday")
	}
}

func venueTimelineMeta(section VenueTimelineSection, loc *time.Location) string {
	return fmt.Sprintf("%s - %s", venueTimelineDateLabel(section.Dates, loc), showCount(len(section.Events)))
}

func venueTimelineToneClass(index int) string {
	return fmt.Sprintf("venue-day-tone-%d", index%4)
}

func venueTimelineDateLabel(dates []time.Time, loc *time.Location) string {
	if len(dates) == 0 {
		return ""
	}
	start := dates[0].In(loc)
	end := dates[len(dates)-1].In(loc)
	if sameLocalDate(start, end, loc) {
		return start.Format("2 Jan")
	}
	if start.Year() == end.Year() && start.Month() == end.Month() {
		return fmt.Sprintf("%d-%d %s", start.Day(), end.Day(), start.Format("Jan"))
	}
	return fmt.Sprintf("%s-%s", start.Format("2 Jan"), end.Format("2 Jan"))
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

func venueNameMap(venues []domain.Venue) map[string]string {
	names := make(map[string]string, len(venues))
	for _, venue := range venues {
		names[venue.Slug] = venue.Name
	}
	return names
}

func venueAreaMap(venues []domain.Venue) map[string]string {
	areas := make(map[string]string, len(venues))
	for _, venue := range venues {
		areas[venue.Slug] = strings.TrimSpace(venue.Neighbourhood)
	}
	return areas
}

func venueAreasList(venues []domain.Venue) []string {
	seen := make(map[string]struct{}, len(venues))
	areas := make([]string, 0, len(venues))
	for _, venue := range venues {
		area := strings.TrimSpace(venue.Neighbourhood)
		if area == "" {
			continue
		}
		if _, ok := seen[area]; ok {
			continue
		}
		seen[area] = struct{}{}
		areas = append(areas, area)
	}
	sort.Strings(areas)
	return areas
}

func eventStatusLabel(event domain.Event) string {
	status := publicEventStatus(event.Status)
	if event.PublicationState == domain.PublicationStateProvisional {
		if status != "" {
			return status + " · Unconfirmed"
		}
		return "Unconfirmed"
	}
	return status
}

func showCount(count int) string {
	if count == 1 {
		return "1 show"
	}
	return fmt.Sprintf("%d shows", count)
}

func weekendWindow(now time.Time, loc *time.Location) (time.Time, time.Time) {
	today := localDayStart(now, loc)
	switch today.Weekday() {
	case time.Friday:
		return today, today.AddDate(0, 0, 3)
	case time.Saturday:
		return today, today.AddDate(0, 0, 2)
	case time.Sunday:
		return today, today.AddDate(0, 0, 1)
	default:
		offset := (int(time.Friday) - int(today.Weekday()) + 7) % 7
		start := today.AddDate(0, 0, offset)
		return start, start.AddDate(0, 0, 3)
	}
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

func buildImportRunRows(ctx context.Context, runs []ingest.ImportRunSummary, groupStore ImportRunReviewGroupStore) ([]ImportRunRow, error) {
	rows := make([]ImportRunRow, 0, len(runs))
	for _, run := range runs {
		row := ImportRunRow{ImportRunSummary: run}
		if groupStore != nil {
			groups, err := groupStore.ListReviewGroupsForImportRun(ctx, run.ID)
			if err != nil {
				return nil, fmt.Errorf("list review groups for import run %d: %w", run.ID, err)
			}
			row.ReviewGroupStatusSummary = reviewGroupStatusSummary(groups)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func reviewGroupStatusSummary(groups []review.GroupSummary) string {
	if len(groups) == 0 {
		return "none"
	}

	counts := make(map[string]int)
	for _, group := range groups {
		status := strings.TrimSpace(group.Status)
		if status == "" {
			status = "unknown"
		}
		counts[status]++
	}

	statuses := make([]string, 0, len(counts))
	for status := range counts {
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool {
		left := reviewStatusSortRank(statuses[i])
		right := reviewStatusSortRank(statuses[j])
		if left == right {
			return statuses[i] < statuses[j]
		}
		return left < right
	})

	parts := make([]string, 0, len(statuses))
	for _, status := range statuses {
		parts = append(parts, fmt.Sprintf("%d %s", counts[status], status))
	}
	return strings.Join(parts, ", ")
}

func buildProvisionalVenueRows(venues []domain.Venue, events []domain.Event, now time.Time, loc *time.Location) []ProvisionalVenueRow {
	provisional := make([]domain.Venue, 0, len(venues))
	for _, venue := range venues {
		if venue.ValidationState == domain.ValidationStateProvisional {
			provisional = append(provisional, venue)
		}
	}

	upcoming := sortEventsForDisplay(upcomingEvents(events, now, loc))
	eventsByVenue := make(map[string][]domain.Event, len(provisional))
	for _, event := range upcoming {
		eventsByVenue[event.VenueSlug] = append(eventsByVenue[event.VenueSlug], event)
	}

	rows := make([]ProvisionalVenueRow, 0, len(provisional))
	for _, venue := range provisional {
		linkedEvents := eventsByVenue[venue.Slug]
		row := ProvisionalVenueRow{
			Venue:              venue,
			UpcomingEventCount: len(linkedEvents),
		}
		if len(linkedEvents) > 0 {
			next := linkedEvents[0]
			row.NextEvent = &next
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		iHasNext := rows[i].NextEvent != nil
		jHasNext := rows[j].NextEvent != nil
		switch {
		case iHasNext && jHasNext:
			if rows[i].NextEvent.Start.Equal(rows[j].NextEvent.Start) {
				if rows[i].Venue.Name == rows[j].Venue.Name {
					return rows[i].Venue.Slug < rows[j].Venue.Slug
				}
				return rows[i].Venue.Name < rows[j].Venue.Name
			}
			return rows[i].NextEvent.Start.Before(rows[j].NextEvent.Start)
		case iHasNext:
			return true
		case jHasNext:
			return false
		default:
			if rows[i].Venue.Name == rows[j].Venue.Name {
				return rows[i].Venue.Slug < rows[j].Venue.Slug
			}
			return rows[i].Venue.Name < rows[j].Venue.Name
		}
	})
	return rows
}

func buildProvisionalRoomRows(venues []domain.Venue, rooms []domain.VenueRoom, events []domain.Event, now time.Time, loc *time.Location) []ProvisionalRoomRow {
	venuesBySlug := make(map[string]domain.Venue, len(venues))
	for _, venue := range venues {
		venuesBySlug[venue.Slug] = venue
	}

	upcoming := sortEventsForDisplay(upcomingEvents(events, now, loc))
	eventsByRoom := make(map[string][]domain.Event)
	for _, event := range upcoming {
		for _, room := range event.Rooms {
			key := event.VenueSlug + "\x00" + room.Slug
			eventsByRoom[key] = append(eventsByRoom[key], event)
		}
	}

	rows := make([]ProvisionalRoomRow, 0, len(rooms))
	for _, room := range rooms {
		if room.ValidationState != domain.ValidationStateProvisional {
			continue
		}
		key := room.VenueSlug + "\x00" + room.Slug
		linkedEvents := eventsByRoom[key]
		row := ProvisionalRoomRow{
			Room:               room,
			Venue:              venuesBySlug[room.VenueSlug],
			UpcomingEventCount: len(linkedEvents),
		}
		if len(linkedEvents) > 0 {
			next := linkedEvents[0]
			row.NextEvent = &next
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		iHasNext := rows[i].NextEvent != nil
		jHasNext := rows[j].NextEvent != nil
		switch {
		case iHasNext && jHasNext:
			if rows[i].NextEvent.Start.Equal(rows[j].NextEvent.Start) {
				if rows[i].Venue.Name == rows[j].Venue.Name {
					return rows[i].Room.Name < rows[j].Room.Name
				}
				return rows[i].Venue.Name < rows[j].Venue.Name
			}
			return rows[i].NextEvent.Start.Before(rows[j].NextEvent.Start)
		case iHasNext:
			return true
		case jHasNext:
			return false
		default:
			if rows[i].Venue.Name == rows[j].Venue.Name {
				if rows[i].Room.Name == rows[j].Room.Name {
					return rows[i].Room.Slug < rows[j].Room.Slug
				}
				return rows[i].Room.Name < rows[j].Room.Name
			}
			return rows[i].Venue.Name < rows[j].Venue.Name
		}
	})
	return rows
}

func reviewStatusSortRank(status string) int {
	switch status {
	case review.StatusOpen:
		return 0
	case review.StatusResolved:
		return 1
	case review.StatusRejected:
		return 2
	default:
		return 3
	}
}

func buildReviewHistoryRows(groups []review.GroupSummary) []ReviewHistoryRow {
	rows := make([]ReviewHistoryRow, 0, len(groups))
	for _, group := range groups {
		rows = append(rows, ReviewHistoryRow{GroupSummary: group})
	}
	return rows
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
		selectedChoice, hasSelectedChoice := reviewChoiceForField(group, field)
		defaultChoice, hasDefaultChoice := group.DefaultChoices[field]
		draftChoice, hasDraftChoice := group.DraftChoices[field]
		defaulted := hasSelectedChoice && !hasDraftChoice && hasDefaultChoice && selectedChoice.CandidateID == defaultChoice.CandidateID
		if detail.IsDuplicate {
			candidate := ""
			if hasSelectedChoice {
				candidate = reviewCandidateLabel(group.Candidates, selectedChoice.CandidateID)
			}
			detail.CanonicalSummaryRows = append(detail.CanonicalSummaryRows, ReviewCanonicalSummaryRow{
				Label:     field.Label(),
				Value:     selectedChoice.Value,
				Candidate: candidate,
				Selected:  hasSelectedChoice,
				Defaulted: defaulted,
			})
		}
		for _, candidate := range group.Candidates {
			value := review.CandidateValue(candidate, field)
			checked := hasSelectedChoice && selectedChoice.CandidateID == candidate.ID
			consensus := hasDefaultChoice && value == defaultChoice.Value
			row.Cells = append(row.Cells, ReviewChoiceCell{
				CandidateID:       candidate.ID,
				Value:             value,
				Checked:           checked,
				Consensus:         consensus,
				SelectedConsensus: checked && consensus,
				Provenance:        candidate.Provenance,
			})
		}
		if detail.IsDuplicate {
			detail.Rows = append(detail.Rows, row)
		}
		if detail.IsSingleton && len(group.Candidates) > 0 {
			detail.SingleCandidateRows = append(detail.SingleCandidateRows, ReviewSingleCandidateRow{
				Label: field.Label(),
				Value: review.CandidateValue(group.Candidates[0], field),
			})
		}
		if hasDraftChoice {
			detail.Preview = append(detail.Preview, ReviewPreviewRow{
				Label:     field.Label(),
				Value:     draftChoice.Value,
				Candidate: reviewCandidateLabel(group.Candidates, draftChoice.CandidateID),
			})
		}
	}
	return detail
}

func reviewChoiceForField(group review.Group, field review.Field) (review.DraftChoice, bool) {
	if choice, ok := group.DraftChoices[field]; ok {
		return choice, true
	}
	if choice, ok := group.DefaultChoices[field]; ok {
		return choice, true
	}
	return review.DraftChoice{}, false
}

func reviewCandidateLabel(candidates []review.Candidate, id int64) string {
	for _, candidate := range candidates {
		if candidate.ID == id {
			if candidate.IsCanonicalSnapshot() {
				return "Live canonical snapshot"
			}
			if candidate.ExternalID != "" {
				return fmt.Sprintf("Candidate %d (%s)", candidate.Position, candidate.ExternalID)
			}
			return fmt.Sprintf("Candidate %d", candidate.Position)
		}
	}
	return "Unknown candidate"
}

func reviewGroupHasCanonicalSnapshot(group review.Group) bool {
	for _, candidate := range group.Candidates {
		if candidate.IsCanonicalSnapshot() {
			return true
		}
	}
	return false
}
