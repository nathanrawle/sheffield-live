package review

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"sheffield-live/internal/domain"
)

type Field string

const (
	FieldName        Field = "name"
	FieldVenueSlug   Field = "venue_slug"
	FieldRoomSlugs   Field = "room_slugs"
	FieldStartAt     Field = "start_at"
	FieldEndAt       Field = "end_at"
	FieldGenre       Field = "genre"
	FieldStatus      Field = "status"
	FieldDescription Field = "description"
	FieldImageURL    Field = "image_url"
	FieldSourceName  Field = "source_name"
	FieldSourceURL   Field = "source_url"
)

type GroupKind string

const (
	GroupKindStandard            GroupKind = "standard"
	GroupKindHistoricalDuplicate GroupKind = "historical_duplicate"
)

func ParseGroupKind(value string) (GroupKind, bool) {
	kind := GroupKind(strings.TrimSpace(value))
	if kind == "" {
		kind = GroupKindStandard
	}
	return kind, kind.Valid()
}

func (k GroupKind) Valid() bool {
	switch k {
	case GroupKindStandard, GroupKindHistoricalDuplicate:
		return true
	default:
		return false
	}
}

const (
	StatusOpen     = "open"
	StatusResolved = "resolved"
	StatusRejected = "rejected"
)

var CanonicalFields = []Field{
	FieldName,
	FieldVenueSlug,
	FieldRoomSlugs,
	FieldStartAt,
	FieldEndAt,
	FieldGenre,
	FieldStatus,
	FieldDescription,
	FieldImageURL,
	FieldSourceName,
	FieldSourceURL,
}

type GroupSummary struct {
	ID                int64
	Kind              GroupKind
	Title             string
	SourceName        string
	SourceURL         string
	Status            string
	Notes             string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	CandidateCount    int
	DraftCount        int
	LatestImportRunID int64
	Authoritative     bool
	SharedStartAt     *time.Time
	SharedVenueSlug   string
	SharedVenueName   string
}

type Group struct {
	ID                          int64
	Kind                        GroupKind
	Title                       string
	SourceName                  string
	SourceURL                   string
	AuthoritativeSourceName     string
	AuthoritativeSourceURL      string
	AuthoritativeSourceEventKey string
	Status                      string
	Notes                       string
	CreatedAt                   time.Time
	UpdatedAt                   time.Time
	LatestImportRunID           int64
	SharedStartAt               *time.Time
	SharedVenueSlug             string
	SharedVenueName             string
	Candidates                  []Candidate
	HistoricalDuplicateActions  map[int64]HistoricalDuplicateActionChoice
	StagedCandidateCount        int
	DraftChoices                map[Field]DraftChoice
	DefaultChoices              map[Field]DraftChoice
}

type Candidate struct {
	ID                                int64
	GroupID                           int64
	Position                          int
	CanonicalEventID                  int64
	ExistingEventID                   int64
	ExternalID                        string
	ExternalIDSourceIdentityDisabled  bool
	Name                              string
	VenueSlug                         string
	VenueText                         string `json:"-"`
	VenueLocationRaw                  string `json:"-"`
	RoomText                          string `json:"-"`
	Rooms                             []domain.VenueRoom
	StartAt                           string
	EndAt                             string
	Genre                             string
	Status                            string
	Description                       string
	ImageURL                          string
	ImageSourceURL                    string
	ImageAlt                          string
	ImageWidth                        int
	ImageHeight                       int
	ImageFocusX                       int
	ImageFocusY                       int
	SourceName                        string
	SourceURL                         string
	SourceURLSourceIdentityDisabled   bool
	CalendarURL                       string
	CalendarURLSourceIdentityDisabled bool
	Provenance                        string
}

type CandidateInput struct {
	CanonicalEventID                  int64
	ExistingEventID                   int64
	ExternalID                        string
	ExternalIDSourceIdentityDisabled  bool
	Name                              string
	VenueSlug                         string
	VenueText                         string `json:"-"`
	VenueLocationRaw                  string `json:"-"`
	RoomText                          string `json:"-"`
	Rooms                             []domain.VenueRoom
	StartAt                           string
	EndAt                             string
	Genre                             string
	Status                            string
	Description                       string
	ImageURL                          string
	ImageSourceURL                    string
	ImageAlt                          string
	ImageWidth                        int
	ImageHeight                       int
	ImageFocusX                       int
	ImageFocusY                       int
	SourceName                        string
	SourceURL                         string
	SourceURLSourceIdentityDisabled   bool
	CalendarURL                       string
	CalendarURLSourceIdentityDisabled bool
	Provenance                        string
}

type DraftChoice struct {
	Field       Field
	CandidateID int64
	Value       string
	UpdatedAt   time.Time
}

type DraftChoiceInput struct {
	Field       Field
	CandidateID int64
}

type HistoricalDuplicateDraftInput struct {
	FieldChoices []DraftChoiceInput
	Actions      map[int64]HistoricalDuplicateActionInput
}

type HistoricalDuplicateResolutionInput struct {
	FieldChoices []DraftChoiceInput
	Actions      map[int64]HistoricalDuplicateActionInput
}

type HistoricalDuplicateAction string

const (
	HistoricalDuplicateActionKeep     HistoricalDuplicateAction = "keep"
	HistoricalDuplicateActionWithhold HistoricalDuplicateAction = "withhold"
)

func (a HistoricalDuplicateAction) Valid() bool {
	switch a {
	case HistoricalDuplicateActionKeep, HistoricalDuplicateActionWithhold:
		return true
	default:
		return false
	}
}

type HistoricalDuplicateActionChoice struct {
	IsCanonical bool
	Action      HistoricalDuplicateAction
	UpdatedAt   time.Time
}

type HistoricalDuplicateActionInput struct {
	IsCanonical bool
	Action      HistoricalDuplicateAction
}

type StageGroupResult struct {
	ID                 int64
	Created            bool
	AutoResolved       bool
	AutoResolvedResult string
	CanonicalEventSlug string
}

func ParseField(value string) (Field, bool) {
	field := Field(strings.TrimSpace(value))
	return field, field.Valid()
}

func (f Field) Valid() bool {
	for _, field := range CanonicalFields {
		if f == field {
			return true
		}
	}
	return false
}

func StatusValid(value string) bool {
	switch value {
	case StatusOpen, StatusResolved, StatusRejected:
		return true
	default:
		return false
	}
}

func ParseOriginImportRunID(notes string) (int64, bool) {
	for _, phrase := range []string{"manual ingest run ", "import run "} {
		if id, ok := parsePositiveIDAfterPhrase(notes, phrase); ok {
			return id, true
		}
	}
	return 0, false
}

func parsePositiveIDAfterPhrase(text, phrase string) (int64, bool) {
	searchFrom := 0
	for {
		idx := strings.Index(text[searchFrom:], phrase)
		if idx < 0 {
			return 0, false
		}
		start := searchFrom + idx + len(phrase)
		end := start
		for end < len(text) && text[end] >= '0' && text[end] <= '9' {
			end++
		}
		if end > start && (end == len(text) || !asciiLetterOrDigit(text[end])) {
			id, err := strconv.ParseInt(text[start:end], 10, 64)
			if err == nil && id > 0 {
				return id, true
			}
		}
		searchFrom = start
	}
}

func asciiLetterOrDigit(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func (f Field) Label() string {
	switch f {
	case FieldName:
		return "Name"
	case FieldVenueSlug:
		return "Venue slug"
	case FieldRoomSlugs:
		return "Rooms"
	case FieldStartAt:
		return "Start"
	case FieldEndAt:
		return "End"
	case FieldGenre:
		return "Genre"
	case FieldStatus:
		return "Status"
	case FieldDescription:
		return "Description"
	case FieldImageURL:
		return "Image"
	case FieldSourceName:
		return "Source name"
	case FieldSourceURL:
		return "Source URL"
	default:
		return string(f)
	}
}

func CandidateValue(candidate Candidate, field Field) string {
	switch field {
	case FieldName:
		return candidate.Name
	case FieldVenueSlug:
		return candidate.VenueSlug
	case FieldRoomSlugs:
		return CandidateRoomValue(candidate)
	case FieldStartAt:
		return candidate.StartAt
	case FieldEndAt:
		return candidate.EndAt
	case FieldGenre:
		return candidate.Genre
	case FieldStatus:
		return candidate.Status
	case FieldDescription:
		return candidate.Description
	case FieldImageURL:
		return candidate.ImageURL
	case FieldSourceName:
		return candidate.SourceName
	case FieldSourceURL:
		return candidate.SourceURL
	default:
		return ""
	}
}

func CandidateRoomValue(candidate Candidate) string {
	if value := RoomSlugsValue(candidate.Rooms); value != "" {
		return value
	}
	return strings.Join(strings.Fields(strings.TrimSpace(candidate.RoomText)), " ")
}

func RoomSlugsValue(rooms []domain.VenueRoom) string {
	values := make([]string, 0, len(rooms))
	seen := make(map[string]struct{}, len(rooms))
	for _, room := range rooms {
		slug := strings.TrimSpace(room.Slug)
		if slug == "" {
			continue
		}
		if _, ok := seen[slug]; ok {
			continue
		}
		seen[slug] = struct{}{}
		values = append(values, slug)
	}
	sort.Strings(values)
	return strings.Join(values, ", ")
}

func (c Candidate) IsCanonicalSnapshot() bool {
	return c.CanonicalEventID > 0
}
