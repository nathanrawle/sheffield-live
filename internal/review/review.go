package review

import (
	"strconv"
	"strings"
	"time"
)

type Field string

const (
	FieldName        Field = "name"
	FieldVenueSlug   Field = "venue_slug"
	FieldStartAt     Field = "start_at"
	FieldEndAt       Field = "end_at"
	FieldGenre       Field = "genre"
	FieldStatus      Field = "status"
	FieldDescription Field = "description"
	FieldSourceName  Field = "source_name"
	FieldSourceURL   Field = "source_url"
)

const (
	StatusOpen     = "open"
	StatusResolved = "resolved"
	StatusRejected = "rejected"
)

var CanonicalFields = []Field{
	FieldName,
	FieldVenueSlug,
	FieldStartAt,
	FieldEndAt,
	FieldGenre,
	FieldStatus,
	FieldDescription,
	FieldSourceName,
	FieldSourceURL,
}

type GroupSummary struct {
	ID             int64
	Title          string
	SourceName     string
	SourceURL      string
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CandidateCount int
	DraftCount     int
}

type Group struct {
	ID           int64
	Title        string
	SourceName   string
	SourceURL    string
	Status       string
	Notes        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Candidates   []Candidate
	DraftChoices map[Field]DraftChoice
}

type Candidate struct {
	ID          int64
	GroupID     int64
	Position    int
	ExternalID  string
	Name        string
	VenueSlug   string
	StartAt     string
	EndAt       string
	Genre       string
	Status      string
	Description string
	SourceName  string
	SourceURL   string
	Provenance  string
}

type GroupInput struct {
	Title      string
	SourceName string
	SourceURL  string
	Notes      string
	StagingKey string
	Candidates []CandidateInput
}

type CandidateInput struct {
	ExternalID  string
	Name        string
	VenueSlug   string
	StartAt     string
	EndAt       string
	Genre       string
	Status      string
	Description string
	SourceName  string
	SourceURL   string
	Provenance  string
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
	case FieldSourceName:
		return candidate.SourceName
	case FieldSourceURL:
		return candidate.SourceURL
	default:
		return ""
	}
}
