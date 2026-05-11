package domain

import (
	"errors"
	"time"
)

type Origin string
type CoverageKind string
type PublicationState string
type ValidationState string

const (
	OriginSeed Origin = "seed"
	OriginTest Origin = "test"
	OriginDev  Origin = "dev"
	OriginLive Origin = "live"

	PublicationStateReviewed    PublicationState = "reviewed"
	PublicationStateProvisional PublicationState = "provisional"

	ValidationStateValidated   ValidationState = "validated"
	ValidationStateProvisional ValidationState = "provisional"

	CoverageKindVenue   CoverageKind = "venue"
	CoverageKindProgram CoverageKind = "program"
)

type Venue struct {
	Slug            string
	Name            string
	Address         string
	Neighbourhood   string
	Description     string
	Website         string
	ValidationState ValidationState
	CoverageKind    CoverageKind
	CoverageNote    string
	Origin          Origin
}

type Event struct {
	Slug               string
	Name               string
	VenueSlug          string
	Start              time.Time
	End                time.Time
	Genre              string
	Status             string
	Description        string
	ImageURL           string
	ImageSourceURL     string
	ImageAlt           string
	ImageWidth         int
	ImageHeight        int
	ImageFocusX        int
	ImageFocusY        int
	SourceName         string
	SourceURL          string
	OfficialListingURL string
	CalendarURL        string
	LastChecked        time.Time
	Origin             Origin
	PublicationState   PublicationState
}

func (e Event) HasEnd() bool {
	return !e.End.IsZero()
}

func (e Event) ValidateCanonical() error {
	if e.Start.IsZero() {
		return errors.New("start time is required")
	}
	if e.HasEnd() && !e.End.After(e.Start) {
		return errors.New("end time must be later than start time")
	}
	return nil
}
