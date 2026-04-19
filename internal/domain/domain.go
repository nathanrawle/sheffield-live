package domain

import "time"

type Origin string

const (
	OriginSeed Origin = "seed"
	OriginTest Origin = "test"
	OriginDev  Origin = "dev"
	OriginLive Origin = "live"
)

type Venue struct {
	Slug          string
	Name          string
	Address       string
	Neighbourhood string
	Description   string
	Website       string
	Origin        Origin
}

type Event struct {
	Slug        string
	Name        string
	VenueSlug   string
	Start       time.Time
	End         time.Time
	Genre       string
	Status      string
	Description string
	SourceName  string
	SourceURL   string
	LastChecked time.Time
	Origin      Origin
}
