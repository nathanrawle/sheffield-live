package store

import "sheffield-live/internal/domain"

type VenueUpdateInput struct {
	Slug          string
	Name          string
	Address       string
	Neighbourhood string
	Description   string
	Website       string
	CoverageKind  domain.CoverageKind
	CoverageNote  string
}
