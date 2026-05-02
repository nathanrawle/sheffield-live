package sqlite

import (
	"context"
	"strings"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
)

type venueMatchStatus uint8

const (
	venueMatchNoMatch venueMatchStatus = iota
	venueMatchResolved
	venueMatchAmbiguous
)

type venueMatchResult struct {
	status venueMatchStatus
	slug   string
	name   string
}

type venueMatcher struct {
	bySlug    map[string][]domain.Venue
	byName    map[string][]domain.Venue
	byAddress map[string][]domain.Venue
}

func loadVenueMatcher(ctx context.Context, q queryer) (venueMatcher, error) {
	venues, err := loadVenues(ctx, q)
	if err != nil {
		return venueMatcher{}, err
	}

	matcher := venueMatcher{
		bySlug:    make(map[string][]domain.Venue, len(venues)),
		byName:    make(map[string][]domain.Venue, len(venues)),
		byAddress: make(map[string][]domain.Venue, len(venues)),
	}
	for _, venue := range venues {
		matcher.add(matcher.bySlug, venue.Slug, venue)
		matcher.add(matcher.byName, normalizedVenueKey(venue.Name), venue)
		matcher.add(matcher.byAddress, normalizedVenueKey(venue.Address), venue)
	}
	return matcher, nil
}

func (m venueMatcher) add(index map[string][]domain.Venue, key string, venue domain.Venue) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	index[key] = append(index[key], venue)
}

func (m venueMatcher) matchCandidate(candidate review.Candidate) venueMatchResult {
	var selected *domain.Venue
	ambiguous := false

	add := func(matches []domain.Venue) {
		if ambiguous || len(matches) == 0 {
			return
		}
		if len(matches) > 1 {
			ambiguous = true
			return
		}
		venue := matches[0]
		if selected == nil {
			selected = &venue
			return
		}
		if selected.Slug != venue.Slug {
			ambiguous = true
		}
	}

	add(m.bySlug[strings.TrimSpace(candidate.VenueSlug)])
	add(m.bySlug[normalizedVenueKey(candidate.VenueText)])
	for _, probe := range venueLocationSlugProbes(candidate.VenueLocationRaw) {
		add(m.bySlug[probe])
	}
	add(m.byName[normalizedVenueKey(candidate.VenueText)])
	add(m.byAddress[normalizedVenueKey(candidate.VenueLocationRaw)])

	switch {
	case ambiguous:
		return venueMatchResult{status: venueMatchAmbiguous}
	case selected == nil:
		return venueMatchResult{status: venueMatchNoMatch}
	default:
		return venueMatchResult{status: venueMatchResolved, slug: selected.Slug, name: selected.Name}
	}
}

func (m venueMatcher) matchSharedVenue(candidates []review.Candidate) venueMatchResult {
	var selected venueMatchResult
	haveSelected := false
	for _, candidate := range candidates {
		if candidate.CanonicalEventID != 0 {
			continue
		}
		match := m.matchCandidate(candidate)
		switch match.status {
		case venueMatchNoMatch:
			return venueMatchResult{status: venueMatchNoMatch}
		case venueMatchAmbiguous:
			return venueMatchResult{status: venueMatchAmbiguous}
		case venueMatchResolved:
			if !haveSelected {
				selected = match
				haveSelected = true
				continue
			}
			if selected.slug != match.slug {
				return venueMatchResult{status: venueMatchAmbiguous}
			}
		}
	}
	if !haveSelected {
		return venueMatchResult{status: venueMatchNoMatch}
	}
	return selected
}

func loadReviewGroupSharedVenue(ctx context.Context, q queryer, matcher venueMatcher, groupID int64) (venueMatchResult, error) {
	candidates, err := loadReviewCandidates(ctx, q, groupID)
	if err != nil {
		return venueMatchResult{}, err
	}
	return matcher.matchSharedVenue(candidates), nil
}

func normalizedVenueKey(value string) string {
	return strings.TrimSpace(ingest.VenueSlugFromText(value))
}

func venueLocationSlugProbes(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	probes := make([]string, 0, 2)
	if head, _, ok := strings.Cut(value, ","); ok {
		if probe := normalizedVenueKey(head); probe != "" {
			probes = append(probes, probe)
		}
	}
	if probe := normalizedVenueKey(value); probe != "" {
		if len(probes) == 0 || probes[0] != probe {
			probes = append(probes, probe)
		}
	}
	return probes
}
