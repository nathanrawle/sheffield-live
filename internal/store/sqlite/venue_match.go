package sqlite

import (
	"context"
	"fmt"
	"strings"
	"unicode"

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
	if match := resolvedVenueSlugMatch(m.bySlug[strings.TrimSpace(candidate.VenueSlug)]); match.status == venueMatchResolved {
		return match
	}

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

func resolvedVenueSlugMatch(matches []domain.Venue) venueMatchResult {
	if len(matches) != 1 {
		return venueMatchResult{status: venueMatchNoMatch}
	}
	return venueMatchResult{
		status: venueMatchResolved,
		slug:   matches[0].Slug,
		name:   matches[0].Name,
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

func ensureProvisionalVenueForCandidateTx(ctx context.Context, tx interface {
	execer
	queryer
}, matcher *venueMatcher, candidate review.Candidate) (venueMatchResult, error) {
	match := matcher.matchCandidate(candidate)
	if match.status != venueMatchNoMatch {
		return match, nil
	}

	slug := provisionalVenueSlug(candidate)
	if slug == "" {
		return venueMatchResult{status: venueMatchNoMatch}, nil
	}
	if existing, ok, err := loadVenueBySlug(ctx, tx, slug); err != nil {
		return venueMatchResult{}, err
	} else if ok {
		matcher.indexVenue(existing)
		return venueMatchResult{status: venueMatchResolved, slug: existing.Slug, name: existing.Name}, nil
	}

	venue, err := provisionalVenueFromCandidate(candidate)
	if err != nil {
		return venueMatchResult{}, err
	}
	if _, err := insertVenue(ctx, tx, venue); err != nil {
		return venueMatchResult{}, err
	}
	matcher.indexVenue(venue)
	return venueMatchResult{status: venueMatchResolved, slug: venue.Slug, name: venue.Name}, nil
}

func resolveReviewVenueTx(ctx context.Context, tx interface {
	execer
	queryer
}, matcher *venueMatcher, candidate review.Candidate) (string, error) {
	match, err := ensureProvisionalVenueForCandidateTx(ctx, tx, matcher, candidate)
	if err != nil {
		return "", err
	}
	switch match.status {
	case venueMatchResolved:
		return match.slug, nil
	case venueMatchAmbiguous:
		return "", fmt.Errorf("venue %q not found", candidate.VenueSlug)
	case venueMatchNoMatch:
		return "", fmt.Errorf("venue %q not found", candidate.VenueSlug)
	default:
		return "", fmt.Errorf("venue %q not found", candidate.VenueSlug)
	}
}

func (m *venueMatcher) indexVenue(venue domain.Venue) {
	m.add(m.bySlug, venue.Slug, venue)
	m.add(m.byName, normalizedVenueKey(venue.Name), venue)
	m.add(m.byAddress, normalizedVenueKey(venue.Address), venue)
}

func reviewCandidateFromInput(input review.CandidateInput) review.Candidate {
	return review.Candidate{
		ExternalID:       strings.TrimSpace(input.ExternalID),
		Name:             strings.TrimSpace(input.Name),
		VenueSlug:        strings.TrimSpace(input.VenueSlug),
		VenueText:        strings.TrimSpace(input.VenueText),
		VenueLocationRaw: strings.TrimSpace(input.VenueLocationRaw),
		StartAt:          strings.TrimSpace(input.StartAt),
		EndAt:            strings.TrimSpace(input.EndAt),
		Genre:            strings.TrimSpace(input.Genre),
		Status:           strings.TrimSpace(input.Status),
		Description:      strings.TrimSpace(input.Description),
		SourceName:       strings.TrimSpace(input.SourceName),
		SourceURL:        strings.TrimSpace(input.SourceURL),
		Provenance:       strings.TrimSpace(input.Provenance),
	}
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

func provisionalVenueFromCandidate(candidate review.Candidate) (domain.Venue, error) {
	slug := provisionalVenueSlug(candidate)
	if slug == "" {
		return domain.Venue{}, fmt.Errorf("review venue slug is required")
	}
	name := provisionalVenueName(candidate, slug)
	return domain.Venue{
		Slug:            slug,
		Name:            name,
		Address:         formatProvisionalVenueAddress(name, candidate.VenueLocationRaw),
		Neighbourhood:   provisionalVenueNeighbourhood(candidate.VenueLocationRaw),
		ValidationState: domain.ValidationStateProvisional,
		Origin:          domain.OriginLive,
	}, nil
}

func formatProvisionalVenueAddress(name, value string) string {
	parts := normalizedVenueAddressParts(value)
	if len(parts) == 0 {
		return ""
	}
	if sameVenueAddressLine(parts[0], name) {
		parts = parts[1:]
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ",\n")
}

func provisionalVenueNeighbourhood(value string) string {
	for _, part := range normalizedVenueAddressParts(value) {
		if neighbourhood, ok := sheffieldNeighbourhoodAliases[normalizedVenueKey(part)]; ok {
			return neighbourhood
		}
	}
	return ""
}

func normalizedVenueAddressParts(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

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
	return parts
}

var sheffieldNeighbourhoodAliases = map[string]string{
	"city-centre":                 "City Centre",
	"sheffield-city-centre":       "City Centre",
	"nether-edge":                 "Nether Edge",
	"sharrow-vale":                "Sharrow Vale",
	"ecclesall":                   "Ecclesall",
	"neepsend":                    "Neepsend",
	"cultural-industries-quarter": "Cultural Industries Quarter",
}

func sameVenueAddressLine(addressLine, venueName string) bool {
	return normalizedAddressNameKey(addressLine) != "" && normalizedAddressNameKey(addressLine) == normalizedAddressNameKey(venueName)
}

func normalizedAddressNameKey(value string) string {
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

func provisionalVenueSlug(candidate review.Candidate) string {
	if slug := venueLocationHeadSlug(candidate.VenueLocationRaw); slug != "" {
		return slug
	}
	if slug := strings.TrimSpace(candidate.VenueSlug); slug != "" {
		return slug
	}
	if slug := venueLocationHeadSlug(candidate.VenueText); slug != "" {
		return slug
	}
	return ""
}

func venueLocationHeadSlug(value string) string {
	probes := venueLocationSlugProbes(value)
	if len(probes) == 0 {
		return ""
	}
	return probes[0]
}

func provisionalVenueName(candidate review.Candidate, slug string) string {
	if name := strings.TrimSpace(candidate.VenueText); name != "" {
		return name
	}
	if raw := strings.TrimSpace(candidate.VenueLocationRaw); raw != "" {
		if head, _, ok := strings.Cut(raw, ","); ok {
			if head = strings.TrimSpace(head); head != "" {
				return head
			}
		}
		return raw
	}
	if name := displayNameFromSlug(slug); name != "" {
		return name
	}
	return slug
}

func displayNameFromSlug(slug string) string {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return ""
	}
	parts := strings.FieldsFunc(slug, func(r rune) bool {
		return r == '-' || r == '_' || r == ' '
	})
	if len(parts) == 0 {
		return ""
	}
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
	}
	return strings.Join(parts, " ")
}
