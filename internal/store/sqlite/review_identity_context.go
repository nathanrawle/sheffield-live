package sqlite

import (
	"strings"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
)

type reviewSourceIdentityContext struct {
	SourceName            string
	SourceURL             string
	Identities            ingest.SourceIdentitySet
	PrimaryObservationKey string
	CandidateProvenance   string
	Entrypoint            string
}

type reviewSourceIdentityMode int

const (
	reviewSourceIdentitySupporting reviewSourceIdentityMode = iota
	reviewSourceIdentityAuthoritative
)

func reviewSourceIdentityContextForCandidateInput(mode reviewSourceIdentityMode, sourceName, sourceURL, authoritativeSourceName, authoritativeSourceURL, authoritativeSourceEventKey string, candidate review.CandidateInput, entrypoint string) reviewSourceIdentityContext {
	ctx := reviewSourceIdentityContext{
		CandidateProvenance: strings.TrimSpace(candidate.Provenance),
		Entrypoint:          strings.TrimSpace(entrypoint),
	}
	switch mode {
	case reviewSourceIdentityAuthoritative:
		ctx.SourceName = firstNonEmptyReviewString(authoritativeSourceName, sourceName, candidate.SourceName)
		ctx.SourceURL = firstNonEmptyReviewString(authoritativeSourceURL, sourceURL, candidate.SourceURL)
	default:
		ctx.SourceName = firstNonEmptyReviewString(candidate.SourceName, sourceName)
		ctx.SourceURL = firstNonEmptyReviewString(candidate.SourceURL, sourceURL)
	}
	ctx.Identities = reviewCandidateInputSourceIdentities(mode, ctx.SourceURL, authoritativeSourceEventKey, candidate)
	ctx.PrimaryObservationKey = ctx.Identities.PrimaryKey()
	return ctx
}

func reviewSourceIdentityContextForCandidate(mode reviewSourceIdentityMode, sourceName, sourceURL, authoritativeSourceName, authoritativeSourceURL, authoritativeSourceEventKey string, candidate review.Candidate, entrypoint string) reviewSourceIdentityContext {
	return reviewSourceIdentityContextForCandidateInput(mode, sourceName, sourceURL, authoritativeSourceName, authoritativeSourceURL, authoritativeSourceEventKey, reviewCandidateInputFromCandidate(candidate), entrypoint)
}

func reviewSourceIdentityContextForSelectedCandidate(mode reviewSourceIdentityMode, sourceName, sourceURL, authoritativeSourceName, authoritativeSourceURL, authoritativeSourceEventKey string, selected map[review.Field]review.Candidate, canonical review.Candidate, staged []review.Candidate, entrypoint string) reviewSourceIdentityContext {
	candidate := authoritativeSourceCandidateForApply(selected, canonical, staged)
	return reviewSourceIdentityContextForCandidate(mode, sourceName, sourceURL, authoritativeSourceName, authoritativeSourceURL, authoritativeSourceEventKey, candidate, entrypoint)
}

func reviewCandidateInputFromCandidate(candidate review.Candidate) review.CandidateInput {
	return review.CandidateInput{
		CanonicalEventID:                  candidate.CanonicalEventID,
		ExistingEventID:                   candidate.ExistingEventID,
		ExternalID:                        strings.TrimSpace(candidate.ExternalID),
		ExternalIDSourceIdentityDisabled:  candidate.ExternalIDSourceIdentityDisabled,
		Name:                              strings.TrimSpace(candidate.Name),
		VenueSlug:                         strings.TrimSpace(candidate.VenueSlug),
		VenueText:                         strings.TrimSpace(candidate.VenueText),
		VenueLocationRaw:                  strings.TrimSpace(candidate.VenueLocationRaw),
		RoomText:                          strings.TrimSpace(candidate.RoomText),
		Rooms:                             append([]domain.VenueRoom(nil), candidate.Rooms...),
		StartAt:                           strings.TrimSpace(candidate.StartAt),
		EndAt:                             strings.TrimSpace(candidate.EndAt),
		Genre:                             strings.TrimSpace(candidate.Genre),
		Status:                            strings.TrimSpace(candidate.Status),
		Description:                       strings.TrimSpace(candidate.Description),
		ImageURL:                          strings.TrimSpace(candidate.ImageURL),
		ImageSourceURL:                    strings.TrimSpace(candidate.ImageSourceURL),
		ImageAlt:                          strings.TrimSpace(candidate.ImageAlt),
		ImageWidth:                        candidate.ImageWidth,
		ImageHeight:                       candidate.ImageHeight,
		ImageFocusX:                       candidate.ImageFocusX,
		ImageFocusY:                       candidate.ImageFocusY,
		SourceName:                        strings.TrimSpace(candidate.SourceName),
		SourceURL:                         strings.TrimSpace(candidate.SourceURL),
		SourceURLSourceIdentityDisabled:   candidate.SourceURLSourceIdentityDisabled,
		CalendarURL:                       strings.TrimSpace(candidate.CalendarURL),
		CalendarURLSourceIdentityDisabled: candidate.CalendarURLSourceIdentityDisabled,
		Provenance:                        strings.TrimSpace(candidate.Provenance),
	}
}

func reviewCandidateInputSourceIdentities(mode reviewSourceIdentityMode, sourceURL, authoritativeSourceEventKey string, candidate review.CandidateInput) ingest.SourceIdentitySet {
	identities := reviewCandidateSourceIdentities(candidate)
	if len(identities.Keys()) > 0 {
		return identities
	}
	switch mode {
	case reviewSourceIdentityAuthoritative:
		fallback := sourceIdentityInputForKey(authoritativeSourceEventKey)
		if !candidate.SourceURLSourceIdentityDisabled {
			fallback.SourceURL = strings.TrimSpace(sourceURL)
		}
		return ingest.SourceIdentities(fallback)
	default:
		if candidate.SourceURLSourceIdentityDisabled {
			return ingest.SourceIdentitySet{}
		}
		return ingest.SourceIdentities(ingest.SourceIdentityInput{SourceURL: strings.TrimSpace(sourceURL)})
	}
}

func firstNonEmptyReviewString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
