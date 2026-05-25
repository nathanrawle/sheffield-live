package sqlite

import (
	"strings"

	"sheffield-live/internal/ingest"
)

type ownedVenueSlugsReviewStageSourceNamer interface {
	OwnedVenueSlugsForReviewStageSourceName(sourceName string) []string
}

func sourceMetadataOwnedVenueSlugsForReviewStageSourceName(sourceMetadata ingest.SourceMetadataLookup, sourceName string) []string {
	if sourceMetadata == nil {
		return nil
	}
	if lookup, ok := sourceMetadata.(ownedVenueSlugsReviewStageSourceNamer); ok {
		return normalizeSourceMetadataOwnedVenueSlugs(lookup.OwnedVenueSlugsForReviewStageSourceName(sourceName))
	}
	if slug := strings.TrimSpace(sourceMetadata.OwnedVenueSlugForReviewStageSourceName(sourceName)); slug != "" {
		return []string{slug}
	}
	return nil
}

func sourceMetadataOwnsReviewStageVenue(sourceMetadata ingest.SourceMetadataLookup, sourceName, venueSlug string) bool {
	venueSlug = strings.TrimSpace(venueSlug)
	if venueSlug == "" {
		return false
	}
	for _, ownedVenueSlug := range sourceMetadataOwnedVenueSlugsForReviewStageSourceName(sourceMetadata, sourceName) {
		if strings.TrimSpace(ownedVenueSlug) == venueSlug {
			return true
		}
	}
	return false
}

func normalizeSourceMetadataOwnedVenueSlugs(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
