package sqlite

import (
	"context"
	"strings"
	"unicode"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
)

const (
	nearTitleMatchTierClean     = "clean_title_near"
	nearTitleMatchTierVariant   = "title_variant_near"
	nearTitleMatchTierHeadliner = "headliner_near"
)

type nearTitleGuardMatch struct {
	record eventRecord
	tier   string
}

func supportingNearTitleGuardMatchesTx(ctx context.Context, q queryer, event domain.Event, sourceMetadata ingest.SourceMetadataLookup) ([]nearTitleGuardMatch, bool, error) {
	if !guardedNearMatchEnabledForEventSource(sourceMetadata, event.SourceName) {
		return nil, false, nil
	}

	material, ok, err := exactIdentityMaterialForEvent(event)
	if err != nil || !ok {
		return nil, false, err
	}

	window := guardedNearMatchWindowForEventSource(sourceMetadata, event.SourceName)
	records, err := loadLiveEventRecordsByVenueAndStartWindowTx(ctx, q, material.venueSlug, material.start.Add(-window), material.start.Add(window))
	if err != nil {
		return nil, false, err
	}

	matches := make([]nearTitleGuardMatch, 0, len(records))
	for _, record := range records {
		tier := nearTitleMatchTier(material.venueSlug, event.Name, record.Event.Name)
		if tier == "" {
			continue
		}
		matches = append(matches, nearTitleGuardMatch{
			record: record,
			tier:   tier,
		})
	}
	if len(matches) == 0 {
		return nil, true, nil
	}
	return matches, true, nil
}

func nearTitleMatchTier(venueSlug, incomingTitle, existingTitle string) string {
	incomingCleanKey := nearTitleCleanMatchKey(incomingTitle, venueSlug)
	existingCleanKey := nearTitleCleanMatchKey(existingTitle, venueSlug)
	if incomingCleanKey != "" && incomingCleanKey == existingCleanKey {
		return nearTitleMatchTierClean
	}

	incomingVariantKey := nearTitleVariantMatchKey(incomingTitle, venueSlug)
	existingVariantKey := nearTitleVariantMatchKey(existingTitle, venueSlug)
	if nearTitleHeuristicKeyUsable(incomingVariantKey) && incomingVariantKey == existingVariantKey {
		return nearTitleMatchTierVariant
	}

	incomingHeadlinerKey := nearTitleHeadlinerMatchKey(incomingTitle, venueSlug)
	existingHeadlinerKey := nearTitleHeadlinerMatchKey(existingTitle, venueSlug)
	if nearTitleHeuristicKeyUsable(incomingHeadlinerKey) && incomingHeadlinerKey == existingHeadlinerKey {
		return nearTitleMatchTierHeadliner
	}

	return ""
}

func nearTitleCleanMatchKey(title, venueSlug string) string {
	return strings.TrimSpace(normalizeExactIdentityCleanTitle(ingest.CleanEventTitleForVenue(title, venueSlug)))
}

func nearTitleVariantMatchKey(title, venueSlug string) string {
	return nearTitleAggressiveMatchKey(ingest.CleanEventTitleForVenue(title, venueSlug))
}

func nearTitleHeadlinerMatchKey(title, venueSlug string) string {
	return nearTitleAggressiveMatchKey(nearTitleHeadlinerPrefix(ingest.CleanEventTitleForVenue(title, venueSlug)))
}

func nearTitleAggressiveMatchKey(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(title))
	lastSeparator := false
	for _, r := range title {
		r = unicode.ToLower(r)
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			builder.WriteRune(r)
			lastSeparator = false
			continue
		}
		if builder.Len() == 0 || lastSeparator {
			continue
		}
		builder.WriteByte('-')
		lastSeparator = true
	}
	return strings.Trim(builder.String(), "-")
}

func nearTitleHeadlinerPrefix(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return ""
	}

	runes := []rune(strings.ToLower(title))
	best := len(runes)
	for i, r := range runes {
		switch r {
		case '&', '+', '/':
			if i < best {
				best = i
			}
		}
	}
	for _, marker := range []string{"with", "featuring", "feat", "ft", "plus", "vs"} {
		if idx := nearTitleWordMarkerIndex(runes, marker); idx >= 0 && idx < best {
			best = idx
		}
	}
	if best == len(runes) {
		return title
	}
	return strings.TrimSpace(string(runes[:best]))
}

func nearTitleWordMarkerIndex(runes []rune, marker string) int {
	markerRunes := []rune(marker)
	if len(markerRunes) == 0 || len(markerRunes) > len(runes) {
		return -1
	}
	for i := 0; i+len(markerRunes) <= len(runes); i++ {
		if i > 0 && nearTitleIsWordRune(runes[i-1]) {
			continue
		}
		after := i + len(markerRunes)
		if after < len(runes) && nearTitleIsWordRune(runes[after]) {
			continue
		}
		matched := true
		for offset, r := range markerRunes {
			if runes[i+offset] != r {
				matched = false
				break
			}
		}
		if matched {
			return i
		}
	}
	return -1
}

func nearTitleHeuristicKeyUsable(key string) bool {
	alnum := 0
	for _, r := range key {
		if !nearTitleIsWordRune(r) {
			continue
		}
		alnum++
		if alnum >= 2 {
			return true
		}
	}
	return false
}

func nearTitleIsWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}
