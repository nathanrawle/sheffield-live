package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"sheffield-live/internal/review"
)

const reviewStageDefaultSourceName = "Sidney & Matilda manual ingest"

type reviewStageCluster struct {
	group review.GroupInput
}

func ReviewGroupsFromReport(report Report) []review.GroupInput {
	if report.Status != importStatusSucceeded {
		return nil
	}

	clusters := make(map[string]*reviewStageCluster)
	var order []string

	for _, calendar := range report.Calendars {
		for _, candidate := range calendar.Candidates {
			key, ok := reviewStageKey(report.Source, candidate)
			if !ok {
				continue
			}

			cluster, exists := clusters[key]
			if !exists {
				cluster = &reviewStageCluster{
					group: review.GroupInput{
						SourceName:  reviewStageSourceName(report),
						SourceURL:   reviewStageFirstNonEmpty(calendar.URL, report.SourceURL),
						ImportRunID: report.ImportRunID,
						Notes:       reviewStageNotes(report),
					},
				}
				clusters[key] = cluster
				order = append(order, key)
			}

			cluster.group.Candidates = append(cluster.group.Candidates, reviewStageCandidateInput(report, calendar, candidate))
		}
	}

	groups := make([]review.GroupInput, 0, len(order))
	for _, key := range order {
		group := clusters[key].group
		group.Title = reviewStageTitle(group)
		group.StagingKey = reviewStageStagingKey(group)
		group.AuthoritativeSourceName, group.AuthoritativeSourceURL, group.AuthoritativeSourceEventKey = reviewStageAuthoritativeSource(report.Source, group)
		groups = append(groups, group)
	}
	return groups
}

func reviewStageAuthoritativeSource(source string, group review.GroupInput) (string, string, string) {
	ownedVenueSlug := strings.TrimSpace(OwnedVenueSlugForSource(source))
	if ownedVenueSlug == "" || len(group.Candidates) == 0 {
		return "", "", ""
	}

	var sourceName string
	var sourceURL string
	var sourceEventKey string
	for _, candidate := range group.Candidates {
		if strings.TrimSpace(candidate.VenueSlug) != ownedVenueSlug {
			return "", "", ""
		}
		candidateEventKey := reviewStageAuthoritativeSourceEventKey(candidate)
		if candidateEventKey == "" {
			return "", "", ""
		}
		candidateSourceName := reviewStageFirstNonEmpty(candidate.SourceName, group.SourceName)
		candidateSourceURL := reviewStageFirstNonEmpty(candidate.SourceURL, group.SourceURL)
		if candidateSourceName == "" || candidateSourceURL == "" {
			return "", "", ""
		}
		if sourceEventKey == "" {
			sourceName = candidateSourceName
			sourceURL = candidateSourceURL
			sourceEventKey = candidateEventKey
			continue
		}
		if candidateSourceName != sourceName || candidateSourceURL != sourceURL || candidateEventKey != sourceEventKey {
			return "", "", ""
		}
	}
	return sourceName, sourceURL, sourceEventKey
}

func reviewStageAuthoritativeSourceEventKey(candidate review.CandidateInput) string {
	return reviewStageFirstNonEmpty(candidate.ExternalID, candidate.SourceURL)
}

func reviewStageStagingKey(group review.GroupInput) string {
	candidateFingerprints := make([]string, 0, len(group.Candidates))
	for _, candidate := range group.Candidates {
		candidateFingerprints = append(candidateFingerprints, reviewStageCandidateFingerprint(candidate))
	}
	sort.Strings(candidateFingerprints)

	sum := sha256.New()
	writeReviewStageHashPart(sum, "review-stage-group:v2")
	for _, fingerprint := range candidateFingerprints {
		writeReviewStageHashPart(sum, fingerprint)
	}
	return "v1:" + hex.EncodeToString(sum.Sum(nil))
}

func reviewStageCandidateFingerprint(candidate review.CandidateInput) string {
	sum := sha256.New()
	writeReviewStageHashPart(sum, "review-stage-candidate:v1")
	writeReviewStageHashPart(sum, candidate.ExternalID)
	writeReviewStageHashPart(sum, candidate.Name)
	writeReviewStageHashPart(sum, candidate.VenueSlug)
	writeReviewStageHashPart(sum, candidate.StartAt)
	writeReviewStageHashPart(sum, candidate.EndAt)
	writeReviewStageHashPart(sum, candidate.Genre)
	writeReviewStageHashPart(sum, candidate.Status)
	writeReviewStageHashPart(sum, candidate.Description)
	return hex.EncodeToString(sum.Sum(nil))
}

func writeReviewStageHashPart(sum interface{ Write([]byte) (int, error) }, value string) {
	_, _ = fmt.Fprintf(sum, "%d:%s\x00", len(value), value)
}

func reviewStageKey(source string, candidate EventCandidate) (string, bool) {
	if uid := strings.TrimSpace(candidate.UID); uid != "" {
		return "uid\x00" + uid, true
	}

	summary := normalizeReviewStageText(candidate.Summary)
	startAt := strings.TrimSpace(candidate.StartAt)
	if summary == "" || startAt == "" {
		return "", false
	}

	return strings.Join([]string{
		"fallback",
		summary,
		startAt,
		reviewStageVenueSlug(source, candidate.Location),
	}, "\x00"), true
}

func reviewStageCandidateInput(report Report, calendar CalendarReport, candidate EventCandidate) review.CandidateInput {
	return review.CandidateInput{
		ExternalID:  strings.TrimSpace(candidate.UID),
		Name:        strings.TrimSpace(candidate.Summary),
		VenueSlug:   reviewStageVenueSlug(report.Source, candidate.Location),
		StartAt:     strings.TrimSpace(candidate.StartAt),
		EndAt:       strings.TrimSpace(candidate.EndAt),
		Genre:       "",
		Status:      reviewStageStatus(candidate.Status),
		Description: strings.TrimSpace(candidate.Description),
		SourceName:  reviewStageSourceName(report),
		SourceURL:   reviewStageFirstNonEmpty(candidate.URL, calendar.URL, report.SourceURL),
		Provenance:  reviewStageProvenance(report, calendar, candidate),
	}
}

func reviewStageTitle(group review.GroupInput) string {
	prefix := "Duplicate review"
	if len(group.Candidates) == 1 {
		prefix = "New listing review"
	}

	for _, candidate := range group.Candidates {
		if name := normalizeReviewStageDisplay(candidate.Name); name != "" {
			return prefix + ": " + name
		}
	}
	return prefix
}

func reviewStageSourceName(report Report) string {
	return ReviewStageSourceNameForSource(report.Source)
}

func reviewStageNotes(report Report) string {
	if report.ImportRunID == 0 {
		return "Created from manual ingest review staging."
	}
	return fmt.Sprintf("Created from manual ingest run %d review staging.", report.ImportRunID)
}

func reviewStageStatus(status string) string {
	status = strings.TrimSpace(status)
	if strings.EqualFold(status, "CONFIRMED") {
		return "Listed"
	}
	return status
}

func reviewStageProvenance(report Report, calendar CalendarReport, candidate EventCandidate) string {
	var parts []string
	if report.ImportRunID != 0 {
		parts = append(parts, fmt.Sprintf("import run %d", report.ImportRunID))
	}
	if calendar.URL != "" {
		parts = append(parts, "calendar "+calendar.URL)
	}
	if candidate.UID != "" {
		parts = append(parts, "UID "+candidate.UID)
	} else if candidate.URL != "" {
		parts = append(parts, "URL "+candidate.URL)
	}
	if len(parts) == 0 {
		return "manual ingest"
	}
	return strings.Join(parts, "; ")
}

func normalizeReviewStageText(value string) string {
	return strings.ToLower(normalizeReviewStageDisplay(value))
}

func normalizeReviewStageDisplay(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func reviewStageFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func reviewStageVenueSlug(source, value string) string {
	if strings.TrimSpace(source) == YellowArchSource {
		lower := strings.ToLower(strings.TrimSpace(value))
		if strings.Contains(lower, "yellow") && strings.Contains(lower, "arch") {
			return "yellow-arch"
		}
	}
	if strings.TrimSpace(source) == LeadmillSource {
		return VenueSlugFromText(leadmillVenueText(value))
	}
	return VenueSlugFromText(value)
}
