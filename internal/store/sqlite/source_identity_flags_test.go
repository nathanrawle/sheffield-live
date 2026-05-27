package sqlite

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

func TestReviewCandidateSourceIdentitiesHonorDisabledFlags(t *testing.T) {
	eventURL := "https://example.test/event/one"
	calendarURL := "https://www.sidneyandmatilda.com/events/dealbreaker?format=ical"
	candidate := review.CandidateInput{
		ExternalID:  "series-uid",
		SourceURL:   eventURL,
		CalendarURL: calendarURL,
	}

	wantAll := []string{
		mustSourceIdentityKey(t, "series-uid"),
		mustSourceIdentityKey(t, eventURL),
		mustSourceIdentityKey(t, calendarURL),
	}
	if got := reviewCandidateSourceIdentities(candidate).Keys(); !reflect.DeepEqual(got, wantAll) {
		t.Fatalf("source identity keys = %#v, want %#v", got, wantAll)
	}

	candidate.ExternalIDSourceIdentityDisabled = true
	candidate.SourceURLSourceIdentityDisabled = true
	wantCalendarOnly := []string{mustSourceIdentityKey(t, calendarURL)}
	if got := reviewCandidateSourceIdentities(candidate).Keys(); !reflect.DeepEqual(got, wantCalendarOnly) {
		t.Fatalf("disabled external/source keys = %#v, want %#v", got, wantCalendarOnly)
	}

	stored := review.Candidate{
		ExternalID:                        candidate.ExternalID,
		ExternalIDSourceIdentityDisabled:  candidate.ExternalIDSourceIdentityDisabled,
		SourceURL:                         candidate.SourceURL,
		SourceURLSourceIdentityDisabled:   candidate.SourceURLSourceIdentityDisabled,
		CalendarURL:                       candidate.CalendarURL,
		CalendarURLSourceIdentityDisabled: true,
	}
	if got := reviewStoredCandidateSourceIdentities(stored).Keys(); len(got) != 0 {
		t.Fatalf("all disabled stored candidate keys = %#v, want none", got)
	}
}

func TestReviewCandidateInputSourceIdentitiesSkipsDisabledFallbackURL(t *testing.T) {
	sourceURL := "https://example.test/shared-listing"
	candidate := review.CandidateInput{
		ExternalID:                       "series-uid",
		ExternalIDSourceIdentityDisabled: true,
		SourceURLSourceIdentityDisabled:  true,
	}

	if got := reviewCandidateInputSourceIdentities(reviewSourceIdentitySupporting, sourceURL, "", candidate).Keys(); len(got) != 0 {
		t.Fatalf("supporting fallback keys = %#v, want none", got)
	}
	if got := reviewCandidateInputSourceIdentities(reviewSourceIdentityAuthoritative, sourceURL, "", candidate).Keys(); len(got) != 0 {
		t.Fatalf("authoritative fallback keys = %#v, want none", got)
	}

	candidate.SourceURLSourceIdentityDisabled = false
	want := []string{mustSourceIdentityKey(t, sourceURL)}
	if got := reviewCandidateInputSourceIdentities(reviewSourceIdentitySupporting, sourceURL, "", candidate).Keys(); !reflect.DeepEqual(got, want) {
		t.Fatalf("enabled fallback keys = %#v, want %#v", got, want)
	}
}

func TestParseEventReviewClusterAutoResolutionCandidatePreservesIdentityFlags(t *testing.T) {
	payload := mustJSONPayload(t, map[string]any{
		"source_authority":      string(seedstore.SourceAuthoritySupporting),
		"source_name":           "Fixture source",
		"source_url":            "https://example.test/source",
		"calendar_url":          "https://www.sidneyandmatilda.com/events/dealbreaker?format=ical",
		"candidate_external_id": "series-uid",
		"candidate_external_id_source_identity_disabled":  true,
		"candidate_source_url_source_identity_disabled":   true,
		"candidate_calendar_url_source_identity_disabled": true,
		"candidate_title":      "Flagged Candidate",
		"candidate_venue_slug": "leadmill",
		"candidate_start_at":   "2026-05-01T19:00:00Z",
		"candidate_end_at":     "2026-05-01T21:00:00Z",
	})
	parsed, err := parseEventReviewClusterAutoResolutionCandidate(seedstore.EventReviewClusterEvidenceSummary{
		EvidenceID: 1,
		SourceID:   2,
		SourceName: "Fallback source",
		SourceURL:  "https://example.test/fallback",
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("parse auto-resolution candidate: %v", err)
	}

	if !parsed.CandidateInput.ExternalIDSourceIdentityDisabled || !parsed.Candidate.ExternalIDSourceIdentityDisabled {
		t.Fatalf("external ID disabled flags not preserved: input=%v candidate=%v", parsed.CandidateInput.ExternalIDSourceIdentityDisabled, parsed.Candidate.ExternalIDSourceIdentityDisabled)
	}
	if !parsed.CandidateInput.SourceURLSourceIdentityDisabled || !parsed.Candidate.SourceURLSourceIdentityDisabled {
		t.Fatalf("source URL disabled flags not preserved: input=%v candidate=%v", parsed.CandidateInput.SourceURLSourceIdentityDisabled, parsed.Candidate.SourceURLSourceIdentityDisabled)
	}
	if !parsed.CandidateInput.CalendarURLSourceIdentityDisabled || !parsed.Candidate.CalendarURLSourceIdentityDisabled {
		t.Fatalf("calendar URL disabled flags not preserved: input=%v candidate=%v", parsed.CandidateInput.CalendarURLSourceIdentityDisabled, parsed.Candidate.CalendarURLSourceIdentityDisabled)
	}
	if got := reviewCandidateSourceIdentities(parsed.CandidateInput).Keys(); len(got) != 0 {
		t.Fatalf("parsed disabled candidate source keys = %#v, want none", got)
	}
}

func TestBuildImportReviewCandidateMaterialPreservesIdentityFlags(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	now := time.Date(2026, time.May, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(7 * time.Hour)
	end := start.Add(2 * time.Hour)
	payload := mustJSONPayload(t, map[string]any{
		"source_authority":      string(seedstore.SourceAuthoritySupporting),
		"source_name":           "Fixture source",
		"source_url":            "https://example.test/source",
		"calendar_url":          "https://www.sidneyandmatilda.com/events/dealbreaker?format=ical",
		"candidate_external_id": "series-uid",
		"candidate_external_id_source_identity_disabled":  true,
		"candidate_source_url_source_identity_disabled":   true,
		"candidate_calendar_url_source_identity_disabled": true,
		"candidate_title":      "Flagged Material",
		"candidate_venue_slug": "leadmill",
		"candidate_start_at":   formatRFC3339UTC(start),
		"candidate_end_at":     formatRFC3339UTC(end),
	})

	material, err := buildImportReviewCandidateMaterialTx(ctx, db, st, seedstore.EventReviewCluster{ID: 123}, seedstore.EventReviewClusterEvidenceSummary{
		EvidenceID: 1,
		SourceID:   2,
		SourceName: "Fallback source",
		SourceURL:  "https://example.test/fallback",
		Payload:    payload,
	}, nil, "test_identity_flags", reviewSourceIdentitySupporting, now)
	if err != nil {
		t.Fatalf("build import review material: %v", err)
	}

	if !material.CandidateInput.ExternalIDSourceIdentityDisabled || !material.Candidate.ExternalIDSourceIdentityDisabled {
		t.Fatalf("external ID disabled flags not preserved: input=%v candidate=%v", material.CandidateInput.ExternalIDSourceIdentityDisabled, material.Candidate.ExternalIDSourceIdentityDisabled)
	}
	if !material.CandidateInput.SourceURLSourceIdentityDisabled || !material.Candidate.SourceURLSourceIdentityDisabled {
		t.Fatalf("source URL disabled flags not preserved: input=%v candidate=%v", material.CandidateInput.SourceURLSourceIdentityDisabled, material.Candidate.SourceURLSourceIdentityDisabled)
	}
	if !material.CandidateInput.CalendarURLSourceIdentityDisabled || !material.Candidate.CalendarURLSourceIdentityDisabled {
		t.Fatalf("calendar URL disabled flags not preserved: input=%v candidate=%v", material.CandidateInput.CalendarURLSourceIdentityDisabled, material.Candidate.CalendarURLSourceIdentityDisabled)
	}
	if got := material.SourceCtx.Identities.Keys(); len(got) != 0 {
		t.Fatalf("materialized source context keys = %#v, want none", got)
	}
}

func TestUpsertEventSourceImagesSkipsDisabledSourceURLFallback(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer st.Close()

	eventID := insertEventSourceImageTestEvent(t, db, "source-image-disabled-identity")
	now := time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := upsertEventSourceImagesTx(ctx, tx, eventID, reviewGroupAuthoritativeLink{}, []review.Candidate{{
		ExternalID:                       "series-uid",
		ExternalIDSourceIdentityDisabled: true,
		Name:                             "Source Image Disabled Identity",
		SourceName:                       "Shared source",
		SourceURL:                        "https://example.test/shared-listing",
		SourceURLSourceIdentityDisabled:  true,
		ImageURL:                         "/media/events/disabled.jpg",
		ImageSourceURL:                   "https://images.example.test/disabled.jpg",
		ImageAlt:                         "Disabled",
		ImageWidth:                       1200,
		ImageHeight:                      800,
	}}, now); err != nil {
		t.Fatalf("upsert disabled image identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit disabled image identity: %v", err)
	}

	var sourceIdentityKey string
	if err := db.QueryRow(`SELECT source_identity_key FROM event_source_images WHERE event_id = ?`, eventID).Scan(&sourceIdentityKey); err != nil {
		t.Fatalf("load source image identity key: %v", err)
	}
	if sourceIdentityKey != "" {
		t.Fatalf("source identity key = %q, want empty", sourceIdentityKey)
	}
}

func mustSourceIdentityKey(t *testing.T, raw string) string {
	t.Helper()
	key, ok := ingest.SourceIdentityKey(raw)
	if !ok {
		t.Fatalf("source identity key for %q was rejected", raw)
	}
	return key
}

func mustJSONPayload(t *testing.T, value map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return string(payload)
}
