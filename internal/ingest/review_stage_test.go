package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/eventidentity"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

func TestReviewClustersFromReportClustersByUID(t *testing.T) {
	report := successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "First listing",
					Location: "Sidney & Matilda",
					URL:      "https://example.test/first",
					Status:   "CONFIRMED",
					StartAt:  "2026-05-01T19:00:00Z",
					EndAt:    "2026-05-01T22:00:00Z",
				},
			},
		},
		CalendarReport{
			URL: "https://calendar.example.test/two.ics",
			Candidates: []EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "Second listing",
					Location: "Sidney & Matilda",
					Status:   "TENTATIVE",
					StartAt:  "2026-05-02T19:00:00Z",
				},
				{
					UID:      "single-uid",
					Summary:  "Singleton",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-03T19:00:00Z",
				},
			},
		},
	)

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 2; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Title, "Duplicate review: First listing"; got != want {
		t.Fatalf("first cluster title = %q, want %q", got, want)
	}
	if got, want := len(clusters[0].Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := clusters[0].Candidates[0].ExternalID, "shared-uid"; got != want {
		t.Fatalf("first external ID = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].Status, "Listed"; got != want {
		t.Fatalf("first status = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, "sidney-and-matilda"; got != want {
		t.Fatalf("first venue slug = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].SourceURL, "https://example.test/first"; got != want {
		t.Fatalf("first source URL = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[1].Name, "Second listing"; got != want {
		t.Fatalf("second candidate name = %q, want %q", got, want)
	}
	if got, want := clusters[1].Title, "New listing review: Singleton"; got != want {
		t.Fatalf("second cluster title = %q, want %q", got, want)
	}
	if got, want := len(clusters[1].Candidates), 1; got != want {
		t.Fatalf("second cluster candidates = %d, want %d", got, want)
	}
}

func TestReviewClustersFromReportStagingKeyIsStableForSameContent(t *testing.T) {
	report := successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:         "shared-uid",
					Summary:     "First listing",
					Location:    "Sidney & Matilda",
					URL:         "https://example.test/first",
					Status:      "CONFIRMED",
					StartAt:     "2026-05-01T19:00:00Z",
					EndAt:       "2026-05-01T22:00:00Z",
					Description: "Description",
				},
			},
		},
	)

	first := ReviewClustersFromReport(report)
	second := ReviewClustersFromReport(report)
	if got, want := len(first), 1; got != want {
		t.Fatalf("first clusters = %d, want %d", got, want)
	}
	if got, want := len(second), 1; got != want {
		t.Fatalf("second clusters = %d, want %d", got, want)
	}
	if first[0].StagingKey == "" {
		t.Fatal("staging key is empty")
	}
	if got, want := first[0].StagingKey, second[0].StagingKey; got != want {
		t.Fatalf("staging key = %q, want %q", got, want)
	}
}

func TestReviewClustersFromReportStagingKeyIgnoresRoomEvidence(t *testing.T) {
	base := ReviewClustersFromReport(successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "Parallel Delusion",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T19:00:00Z",
					EndAt:    "2026-05-01T22:00:00Z",
				},
			},
		},
	))
	enriched := ReviewClustersFromReport(successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "Parallel Delusion",
					Location: "Sidney & Matilda",
					RoomText: "FACTORY",
					Rooms:    []RoomCandidate{{Slug: "factory", Name: "Factory"}},
					StartAt:  "2026-05-01T19:00:00Z",
					EndAt:    "2026-05-01T22:00:00Z",
				},
			},
		},
	))

	if got, want := len(base), 1; got != want {
		t.Fatalf("base clusters = %d, want %d", got, want)
	}
	if got, want := len(enriched), 1; got != want {
		t.Fatalf("enriched clusters = %d, want %d", got, want)
	}
	if got, want := base[0].StagingKey, enriched[0].StagingKey; got != want {
		t.Fatalf("staging key = %q, want %q", got, want)
	}
	if got, want := enriched[0].Candidates[0].RoomText, "FACTORY"; got != want {
		t.Fatalf("room text = %q, want %q", got, want)
	}
	if got, want := review.RoomSlugsValue(enriched[0].Candidates[0].Rooms), "factory"; got != want {
		t.Fatalf("room slugs = %q, want %q", got, want)
	}
}

func TestReviewClustersFromReportStagingKeyChangesWhenStableContentChanges(t *testing.T) {
	base := ReviewClustersFromReport(successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "First listing",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T19:00:00Z",
					EndAt:    "2026-05-01T22:00:00Z",
				},
			},
		},
	))
	changed := ReviewClustersFromReport(successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "First listing",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T19:00:00Z",
					EndAt:    "2026-05-01T23:00:00Z",
				},
			},
		},
	))

	if got, want := len(base), 1; got != want {
		t.Fatalf("base clusters = %d, want %d", got, want)
	}
	if got, want := len(changed), 1; got != want {
		t.Fatalf("changed clusters = %d, want %d", got, want)
	}
	if got, want := base[0].StagingKey == changed[0].StagingKey, false; got != want {
		t.Fatalf("staging key changed = %v, want %v", got, want)
	}
}

func TestReviewClustersFromReportStagingKeyIsOrderInsensitiveForDuplicateCandidates(t *testing.T) {
	base := ReviewStageClusterInput{
		Title:      "Duplicate review",
		SourceName: "Fixture ICS",
		SourceURL:  "https://source.example.test/base",
		Notes:      "notes",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "duplicate",
				Name:        "First duplicate",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T20:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "One",
			},
			{
				ExternalID:  "duplicate",
				Name:        "Second duplicate",
				VenueSlug:   "sidney-and-matilda",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T21:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Two",
			},
		},
	}
	reversed := base
	reversed.Candidates = append([]review.CandidateInput(nil), base.Candidates...)
	reversed.Candidates[0], reversed.Candidates[1] = reversed.Candidates[1], reversed.Candidates[0]

	if got, want := reviewStageStagingKey(base), reviewStageStagingKey(reversed); got != want {
		t.Fatalf("staging key = %q, want %q", got, want)
	}
}

func TestReviewClustersFromReportStagingKeyIgnoresTitleNotesSourceMetadataAndProvenance(t *testing.T) {
	base := ReviewStageClusterInput{
		Title:      "Title A",
		SourceName: "Fixture ICS",
		SourceURL:  "https://source.example.test/original",
		Notes:      "notes A",
		Candidates: []review.CandidateInput{
			{
				ExternalID:  "candidate-a",
				Name:        "Candidate A",
				VenueSlug:   "leadmill",
				StartAt:     "2026-05-01T19:00:00Z",
				EndAt:       "2026-05-01T22:00:00Z",
				Genre:       "Indie",
				Status:      "Listed",
				Description: "Description",
				SourceName:  "Candidate source A",
				SourceURL:   "https://candidate.example.test/original",
				Provenance:  "fixture UID candidate-a",
			},
		},
	}
	changed := base
	changed.Title = "Title B"
	changed.Notes = "notes B"
	changed.SourceName = "Different source"
	changed.SourceURL = "https://source.example.test/changed"
	changed.Candidates = append([]review.CandidateInput(nil), base.Candidates...)
	changed.Candidates[0].SourceName = "Candidate source B"
	changed.Candidates[0].SourceURL = "https://candidate.example.test/changed"
	changed.Candidates[0].Provenance = "fixture UID different"

	if got, want := reviewStageStagingKey(base), reviewStageStagingKey(changed); got != want {
		t.Fatalf("staging key = %q, want %q", got, want)
	}
}

func TestReviewClustersFromReportStagingKeyIgnoresCandidateImageFields(t *testing.T) {
	base := ReviewStageClusterInput{
		Title:      "Title",
		SourceName: "Fixture ICS",
		SourceURL:  "https://source.example.test/original",
		Notes:      "notes",
		Candidates: []review.CandidateInput{
			{
				ExternalID:     "candidate-a",
				Name:           "Candidate A",
				VenueSlug:      "leadmill",
				StartAt:        "2026-05-01T19:00:00Z",
				EndAt:          "2026-05-01T22:00:00Z",
				Genre:          "Indie",
				Status:         "Listed",
				Description:    "Description",
				ImageURL:       "https://example.test/one.jpg",
				ImageSourceURL: "https://example.test/source-one.jpg",
				ImageAlt:       "Poster one",
				ImageWidth:     320,
				ImageHeight:    180,
				ImageFocusX:    10,
				ImageFocusY:    90,
			},
		},
	}
	changed := base
	changed.Candidates = append([]review.CandidateInput(nil), base.Candidates...)
	changed.Candidates[0].ImageURL = "https://example.test/two.jpg"
	changed.Candidates[0].ImageSourceURL = "https://example.test/source-two.jpg"
	changed.Candidates[0].ImageAlt = "Poster two"
	changed.Candidates[0].ImageWidth = 640
	changed.Candidates[0].ImageHeight = 360
	changed.Candidates[0].ImageFocusX = 0
	changed.Candidates[0].ImageFocusY = 0

	if got, want := reviewStageStagingKey(base), reviewStageStagingKey(changed); got != want {
		t.Fatalf("staging key = %q, want %q", got, want)
	}
}

func TestReviewStageEventReviewEvidenceInputsUseSourceIdentityKeysAndAuthorityKey(t *testing.T) {
	inputs := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:                       "Duplicate review: Shared title",
		SourceName:                  "Fixture ingest",
		SourceURL:                   "https://source.example.test/events/",
		AuthoritativeSourceName:     "Authoritative ingest",
		AuthoritativeSourceURL:      "https://authority.example.test/event/1",
		AuthoritativeSourceEventKey: "uid:authoritative-event",
		Notes:                       "notes",
		Candidates: []review.CandidateInput{{
			ExternalID: "shared-uid",
			Name:       "Shared title",
			VenueSlug:  "leadmill",
			StartAt:    "2026-05-01T19:00:00Z",
		}},
	})

	if got, want := len(inputs), 1; got != want {
		t.Fatalf("inputs = %d, want %d", got, want)
	}
	if got, want := inputs[0].SourceIdentityKeys, []string{"uid:authoritative-event", "uid:shared-uid"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("source identity keys = %#v, want %#v", got, want)
	}
	if got, want := inputs[0].SourceName, "Authoritative ingest"; got != want {
		t.Fatalf("source name = %q, want %q", got, want)
	}
	if got, want := inputs[0].SourceURL, "https://authority.example.test/event/1"; got != want {
		t.Fatalf("source url = %q, want %q", got, want)
	}
	if got, want := inputs[0].ConflictType, seedstore.EventReviewConflictTypeImportReview; got != want {
		t.Fatalf("conflict type = %q, want %q", got, want)
	}
	if got, want := inputs[0].ConflictReason, seedstore.EventReviewConflictReasonIngestCandidate; got != want {
		t.Fatalf("conflict reason = %q, want %q", got, want)
	}
	if inputs[0].WeakEvidence {
		t.Fatalf("weak evidence = true, want false")
	}
	if got, want := inputs[0].SourceAuthority, seedstore.SourceAuthorityAuthoritative; got != want {
		t.Fatalf("source authority = %q, want %q", got, want)
	}
}

func TestReviewStageEventReviewEvidenceInputsUseNormalizedURLIdentityKey(t *testing.T) {
	inputs := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:      "Duplicate review: URL",
		SourceName: "Fixture ingest",
		SourceURL:  "https://source.example.test/events/",
		Candidates: []review.CandidateInput{{
			SourceURL: "HTTPS://Example.Test/Event/One/",
		}},
	})

	if got, want := len(inputs), 1; got != want {
		t.Fatalf("inputs = %d, want %d", got, want)
	}
	if got, want := inputs[0].SourceIdentityKeys, []string{"url:https://example.test/Event/One"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("source identity keys = %#v, want %#v", got, want)
	}
	if inputs[0].WeakEvidence {
		t.Fatalf("weak evidence = true, want false")
	}
}

func TestReviewStageEventReviewEvidenceInputsDeriveExactIdentityKey(t *testing.T) {
	inputs := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:      "Duplicate review: Exact",
		SourceName: "Fixture ingest",
		SourceURL:  "https://source.example.test/events/",
		Candidates: []review.CandidateInput{{
			Name:      "The Exact Title",
			VenueSlug: "leadmill",
			StartAt:   "2026-05-01T19:00:00Z",
		}},
	})

	if got, want := len(inputs), 1; got != want {
		t.Fatalf("inputs = %d, want %d", got, want)
	}
	start, err := time.Parse(time.RFC3339, "2026-05-01T19:00:00Z")
	if err != nil {
		t.Fatalf("parse start time: %v", err)
	}
	wantKey := eventidentity.BuildKey(eventidentity.ExactKeyVersion, "leadmill", start, "The Exact Title")
	if got, want := inputs[0].ExactIdentityKeys, []string{wantKey}; !reflect.DeepEqual(got, want) {
		t.Fatalf("exact identity keys = %#v, want %#v", got, want)
	}
	if inputs[0].WeakEvidence {
		t.Fatalf("weak evidence = true, want false")
	}
}

func TestReviewStageEventReviewEvidenceFingerprintChangesWithSourceMetadata(t *testing.T) {
	base := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:      "Duplicate review: Fingerprint",
		Notes:      "base notes",
		SourceName: "Fixture ingest",
		SourceURL:  "https://source.example.test/events/",
		Candidates: []review.CandidateInput{{
			ExternalID: "shared-uid",
			Name:       "Fingerprint Title",
			VenueSlug:  "leadmill",
			StartAt:    "2026-05-01T19:00:00Z",
		}},
	})[0]
	changedGroup := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:      "New listing review: Fingerprint",
		Notes:      "changed notes",
		SourceName: "Fixture ingest",
		SourceURL:  "https://source.example.test/events/",
		Candidates: []review.CandidateInput{{
			ExternalID: "shared-uid",
			Name:       "Fingerprint Title",
			VenueSlug:  "leadmill",
			StartAt:    "2026-05-01T19:00:00Z",
		}},
	})[0]
	changedSourceURL := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:      "Duplicate review: Fingerprint",
		Notes:      "base notes",
		SourceName: "Fixture ingest",
		SourceURL:  "https://source.example.test/changed/",
		Candidates: []review.CandidateInput{{
			ExternalID: "shared-uid",
			Name:       "Fingerprint Title",
			VenueSlug:  "leadmill",
			StartAt:    "2026-05-01T19:00:00Z",
		}},
	})[0]
	changedSourceIdentity := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:      "Duplicate review: Fingerprint",
		SourceName: "Fixture ingest",
		SourceURL:  "https://source.example.test/events/",
		Candidates: []review.CandidateInput{{
			ExternalID: "different-uid",
			Name:       "Fingerprint Title",
			VenueSlug:  "leadmill",
			StartAt:    "2026-05-01T19:00:00Z",
		}},
	})[0]

	if got, want := base.EvidenceFingerprint == changedSourceURL.EvidenceFingerprint, false; got != want {
		t.Fatalf("fingerprint changed with source url = %v, want %v", got, want)
	}
	if got, want := base.EvidenceFingerprint == changedSourceIdentity.EvidenceFingerprint, false; got != want {
		t.Fatalf("fingerprint changed with source identity = %v, want %v", got, want)
	}
	if got, want := base.EvidenceFingerprint == changedGroup.EvidenceFingerprint, true; got != want {
		t.Fatalf("fingerprint changed with cluster title/notes = %v, want %v", got, want)
	}
}

func TestReviewStageEventReviewEvidenceFingerprintUsesStableCandidateMaterialForWeakEvidence(t *testing.T) {
	base := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:      "Duplicate review: Weak evidence",
		SourceName: "Fixture ingest",
		SourceURL:  "https://source.example.test/events/",
		Candidates: []review.CandidateInput{{
			Name:      "Payload Title",
			VenueText: "The Leadmill",
			RoomText:  "Main room",
			StartAt:   "2026-05-01T19:00:00Z",
		}},
	})[0]
	changed := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:      "Duplicate review: Weak evidence",
		SourceName: "Fixture ingest",
		SourceURL:  "https://source.example.test/events/",
		Candidates: []review.CandidateInput{{
			Name:      "Payload Title",
			VenueText: "The Leadmill",
			RoomText:  "Side room",
			StartAt:   "2026-05-01T19:00:00Z",
		}},
	})[0]

	if len(base.SourceIdentityKeys) != 0 {
		t.Fatalf("base source identity keys = %#v, want none", base.SourceIdentityKeys)
	}
	if len(base.ExactIdentityKeys) != 0 {
		t.Fatalf("base exact identity keys = %#v, want none", base.ExactIdentityKeys)
	}
	if len(changed.SourceIdentityKeys) != 0 {
		t.Fatalf("changed source identity keys = %#v, want none", changed.SourceIdentityKeys)
	}
	if len(changed.ExactIdentityKeys) != 0 {
		t.Fatalf("changed exact identity keys = %#v, want none", changed.ExactIdentityKeys)
	}
	if !base.WeakEvidence {
		t.Fatal("base weak evidence = false, want true")
	}
	if !changed.WeakEvidence {
		t.Fatal("changed weak evidence = false, want true")
	}
	if got, want := base.EvidenceFingerprint == changed.EvidenceFingerprint, false; got != want {
		t.Fatalf("fingerprint changed with candidate evidence = %v, want %v", got, want)
	}
}

func TestReviewStageEventReviewEvidencePayloadIncludesUIFields(t *testing.T) {
	inputs := ReviewStageClusterEventReviewEvidenceInputs(ReviewStageClusterInput{
		Title:                       "Duplicate review: Payload",
		SourceName:                  "Fixture ingest",
		SourceURL:                   "https://source.example.test/events/",
		AuthoritativeSourceName:     "Authoritative source",
		AuthoritativeSourceURL:      "https://authority.example.test/event/one",
		AuthoritativeSourceEventKey: "uid:authority",
		Notes:                       "notes",
		Candidates: []review.CandidateInput{{
			ExternalID:       "payload-uid",
			Name:             "Payload Title",
			VenueSlug:        "leadmill",
			VenueText:        "The Leadmill",
			VenueLocationRaw: "The Leadmill, Sheffield",
			RoomText:         "Main room",
			Rooms: []domain.VenueRoom{{
				VenueSlug: "leadmill",
				Slug:      "main-room",
				Name:      "Main room",
			}},
			StartAt:        "2026-05-01T19:00:00Z",
			EndAt:          "2026-05-01T22:00:00Z",
			Genre:          "Indie",
			Status:         "Listed",
			Description:    "A show",
			ImageURL:       "https://example.test/image.jpg",
			ImageSourceURL: "https://example.test/source-image.jpg",
			ImageAlt:       "Poster",
			ImageWidth:     640,
			ImageHeight:    480,
			ImageFocusX:    12,
			ImageFocusY:    34,
			SourceName:     "Candidate source",
			SourceURL:      "https://example.test/event",
			CalendarURL:    "https://example.test/calendar.ics",
			Provenance:     "manual ingest",
		}},
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(inputs[0].Payload), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got, want := payload["group_title"], "Duplicate review: Payload"; got != want {
		t.Fatalf("cluster title = %#v, want %#v", got, want)
	}
	if got, want := payload["group_source_name"], "Fixture ingest"; got != want {
		t.Fatalf("cluster source name = %#v, want %#v", got, want)
	}
	if got, want := payload["group_authoritative_source_name"], "Authoritative source"; got != want {
		t.Fatalf("cluster authoritative source name = %#v, want %#v", got, want)
	}
	if got, want := payload["source_name"], "Candidate source"; got != want {
		t.Fatalf("source name = %#v, want %#v", got, want)
	}
	if got, want := payload["group_notes"], "notes"; got != want {
		t.Fatalf("cluster notes = %#v, want %#v", got, want)
	}
	if got, want := payload["candidate_venue_text"], "The Leadmill"; got != want {
		t.Fatalf("candidate venue text = %#v, want %#v", got, want)
	}
	if got, want := payload["candidate_venue_location_raw"], "The Leadmill, Sheffield"; got != want {
		t.Fatalf("candidate venue location raw = %#v, want %#v", got, want)
	}
	if got, want := payload["candidate_room_text"], "Main room"; got != want {
		t.Fatalf("candidate room text = %#v, want %#v", got, want)
	}
	rooms, ok := payload["candidate_rooms"].([]any)
	if !ok || len(rooms) != 1 {
		t.Fatalf("candidate rooms = %#v, want one room", payload["candidate_rooms"])
	}
	if got, want := payload["candidate_image_url"], "https://example.test/image.jpg"; got != want {
		t.Fatalf("candidate image url = %#v, want %#v", got, want)
	}
	if got, want := payload["candidate_image_source_url"], "https://example.test/source-image.jpg"; got != want {
		t.Fatalf("candidate image source url = %#v, want %#v", got, want)
	}
	if got, want := payload["candidate_image_alt"], "Poster"; got != want {
		t.Fatalf("candidate image alt = %#v, want %#v", got, want)
	}
	if got, want := payload["candidate_image_width"], float64(640); got != want {
		t.Fatalf("candidate image width = %#v, want %#v", got, want)
	}
	if got, want := payload["candidate_image_height"], float64(480); got != want {
		t.Fatalf("candidate image height = %#v, want %#v", got, want)
	}
	if got, want := payload["source_url"], "https://example.test/event"; got != want {
		t.Fatalf("source url = %#v, want %#v", got, want)
	}
	if got, want := payload["calendar_url"], "https://example.test/calendar.ics"; got != want {
		t.Fatalf("calendar url = %#v, want %#v", got, want)
	}
	if got, want := payload["provenance"], "manual ingest"; got != want {
		t.Fatalf("provenance = %#v, want %#v", got, want)
	}
	if got, want := payload["source_authority"], "authoritative"; got != want {
		t.Fatalf("source authority = %#v, want %#v", got, want)
	}
}

func TestReviewClustersFromReportClustersByFallback(t *testing.T) {
	report := successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					Summary:  "  Big   Night  ",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T19:00:00Z",
				},
				{
					Summary:  "big night",
					Location: "sidney matilda",
					StartAt:  "2026-05-01T19:00:00Z",
				},
				{
					Summary:  "big night",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T20:00:00Z",
				},
			},
		},
	)

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 2; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Title, "Duplicate review: Big Night"; got != want {
		t.Fatalf("first cluster title = %q, want %q", got, want)
	}
	if got, want := len(clusters[0].Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := clusters[0].Candidates[0].Name, "Big Night"; got != want {
		t.Fatalf("first candidate name = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[1].Name, "big night"; got != want {
		t.Fatalf("second candidate name = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, "sidney-and-matilda"; got != want {
		t.Fatalf("first venue slug = %q, want %q", got, want)
	}
	if got, want := clusters[1].Title, "New listing review: big night"; got != want {
		t.Fatalf("second cluster title = %q, want %q", got, want)
	}
	if got, want := len(clusters[1].Candidates), 1; got != want {
		t.Fatalf("second cluster candidates = %d, want %d", got, want)
	}
}

func TestReviewClustersFromReportCleansVenueAffixBeforeFallbackClustering(t *testing.T) {
	report := successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					Summary:  "Late Junction - Yellow Arch",
					Location: "Yellow Arch Studios",
					StartAt:  "2026-05-01T19:00:00Z",
				},
				{
					Summary:  "Late Junction",
					Location: "Yellow Arch Studios",
					StartAt:  "2026-05-01T19:00:00Z",
				},
			},
		},
	)

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Title, "Duplicate review: Late Junction"; got != want {
		t.Fatalf("cluster title = %q, want %q", got, want)
	}
	if got, want := len(clusters[0].Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	for i, candidate := range clusters[0].Candidates {
		if got, want := candidate.Name, "Late Junction"; got != want {
			t.Fatalf("candidate %d name = %q, want %q", i, got, want)
		}
	}
}

func TestReviewClustersFromReportEmitsSingletons(t *testing.T) {
	report := successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:      "one",
					Summary:  "One",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T19:00:00Z",
				},
				{
					Summary:  "Two",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-02T19:00:00Z",
				},
			},
		},
	)

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 2; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Title, "New listing review: One"; got != want {
		t.Fatalf("first cluster title = %q, want %q", got, want)
	}
	if got, want := clusters[1].Title, "New listing review: Two"; got != want {
		t.Fatalf("second cluster title = %q, want %q", got, want)
	}
	if got, want := clusters[0].Notes, "Created from manual ingest run 42 review staging."; got != want {
		t.Fatalf("notes = %q, want %q", got, want)
	}
}

func TestReviewClustersFromReportKeepsDistinctVenueSlug(t *testing.T) {
	report := successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:      "one",
					Summary:  "One",
					Location: "Rivelin Works",
					StartAt:  "2026-05-01T19:00:00Z",
				},
			},
		},
	)

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, "rivelin-works"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
}

func TestReviewGroupsFromYellowArchReportUsesCanonicalVenueSlugAndSourceName(t *testing.T) {
	report := Report{
		Source:      YellowArchSource,
		SourceURL:   "https://www.yellowarch.com/events/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://www.yellowarch.com/events/",
				Candidates: []EventCandidate{
					{
						Summary:  "One",
						Location: "Yellow Arch Studios",
						URL:      "https://www.yellowarch.com/event/one/",
						StartAt:  "2026-05-01T19:00:00Z",
						EndAt:    "2026-05-01T22:00:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].SourceName, "Yellow Arch manual ingest"; got != want {
		t.Fatalf("source name = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, "yellow-arch"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceName, "Yellow Arch manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceURL, "https://www.yellowarch.com/event/one/"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceEventKey, "url:https://www.yellowarch.com/event/one"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
}

func TestReviewGroupsFromDeliciousClamReportUsesCanonicalVenueSlugAndSourceName(t *testing.T) {
	report := Report{
		Source:      DeliciousClamSource,
		SourceURL:   "https://www.deliciousclam.co.uk/events",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://www.skiddle.com/e/42362090",
				Candidates: []EventCandidate{
					{
						Summary:  "DC Presents: Screensaver / Knorke / Strixen",
						Location: deliciousClamVenueName,
						URL:      "https://www.skiddle.com/e/42362090",
						StartAt:  "2026-05-26T18:30:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].SourceName, "Delicious Clam manual ingest"; got != want {
		t.Fatalf("source name = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, deliciousClamVenueSlug; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceName, "Delicious Clam manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceURL, "https://www.skiddle.com/e/42362090"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := clusters[0].AuthoritativeSourceEventKey, "url:https://www.skiddle.com/e/42362090"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
}

func TestReviewGroupsFromLeadmillReportUsesCanonicalVenueSlugAndSourceName(t *testing.T) {
	report := Report{
		Source:      LeadmillSource,
		SourceURL:   "https://leadmill.co.uk/live/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://leadmill.co.uk/listings/?ical=1",
				Candidates: []EventCandidate{
					{
						Summary:     "One",
						Location:    "Yellow Arch",
						LocationRaw: "Yellow Arch, 30-36 Burton Road, Neepsend, S3 8BX",
						URL:         "https://leadmill.co.uk/event/one/",
						StartAt:     "2026-05-01T19:00:00Z",
						EndAt:       "2026-05-01T22:00:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].SourceName, "The Leadmill manual ingest"; got != want {
		t.Fatalf("source name = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, "yellow-arch"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueText, "Yellow Arch"; got != want {
		t.Fatalf("venue text = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueLocationRaw, "Yellow Arch, 30-36 Burton Road, Neepsend, S3 8BX"; got != want {
		t.Fatalf("venue location raw = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].SourceURL, "https://leadmill.co.uk/event/one/"; got != want {
		t.Fatalf("candidate source url = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].CalendarURL, "https://leadmill.co.uk/listings/?ical=1"; got != want {
		t.Fatalf("candidate calendar url = %q, want %q", got, want)
	}
	if got := clusters[0].AuthoritativeSourceEventKey; got != "" {
		t.Fatalf("authoritative source event key = %q, want empty", got)
	}
}

func TestReviewGroupsFromLeadmillCalendarUsesListingsFallbackWhenCandidateHasNoDetailURL(t *testing.T) {
	report := Report{
		Source:      LeadmillSource,
		SourceURL:   "https://leadmill.co.uk/live/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://leadmill.co.uk/listings/?ical=1",
				Candidates: []EventCandidate{
					{
						Summary:  "One Night",
						Location: "The Leadmill",
						StartAt:  "2026-05-01T19:00:00Z",
						EndAt:    "2026-05-01T22:00:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Candidates[0].SourceURL, "https://leadmill.co.uk/live/#:~:text=One%20Night"; got != want {
		t.Fatalf("candidate source url = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].CalendarURL, "https://leadmill.co.uk/listings/?ical=1"; got != want {
		t.Fatalf("candidate calendar url = %q, want %q", got, want)
	}
}

func TestReviewClustersFromReportUsesPerCalendarPageFallbackWhenCandidateHasNoDetailURL(t *testing.T) {
	report := Report{
		Source:      TheGreystonesSource,
		SourceURL:   "https://www.mygreystones.co.uk/events/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://www.mygreystones.co.uk/april/",
				Candidates: []EventCandidate{
					{
						Summary:  "April Night",
						Location: "The Greystones",
						StartAt:  "2026-05-01T19:00:00Z",
						EndAt:    "2026-05-01T22:00:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Candidates[0].SourceURL, "https://www.mygreystones.co.uk/april/#:~:text=April%20Night"; got != want {
		t.Fatalf("candidate source url = %q, want %q", got, want)
	}
	if got := clusters[0].Candidates[0].CalendarURL; got != "" {
		t.Fatalf("candidate calendar url = %q, want empty", got)
	}
	if got := clusters[0].AuthoritativeSourceName; got != "" {
		t.Fatalf("authoritative source name = %q, want empty", got)
	}
	if got := clusters[0].AuthoritativeSourceURL; got != "" {
		t.Fatalf("authoritative source url = %q, want empty", got)
	}
	if got := clusters[0].AuthoritativeSourceEventKey; got != "" {
		t.Fatalf("authoritative source event key = %q, want empty", got)
	}
}

func TestReviewGroupsFromLeadmillReportTruncatesEscapedCommaVenueHeadSlug(t *testing.T) {
	report := Report{
		Source:      LeadmillSource,
		SourceURL:   "https://leadmill.co.uk/live/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://leadmill.co.uk/listings/?ical=1",
				Candidates: []EventCandidate{
					{
						Summary:     "One",
						Location:    "Memorial Hall, Barkers Pool",
						LocationRaw: "Memorial Hall\\, Barkers Pool, Sheffield, S1 2JA",
						URL:         "https://leadmill.co.uk/event/one/",
						StartAt:     "2026-05-01T19:00:00Z",
						EndAt:       "2026-05-01T22:00:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, "memorial-hall"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
}

func TestReviewClustersFromReportTruncatesEscapedRawVenueHeadForSlug(t *testing.T) {
	report := Report{
		Source:      DefaultSource,
		SourceURL:   "https://example.test/calendar.ics",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				URL: "https://example.test/calendar.ics",
				Candidates: []EventCandidate{
					{
						Summary:     "One",
						Location:    "Memorial Hall, Barkers Pool, Sheffield, S1 2JA",
						LocationRaw: "Memorial Hall\\, Barkers Pool, Sheffield, S1 2JA",
						StartAt:     "2026-05-01T19:00:00Z",
						EndAt:       "2026-05-01T22:00:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueSlug, "memorial-hall"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := clusters[0].Candidates[0].VenueLocationRaw, "Memorial Hall\\, Barkers Pool, Sheffield, S1 2JA"; got != want {
		t.Fatalf("venue location raw = %q, want %q", got, want)
	}
}

func TestReviewStageVenueSlugUsesSourceNormalizer(t *testing.T) {
	catalog, err := DefaultCatalog()
	if err != nil {
		t.Fatalf("default catalog: %v", err)
	}

	candidate := EventCandidate{Location: "Imaginary Hall, 1 Void Street, Sheffield"}
	if got, want := reviewStageVenueSlug(catalog, LeadmillSource, candidate), "imaginary-hall"; got != want {
		t.Fatalf("reviewStageVenueSlug(...) = %q, want %q", got, want)
	}
}

func TestReviewClustersFromReportSkipsAuthoritativeGroupMetadataWhenCandidatesDisagree(t *testing.T) {
	report := Report{
		Source:      DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars: []CalendarReport{
			{
				Candidates: []EventCandidate{
					{
						UID:      "shared-uid",
						Summary:  "One",
						Location: "Sidney & Matilda",
						URL:      "https://example.test/one",
						StartAt:  "2026-05-01T19:00:00Z",
						EndAt:    "2026-05-01T22:00:00Z",
					},
					{
						UID:      "shared-uid",
						Summary:  "Two",
						Location: "Sidney & Matilda",
						URL:      "https://example.test/two",
						StartAt:  "2026-05-01T19:05:00Z",
						EndAt:    "2026-05-01T22:05:00Z",
					},
				},
			},
		},
	}

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 1; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}
	if got := clusters[0].AuthoritativeSourceName; got != "" {
		t.Fatalf("authoritative source name = %q, want empty", got)
	}
	if got := clusters[0].AuthoritativeSourceURL; got != "" {
		t.Fatalf("authoritative source url = %q, want empty", got)
	}
	if got := clusters[0].AuthoritativeSourceEventKey; got != "" {
		t.Fatalf("authoritative source event key = %q, want empty", got)
	}
}

func TestReviewClustersFromReportPreservesStableOrder(t *testing.T) {
	report := successfulReviewStageReport(
		CalendarReport{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []EventCandidate{
				{
					UID:      "uid-b",
					Summary:  "B first",
					Location: "Venue B",
					StartAt:  "2026-05-01T19:00:00Z",
				},
				{
					Summary:  "A first",
					Location: "Venue A",
					StartAt:  "2026-05-02T19:00:00Z",
				},
				{
					UID:      "uid-b",
					Summary:  "B second",
					Location: "Venue B",
					StartAt:  "2026-05-01T20:00:00Z",
				},
				{
					Summary:  "A FIRST",
					Location: "Venue A",
					StartAt:  "2026-05-02T19:00:00Z",
				},
				{
					UID:      "uid-c",
					Summary:  "C first",
					Location: "Venue C",
					StartAt:  "2026-05-03T19:00:00Z",
				},
				{
					UID:      "uid-c",
					Summary:  "C second",
					Location: "Venue C",
					StartAt:  "2026-05-03T20:00:00Z",
				},
			},
		},
	)

	clusters := ReviewClustersFromReport(report)
	if got, want := len(clusters), 3; got != want {
		t.Fatalf("clusters = %d, want %d", got, want)
	}

	assertCandidateNames(t, clusters[0].Candidates, []string{"B first", "B second"})
	assertCandidateNames(t, clusters[1].Candidates, []string{"A first", "A FIRST"})
	assertCandidateNames(t, clusters[2].Candidates, []string{"C first", "C second"})
}

func successfulReviewStageReport(calendars ...CalendarReport) Report {
	return Report{
		Source:      DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 42,
		Status:      importStatusSucceeded,
		Calendars:   calendars,
	}
}

func assertCandidateNames(t *testing.T, candidates []review.CandidateInput, want []string) {
	t.Helper()
	if len(candidates) != len(want) {
		t.Fatalf("candidate count = %d, want %d", len(candidates), len(want))
	}
	for i, candidate := range candidates {
		if candidate.Name != want[i] {
			t.Fatalf("candidate %d name = %q, want %q", i, candidate.Name, want[i])
		}
	}
}

func TestCopyCandidateImagesRefetchesCachedAssetWhenLiveFetchSucceeds(t *testing.T) {
	ctx := context.Background()
	store := &testCandidateImageStore{
		asset: ImageAsset{
			SourceURL:   "https://example.test/cached.jpg",
			PublicURL:   "https://cdn.example.test/cached.jpg",
			StoragePath: "events/cached.jpg",
			ContentType: "image/jpeg",
			Width:       100,
			Height:      80,
			FocusX:      11,
			FocusY:      22,
		},
		loadOK: true,
	}
	fetcher := &testCandidateImageFetcher{
		result: FetchResult{
			URL:         "https://example.test/cached.jpg",
			StatusCode:  http.StatusOK,
			ContentType: "image/jpeg",
			Body:        []byte("fresh-body"),
		},
	}
	storage := &testCandidateImageStorage{
		asset: ImageAsset{
			SourceURL:   "https://example.test/cached.jpg",
			PublicURL:   "https://cdn.example.test/fresh.jpg",
			StoragePath: "events/fresh.jpg",
			ContentType: "image/jpeg",
			Width:       120,
			Height:      90,
			FocusX:      0,
			FocusY:      125,
		},
	}

	candidates, warnings := copyCandidateImages(ctx, store, fetcher, storage, []EventCandidate{
		{
			Summary:        "Poster night",
			ImageSourceURL: "https://example.test/cached.jpg",
		},
	})

	if len(warnings) != 0 {
		t.Fatalf("warnings = %v, want none", warnings)
	}
	if got, want := len(fetcher.calls), 1; got != want {
		t.Fatalf("fetch calls = %d, want %d", got, want)
	}
	if got, want := len(storage.calls), 1; got != want {
		t.Fatalf("store calls = %d, want %d", got, want)
	}
	if got, want := len(store.loadCalls), 1; got != want {
		t.Fatalf("load calls = %d, want %d", got, want)
	}
	if got, want := len(store.savedAssets), 1; got != want {
		t.Fatalf("saved assets = %d, want %d", got, want)
	}
	if got, want := candidates[0].ImageURL, "https://cdn.example.test/fresh.jpg"; got != want {
		t.Fatalf("image url = %q, want %q", got, want)
	}
	if got, want := candidates[0].ImageWidth, 120; got != want {
		t.Fatalf("image width = %d, want %d", got, want)
	}
	if got, want := candidates[0].ImageHeight, 90; got != want {
		t.Fatalf("image height = %d, want %d", got, want)
	}
	if got, want := candidates[0].ImageFocusX, 0; got != want {
		t.Fatalf("image focus x = %d, want %d", got, want)
	}
	if got, want := candidates[0].ImageFocusY, 100; got != want {
		t.Fatalf("image focus y = %d, want %d", got, want)
	}
}

func TestCopyCandidateImagesFallsBackToCachedAssetOnLiveFailures(t *testing.T) {
	tests := []struct {
		name          string
		configure     func(fetcher *testCandidateImageFetcher, storage *testCandidateImageStorage, store *testCandidateImageStore)
		wantWarning   string
		wantImageURL  string
		wantFocusY    int
		wantSaveCount int
	}{
		{
			name: "fetch failure",
			configure: func(fetcher *testCandidateImageFetcher, storage *testCandidateImageStorage, store *testCandidateImageStore) {
				fetcher.err = errors.New("fetch failed")
			},
			wantWarning:   "fetch image",
			wantImageURL:  "https://cdn.example.test/cached.jpg",
			wantFocusY:    22,
			wantSaveCount: 0,
		},
		{
			name: "store failure",
			configure: func(fetcher *testCandidateImageFetcher, storage *testCandidateImageStorage, store *testCandidateImageStore) {
				storage.err = errors.New("store failed")
			},
			wantWarning:   "store image",
			wantImageURL:  "https://cdn.example.test/cached.jpg",
			wantFocusY:    22,
			wantSaveCount: 0,
		},
		{
			name: "save failure",
			configure: func(fetcher *testCandidateImageFetcher, storage *testCandidateImageStorage, store *testCandidateImageStore) {
				store.saveErr = errors.New("save failed")
			},
			wantWarning:   "save image asset",
			wantImageURL:  "https://cdn.example.test/cached.jpg",
			wantFocusY:    22,
			wantSaveCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := &testCandidateImageStore{
				asset: ImageAsset{
					SourceURL:   "https://example.test/cached.jpg",
					PublicURL:   "https://cdn.example.test/cached.jpg",
					StoragePath: "events/cached.jpg",
					ContentType: "image/jpeg",
					Width:       100,
					Height:      80,
					FocusX:      11,
					FocusY:      22,
				},
				loadOK: true,
			}
			fetcher := &testCandidateImageFetcher{
				result: FetchResult{
					URL:         "https://example.test/cached.jpg",
					StatusCode:  http.StatusOK,
					ContentType: "image/jpeg",
					Body:        []byte("fresh-body"),
				},
			}
			storage := &testCandidateImageStorage{
				asset: ImageAsset{
					SourceURL:   "https://example.test/cached.jpg",
					PublicURL:   "https://cdn.example.test/fresh.jpg",
					StoragePath: "events/fresh.jpg",
					ContentType: "image/jpeg",
					Width:       120,
					Height:      90,
					FocusX:      0,
					FocusY:      125,
				},
			}
			tc.configure(fetcher, storage, store)

			candidates, warnings := copyCandidateImages(ctx, store, fetcher, storage, []EventCandidate{
				{
					Summary:        "Poster night",
					ImageSourceURL: "https://example.test/cached.jpg",
				},
			})

			if len(warnings) != 1 {
				t.Fatalf("warnings = %v, want 1 warning", warnings)
			}
			if !strings.Contains(warnings[0], tc.wantWarning) {
				t.Fatalf("warning = %q, want contains %q", warnings[0], tc.wantWarning)
			}
			if got, want := candidates[0].ImageURL, tc.wantImageURL; got != want {
				t.Fatalf("image url = %q, want %q", got, want)
			}
			if got, want := candidates[0].ImageFocusY, tc.wantFocusY; got != want {
				t.Fatalf("image focus y = %d, want %d", got, want)
			}
			if got, want := len(store.savedAssets), tc.wantSaveCount; got != want {
				t.Fatalf("saved assets = %d, want %d", got, want)
			}
		})
	}
}

func TestCopyCandidateImagesRejectsPrivateRedirectTargets(t *testing.T) {
	ctx := context.Background()
	store := &testCandidateImageStore{}
	fetcher := &testCandidateImageFetcher{
		redirectURL: "http://127.0.0.1/private-image.jpg",
		result: FetchResult{
			URL:         "https://example.test/poster.jpg",
			StatusCode:  http.StatusOK,
			ContentType: "image/jpeg",
			Body:        []byte("image-body"),
		},
	}
	storage := &testCandidateImageStorage{
		asset: ImageAsset{
			SourceURL: "https://example.test/poster.jpg",
			PublicURL: "/media/events/poster.jpg",
		},
	}

	candidates, warnings := copyCandidateImages(ctx, store, fetcher, storage, []EventCandidate{
		{
			Summary:        "Private redirect poster",
			ImageSourceURL: "https://example.test/poster.jpg",
		},
	})

	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one warning", warnings)
	}
	if !strings.Contains(warnings[0], "127.0.0.1") {
		t.Fatalf("warning = %q, want rejected loopback target", warnings[0])
	}
	if got := len(storage.calls); got != 0 {
		t.Fatalf("storage calls = %d, want 0", got)
	}
	if got := candidates[0].ImageURL; got != "" {
		t.Fatalf("image url = %q, want blank", got)
	}
}

type testCandidateImageStore struct {
	asset       ImageAsset
	loadOK      bool
	loadErr     error
	saveErr     error
	loadCalls   []string
	savedAssets []ImageAsset
}

func (s *testCandidateImageStore) LoadImageAsset(_ context.Context, sourceURL string) (ImageAsset, bool, error) {
	s.loadCalls = append(s.loadCalls, sourceURL)
	if s.loadErr != nil {
		return ImageAsset{}, false, s.loadErr
	}
	return s.asset, s.loadOK, nil
}

func (s *testCandidateImageStore) SaveImageAsset(_ context.Context, asset ImageAsset) error {
	s.savedAssets = append(s.savedAssets, asset)
	return s.saveErr
}

func (s *testCandidateImageStore) EnsureSource(context.Context, string, string) (int64, error) {
	return 0, nil
}

func (s *testCandidateImageStore) CreateImportRun(context.Context, string, string) (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

func (s *testCandidateImageStore) CreateSnapshot(context.Context, int64, *int64, time.Time, string) (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

func (s *testCandidateImageStore) FinishImportRun(context.Context, int64, string, string) (time.Time, error) {
	return time.Time{}, nil
}

type testCandidateImageFetcher struct {
	calls       []string
	redirectURL string
	result      FetchResult
	err         error
}

func (f *testCandidateImageFetcher) Fetch(_ context.Context, url string) (FetchResult, error) {
	f.calls = append(f.calls, url)
	if f.err != nil {
		return FetchResult{}, f.err
	}
	return f.result, nil
}

func (f *testCandidateImageFetcher) FetchWithURLValidator(ctx context.Context, url string, validate func(string) error) (FetchResult, error) {
	if validate != nil {
		if err := validate(url); err != nil {
			return FetchResult{}, err
		}
		if f.redirectURL != "" {
			if err := validate(f.redirectURL); err != nil {
				return FetchResult{}, err
			}
		}
	}
	return f.Fetch(ctx, url)
}

type testCandidateImageStorage struct {
	calls []string
	asset ImageAsset
	err   error
}

func (s *testCandidateImageStorage) StoreImage(_ context.Context, sourceURL string, result FetchResult) (ImageAsset, error) {
	s.calls = append(s.calls, sourceURL)
	if s.err != nil {
		return ImageAsset{}, s.err
	}
	return s.asset, nil
}
