package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
)

type testDisabledNearSourceMetadata struct {
	testSupportingSourceMetadata
}

func (testDisabledNearSourceMetadata) GuardedNearMatchDisabledForReviewStageSourceName(string) bool {
	return true
}

func TestNearTitleMatchTier(t *testing.T) {
	cases := []struct {
		name     string
		incoming string
		existing string
		want     string
	}{
		{
			name:     "clean title whitespace",
			incoming: "Late   Junction",
			existing: "Late Junction",
			want:     nearTitleMatchTierClean,
		},
		{
			name:     "dash variant",
			incoming: "Joe Carnall Jnr - Celebrates 20 years of Milburns Well Well Well",
			existing: "Joe Carnall Jnr – Celebrates 20 years of Milburns Well Well Well",
			want:     nearTitleMatchTierVariant,
		},
		{
			name:     "accented dash variant",
			incoming: "Beyoncé - Live",
			existing: "Beyoncé – Live",
			want:     nearTitleMatchTierVariant,
		},
		{
			name:     "unspaced dash variant",
			incoming: "Title-Subtitle",
			existing: "Title–Subtitle",
			want:     nearTitleMatchTierVariant,
		},
		{
			name:     "headliner only",
			incoming: "Jane Doe + The Openers",
			existing: "Jane Doe",
			want:     nearTitleMatchTierHeadliner,
		},
		{
			name:     "slash headliner only",
			incoming: "Jane Doe/The Openers",
			existing: "Jane Doe",
			want:     nearTitleMatchTierHeadliner,
		},
		{
			name:     "short headliner ignored",
			incoming: "A + Support",
			existing: "A",
			want:     "",
		},
		{
			name:     "punctuation ignored",
			incoming: "!!!",
			existing: "???",
			want:     "",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := nearTitleMatchTier("yellow-arch", tt.incoming, tt.existing); got != tt.want {
				t.Fatalf("near title tier = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSupportingSingletonNearTitleGuardBlocksPunctuationVariantDuplicate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path, testSupportingSourceMetadata{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	defer db.Close()

	const (
		existingTitle = "Joe Carnall Jnr – Celebrates 20 years of Milburns Well Well Well"
		incomingTitle = "Joe Carnall Jnr - Celebrates 20 years of Milburns Well Well Well"
		existingStart = "2026-06-06T18:30:00Z"
		incomingStart = "2026-06-06T18:00:00Z"
	)
	sourceID := mustEnsureSourceID(t, st, "Yellow Arch manual ingest", "https://www.yellowarch.com/event/joe-carnall-jnr-celebrates-20-years-of-milburns-well-well-well/")
	existingSlug := mustLiveEventSlug(t, existingTitle, "yellow-arch", existingStart)
	incomingSlug := mustLiveEventSlug(t, incomingTitle, "yellow-arch", incomingStart)
	mustInsertRepairLegacyEvent(t, db, sourceID, existingSlug, "yellow-arch", existingTitle, existingStart, "Existing authoritative description.")

	beforeEvents := mustCount(t, db, "events")
	beforeLinks := mustCount(t, db, "event_source_links")
	beforeObservations := mustCount(t, db, "event_source_attribute_observations")

	slug, promoted, err := st.PromoteSingletonReviewClusterIfMissing(ctx, ingest.ReviewStageClusterInput{
		ImportRunID: 6060618,
		Title:       incomingTitle,
		SourceName:  testSupportingSourceName,
		SourceURL:   testSupportingSourceURL,
		Candidates: []review.CandidateInput{{
			ExternalID:  "leadmill-joe-carnall-20260606",
			Name:        incomingTitle,
			VenueSlug:   "yellow-arch",
			StartAt:     incomingStart,
			EndAt:       "2026-06-06T22:00:00Z",
			Genre:       "Live",
			Status:      "Listed",
			Description: "Supporting listing description.",
			SourceName:  testSupportingSourceName,
			SourceURL:   "https://leadmill.co.uk/event/joe-carnall-jnr-celebrates-20-years-of-milburns-well-well-well-yellow-arch/",
			CalendarURL: testSupportingSourceURL,
		}},
	})
	if err != nil {
		t.Fatalf("promote singleton review cluster: %v", err)
	}
	if promoted {
		t.Fatal("promoted = true, want false")
	}
	if slug != "" {
		t.Fatalf("slug = %q, want empty", slug)
	}
	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events = %d, want %d", got, beforeEvents)
	}
	if got := mustCount(t, db, "event_source_links"); got != beforeLinks {
		t.Fatalf("event_source_links = %d, want %d", got, beforeLinks)
	}
	if got := mustCount(t, db, "event_source_attribute_observations"); got <= beforeObservations {
		t.Fatalf("event_source_attribute_observations = %d, want greater than %d", got, beforeObservations)
	}
	if _, ok := st.EventBySlug(incomingSlug); ok {
		t.Fatalf("incoming duplicate slug %q exists", incomingSlug)
	}
	if tier := nearTitleMatchTier("yellow-arch", incomingTitle, existingTitle); tier != nearTitleMatchTierVariant {
		t.Fatalf("near title tier = %q, want %q", tier, nearTitleMatchTierVariant)
	}
}

func TestGuardedNearLiveEventMatchDisabledReturnsNoRecords(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path, testDisabledNearSourceMetadata{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	defer db.Close()

	const (
		title   = "Near Disabled Event"
		startAt = "2026-06-06T18:30:00Z"
	)
	sourceID := mustEnsureSourceID(t, st, "Fixture authoritative source", "https://fixture.example.test/near-disabled/")
	slug := mustLiveEventSlug(t, title, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, slug, "leadmill", title, startAt, "Existing description.")

	start, err := time.Parse(time.RFC3339, startAt)
	if err != nil {
		t.Fatalf("parse start: %v", err)
	}
	records, enabled, err := guardedNearLiveEventMatchForEventTx(ctx, st.db, domain.Event{
		Name:             title,
		VenueSlug:        "leadmill",
		Start:            start.Add(-30 * time.Minute),
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateProvisional,
		SourceName:       testSupportingSourceName,
	}, testDisabledNearSourceMetadata{})
	if err != nil {
		t.Fatalf("guarded near match: %v", err)
	}
	if enabled {
		t.Fatal("enabled = true, want false")
	}
	if len(records) != 0 {
		t.Fatalf("records = %d, want 0", len(records))
	}
}
