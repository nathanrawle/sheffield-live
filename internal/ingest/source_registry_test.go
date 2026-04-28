package ingest

import "testing"

func TestOwnedVenueSourceRegistryConsistency(t *testing.T) {
	for _, source := range RegisteredSourceKeys() {
		ownedVenueSlug := OwnedVenueSlugForSource(source)
		if ownedVenueSlug == "" {
			continue
		}

		reviewStageSourceName := ReviewStageSourceNameForSource(source)
		if reviewStageSourceName == "" {
			t.Fatalf("review stage source name missing for owned-venue source %q", source)
		}
		if got := OwnedVenueSlugForReviewStageSourceName(reviewStageSourceName); got != ownedVenueSlug {
			t.Fatalf("owned venue slug for review-stage source %q = %q, want %q", reviewStageSourceName, got, ownedVenueSlug)
		}
	}

	if got, want := OwnedVenueSlugForReviewStageSourceName("Corporation Sheffield manual ingest"), "corporation"; got != want {
		t.Fatalf("corporation owned venue slug = %q, want %q", got, want)
	}
}
