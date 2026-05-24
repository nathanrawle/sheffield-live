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
	if got, want := OwnedVenueSlugForReviewStageSourceName("Jazz at The Lescar manual ingest"), "lescar"; got != want {
		t.Fatalf("jazz at the lescar owned venue slug = %q, want %q", got, want)
	}
	if got, want := OwnedVenueSlugForReviewStageSourceName("The Greystones manual ingest"), "greystones"; got != want {
		t.Fatalf("the greystones owned venue slug = %q, want %q", got, want)
	}
	if got, want := OwnedVenueSlugForReviewStageSourceName("Hallamshire Hotel manual ingest"), "hallamshire-hotel"; got != want {
		t.Fatalf("hallamshire hotel owned venue slug = %q, want %q", got, want)
	}
	if got, want := OwnedVenueSlugForReviewStageSourceName("Network Sheffield manual ingest"), "network-sheffield"; got != want {
		t.Fatalf("network sheffield owned venue slug = %q, want %q", got, want)
	}
}
