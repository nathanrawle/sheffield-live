package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/review"
)

func TestVenueMatchDirectSlugMatch(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	matcher, err := loadVenueMatcher(ctx, st.db)
	if err != nil {
		t.Fatalf("load venue matcher: %v", err)
	}

	match := matcher.matchCandidate(review.Candidate{VenueSlug: "leadmill"})
	assertVenueMatch(t, match, venueMatchResolved, "leadmill", "The Leadmill")
}

func TestVenueMatchNameBasedMatch(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	matcher, err := loadVenueMatcher(ctx, st.db)
	if err != nil {
		t.Fatalf("load venue matcher: %v", err)
	}

	match := matcher.matchCandidate(review.Candidate{VenueText: "Yellow Arch Studios"})
	assertVenueMatch(t, match, venueMatchResolved, "yellow-arch", "Yellow Arch Studios")
}

func TestVenueMatchAddressBasedMatch(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	matcher, err := loadVenueMatcher(ctx, st.db)
	if err != nil {
		t.Fatalf("load venue matcher: %v", err)
	}

	match := matcher.matchCandidate(review.Candidate{VenueLocationRaw: "Rivelin Works, 46 Sidney Street, Sheffield"})
	assertVenueMatch(t, match, venueMatchResolved, "sidney-and-matilda", "Sidney & Matilda")
}

func TestVenueMatchAmbiguousResult(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if _, err := st.db.Exec(`
		INSERT INTO venues (
			slug,
			name,
			address,
			neighbourhood,
			description,
			website,
			origin
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "sidney-and-matilda-duplicate", "Sidney & Matilda Duplicate", "Rivelin Works, 46 Sidney Street, Sheffield", "Cultural Industries Quarter", "Duplicate venue", "", string(domain.OriginTest)); err != nil {
		t.Fatalf("insert duplicate venue: %v", err)
	}

	matcher, err := loadVenueMatcher(ctx, st.db)
	if err != nil {
		t.Fatalf("load venue matcher: %v", err)
	}

	match := matcher.matchCandidate(review.Candidate{VenueLocationRaw: "Rivelin Works, 46 Sidney Street, Sheffield"})
	if match.status != venueMatchAmbiguous {
		t.Fatalf("match status = %v, want ambiguous", match.status)
	}
}

func TestVenueMatchNoMatchResult(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "sheffield-live.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	matcher, err := loadVenueMatcher(ctx, st.db)
	if err != nil {
		t.Fatalf("load venue matcher: %v", err)
	}

	match := matcher.matchCandidate(review.Candidate{
		VenueSlug:        "imaginary-hall",
		VenueText:        "Imaginary Hall",
		VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
	})
	if match.status != venueMatchNoMatch {
		t.Fatalf("match status = %v, want no match", match.status)
	}
}

func TestProvisionalVenueFromCandidateFormatsAddress(t *testing.T) {
	venue, err := provisionalVenueFromCandidate(review.Candidate{
		VenueText:        "Memorial Hall",
		VenueLocationRaw: "Memorial Hall, Barkers Pool, Sheffield, S1 2JA",
	})
	if err != nil {
		t.Fatalf("provisional venue: %v", err)
	}

	if got, want := venue.Address, "Memorial Hall,\nBarkers Pool,\nSheffield,\nS1 2JA"; got != want {
		t.Fatalf("venue address = %q, want %q", got, want)
	}
}

func assertVenueMatch(t *testing.T, match venueMatchResult, wantStatus venueMatchStatus, wantSlug, wantName string) {
	t.Helper()

	if match.status != wantStatus {
		t.Fatalf("match status = %v, want %v", match.status, wantStatus)
	}
	if match.slug != wantSlug {
		t.Fatalf("match slug = %q, want %q", match.slug, wantSlug)
	}
	if match.name != wantName {
		t.Fatalf("match name = %q, want %q", match.name, wantName)
	}
}
