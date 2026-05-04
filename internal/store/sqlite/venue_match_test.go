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

func TestVenueMatchExplicitSlugWinsOverAmbiguousEvidence(t *testing.T) {
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

	match := matcher.matchCandidate(review.Candidate{
		VenueSlug:        "leadmill",
		VenueText:        "Sidney & Matilda",
		VenueLocationRaw: "Rivelin Works, 46 Sidney Street, Sheffield",
	})
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

func TestProvisionalVenueSlugPrefersLocationHeadOverFullVenueText(t *testing.T) {
	candidate := review.Candidate{
		VenueSlug:        "imaginary-hall-1-void-street-sheffield-s1-2ja",
		VenueText:        "Imaginary Hall, 1 Void Street, Sheffield, S1 2JA",
		VenueLocationRaw: "Imaginary Hall, 1 Void Street, Sheffield, S1 2JA",
	}

	if got, want := provisionalVenueSlug(candidate), "imaginary-hall"; got != want {
		t.Fatalf("provisional venue slug = %q, want %q", got, want)
	}
}

func TestProvisionalVenueFromCandidateFormatsAddress(t *testing.T) {
	tests := []struct {
		name              string
		candidate         review.Candidate
		wantAddress       string
		wantNeighbourhood string
	}{
		{
			name: "drops duplicated venue name and derives city centre",
			candidate: review.Candidate{
				VenueText:        "Memorial Hall",
				VenueLocationRaw: "Memorial Hall, Barkers Pool, Sheffield City Centre, Sheffield, S1 2JA",
			},
			wantAddress:       "Barkers Pool,\nSheffield City Centre,\nSheffield,\nS1 2JA",
			wantNeighbourhood: "City Centre",
		},
		{
			name: "keeps non-duplicate first line",
			candidate: review.Candidate{
				VenueText:        "Yellow Arch Studios",
				VenueLocationRaw: "Yellow Arch Road, Neepsend, Sheffield, S3 8BX",
			},
			wantAddress:       "Yellow Arch Road,\nNeepsend,\nSheffield,\nS3 8BX",
			wantNeighbourhood: "Neepsend",
		},
		{
			name: "drops leading the variant",
			candidate: review.Candidate{
				VenueText:        "Leadmill",
				VenueLocationRaw: "The Leadmill, 6 Leadmill Road, Sheffield City Centre, Sheffield S1 4SE",
			},
			wantAddress:       "6 Leadmill Road,\nSheffield City Centre,\nSheffield S1 4SE",
			wantNeighbourhood: "City Centre",
		},
		{
			name: "drops ampersand and and variant",
			candidate: review.Candidate{
				VenueText:        "Sidney & Matilda",
				VenueLocationRaw: "Sidney and Matilda, Rivelin Works, Cultural Industries Quarter, Sheffield",
			},
			wantAddress:       "Rivelin Works,\nCultural Industries Quarter,\nSheffield",
			wantNeighbourhood: "Cultural Industries Quarter",
		},
		{
			name: "derives cultural industries quarter",
			candidate: review.Candidate{
				VenueText:        "Sidney & Matilda",
				VenueLocationRaw: "Rivelin Works, Cultural Industries Quarter, Sheffield",
			},
			wantAddress:       "Rivelin Works,\nCultural Industries Quarter,\nSheffield",
			wantNeighbourhood: "Cultural Industries Quarter",
		},
		{
			name: "does not derive neighbourhood from landmark alone",
			candidate: review.Candidate{
				VenueText:        "Memorial Hall",
				VenueLocationRaw: "Memorial Hall, Barkers Pool, Sheffield, S1 2JA",
			},
			wantAddress:       "Barkers Pool,\nSheffield,\nS1 2JA",
			wantNeighbourhood: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			venue, err := provisionalVenueFromCandidate(tc.candidate)
			if err != nil {
				t.Fatalf("provisional venue: %v", err)
			}
			if got := venue.Address; got != tc.wantAddress {
				t.Fatalf("venue address = %q, want %q", got, tc.wantAddress)
			}
			if got := venue.Neighbourhood; got != tc.wantNeighbourhood {
				t.Fatalf("venue neighbourhood = %q, want %q", got, tc.wantNeighbourhood)
			}
		})
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
