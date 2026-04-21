package ingest

import (
	"testing"

	"sheffield-live/internal/review"
)

func TestReviewGroupsFromReportClustersByUID(t *testing.T) {
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

	groups := ReviewGroupsFromReport(report)
	if got, want := len(groups), 2; got != want {
		t.Fatalf("groups = %d, want %d", got, want)
	}
	if got, want := groups[0].Title, "Duplicate review: First listing"; got != want {
		t.Fatalf("first group title = %q, want %q", got, want)
	}
	if got, want := len(groups[0].Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := groups[0].Candidates[0].ExternalID, "shared-uid"; got != want {
		t.Fatalf("first external ID = %q, want %q", got, want)
	}
	if got, want := groups[0].Candidates[0].Status, "Listed"; got != want {
		t.Fatalf("first status = %q, want %q", got, want)
	}
	if got, want := groups[0].Candidates[0].VenueSlug, "sidney-and-matilda"; got != want {
		t.Fatalf("first venue slug = %q, want %q", got, want)
	}
	if got, want := groups[0].Candidates[0].SourceURL, "https://example.test/first"; got != want {
		t.Fatalf("first source URL = %q, want %q", got, want)
	}
	if got, want := groups[0].Candidates[1].Name, "Second listing"; got != want {
		t.Fatalf("second candidate name = %q, want %q", got, want)
	}
	if got, want := groups[1].Title, "New listing review: Singleton"; got != want {
		t.Fatalf("second group title = %q, want %q", got, want)
	}
	if got, want := len(groups[1].Candidates), 1; got != want {
		t.Fatalf("second group candidates = %d, want %d", got, want)
	}
}

func TestReviewGroupsFromReportClustersByFallback(t *testing.T) {
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

	groups := ReviewGroupsFromReport(report)
	if got, want := len(groups), 2; got != want {
		t.Fatalf("groups = %d, want %d", got, want)
	}
	if got, want := groups[0].Title, "Duplicate review: Big Night"; got != want {
		t.Fatalf("first group title = %q, want %q", got, want)
	}
	if got, want := len(groups[0].Candidates), 2; got != want {
		t.Fatalf("candidates = %d, want %d", got, want)
	}
	if got, want := groups[0].Candidates[0].Name, "Big   Night"; got != want {
		t.Fatalf("first candidate name = %q, want %q", got, want)
	}
	if got, want := groups[0].Candidates[1].Name, "big night"; got != want {
		t.Fatalf("second candidate name = %q, want %q", got, want)
	}
	if got, want := groups[0].Candidates[0].VenueSlug, "sidney-and-matilda"; got != want {
		t.Fatalf("first venue slug = %q, want %q", got, want)
	}
	if got, want := groups[1].Title, "New listing review: big night"; got != want {
		t.Fatalf("second group title = %q, want %q", got, want)
	}
	if got, want := len(groups[1].Candidates), 1; got != want {
		t.Fatalf("second group candidates = %d, want %d", got, want)
	}
}

func TestReviewGroupsFromReportEmitsSingletons(t *testing.T) {
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

	groups := ReviewGroupsFromReport(report)
	if got, want := len(groups), 2; got != want {
		t.Fatalf("groups = %d, want %d", got, want)
	}
	if got, want := groups[0].Title, "New listing review: One"; got != want {
		t.Fatalf("first group title = %q, want %q", got, want)
	}
	if got, want := groups[1].Title, "New listing review: Two"; got != want {
		t.Fatalf("second group title = %q, want %q", got, want)
	}
	if got, want := groups[0].Notes, "Created from manual ingest run 42 review staging."; got != want {
		t.Fatalf("notes = %q, want %q", got, want)
	}
}

func TestReviewGroupsFromReportPreservesStableOrder(t *testing.T) {
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

	groups := ReviewGroupsFromReport(report)
	if got, want := len(groups), 3; got != want {
		t.Fatalf("groups = %d, want %d", got, want)
	}

	assertCandidateNames(t, groups[0].Candidates, []string{"B first", "B second"})
	assertCandidateNames(t, groups[1].Candidates, []string{"A first", "A FIRST"})
	assertCandidateNames(t, groups[2].Candidates, []string{"C first", "C second"})
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
