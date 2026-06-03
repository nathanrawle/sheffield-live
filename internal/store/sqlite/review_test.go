package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

const (
	testSupportingSourceName = "Fixture supporting manual ingest"
	testSupportingSourceURL  = "https://fixture.example.test/listings/"
)

type testSupportingSourceMetadata struct{}

func (testSupportingSourceMetadata) OwnedVenueSlugForSource(string) string {
	return ""
}

func (testSupportingSourceMetadata) ReviewStageSourceNameForSource(source string) string {
	if source == "fixture-supporting" {
		return testSupportingSourceName
	}
	return ""
}

func (testSupportingSourceMetadata) OwnedVenueSlugForReviewStageSourceName(string) string {
	return ""
}

func (testSupportingSourceMetadata) NonAuthoritativeSingletonVenueSlugForSource(source string) string {
	if source == "fixture-supporting" {
		return "leadmill"
	}
	return ""
}

func (testSupportingSourceMetadata) NonAuthoritativeSingletonVenueSlugForReviewStageSourceName(sourceName string) string {
	if sourceName == testSupportingSourceName {
		return "leadmill"
	}
	return ""
}

func (testSupportingSourceMetadata) GuardedNearMatchDisabledForSource(string) bool {
	return false
}

func (testSupportingSourceMetadata) GuardedNearMatchWindowForSource(string) time.Duration {
	return 0
}

func (testSupportingSourceMetadata) GuardedNearMatchDisabledForReviewStageSourceName(string) bool {
	return false
}

func (testSupportingSourceMetadata) GuardedNearMatchWindowForReviewStageSourceName(string) time.Duration {
	return 0
}

func (testSupportingSourceMetadata) ListingsURLForSourceName(sourceName string) string {
	if sourceName == testSupportingSourceName {
		return testSupportingSourceURL
	}
	return ""
}

func TestShouldReplaceDescriptionPolicy(t *testing.T) {
	cases := []struct {
		name     string
		existing string
		incoming string
		want     bool
	}{
		{name: "blank existing clean incoming", existing: "", incoming: "A clean event description with useful details.", want: true},
		{name: "generated existing clean incoming", existing: "#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; } @media screen {}", incoming: "A clean event description with useful details.", want: true},
		{name: "clean existing clean incoming", existing: "Existing clean description.", incoming: "New clean description.", want: false},
		{name: "blank incoming", existing: "", incoming: "", want: false},
		{name: "generated incoming", existing: "", incoming: "#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; }", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldReplaceDescription(tc.existing, tc.incoming); got != tc.want {
				t.Fatalf("shouldReplaceDescription(%q, %q) = %v, want %v", tc.existing, tc.incoming, got, tc.want)
			}
		})
	}
}

func TestAuthoritativeDescriptionUsablePolicy(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "blank", value: "   ", want: false},
		{name: "first line", value: "First line", want: true},
		{name: "generated block", value: "#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; }", want: false},
		{name: "generated media query", value: "@media screen {}", want: false},
		{name: "generated script", value: "<script>alert('x')</script>", want: false},
		{name: "buy tickets", value: "buy tickets", want: false},
		{name: "basement buy tickets", value: "basement buy tickets", want: false},
		{name: "tickets", value: "tickets", want: false},
		{name: "book tickets", value: "book tickets", want: false},
		{name: "read more", value: "read more", want: false},
		{name: "find out more", value: "find out more", want: false},
		{name: "more info", value: "more info", want: false},
		{name: "event details", value: "event details", want: false},
		{name: "click here", value: "click here", want: false},
		{name: "back to events", value: "back to events", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := authoritativeDescriptionUsable(tc.value); got != tc.want {
				t.Fatalf("authoritativeDescriptionUsable(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestRepairEventDescriptionsFromReportUpdatesOnlyEligibleDescriptions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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

	blankSlug := mustPromoteCafeNo9Event(t, st, "blank-eligible", "Blank Eligible", "2026-05-10T18:30:00Z", "")
	generatedSlug := mustPromoteCafeNo9Event(t, st, "generated-eligible", "Generated Eligible", "2026-05-11T18:30:00Z", "#block-d4b153a9777175667262 { --tweak-text-block-radius: 0px; } @media screen {}")
	cleanSlug := mustPromoteCafeNo9Event(t, st, "clean-preserved", "Clean Preserved", "2026-05-12T18:30:00Z", "Existing clean description.")
	beforeGroups := mustCount(t, db, "review_groups")
	beforeEvents := mustCount(t, db, "events")
	beforeRepairRuns := mustCount(t, db, "repair_runs")

	repair, err := st.RepairEventDescriptionsFromReport(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate("blank-eligible", "Blank Eligible", "2026-05-10T18:30:00Z", "Replacement description for the blank event."),
				cafeNo9RepairCandidate("generated-eligible", "Generated Eligible", "2026-05-11T18:30:00Z", "Replacement description for generated markup."),
				cafeNo9RepairCandidate("clean-preserved", "Clean Preserved", "2026-05-12T18:30:00Z", "Incoming description should not replace clean existing text."),
				cafeNo9RepairCandidate("new-skipped", "New Skipped", "2026-05-13T18:30:00Z", "No existing event should be created by repair."),
			},
		}},
	})
	if err != nil {
		t.Fatalf("repair descriptions: %v", err)
	}

	if repair.Repaired != 2 {
		t.Fatalf("repaired = %d, want 2", repair.Repaired)
	}
	if repair.Unchanged != 1 {
		t.Fatalf("unchanged = %d, want 1", repair.Unchanged)
	}
	if repair.Skipped != 1 {
		t.Fatalf("skipped = %d, want 1", repair.Skipped)
	}
	if repair.RepairRunID == 0 {
		t.Fatalf("repair run id = 0, want created repair run")
	}
	if got := mustCount(t, db, "repair_runs"); got != beforeRepairRuns+1 {
		t.Fatalf("repair runs = %d, want %d", got, beforeRepairRuns+1)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeGroups {
		t.Fatalf("review groups = %d, want unchanged %d", got, beforeGroups)
	}
	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events = %d, want unchanged %d", got, beforeEvents)
	}

	assertEventDescription(t, st, blankSlug, "Replacement description for the blank event.")
	assertEventDescription(t, st, generatedSlug, "Replacement description for generated markup.")
	assertEventDescription(t, st, cleanSlug, "Existing clean description.")
}

func TestRepairEventDescriptionsFromReportDryRunDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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

	slug := mustPromoteCafeNo9Event(t, st, "dry-run-eligible", "Dry Run Eligible", "2026-05-10T18:30:00Z", "")
	beforeRepairRuns := mustCount(t, db, "repair_runs")

	repair, err := st.RepairEventDescriptionsFromReportWithApply(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate("dry-run-eligible", "Dry Run Eligible", "2026-05-10T18:30:00Z", "Dry-run replacement description."),
			},
		}},
	}, false)
	if err != nil {
		t.Fatalf("dry-run repair descriptions: %v", err)
	}
	if !repair.DryRun || repair.Applied {
		t.Fatalf("dry/apply = %v/%v, want true/false", repair.DryRun, repair.Applied)
	}
	if repair.Repaired != 1 || repair.RepairRunID != 0 {
		t.Fatalf("repair = %#v, want one reported repair without repair run", repair)
	}
	assertEventDescription(t, st, slug, "")
	if got := mustCount(t, db, "repair_runs"); got != beforeRepairRuns {
		t.Fatalf("repair runs = %d, want unchanged %d", got, beforeRepairRuns)
	}
}

func TestRepairEventDescriptionsFromReportSkipsSameSlugUnderDifferentSource(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		uid     = "cross-source-slug"
		name    = "Cross Source Slug"
		startAt = "2026-05-14T18:30:00Z"
	)
	_ = mustEnsureSourceID(t, st, "Cafe No. 9 manual ingest", cafeNo9RepairSourceURL(uid))
	otherSourceID := mustEnsureSourceID(t, st, "Different manual ingest", "https://different.example.test/events")
	slug := mustLiveEventSlug(t, name, "cafe-no-9", startAt)
	mustInsertRepairLegacyEvent(t, db, otherSourceID, slug, "cafe-no-9", name, startAt, "")

	repair, err := st.RepairEventDescriptionsFromReport(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate(uid, name, startAt, "Replacement description for the cross-source slug case."),
			},
		}},
	})
	if err != nil {
		t.Fatalf("repair descriptions: %v", err)
	}

	if repair.Repaired != 0 || repair.Unchanged != 0 || repair.Skipped != 1 {
		t.Fatalf("repair counts = repaired %d unchanged %d skipped %d, want 0 0 1", repair.Repaired, repair.Unchanged, repair.Skipped)
	}
	assertEventDescription(t, st, slug, "")
}

func TestRepairEventDescriptionsFromReportSkipsSameFingerprintUnderDifferentSource(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		uid     = "cross-source-fingerprint"
		name    = "Cross Source Fingerprint"
		startAt = "2026-05-15T18:30:00Z"
	)
	_ = mustEnsureSourceID(t, st, "Cafe No. 9 manual ingest", cafeNo9RepairSourceURL(uid))
	otherSourceID := mustEnsureSourceID(t, st, "Different manual ingest", "https://different.example.test/events")
	mustInsertRepairLegacyEvent(t, db, otherSourceID, "legacy-cross-source-fingerprint", "cafe-no-9", name, startAt, "")

	repair, err := st.RepairEventDescriptionsFromReport(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate(uid, name, startAt, "Replacement description for the cross-source fingerprint case."),
			},
		}},
	})
	if err != nil {
		t.Fatalf("repair descriptions: %v", err)
	}

	if repair.Repaired != 0 || repair.Unchanged != 0 || repair.Skipped != 1 {
		t.Fatalf("repair counts = repaired %d unchanged %d skipped %d, want 0 0 1", repair.Repaired, repair.Unchanged, repair.Skipped)
	}
	assertEventDescription(t, st, "legacy-cross-source-fingerprint", "")
}

func TestRepairEventDescriptionsFromReportRepairsSameSourceLegacyEventWithoutLink(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		uid     = "same-source-legacy"
		name    = "Same Source Legacy"
		startAt = "2026-05-16T18:30:00Z"
	)
	sourceID := mustEnsureSourceID(t, st, "Cafe No. 9 manual ingest", cafeNo9RepairSourceURL(uid))
	slug := mustLiveEventSlug(t, name, "cafe-no-9", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, slug, "cafe-no-9", name, startAt, "")
	beforeLinks := mustCount(t, db, "event_source_links")

	repair, err := st.RepairEventDescriptionsFromReport(ctx, mustReviewCatalog(t), ingest.Report{
		Source:    ingest.CafeNo9Source,
		SourceURL: "https://www.wegottickets.com/Cafe9",
		Status:    "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{
				cafeNo9RepairCandidate(uid, name, startAt, "Replacement description for the same-source legacy event."),
			},
		}},
	})
	if err != nil {
		t.Fatalf("repair descriptions: %v", err)
	}

	if repair.Repaired != 1 || repair.Unchanged != 0 || repair.Skipped != 0 {
		t.Fatalf("repair counts = repaired %d unchanged %d skipped %d, want 1 0 0", repair.Repaired, repair.Unchanged, repair.Skipped)
	}
	assertEventDescription(t, st, slug, "Replacement description for the same-source legacy event.")
	if got := mustCount(t, db, "event_source_links"); got != beforeLinks {
		t.Fatalf("event_source_links rows = %d, want unchanged %d", got, beforeLinks)
	}
}

func TestRepairEventTitlesFromReportDryRunDoesNotMutateAuthoritativeEvent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		startAt   = "2026-05-10T18:30:00Z"
		source    = "Yellow Arch manual ingest"
		sourceURL = "https://www.yellowarch.com/event/late-junction/"
		dirty     = "Late Junction - Yellow Arch Studios"
		clean     = "Late Junction"
	)
	sourceID := mustEnsureSourceID(t, st, source, sourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "yellow-arch", startAt)
	cleanSlug := mustLiveEventSlug(t, clean, "yellow-arch", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "yellow-arch", dirty, startAt, "Existing description.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustReviewCatalog(t), yellowArchTitleRepairReport(startAt, dirty), false)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}

	if !repair.DryRun || repair.Applied {
		t.Fatalf("dry/apply flags = %v/%v, want true/false", repair.DryRun, repair.Applied)
	}
	if got, want := repair.Repaired, 1; got != want {
		t.Fatalf("repaired = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Result, "would_repair"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event review clusters = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair runs = %d, want 0", got)
	}
	if _, ok := st.EventBySlug(cleanSlug); ok {
		t.Fatalf("clean slug %q exists after dry run", cleanSlug)
	}
	event, ok := st.EventBySlug(dirtySlug)
	if !ok {
		t.Fatalf("missing dirty slug %q", dirtySlug)
	}
	if event.Name != dirty {
		t.Fatalf("name = %q, want %q", event.Name, dirty)
	}
}

func TestRepairEventTitlesFromReportDryRunDoesNotStageSupportingReview(t *testing.T) {
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
		startAt = "2026-05-10T18:30:00Z"
		dirty   = "Late Junction at The Leadmill"
	)
	sourceID := mustEnsureSourceID(t, st, testSupportingSourceName, testSupportingSourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Existing description.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), false)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if !repair.DryRun || repair.Applied {
		t.Fatalf("dry/apply flags = %v/%v, want true/false", repair.DryRun, repair.Applied)
	}
	if got, want := repair.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event review clusters created = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Result, "would_create_event_review"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if repair.RepairRunID != 0 {
		t.Fatalf("repair run id = %d, want 0", repair.RepairRunID)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event review clusters = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair runs = %d, want 0", got)
	}
	event, ok := st.EventBySlug(dirtySlug)
	if !ok {
		t.Fatalf("missing dirty slug %q", dirtySlug)
	}
	if event.Name != dirty {
		t.Fatalf("name = %q, want %q", event.Name, dirty)
	}
}

func TestRepairEventTitlesFromReportAppliesAuthoritativeLegacyEvent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		startAt   = "2026-05-10T18:30:00Z"
		source    = "Yellow Arch manual ingest"
		sourceURL = "https://www.yellowarch.com/event/late-junction/"
		dirty     = "Late Junction - Yellow Arch Studios"
		clean     = "Late Junction"
	)
	sourceID := mustEnsureSourceID(t, st, source, sourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "yellow-arch", startAt)
	cleanSlug := mustLiveEventSlug(t, clean, "yellow-arch", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "yellow-arch", dirty, startAt, "Existing description.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustReviewCatalog(t), yellowArchTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}

	if repair.DryRun || !repair.Applied {
		t.Fatalf("dry/apply flags = %v/%v, want false/true", repair.DryRun, repair.Applied)
	}
	if got, want := repair.Repaired, 1; got != want {
		t.Fatalf("repaired = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Result, "repaired"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if repair.RepairRunID != 0 {
		t.Fatalf("repair run id = %d, want 0", repair.RepairRunID)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event review clusters = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair runs = %d, want 0", got)
	}
	if _, ok := st.EventBySlug(dirtySlug); ok {
		t.Fatalf("dirty slug %q still exists", dirtySlug)
	}
	event, ok := st.EventBySlug(cleanSlug)
	if !ok {
		t.Fatalf("missing clean slug %q", cleanSlug)
	}
	if event.Name != clean {
		t.Fatalf("name = %q, want %q", event.Name, clean)
	}
	if got := mustCount(t, db, "event_source_links"); got != 1 {
		t.Fatalf("event source links = %d, want 1", got)
	}
}

func TestTitleRepairMatchersIgnoreWithheldRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		startAt   = "2026-05-10T18:30:00Z"
		source    = "Yellow Arch manual ingest"
		sourceURL = "https://www.yellowarch.com/events/"
		title     = "Late Junction"
	)
	sourceID := mustEnsureSourceID(t, st, source, sourceURL)
	slug := mustLiveEventSlug(t, title, "yellow-arch", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, slug, "yellow-arch", title, startAt, "Withheld duplicate.")
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?, withheld_reason = ?
		WHERE slug = ?
	`, string(domain.PublicationStateWithheld), "duplicate listing", slug); err != nil {
		t.Fatalf("withhold title repair target: %v", err)
	}

	incomingStart, err := time.Parse(time.RFC3339, startAt)
	if err != nil {
		t.Fatalf("parse start time: %v", err)
	}
	incoming := domain.Event{
		Name:      title,
		VenueSlug: "yellow-arch",
		Start:     incomingStart,
		Origin:    domain.OriginLive,
	}
	if records, err := matchingLiveEventRecordsByCleanTitleTx(ctx, db, incoming); err != nil {
		t.Fatalf("clean-title matcher: %v", err)
	} else if len(records) != 0 {
		t.Fatalf("clean-title matcher returned withheld event: %+v", records)
	}
	if records, err := matchingLiveEventRecordsByCleanTitleAndSourceTx(ctx, db, sourceID, incoming); err != nil {
		t.Fatalf("clean-title source matcher: %v", err)
	} else if len(records) != 0 {
		t.Fatalf("clean-title source matcher returned withheld event: %+v", records)
	}
}

func TestRepairEventTitlesFromReportUsesAuthoritativeSourceForLinkedICS(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		startAt   = "2026-05-10T18:30:00Z"
		source    = "The Leadmill manual ingest"
		feedURL   = "https://leadmill.co.uk/listings/?ical=1"
		detailURL = "https://leadmill.co.uk/event/feed-detail/"
		uid       = "leadmill-feed-1"
		dirty     = "Feed Detail - The Leadmill"
		clean     = "Feed Detail"
	)
	sourceID := mustEnsureSourceID(t, st, source, feedURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	cleanSlug := mustLiveEventSlug(t, clean, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Existing description.")
	mustInsertAuthoritativeSourceLink(t, db, dirtySlug, source, feedURL, uid)

	repair, err := st.RepairEventTitlesFromReport(ctx, mustReviewCatalog(t), leadmillTitleRepairReport(startAt, uid, dirty, detailURL), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}

	if got, want := repair.Repaired, 1; got != want {
		t.Fatalf("repaired = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Result, "repaired"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if got, want := repair.Changes[0].MatchKind, "authoritative_link"; got != want {
		t.Fatalf("match kind = %q, want %q", got, want)
	}
	if got, want := repair.Changes[0].SourceURL, feedURL; got != want {
		t.Fatalf("repair source URL = %q, want authoritative feed %q", got, want)
	}
	if _, ok := st.EventBySlug(dirtySlug); ok {
		t.Fatalf("dirty slug %q still exists", dirtySlug)
	}
	event, ok := st.EventBySlug(cleanSlug)
	if !ok {
		t.Fatalf("missing clean slug %q", cleanSlug)
	}
	if event.Name != clean {
		t.Fatalf("name = %q, want %q", event.Name, clean)
	}
	if got := mustCount(t, db, "event_source_links"); got != 3 {
		t.Fatalf("event source links = %d, want 3", got)
	}

	rows, err := db.Query(`
		SELECT s.url, l.source_event_key
		FROM events e
		JOIN event_source_links l ON l.event_id = e.id
		JOIN sources s ON s.id = l.source_id
		WHERE e.slug = ?
	`, cleanSlug)
	if err != nil {
		t.Fatalf("load repaired event source links: %v", err)
	}
	defer rows.Close()

	linkedKeys := make(map[string]bool)
	for rows.Next() {
		var linkedSourceURL string
		var linkedSourceEventKey string
		if err := rows.Scan(&linkedSourceURL, &linkedSourceEventKey); err != nil {
			t.Fatalf("scan repaired event source link: %v", err)
		}
		if linkedSourceURL != feedURL {
			t.Fatalf("linked source URL = %q, want %q", linkedSourceURL, feedURL)
		}
		linkedKeys[linkedSourceEventKey] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate repaired event source links: %v", err)
	}
	canonicalKey, ok := ingest.SourceIdentityKey(uid)
	if !ok {
		t.Fatalf("canonical source identity key for %q not available", uid)
	}
	detailKey, ok := ingest.SourceIdentityKey(detailURL)
	if !ok {
		t.Fatalf("canonical source identity key for %q not available", detailURL)
	}
	for _, want := range []string{uid, canonicalKey, detailKey} {
		if !linkedKeys[want] {
			t.Fatalf("missing linked source event key %q in %#v", want, linkedKeys)
		}
	}
}

func TestRepairEventTitlesFromReportSkipsWhenAuthoritativeSourceLinkIsAmbiguous(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		startAt   = "2026-05-10T18:30:00Z"
		source    = "The Leadmill manual ingest"
		feedURL   = "https://leadmill.co.uk/listings/?ical=1"
		detailURL = "https://leadmill.co.uk/event/feed-detail/"
		uid       = "leadmill-feed-1"
		dirty     = "Feed Detail - The Leadmill"
	)
	sourceID := mustEnsureSourceID(t, st, source, feedURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Existing description.")
	var dirtyID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, dirtySlug).Scan(&dirtyID); err != nil {
		t.Fatalf("lookup dirty event id: %v", err)
	}
	otherSlug := mustLiveEventSlug(t, "Linked Other Event", "leadmill", "2026-05-10T19:30:00Z")
	mustInsertRepairLegacyEvent(t, db, sourceID, otherSlug, "leadmill", "Linked Other Event", "2026-05-10T19:30:00Z", "Other description.")
	var otherID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, otherSlug).Scan(&otherID); err != nil {
		t.Fatalf("lookup other event id: %v", err)
	}

	uidKey, ok := ingest.SourceIdentityKey(uid)
	if !ok {
		t.Fatalf("normalize uid %q", uid)
	}
	urlKey, ok := ingest.SourceIdentityKey(detailURL)
	if !ok {
		t.Fatalf("normalize detail url %q", detailURL)
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			event_id,
			source_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 1, ?, ?)
	`, dirtyID, sourceID, uidKey, "2026-05-09T10:00:00Z", "2026-05-09T10:00:00Z"); err != nil {
		t.Fatalf("insert uid source link: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			event_id,
			source_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 1, ?, ?)
	`, otherID, sourceID, urlKey, "2026-05-09T10:00:00Z", "2026-05-09T10:00:00Z"); err != nil {
		t.Fatalf("insert url source link: %v", err)
	}

	repair, err := st.RepairEventTitlesFromReport(ctx, mustReviewCatalog(t), leadmillTitleRepairReport(startAt, uid, dirty, detailURL), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if got, want := repair.Skipped, 1; got != want {
		t.Fatalf("skipped = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Reason, "ambiguous authoritative match"; got != want {
		t.Fatalf("reason = %q, want %q", got, want)
	}
	if event, ok := st.EventBySlug(dirtySlug); !ok {
		t.Fatalf("missing dirty slug %q", dirtySlug)
	} else if event.Name != dirty {
		t.Fatalf("dirty event name = %q, want %q", event.Name, dirty)
	}
	var dirtyLinkEventID int64
	if err := db.QueryRow(`SELECT event_id FROM event_source_links WHERE source_id = ? AND source_event_key = ?`, sourceID, uidKey).Scan(&dirtyLinkEventID); err != nil {
		t.Fatalf("lookup uid source link: %v", err)
	}
	if got, want := dirtyLinkEventID, dirtyID; got != want {
		t.Fatalf("uid source link event_id = %d, want %d", got, want)
	}
	var otherLinkEventID int64
	if err := db.QueryRow(`SELECT event_id FROM event_source_links WHERE source_id = ? AND source_event_key = ?`, sourceID, urlKey).Scan(&otherLinkEventID); err != nil {
		t.Fatalf("lookup url source link: %v", err)
	}
	if got, want := otherLinkEventID, otherID; got != want {
		t.Fatalf("url source link event_id = %d, want %d", got, want)
	}
}

func TestRepairEventTitlesFromReportStagesSupportingReview(t *testing.T) {
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
		startAt = "2026-05-10T18:30:00Z"
		dirty   = "Late Junction at The Leadmill"
	)
	sourceID := mustEnsureSourceID(t, st, testSupportingSourceName, testSupportingSourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Existing description.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if got, want := repair.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event review clusters created = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Result, "event_review_created"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if repair.RepairRunID <= 0 {
		t.Fatal("missing repair run id")
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 1 {
		t.Fatalf("event review clusters = %d, want 1", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review groups = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 1 {
		t.Fatalf("repair runs = %d, want 1", got)
	}
	clusterID := repair.Changes[0].EventReviewClusterID
	cluster, ok, err := st.LoadEventReviewCluster(ctx, clusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("event review cluster not found")
	}
	if cluster.Summary.CanonicalEventID == nil {
		t.Fatal("missing canonical event id")
	}
	if got, want := cluster.Summary.StagingKeyVersion, eventTitleRepairStagingKeyVersion; got != want {
		t.Fatalf("staging key version = %d, want %d", got, want)
	}
	if cluster.Summary.StagingKey == nil || !strings.HasPrefix(*cluster.Summary.StagingKey, "title-repair:") {
		t.Fatalf("staging key = %v, want title-repair prefix", cluster.Summary.StagingKey)
	}
	if got, want := cluster.Summary.ConflictType, eventTitleRepairConflictType; got != want {
		t.Fatalf("conflict type = %q, want %q", got, want)
	}
	if got, want := cluster.Summary.ConflictReason, eventTitleRepairConflictReasonSupportingCleanTitle; got != want {
		t.Fatalf("conflict reason = %q, want %q", got, want)
	}
	if *cluster.Summary.CanonicalEventID != repair.Changes[0].EventID {
		t.Fatalf("canonical event id = %v, want %d", cluster.Summary.CanonicalEventID, repair.Changes[0].EventID)
	}
	if cluster.Summary.LatestRepairRunID == nil || *cluster.Summary.LatestRepairRunID != repair.RepairRunID {
		t.Fatalf("latest repair run id = %v, want %d", cluster.Summary.LatestRepairRunID, repair.RepairRunID)
	}
	if cluster.Summary.Status != seedstore.EventReviewClusterStatusOpen {
		t.Fatalf("cluster status = %q, want open", cluster.Summary.Status)
	}
	if got, want := len(cluster.Evidence), 2; got != want {
		t.Fatalf("cluster evidence = %d, want %d", got, want)
	}
	if got, want := len(cluster.CanonicalChoices), 3; got != want {
		t.Fatalf("canonical choices = %d, want %d", got, want)
	}
	if got, want := len(cluster.DraftChoices), 2; got != want {
		t.Fatalf("draft choices = %d, want %d", got, want)
	}
	if got, want := len(cluster.LiveActions), 0; got != want {
		t.Fatalf("live actions = %d, want %d", got, want)
	}
	event, ok := st.EventBySlug(dirtySlug)
	if !ok {
		t.Fatalf("missing dirty slug %q", dirtySlug)
	}
	if event.Name != dirty {
		t.Fatalf("name = %q, want unchanged %q", event.Name, dirty)
	}

	firstRepairRunID := repair.RepairRunID
	repair, err = st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles again: %v", err)
	}
	if got, want := repair.EventReviewClustersReused, 1; got != want {
		t.Fatalf("event review clusters reused = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Result, "event_review_reused"; got != want {
		t.Fatalf("rerun result = %q, want %q", got, want)
	}
	if repair.RepairRunID == 0 || repair.RepairRunID == firstRepairRunID {
		t.Fatal("missing distinct rerun repair id")
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 1 {
		t.Fatalf("event review clusters after rerun = %d, want 1", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 2 {
		t.Fatalf("repair runs after rerun = %d, want 2", got)
	}
}

func TestRepairEventTitlesFromReportJSONUsesEventReviewClusterFields(t *testing.T) {
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
		startAt = "2026-05-10T18:30:00Z"
		dirty   = "Late Junction at The Leadmill"
	)
	sourceID := mustEnsureSourceID(t, st, testSupportingSourceName, testSupportingSourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Existing description.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if repair.Changes[0].EventReviewClusterID == 0 {
		t.Fatal("missing staged event-review cluster id")
	}

	raw, err := json.Marshal(repair)
	if err != nil {
		t.Fatalf("marshal title repair report: %v", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode title repair report: %v", err)
	}
	for _, key := range []string{"review_groups_created", "review_groups_reused", "review_group_id", "review_created", "review_reused"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("title repair report contains legacy key %q: %s", key, raw)
		}
	}
	if got, want := string(payload["event_review_clusters_created"]), "1"; got != want {
		t.Fatalf("event_review_clusters_created = %s, want %s", got, want)
	}

	var changes []map[string]json.RawMessage
	if err := json.Unmarshal(payload["changes"], &changes); err != nil {
		t.Fatalf("decode title repair changes: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	for _, key := range []string{"review_group_id", "review_created", "review_reused"} {
		if _, ok := changes[0][key]; ok {
			t.Fatalf("title repair change contains legacy key %q: %s", key, raw)
		}
	}
	var clusterID int64
	if err := json.Unmarshal(changes[0]["event_review_cluster_id"], &clusterID); err != nil {
		t.Fatalf("decode event_review_cluster_id: %v", err)
	}
	if got, want := clusterID, repair.Changes[0].EventReviewClusterID; got != want {
		t.Fatalf("event_review_cluster_id = %d, want %d", got, want)
	}
	var status string
	if err := json.Unmarshal(changes[0]["event_review_cluster_status"], &status); err != nil {
		t.Fatalf("decode event_review_cluster_status: %v", err)
	}
	if got, want := status, string(seedstore.EventReviewClusterStatusOpen); got != want {
		t.Fatalf("event_review_cluster_status = %q, want %q", got, want)
	}
}

func TestRepairEventTitlesFromReportStagesSupportingReviewWhenCleanDuplicateExists(t *testing.T) {
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
		startAt = "2026-05-10T18:30:00Z"
		dirty   = "Late Junction at The Leadmill"
		clean   = "Late Junction"
	)
	sourceID := mustEnsureSourceID(t, st, testSupportingSourceName, testSupportingSourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	cleanSlug := mustLiveEventSlug(t, clean, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Dirty duplicate.")
	mustInsertRepairLegacyEvent(t, db, sourceID, cleanSlug, "leadmill", clean, startAt, "Clean duplicate.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if got, want := repair.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event review clusters created = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Result, "event_review_created"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if repair.RepairRunID <= 0 {
		t.Fatal("missing repair run id")
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 1 {
		t.Fatalf("event review clusters = %d, want 1", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review groups = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 1 {
		t.Fatalf("repair runs = %d, want 1", got)
	}

	cluster, ok, err := st.LoadEventReviewCluster(ctx, repair.Changes[0].EventReviewClusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("event review cluster not found")
	}
	if cluster.Summary.CanonicalEventID == nil {
		t.Fatal("missing canonical event id")
	}
	if got, want := cluster.Summary.StagingKeyVersion, eventTitleRepairStagingKeyVersion; got != want {
		t.Fatalf("staging key version = %d, want %d", got, want)
	}
	if cluster.Summary.StagingKey == nil || !strings.HasPrefix(*cluster.Summary.StagingKey, "title-repair:") {
		t.Fatalf("staging key = %v, want title-repair prefix", cluster.Summary.StagingKey)
	}
	if got, want := cluster.Summary.ConflictType, eventTitleRepairConflictType; got != want {
		t.Fatalf("conflict type = %q, want %q", got, want)
	}
	if got, want := cluster.Summary.ConflictReason, eventTitleRepairConflictReasonSupportingCleanTitle; got != want {
		t.Fatalf("conflict reason = %q, want %q", got, want)
	}
	if *cluster.Summary.CanonicalEventID != repair.Changes[0].EventID {
		t.Fatalf("canonical event id = %v, want %d", cluster.Summary.CanonicalEventID, repair.Changes[0].EventID)
	}
	if cluster.Summary.LatestRepairRunID == nil || *cluster.Summary.LatestRepairRunID != repair.RepairRunID {
		t.Fatalf("latest repair run id = %v, want %d", cluster.Summary.LatestRepairRunID, repair.RepairRunID)
	}
	if got, want := len(cluster.Evidence), 3; got != want {
		t.Fatalf("cluster evidence = %d, want %d", got, want)
	}
	if got, want := len(cluster.CanonicalChoices), 3; got != want {
		t.Fatalf("canonical choices = %d, want %d", got, want)
	}
	if got, want := len(cluster.DraftChoices), 2; got != want {
		t.Fatalf("draft choices = %d, want %d", got, want)
	}

	var duplicateEvidenceCount int
	for _, evidence := range cluster.Evidence {
		if evidence.EventSlug == cleanSlug {
			duplicateEvidenceCount++
		}
	}
	if duplicateEvidenceCount != 1 {
		t.Fatalf("clean duplicate evidence count = %d, want 1", duplicateEvidenceCount)
	}
}

func TestRepairEventTitlesFromReportSkipsSeparatedSupportingSlugConflict(t *testing.T) {
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
		startAt      = "2026-05-10T18:30:00Z"
		cleanStartAt = "2026-05-11T18:30:00Z"
		dirty        = "Late Junction at The Leadmill"
		clean        = "Late Junction"
	)
	sourceID := mustEnsureSourceID(t, st, testSupportingSourceName, testSupportingSourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	cleanSlug := mustLiveEventSlug(t, clean, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Dirty duplicate.")
	mustInsertRepairLegacyEvent(t, db, sourceID, cleanSlug, "leadmill", clean, cleanStartAt, "Clean duplicate at another time.")
	dirtyID := mustEventIDBySlug(t, db, dirtySlug)
	cleanID := mustEventIDBySlug(t, db, cleanSlug)
	if _, err := insertEventReviewSeparation(t, db,
		seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(dirtyID),
			EventID: int64Ptr(dirtyID),
		},
		seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(cleanID),
			EventID: int64Ptr(cleanID),
		},
		true,
		"title repair separated slug conflict",
		time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert title repair separation: %v", err)
	}

	repair, err := st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if got, want := repair.EventReviewClustersCreated, 0; got != want {
		t.Fatalf("event review clusters created = %d, want %d", got, want)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event review clusters = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair runs = %d, want 0", got)
	}
	if len(repair.Changes) != 1 || repair.Changes[0].Result != "skipped" || repair.Changes[0].Reason != "target slug conflict is already marked separate" {
		t.Fatalf("repair changes = %#v", repair.Changes)
	}
}

func TestRepairEventTitlesFromReportSharesOneRepairRunAcrossMultipleStages(t *testing.T) {
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
		startAtA = "2026-05-10T18:30:00Z"
		startAtB = "2026-05-10T20:30:00Z"
		dirtyA   = "Late Junction at The Leadmill"
		dirtyB   = "Another Late Junction at The Leadmill"
	)
	sourceID := mustEnsureSourceID(t, st, testSupportingSourceName, testSupportingSourceURL)
	dirtySlugA := mustLiveEventSlug(t, dirtyA, "leadmill", startAtA)
	dirtySlugB := mustLiveEventSlug(t, dirtyB, "leadmill", startAtB)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlugA, "leadmill", dirtyA, startAtA, "Existing description A.")
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlugB, "leadmill", dirtyB, startAtB, "Existing description B.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReportPair(startAtA, dirtyA, startAtB, dirtyB), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if repair.RepairRunID <= 0 {
		t.Fatal("missing repair run id")
	}
	if got, want := repair.EventReviewClustersCreated, 2; got != want {
		t.Fatalf("event review clusters created = %d, want %d", got, want)
	}
	if got := mustCount(t, db, "repair_runs"); got != 1 {
		t.Fatalf("repair runs = %d, want 1", got)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 2 {
		t.Fatalf("event review clusters = %d, want 2", got)
	}
	for _, change := range repair.Changes {
		cluster, ok, err := st.LoadEventReviewCluster(ctx, change.EventReviewClusterID)
		if err != nil {
			t.Fatalf("load event review cluster %d: %v", change.EventReviewClusterID, err)
		}
		if !ok {
			t.Fatalf("event review cluster %d not found", change.EventReviewClusterID)
		}
		if cluster.Summary.LatestRepairRunID == nil || *cluster.Summary.LatestRepairRunID != repair.RepairRunID {
			t.Fatalf("cluster %d latest repair run id = %v, want %d", change.EventReviewClusterID, cluster.Summary.LatestRepairRunID, repair.RepairRunID)
		}
	}
}

func TestRepairEventTitlesFromReportStagesAuthoritativeReviewWhenCleanDuplicateExists(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		startAt   = "2026-05-10T18:30:00Z"
		source    = "Yellow Arch manual ingest"
		sourceURL = "https://www.yellowarch.com/event/late-junction/"
		dirty     = "Late Junction - Yellow Arch Studios"
		clean     = "Late Junction"
	)
	sourceID := mustEnsureSourceID(t, st, source, sourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "yellow-arch", startAt)
	cleanSlug := mustLiveEventSlug(t, clean, "yellow-arch", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "yellow-arch", dirty, startAt, "Dirty duplicate.")
	mustInsertRepairLegacyEvent(t, db, sourceID, cleanSlug, "yellow-arch", clean, startAt, "Clean duplicate.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustReviewCatalog(t), yellowArchTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if got, want := repair.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event review clusters created = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Result, "event_review_created"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
	if repair.RepairRunID <= 0 {
		t.Fatal("missing repair run id")
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 1 {
		t.Fatalf("event review clusters = %d, want 1", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review groups = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 1 {
		t.Fatalf("repair runs = %d, want 1", got)
	}
	cluster, ok, err := st.LoadEventReviewCluster(ctx, repair.Changes[0].EventReviewClusterID)
	if err != nil {
		t.Fatalf("load event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("event review cluster not found")
	}
	if got, want := len(cluster.Evidence), 3; got != want {
		t.Fatalf("cluster evidence = %d, want %d", got, want)
	}
	if got, want := len(cluster.CanonicalChoices), 3; got != want {
		t.Fatalf("canonical choices = %d, want %d", got, want)
	}
	if got, want := len(cluster.DraftChoices), 2; got != want {
		t.Fatalf("draft choices = %d, want %d", got, want)
	}
	foundConflictEvidence := false
	for _, evidence := range cluster.Evidence {
		if evidence.EventSlug == cleanSlug {
			foundConflictEvidence = true
			break
		}
	}
	if !foundConflictEvidence {
		t.Fatal("missing clean conflict evidence in event review cluster")
	}
	if cluster.Summary.LatestRepairRunID == nil || *cluster.Summary.LatestRepairRunID != repair.RepairRunID {
		t.Fatalf("latest repair run id = %v, want %d", cluster.Summary.LatestRepairRunID, repair.RepairRunID)
	}
}

func TestRepairEventTitlesFromReportReusesTerminalEventReviewClusterWithoutMutation(t *testing.T) {
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
		startAt = "2026-05-10T18:30:00Z"
		dirty   = "Late Junction at The Leadmill"
	)
	sourceID := mustEnsureSourceID(t, st, testSupportingSourceName, testSupportingSourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Existing description.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("initial repair event titles: %v", err)
	}
	clusterID := repair.Changes[0].EventReviewClusterID
	before, ok, err := st.LoadEventReviewCluster(ctx, clusterID)
	if err != nil {
		t.Fatalf("load initial event review cluster: %v", err)
	}
	if !ok {
		t.Fatal("initial event review cluster not found")
	}
	if before.Summary.Status != seedstore.EventReviewClusterStatusOpen {
		t.Fatalf("cluster status = %q, want open", before.Summary.Status)
	}
	beforeUpdatedAt := before.Summary.UpdatedAt
	beforeEvidenceCount := len(before.Evidence)
	beforeCanonicalChoices := len(before.CanonicalChoices)
	beforeDraftChoices := len(before.DraftChoices)
	beforeLiveActions := len(before.LiveActions)

	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET status = ?
		WHERE id = ?
	`, string(seedstore.EventReviewClusterStatusResolved), clusterID); err != nil {
		t.Fatalf("close cluster: %v", err)
	}
	insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")

	repair, err = st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("rerun repair event titles: %v", err)
	}
	if got, want := repair.Changes[0].Result, "event_review_terminal_reused"; got != want {
		t.Fatalf("rerun result = %q, want %q", got, want)
	}
	if got, want := repair.Changes[0].EventReviewClusterStatus, string(seedstore.EventReviewClusterStatusResolved); got != want {
		t.Fatalf("rerun cluster status = %q, want %q", got, want)
	}
	if got, want := repair.EventReviewClustersTerminalReused, 1; got != want {
		t.Fatalf("terminal reused count = %d, want %d", got, want)
	}
	if got := mustCount(t, db, "repair_runs"); got != 2 {
		t.Fatalf("repair runs after terminal reuse = %d, want 2", got)
	}
	after, ok, err := st.LoadEventReviewCluster(ctx, clusterID)
	if err != nil {
		t.Fatalf("reload terminal cluster: %v", err)
	}
	if !ok {
		t.Fatal("terminal event review cluster not found")
	}
	if after.Summary.Status != seedstore.EventReviewClusterStatusResolved {
		t.Fatalf("cluster status = %q, want resolved", after.Summary.Status)
	}
	if after.Summary.LatestRepairRunID == nil || *after.Summary.LatestRepairRunID != repair.RepairRunID {
		t.Fatalf("latest repair run id = %v, want %d", after.Summary.LatestRepairRunID, repair.RepairRunID)
	}
	if !after.Summary.UpdatedAt.Equal(beforeUpdatedAt) {
		t.Fatalf("updated_at = %s, want %s", after.Summary.UpdatedAt, beforeUpdatedAt)
	}
	if got, want := len(after.Evidence), beforeEvidenceCount; got != want {
		t.Fatalf("evidence count = %d, want %d", got, want)
	}
	if got, want := len(after.CanonicalChoices), beforeCanonicalChoices; got != want {
		t.Fatalf("canonical choices = %d, want %d", got, want)
	}
	if got, want := len(after.DraftChoices), beforeDraftChoices; got != want {
		t.Fatalf("draft choices = %d, want %d", got, want)
	}
	if got, want := len(after.LiveActions), beforeLiveActions; got != want {
		t.Fatalf("live actions = %d, want %d", got, want)
	}
}

func TestRepairEventTitlesFromReportSkipsAuthoritativeConflictWithDifferentIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
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
		startAt       = "2026-05-10T18:30:00Z"
		conflictStart = "2026-05-11T18:30:00Z"
		source        = "Yellow Arch manual ingest"
		sourceURL     = "https://www.yellowarch.com/event/late-junction/"
		dirty         = "Late Junction - Yellow Arch Studios"
		clean         = "Late Junction"
	)
	sourceID := mustEnsureSourceID(t, st, source, sourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "yellow-arch", startAt)
	cleanSlug := mustLiveEventSlug(t, clean, "yellow-arch", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "yellow-arch", dirty, startAt, "Dirty duplicate.")
	mustInsertRepairLegacyEvent(t, db, sourceID, cleanSlug, "yellow-arch", "Different Late Junction", conflictStart, "Different event.")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustReviewCatalog(t), yellowArchTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if got, want := repair.Skipped, 1; got != want {
		t.Fatalf("skipped = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Reason, "target slug already belongs to another event"; got != want {
		t.Fatalf("reason = %q, want %q", got, want)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event review clusters = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair runs = %d, want 0", got)
	}
	if _, ok := st.EventBySlug(dirtySlug); !ok {
		t.Fatalf("dirty slug %q should remain", dirtySlug)
	}
}

func TestRepairEventTitlesFromReportDoesNotStageSupportingReviewForAuthoritativeEvent(t *testing.T) {
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
		startAt = "2026-05-10T18:30:00Z"
		dirty   = "Late Junction at The Leadmill"
	)
	sourceID := mustEnsureSourceID(t, st, testSupportingSourceName, testSupportingSourceURL)
	dirtySlug := mustLiveEventSlug(t, dirty, "leadmill", startAt)
	mustInsertRepairLegacyEvent(t, db, sourceID, dirtySlug, "leadmill", dirty, startAt, "Existing description.")
	event, ok := st.EventBySlug(dirtySlug)
	if !ok {
		t.Fatalf("missing dirty slug %q", dirtySlug)
	}
	mustInsertAuthoritativeSourceLink(t, db, event.Slug, "External authoritative source", "https://authority.example.test/events", "authority-1")

	repair, err := st.RepairEventTitlesFromReport(ctx, mustSupportingReviewCatalog(t), supportingTitleRepairReport(startAt, dirty), true)
	if err != nil {
		t.Fatalf("repair event titles: %v", err)
	}
	if got, want := repair.Skipped, 1; got != want {
		t.Fatalf("skipped = %d, want %d", got, want)
	}
	if got, want := repair.Changes[0].Reason, "matched event has authoritative source link"; got != want {
		t.Fatalf("reason = %q, want %q", got, want)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event review clusters = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair runs = %d, want 0", got)
	}
}

func TestLoadEventPreservesExplicitZeroZeroFocus(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := mustEnsureReviewTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	eventSlug := "zero-focus-event"
	if _, err := st.db.Exec(`
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			last_checked_at,
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, eventSlug, venueID, sourceID, "Zero Focus Event", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Zero focus description", "", "", "", 0, 0, 0, 0, "2026-05-09T10:00:00Z", string(domain.OriginLive), string(domain.PublicationStateReviewed)); err != nil {
		t.Fatalf("insert event: %v", err)
	}

	event, ok, err := loadEventBySlug(ctx, st.db, eventSlug)
	if err != nil {
		t.Fatalf("load event: %v", err)
	}
	if !ok {
		t.Fatal("event not found")
	}
	if event.ImageFocusX != 0 || event.ImageFocusY != 0 {
		t.Fatalf("event focus = %d,%d, want 0,0", event.ImageFocusX, event.ImageFocusY)
	}
}

func TestUpsertEventTxPreservesExistingImageFieldsWhenImageURLBlank(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := mustEnsureReviewTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	slug := "review-upsert-blank-image"
	if _, err := st.db.Exec(`
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			last_checked_at,
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, "Original name", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Original description", "/media/events/original.jpg", "https://example.test/original.jpg", "Original alt", 640, 360, 25, 75, "2026-05-09T10:00:00Z", string(domain.OriginLive), string(domain.PublicationStateReviewed)); err != nil {
		t.Fatalf("insert event: %v", err)
	}

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err := upsertEventTx(ctx, tx, domain.Event{
		Slug:             slug,
		Name:             "Updated name",
		VenueSlug:        "leadmill",
		Start:            time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC),
		End:              time.Date(2026, time.May, 10, 22, 0, 0, 0, time.UTC),
		Genre:            "Indie",
		Status:           "Listed",
		Description:      "Updated description",
		ImageURL:         "",
		ImageSourceURL:   "",
		ImageAlt:         "",
		ImageWidth:       0,
		ImageHeight:      0,
		ImageFocusX:      0,
		ImageFocusY:      0,
		SourceName:       "Review test source",
		SourceURL:        "https://example.test/review-test",
		LastChecked:      time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateReviewed,
	}, time.Date(2026, time.May, 9, 10, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("upsert event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	assertEventImageFields(t, st.db, slug, "/media/events/original.jpg", "https://example.test/original.jpg", "Original alt", 640, 360, 25, 75)
}

func TestUpsertEventTxReplacesImageFieldsWhenImageURLNonEmpty(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	sourceID := mustEnsureReviewTestSource(t, st.db)
	venueID := lookupStoreVenueID(t, st.db, "leadmill")
	slug := "review-upsert-replace-image"
	if _, err := st.db.Exec(`
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			image_url,
			image_source_url,
			image_alt,
			image_width,
			image_height,
			image_focus_x,
			image_focus_y,
			last_checked_at,
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, "Original name", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Original description", "/media/events/original.jpg", "https://example.test/original.jpg", "Original alt", 640, 360, 25, 75, "2026-05-09T10:00:00Z", string(domain.OriginLive), string(domain.PublicationStateReviewed)); err != nil {
		t.Fatalf("insert event: %v", err)
	}

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err := upsertEventTx(ctx, tx, domain.Event{
		Slug:             slug,
		Name:             "Updated name",
		VenueSlug:        "leadmill",
		Start:            time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC),
		End:              time.Date(2026, time.May, 10, 22, 0, 0, 0, time.UTC),
		Genre:            "Indie",
		Status:           "Listed",
		Description:      "Updated description",
		ImageURL:         "/media/events/replaced.jpg",
		ImageSourceURL:   "https://example.test/replaced.jpg",
		ImageAlt:         "Replaced alt",
		ImageWidth:       1200,
		ImageHeight:      800,
		ImageFocusX:      0,
		ImageFocusY:      0,
		SourceName:       "Review test source",
		SourceURL:        "https://example.test/review-test",
		LastChecked:      time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateReviewed,
	}, time.Date(2026, time.May, 9, 10, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("upsert event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	assertEventImageFields(t, st.db, slug, "/media/events/replaced.jpg", "https://example.test/replaced.jpg", "Replaced alt", 1200, 800, 0, 0)
}

func assertEventImageFields(t *testing.T, db *sql.DB, slug, wantURL, wantSourceURL, wantAlt string, wantWidth, wantHeight, wantFocusX, wantFocusY int) {
	t.Helper()

	var gotURL, gotSourceURL, gotAlt string
	var gotWidth, gotHeight, gotFocusX, gotFocusY int
	if err := db.QueryRow(`
		SELECT image_url, image_source_url, image_alt, image_width, image_height, image_focus_x, image_focus_y
		FROM events
		WHERE slug = ?
	`, slug).Scan(&gotURL, &gotSourceURL, &gotAlt, &gotWidth, &gotHeight, &gotFocusX, &gotFocusY); err != nil {
		t.Fatalf("scan event image fields: %v", err)
	}
	if gotURL != wantURL || gotSourceURL != wantSourceURL || gotAlt != wantAlt || gotWidth != wantWidth || gotHeight != wantHeight || gotFocusX != wantFocusX || gotFocusY != wantFocusY {
		t.Fatalf("event image fields = %q,%q,%q,%d,%d,%d,%d, want %q,%q,%q,%d,%d,%d,%d", gotURL, gotSourceURL, gotAlt, gotWidth, gotHeight, gotFocusX, gotFocusY, wantURL, wantSourceURL, wantAlt, wantWidth, wantHeight, wantFocusX, wantFocusY)
	}
}

func TestPromoteSingletonReviewClusterIfMissingTreatsUniversityMultiVenueAsAuthoritative(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	insertLegacyVenue(t, st.db, "octagon-centre", "Octagon Centre", domain.OriginLive)

	sourceName := "University of Sheffield Performance Venues manual ingest"
	sourceURL := "https://performancevenues.group.shef.ac.uk/event/man-in-the-mirror/"
	slug, promoted, err := st.PromoteSingletonReviewClusterIfMissing(ctx, ingest.ReviewStageClusterInput{
		Title:      "University of Sheffield Performance Venues singleton",
		SourceName: sourceName,
		SourceURL:  sourceURL,
		Candidates: []review.CandidateInput{{
			ExternalID: "pv-1",
			Name:       "Man in the Mirror",
			VenueSlug:  "octagon-centre",
			StartAt:    "2026-08-07T19:30:00Z",
			Status:     "Listed",
			SourceName: sourceName,
			SourceURL:  sourceURL,
		}},
	})
	if err != nil {
		t.Fatalf("promote university singleton: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if slug == "" {
		t.Fatal("slug = empty, want promoted authoritative event")
	}

	event, ok := st.EventBySlug(slug)
	if !ok {
		t.Fatalf("event %q not found", slug)
	}
	if got, want := event.VenueSlug, "octagon-centre"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := event.SourceName, sourceName; got != want {
		t.Fatalf("source name = %q, want %q", got, want)
	}
	if got, want := event.PublicationState, domain.PublicationStateReviewed; got != want {
		t.Fatalf("publication state = %q, want %q", got, want)
	}
}

func TestPromoteSingletonReviewClusterIfMissingPromotesNetworkRoomUnderCanonicalVenue(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceName := "Network Sheffield manual ingest"
	sourceURL := "https://www.networksheffield.co.uk/event/godeth-network-sheffield/"
	sourceEventKey, ok := ingest.SourceIdentityKey(sourceURL)
	if !ok {
		t.Fatalf("source identity key not available for %q", sourceURL)
	}

	slug, promoted, err := st.PromoteSingletonReviewClusterIfMissing(ctx, ingest.ReviewStageClusterInput{
		Title:                       "Network Sheffield singleton",
		SourceName:                  sourceName,
		SourceURL:                   sourceURL,
		AuthoritativeSourceName:     sourceName,
		AuthoritativeSourceURL:      sourceURL,
		AuthoritativeSourceEventKey: sourceEventKey,
		Candidates: []review.CandidateInput{{
			ExternalID:       sourceURL,
			Name:             "GODETH",
			VenueSlug:        "network",
			VenueText:        "Network",
			VenueLocationRaw: "Network, 14 Matilda St, Sheffield City Centre, Sheffield S1, UK",
			RoomText:         "Network 3",
			Rooms: []domain.VenueRoom{{
				VenueSlug: "network",
				Slug:      "network-3",
				Name:      "Network 3",
			}},
			StartAt:     "2026-07-25T19:00:00Z",
			Status:      "Listed",
			Description: "Live music at Network.",
			SourceName:  sourceName,
			SourceURL:   sourceURL,
			CalendarURL: sourceURL,
		}},
	})
	if err != nil {
		t.Fatalf("promote network singleton: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	if slug == "" {
		t.Fatal("slug = empty, want promoted authoritative event")
	}

	venue, ok := st.VenueBySlug("network")
	if !ok {
		t.Fatal("network venue not found")
	}
	if got, want := venue.Name, "Network"; got != want {
		t.Fatalf("venue name = %q, want %q", got, want)
	}
	if got, want := venue.ValidationState, domain.ValidationStateProvisional; got != want {
		t.Fatalf("venue validation state = %q, want %q", got, want)
	}
	if _, ok := st.VenueBySlug("network-sheffield"); ok {
		t.Fatal("legacy network-sheffield venue was created")
	}

	event, ok := st.EventBySlug(slug)
	if !ok {
		t.Fatalf("event %q not found", slug)
	}
	if got, want := event.VenueSlug, "network"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
	if got, want := event.RoomText, "Network 3"; got != want {
		t.Fatalf("room text = %q, want %q", got, want)
	}
	if got, want := review.RoomSlugsValue(event.Rooms), "network-3"; got != want {
		t.Fatalf("room slugs = %q, want %q", got, want)
	}
	if len(event.Rooms) != 1 || event.Rooms[0].VenueSlug != "network" {
		t.Fatalf("event rooms = %#v, want one room under network", event.Rooms)
	}
	if got, want := event.SourceName, sourceName; got != want {
		t.Fatalf("source name = %q, want %q", got, want)
	}
	if got, want := event.PublicationState, domain.PublicationStateReviewed; got != want {
		t.Fatalf("publication state = %q, want %q", got, want)
	}
}

func TestPromoteSingletonReviewClusterIfMissingSkipsInferredAuthoritativeStart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceName := "Fixture authoritative manual ingest"
	sourceURL := "https://fixture.example.test/calendar.ics"
	beforeEvents := mustCount(t, st.db, "events")

	slug, promoted, err := st.PromoteSingletonReviewClusterIfMissing(ctx, ingest.ReviewStageClusterInput{
		Title:                       "Fixture authoritative singleton",
		SourceName:                  sourceName,
		SourceURL:                   sourceURL,
		AuthoritativeSourceName:     sourceName,
		AuthoritativeSourceURL:      sourceURL,
		AuthoritativeSourceEventKey: "uid:fixture-inferred",
		Candidates: []review.CandidateInput{{
			ExternalID:      "fixture-inferred",
			Name:            "Fixture Inferred",
			VenueSlug:       "leadmill",
			StartAt:         "2026-05-10T18:30:00Z",
			StartAtInferred: true,
			StartAtBasis:    "source fallback 19:30 Europe/London",
			Status:          "Listed",
			SourceName:      sourceName,
			SourceURL:       sourceURL,
		}},
	})
	if err != nil {
		t.Fatalf("promote inferred authoritative singleton: %v", err)
	}
	if promoted || slug != "" {
		t.Fatalf("promotion = (%q, %v), want no-op", slug, promoted)
	}
	if got := mustCount(t, st.db, "events"); got != beforeEvents {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents)
	}
}

func TestPromoteSingletonReviewClusterIfMissingSkipsInferredSupportingStart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceName := "Fixture supporting manual ingest"
	sourceURL := "https://fixture.example.test/listings/"
	beforeEvents := mustCount(t, st.db, "events")

	slug, promoted, err := st.PromoteSingletonReviewClusterIfMissing(ctx, ingest.ReviewStageClusterInput{
		Title:      "Fixture supporting singleton",
		SourceName: sourceName,
		SourceURL:  sourceURL,
		Candidates: []review.CandidateInput{{
			ExternalID:      "fixture-inferred",
			Name:            "Fixture Inferred",
			VenueSlug:       "leadmill",
			StartAt:         "2026-05-10T18:30:00Z",
			StartAtInferred: true,
			StartAtBasis:    "source fallback 19:30 Europe/London",
			Status:          "Listed",
			SourceName:      sourceName,
			SourceURL:       sourceURL,
		}},
	})
	if err != nil {
		t.Fatalf("promote inferred supporting singleton: %v", err)
	}
	if promoted || slug != "" {
		t.Fatalf("promotion = (%q, %v), want no-op", slug, promoted)
	}
	if got := mustCount(t, st.db, "events"); got != beforeEvents {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents)
	}
}

func mustReviewCatalog(t *testing.T) *ingest.Catalog {
	t.Helper()

	catalog, err := ingest.LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load repo catalog: %v", err)
	}
	return catalog
}

func mustSupportingReviewCatalog(t *testing.T) *ingest.Catalog {
	t.Helper()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "01-fixture-supporting.yaml"), []byte(`key: fixture-supporting
name: Fixture supporting listings
url: https://fixture.example.test/listings/
review_stage_source_name: Fixture supporting manual ingest
import_run_notes: fixture supporting ingest
non_authoritative_singleton_venue_slug: leadmill
mode: source_page
source_page:
  source_page_parser: jazz_at_the_lescar
`), 0o600); err != nil {
		t.Fatalf("write supporting source catalog: %v", err)
	}
	catalog, err := ingest.LoadCatalog(dir)
	if err != nil {
		t.Fatalf("load supporting source catalog: %v", err)
	}
	return catalog
}

func cafeNo9RepairSourceURL(uid string) string {
	return "https://www.wegottickets.com/event/" + uid
}

func mustPromoteCafeNo9Event(t *testing.T, st *Store, externalID, name, startAt, description string) string {
	t.Helper()

	sourceURL := cafeNo9RepairSourceURL(externalID)
	slug, promoted, err := st.PromoteSingletonReviewClusterIfMissing(context.Background(), ingest.ReviewStageClusterInput{
		Title:                  "Cafe No. 9 singleton",
		SourceName:             "Cafe No. 9 manual ingest",
		SourceURL:              sourceURL,
		AuthoritativeSourceURL: sourceURL,
		Candidates: []review.CandidateInput{{
			ExternalID:  externalID,
			Name:        name,
			VenueSlug:   "cafe-no-9",
			StartAt:     startAt,
			Status:      "Listed",
			Description: description,
			SourceName:  "Cafe No. 9 manual ingest",
			SourceURL:   sourceURL,
		}},
	})
	if err != nil {
		t.Fatalf("promote Cafe No. 9 event: %v", err)
	}
	if !promoted {
		t.Fatal("promoted = false, want true")
	}
	return slug
}

func cafeNo9RepairCandidate(uid, summary, startAt, description string) ingest.EventCandidate {
	return ingest.EventCandidate{
		UID:         uid,
		Summary:     summary,
		Description: description,
		Location:    "Cafe No. 9",
		URL:         cafeNo9RepairSourceURL(uid),
		Status:      "Listed",
		StartAt:     startAt,
	}
}

func yellowArchTitleRepairReport(startAt, summary string) ingest.Report {
	return ingest.Report{
		Source:      ingest.YellowArchSource,
		SourceURL:   "https://www.yellowarch.com/events/",
		ImportRunID: 42,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.yellowarch.com/events/",
			Candidates: []ingest.EventCandidate{{
				Summary:  summary,
				Location: "Yellow Arch Studios",
				URL:      "https://www.yellowarch.com/event/late-junction/",
				StartAt:  startAt,
				Status:   "Listed",
			}},
		}},
	}
}

func leadmillTitleRepairReport(startAt, uid, summary, detailURL string) ingest.Report {
	return ingest.Report{
		Source:      ingest.LeadmillSource,
		SourceURL:   "https://leadmill.co.uk/live/",
		ImportRunID: 44,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://leadmill.co.uk/listings/?ical=1",
			Candidates: []ingest.EventCandidate{{
				UID:      uid,
				Summary:  summary,
				Location: "The Leadmill",
				URL:      detailURL,
				StartAt:  startAt,
				Status:   "Listed",
			}},
		}},
	}
}

func assertEventSourceLinkKeySet(t *testing.T, db *sql.DB, eventSlug string, want []string) {
	t.Helper()

	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, eventSlug).Scan(&eventID); err != nil {
		t.Fatalf("lookup event id for %q: %v", eventSlug, err)
	}

	rows, err := db.Query(`
		SELECT source_event_key
		FROM event_source_links
		WHERE event_id = ?
	`, eventID)
	if err != nil {
		t.Fatalf("load event source links for %q: %v", eventSlug, err)
	}
	defer rows.Close()

	got := make(map[string]struct{})
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatalf("scan event source link for %q: %v", eventSlug, err)
		}
		got[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate event source links for %q: %v", eventSlug, err)
	}
	if gotCount, wantCount := len(got), len(want); gotCount != wantCount {
		t.Fatalf("event source link keys = %d, want %d (%#v)", gotCount, wantCount, got)
	}
	for _, wantKey := range want {
		if _, ok := got[wantKey]; !ok {
			t.Fatalf("missing event source link key %q in %#v", wantKey, got)
		}
	}
}

func supportingTitleRepairReport(startAt, summary string) ingest.Report {
	return ingest.Report{
		Source:      "fixture-supporting",
		SourceURL:   testSupportingSourceURL,
		ImportRunID: 43,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: testSupportingSourceURL,
			Candidates: []ingest.EventCandidate{{
				Summary:  summary,
				Location: "The Leadmill",
				URL:      testSupportingSourceURL + "#late-junction",
				StartAt:  startAt,
				Status:   "Listed",
			}},
		}},
	}
}

func supportingTitleRepairReportPair(startAtA, summaryA, startAtB, summaryB string) ingest.Report {
	return ingest.Report{
		Source:      "fixture-supporting",
		SourceURL:   testSupportingSourceURL,
		ImportRunID: 45,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{
			{
				URL: testSupportingSourceURL,
				Candidates: []ingest.EventCandidate{{
					Summary:  summaryA,
					Location: "The Leadmill",
					URL:      testSupportingSourceURL + "#pair-a",
					StartAt:  startAtA,
					Status:   "Listed",
				}},
			},
			{
				URL: testSupportingSourceURL,
				Candidates: []ingest.EventCandidate{{
					Summary:  summaryB,
					Location: "The Leadmill",
					URL:      testSupportingSourceURL + "#pair-b",
					StartAt:  startAtB,
					Status:   "Listed",
				}},
			},
		},
	}
}

func mustEnsureSourceID(t *testing.T, st *Store, sourceName, sourceURL string) int64 {
	t.Helper()

	sourceID, err := st.EnsureSource(context.Background(), sourceName, sourceURL)
	if err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	return sourceID
}

func mustLiveEventSlug(t *testing.T, name, venueSlug, startAt string) string {
	t.Helper()

	start, err := time.Parse(time.RFC3339, startAt)
	if err != nil {
		t.Fatalf("parse start time: %v", err)
	}
	slug, err := buildLiveEventSlug(name, venueSlug, start)
	if err != nil {
		t.Fatalf("build live event slug: %v", err)
	}
	return slug
}

func mustInsertRepairLegacyEvent(t *testing.T, db *sql.DB, sourceID int64, slug, venueSlug, name, startAt, description string) {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, venueSlug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			last_checked_at,
			origin
		) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, name, startAt, "Test", "Listed", description, "2026-05-01T10:00:00Z", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert repair legacy event: %v", err)
	}
}

func mustInsertAuthoritativeSourceLink(t *testing.T, db *sql.DB, eventSlug, sourceName, sourceURL, sourceEventKey string) {
	t.Helper()

	var eventID int64
	if err := db.QueryRow(`
		SELECT id
		FROM events
		WHERE slug = ?
	`, eventSlug).Scan(&eventID); err != nil {
		t.Fatalf("lookup event %q: %v", eventSlug, err)
	}
	if _, err := db.Exec(`
		INSERT OR IGNORE INTO sources (name, url)
		VALUES (?, ?)
	`, sourceName, sourceURL); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`
		SELECT id
		FROM sources
		WHERE name = ? AND url = ?
	`, sourceName, sourceURL).Scan(&sourceID); err != nil {
		t.Fatalf("lookup source: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			source_id,
			event_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 1, ?, ?)
	`, sourceID, eventID, sourceEventKey, "2026-05-01T10:00:00Z", "2026-05-01T10:00:00Z"); err != nil {
		t.Fatalf("insert event source link: %v", err)
	}
}

func assertEventDescription(t *testing.T, st *Store, slug, want string) {
	t.Helper()

	event, ok := st.EventBySlug(slug)
	if !ok {
		t.Fatalf("missing event %q", slug)
	}
	if event.Description != want {
		t.Fatalf("%s description = %q, want %q", slug, event.Description, want)
	}
}

func mustLoadSlugAlias(t *testing.T, db *sql.DB, aliasSlug string) (int64, string, int64) {
	t.Helper()

	var targetEventID int64
	var reason string
	var repairRunID sql.NullInt64
	if err := db.QueryRow(`
		SELECT target_event_id, COALESCE(reason, ''), repair_run_id
		FROM slug_aliases
		WHERE alias_slug = ?
			AND target_kind = ?
	`, aliasSlug, string(seedstore.SlugAliasTargetKindEvent)).Scan(&targetEventID, &reason, &repairRunID); err != nil {
		t.Fatalf("load slug alias %q: %v", aliasSlug, err)
	}
	if !repairRunID.Valid {
		return targetEventID, reason, 0
	}
	return targetEventID, reason, repairRunID.Int64
}

func mustCountExactIdentityRows(t *testing.T, db *sql.DB, eventID int64) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_exact_identities
		WHERE event_id = ?
			AND active = 1
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count exact identity rows for event %d: %v", eventID, err)
	}
	return count
}

type secondarySourceInfoRow struct {
	EventID   int64
	SourceID  int64
	VenueSlug string
	EventName string
	StartAt   string
	InfoType  string
	Value     string
}

func loadSecondarySourceInfoRows(t *testing.T, db *sql.DB) []secondarySourceInfoRow {
	t.Helper()

	rows, err := db.Query(`
		SELECT
			event_id,
			source_id,
			venue_slug,
			event_name,
			start_at,
			info_type,
			value
		FROM event_secondary_source_info
		ORDER BY info_type, id
	`)
	if err != nil {
		t.Fatalf("query secondary source info rows: %v", err)
	}
	defer rows.Close()

	var result []secondarySourceInfoRow
	for rows.Next() {
		var row secondarySourceInfoRow
		if err := rows.Scan(&row.EventID, &row.SourceID, &row.VenueSlug, &row.EventName, &row.StartAt, &row.InfoType, &row.Value); err != nil {
			t.Fatalf("scan secondary source info row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate secondary source info rows: %v", err)
	}
	return result
}

func mustEnsureReviewTestSource(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	if _, err := db.Exec(`
		INSERT OR IGNORE INTO sources (name, url)
		VALUES (?, ?)
	`, "Review test source", "https://example.test/review-test"); err != nil {
		t.Fatalf("insert review test source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`
		SELECT id
		FROM sources
		WHERE name = ? AND url = ?
	`, "Review test source", "https://example.test/review-test").Scan(&sourceID); err != nil {
		t.Fatalf("lookup review test source id: %v", err)
	}
	return sourceID
}

func mustInsertReviewTestEvent(t *testing.T, db *sql.DB, sourceID int64, slug, venueSlug, name string) int64 {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, venueSlug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id %q: %v", venueSlug, err)
	}
	res, err := db.Exec(`
		INSERT INTO events (
			slug,
			venue_id,
			source_id,
			name,
			start_at,
			end_at,
			genre,
			status,
			description,
			last_checked_at,
			origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, name, "2026-05-01T19:00:00Z", "2026-05-01T22:00:00Z", "Indie", "Listed", "Review test event", "2026-04-30T10:00:00Z", string(domain.OriginLive))
	if err != nil {
		t.Fatalf("insert review test event %q: %v", slug, err)
	}
	eventID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("review test event id: %v", err)
	}
	return eventID
}
