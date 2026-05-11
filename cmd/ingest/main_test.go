package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	"sheffield-live/internal/store/sqlite"
)

func testSourceCatalog(t *testing.T) *ingest.Catalog {
	t.Helper()
	catalog, err := ingest.LoadRepoCatalog()
	if err != nil {
		t.Fatalf("load repo catalog: %v", err)
	}
	return catalog
}

func TestCreateReviewGroupsFromReportStagesReviewGroups(t *testing.T) {
	st := &fakeReviewStageStore{results: []fakeReviewStageResult{
		{id: 101, created: true},
		{id: 102, created: false},
	}}
	report := successfulManualReportForReviewStage()

	stage, err := createReviewGroupsFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("stage review groups: %v", err)
	}

	if got, want := len(st.inputs), 2; got != want {
		t.Fatalf("created groups = %d, want %d", got, want)
	}
	if got, want := stage.GroupsCreated, 1; got != want {
		t.Fatalf("stage groups created = %d, want %d", got, want)
	}
	if got, want := stage.GroupsReused, 1; got != want {
		t.Fatalf("stage groups reused = %d, want %d", got, want)
	}
	if got, want := stage.CandidateCount, 3; got != want {
		t.Fatalf("stage candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.Groups), 2; got != want {
		t.Fatalf("stage groups = %d, want %d", got, want)
	}
	if got, want := stage.Groups[0].ID, int64(101); got != want {
		t.Fatalf("stage group ID = %d, want %d", got, want)
	}
	if got, want := stage.Groups[0].CandidateCount, 2; got != want {
		t.Fatalf("first stage group candidates = %d, want %d", got, want)
	}
	if got, want := stage.Groups[0].Result, "created"; got != want {
		t.Fatalf("first stage group result = %q, want %q", got, want)
	}
	if got, want := stage.Groups[1].ID, int64(102); got != want {
		t.Fatalf("second stage group ID = %d, want %d", got, want)
	}
	if got, want := stage.Groups[1].CandidateCount, 1; got != want {
		t.Fatalf("second stage group candidates = %d, want %d", got, want)
	}
	if got, want := stage.Groups[1].Result, "reused"; got != want {
		t.Fatalf("second stage group result = %q, want %q", got, want)
	}
	if got, want := st.inputs[0].Title, "Duplicate review: Duplicate one"; got != want {
		t.Fatalf("first staged title = %q, want %q", got, want)
	}
	if got, want := st.inputs[1].Title, "New listing review: Singleton"; got != want {
		t.Fatalf("second staged title = %q, want %q", got, want)
	}
}

func TestCreateReviewGroupsFromReportAutoPromotesOwnedVenueSingleton(t *testing.T) {
	st := &fakeReviewStageStore{
		promotionResults: []fakePromotionResult{{
			eventSlug: "live-late-junction-yellow-arch-20260510183000",
			promoted:  true,
		}},
	}
	report := ingest.Report{
		Source:      ingest.YellowArchSource,
		SourceURL:   "https://www.yellowarch.com/events/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.yellowarch.com/events/",
			Candidates: []ingest.EventCandidate{{
				UID:      "yellow-arch-1",
				Summary:  "Late Junction",
				Location: "Yellow Arch Studios",
				StartAt:  "2026-05-10T18:30:00Z",
				EndAt:    "2026-05-10T22:00:00Z",
			}},
		}},
	}

	stage, err := createReviewGroupsFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}

	if got, want := stage.CandidateCount, 1; got != want {
		t.Fatalf("candidate count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 0; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := stage.AutoPromotedCount, 1; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := len(stage.Groups), 0; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}
	if got, want := len(stage.AutoPromoted), 1; got != want {
		t.Fatalf("auto promoted = %d, want %d", got, want)
	}
	if got, want := stage.AutoPromoted[0].EventSlug, "live-late-junction-yellow-arch-20260510183000"; got != want {
		t.Fatalf("event slug = %q, want %q", got, want)
	}
	if got, want := len(st.inputs), 0; got != want {
		t.Fatalf("staged review groups = %d, want %d", got, want)
	}
	if got, want := len(st.promotedInputs), 1; got != want {
		t.Fatalf("promoted inputs = %d, want %d", got, want)
	}
}

func TestCreateReviewGroupsFromReportAutoPromotesCafeNo9SingletonWithoutEndTime(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	report := ingest.Report{
		Source:      ingest.CafeNo9Source,
		SourceURL:   "https://www.wegottickets.com/Cafe9",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{{
				UID:      "cafe-no-9-1",
				Summary:  "Cafe No. 9 Late Show",
				Location: "Cafe No. 9",
				StartAt:  "2026-05-10T18:30:00Z",
			}},
		}},
	}

	stage, err := createReviewGroupsFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}
	if got, want := stage.AutoPromotedCount, 1; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 0; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.Groups), 0; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}
	if got, want := len(stage.AutoPromoted), 1; got != want {
		t.Fatalf("auto promoted = %d, want %d", got, want)
	}

	event, ok := st.EventBySlug(stage.AutoPromoted[0].EventSlug)
	if !ok {
		t.Fatalf("missing published event %q", stage.AutoPromoted[0].EventSlug)
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for unknown end", event.End)
	}
}

func TestCreateReviewGroupsFromReportAutoPromotesJazzAtTheLescarSingleton(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	report := ingest.Report{
		Source:      ingest.JazzAtTheLescarSource,
		SourceURL:   "http://www.jazzatthelescar.com/index.html",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "http://www.jazzatthelescar.com/index.html",
			Candidates: []ingest.EventCandidate{{
				UID:      "jazz-lescar-1",
				Summary:  "Jazz at The Lescar Quartet",
				Location: "The Lescar",
				StartAt:  "2026-05-14T19:30:00Z",
				EndAt:    "2026-05-14T22:00:00Z",
			}},
		}},
	}

	stage, err := createReviewGroupsFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}
	if got, want := stage.AutoPromotedCount, 1; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 0; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.Groups), 0; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}

	event, ok := st.EventBySlug(stage.AutoPromoted[0].EventSlug)
	if !ok {
		t.Fatalf("missing published event %q", stage.AutoPromoted[0].EventSlug)
	}
	if got, want := event.VenueSlug, "lescar"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
}

func TestCreateReviewGroupsFromReportAutoPromotesSingletonAtNewProvisionalVenue(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	report := ingest.Report{
		Source:      ingest.TheGreystonesSource,
		SourceURL:   "https://www.mygreystones.co.uk/events/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.mygreystones.co.uk/events/",
			Candidates: []ingest.EventCandidate{{
				UID:         "new-venue-1",
				Summary:     "Offsite Roots Night",
				Location:    "Imaginary Hall",
				LocationRaw: "Imaginary Hall, 1 Void Street, Sheffield",
				StartAt:     "2026-05-16T19:30:00Z",
				EndAt:       "2026-05-16T22:00:00Z",
			}},
		}},
	}

	stage, err := createReviewGroupsFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}
	if got, want := stage.AutoPromotedCount, 1; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 0; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.Groups), 0; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}

	venue, ok := st.VenueBySlug("imaginary-hall")
	if !ok {
		t.Fatal("provisional venue not found")
	}
	if got, want := venue.ValidationState, domain.ValidationStateProvisional; got != want {
		t.Fatalf("venue validation state = %q, want %q", got, want)
	}

	event, ok := st.EventBySlug(stage.AutoPromoted[0].EventSlug)
	if !ok {
		t.Fatalf("missing published event %q", stage.AutoPromoted[0].EventSlug)
	}
	if got, want := event.VenueSlug, "imaginary-hall"; got != want {
		t.Fatalf("event venue slug = %q, want %q", got, want)
	}
}

func TestCreateReviewGroupsFromReportResolvesMatchingStaleCafeNo9Singleton(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	report := ingest.Report{
		Source:      ingest.CafeNo9Source,
		SourceURL:   "https://www.wegottickets.com/Cafe9",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.wegottickets.com/Cafe9",
			Candidates: []ingest.EventCandidate{{
				UID:      "cafe-no-9-stale-1",
				Summary:  "Cafe No. 9 Late Show",
				Location: "Cafe No. 9",
				StartAt:  "2026-05-10T18:30:00Z",
			}},
		}},
	}
	groups := ingest.ReviewGroupsFromReport(report)
	if got, want := len(groups), 1; got != want {
		t.Fatalf("seed groups = %d, want %d", got, want)
	}
	stageResult, err := st.StageReviewGroup(ctx, groups[0])
	if err != nil {
		t.Fatalf("stage stale review group: %v", err)
	}
	staleGroupID := stageResult.ID
	if !stageResult.Created {
		t.Fatal("created = false, want true")
	}

	stage, err := createReviewGroupsFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}
	if got, want := stage.AutoPromotedCount, 1; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.GroupsCreated, 0; got != want {
		t.Fatalf("groups created = %d, want %d", got, want)
	}
	if got, want := stage.GroupsReused, 0; got != want {
		t.Fatalf("groups reused = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 0; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.Groups), 0; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}

	group, ok, err := st.LoadReviewGroup(ctx, staleGroupID)
	if err != nil {
		t.Fatalf("load stale review group: %v", err)
	}
	if !ok {
		t.Fatal("stale review group not found")
	}
	if group.Status != review.StatusResolved {
		t.Fatalf("stale group status = %q, want %q", group.Status, review.StatusResolved)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got, want := countRows(t, db, "review_groups"), 1; got != want {
		t.Fatalf("review_groups rows = %d, want %d", got, want)
	}
}

func TestCreateReviewGroupsFromReportKeepsOffsiteLeadmillSingletonInReview(t *testing.T) {
	st := &fakeReviewStageStore{results: []fakeReviewStageResult{{id: 101, created: true}}}
	report := ingest.Report{
		Source:      ingest.LeadmillSource,
		SourceURL:   "https://leadmill.co.uk/live/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://leadmill.co.uk/listings/?ical=1",
			Candidates: []ingest.EventCandidate{{
				UID:      "leadmill-offsite-1",
				Summary:  "Offsite Show",
				Location: "Yellow Arch Studios",
				StartAt:  "2026-05-10T18:30:00Z",
				EndAt:    "2026-05-10T22:00:00Z",
			}},
		}},
	}

	stage, err := createReviewGroupsFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}

	if got, want := stage.GroupsCreated, 1; got != want {
		t.Fatalf("groups created = %d, want %d", got, want)
	}
	if got, want := stage.AutoPromotedCount, 0; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 1; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(st.promotedInputs), 1; got != want {
		t.Fatalf("promoted inputs = %d, want %d", got, want)
	}
	if got, want := len(st.inputs), 1; got != want {
		t.Fatalf("staged review groups = %d, want %d", got, want)
	}
}

func TestCreateReviewGroupsFromReportStagesDuplicateJazzAtTheLescarGroup(t *testing.T) {
	st := &fakeReviewStageStore{results: []fakeReviewStageResult{{id: 301, created: true}}}
	report := ingest.Report{
		Source:      ingest.JazzAtTheLescarSource,
		SourceURL:   "http://www.jazzatthelescar.com/index.html",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "http://www.jazzatthelescar.com/index.html",
			Candidates: []ingest.EventCandidate{
				{
					UID:      "jazz-duplicate-1",
					Summary:  "Jazz Residency Early Set",
					Location: "The Lescar",
					StartAt:  "2026-05-12T18:00:00Z",
					EndAt:    "2026-05-12T19:30:00Z",
				},
				{
					UID:      "jazz-duplicate-1",
					Summary:  "Jazz Residency Late Set",
					Location: "The Lescar",
					StartAt:  "2026-05-12T20:00:00Z",
					EndAt:    "2026-05-12T21:30:00Z",
				},
			},
		}},
	}

	stage, err := createReviewGroupsFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}

	if got, want := stage.AutoPromotedCount, 0; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.GroupsCreated, 1; got != want {
		t.Fatalf("groups created = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 2; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(st.promotedInputs), 0; got != want {
		t.Fatalf("promoted inputs = %d, want %d", got, want)
	}
	if got, want := len(st.inputs), 1; got != want {
		t.Fatalf("staged review groups = %d, want %d", got, want)
	}
}

func TestCreateReviewGroupsFromReportKeepsWrongVenueJazzAtTheLescarSingletonInReview(t *testing.T) {
	st := &fakeReviewStageStore{results: []fakeReviewStageResult{{id: 302, created: true}}}
	report := ingest.Report{
		Source:      ingest.JazzAtTheLescarSource,
		SourceURL:   "http://www.jazzatthelescar.com/index.html",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "http://www.jazzatthelescar.com/index.html",
			Candidates: []ingest.EventCandidate{{
				UID:      "jazz-offsite-1",
				Summary:  "Jazz offsite special",
				Location: "Yellow Arch Studios",
				StartAt:  "2026-05-15T19:30:00Z",
				EndAt:    "2026-05-15T22:00:00Z",
			}},
		}},
	}

	stage, err := createReviewGroupsFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}

	if got, want := stage.GroupsCreated, 1; got != want {
		t.Fatalf("groups created = %d, want %d", got, want)
	}
	if got, want := stage.AutoPromotedCount, 0; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 1; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(st.promotedInputs), 1; got != want {
		t.Fatalf("promoted inputs = %d, want %d", got, want)
	}
	if got, want := len(st.inputs), 1; got != want {
		t.Fatalf("staged review groups = %d, want %d", got, want)
	}
}

func TestCreateReviewGroupsFromReportFallsBackToReviewWhenAutoPromoteSeesExistingCanonical(t *testing.T) {
	st := &fakeReviewStageStore{
		results: []fakeReviewStageResult{{id: 201, created: true}},
		promotionResults: []fakePromotionResult{{
			eventSlug: "live-solo-show-sidney-and-matilda-20260503190000",
			promoted:  false,
		}},
	}
	report := ingest.Report{
		Source:      ingest.DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []ingest.EventCandidate{{
				UID:      "singleton",
				Summary:  "Solo Show",
				Location: "Sidney & Matilda",
				StartAt:  "2026-05-03T19:00:00Z",
				EndAt:    "2026-05-03T22:00:00Z",
			}},
		}},
	}

	stage, err := createReviewGroupsFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}

	if got, want := stage.AutoPromotedCount, 0; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.GroupsCreated, 1; got != want {
		t.Fatalf("groups created = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 1; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(st.inputs), 1; got != want {
		t.Fatalf("staged review groups = %d, want %d", got, want)
	}
}

func TestCreateReviewGroupsFromReportReusesExistingGroupWhenOnlySourceMetadataDiffers(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	firstReport := ingest.Report{
		Source:      "Source A",
		SourceURL:   "https://source-a.example.test/events.ics",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{
			{
				Candidates: []ingest.EventCandidate{
					{
						UID:      "duplicate",
						Summary:  "Duplicate one",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-01T19:00:00Z",
						EndAt:    "2026-05-01T20:00:00Z",
					},
					{
						UID:      "duplicate",
						Summary:  "Duplicate two",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-02T19:00:00Z",
						EndAt:    "2026-05-02T20:00:00Z",
					},
				},
			},
		},
	}
	firstStage, err := createReviewGroupsFromReport(ctx, st, testSourceCatalog(t), firstReport)
	if err != nil {
		t.Fatalf("stage first report: %v", err)
	}
	if got, want := firstStage.GroupsCreated, 1; got != want {
		t.Fatalf("first stage groups created = %d, want %d", got, want)
	}
	if got, want := firstStage.GroupsReused, 0; got != want {
		t.Fatalf("first stage groups reused = %d, want %d", got, want)
	}
	if got, want := len(firstStage.Groups), 1; got != want {
		t.Fatalf("first stage groups = %d, want %d", got, want)
	}

	secondReport := ingest.Report{
		Source:      "Source B",
		SourceURL:   "https://source-b.example.test/events.ics",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{
			{
				Candidates: []ingest.EventCandidate{
					{
						UID:      "duplicate",
						Summary:  "Duplicate one",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-01T19:00:00Z",
						EndAt:    "2026-05-01T20:00:00Z",
					},
					{
						UID:      "duplicate",
						Summary:  "Duplicate two",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-02T19:00:00Z",
						EndAt:    "2026-05-02T20:00:00Z",
					},
				},
			},
		},
	}
	secondStage, err := createReviewGroupsFromReport(ctx, st, testSourceCatalog(t), secondReport)
	if err != nil {
		t.Fatalf("stage second report: %v", err)
	}
	if got, want := secondStage.GroupsCreated, 0; got != want {
		t.Fatalf("second stage groups created = %d, want %d", got, want)
	}
	if got, want := secondStage.GroupsReused, 1; got != want {
		t.Fatalf("second stage groups reused = %d, want %d", got, want)
	}
	if got, want := len(secondStage.Groups), 1; got != want {
		t.Fatalf("second stage groups = %d, want %d", got, want)
	}
	if got, want := firstStage.Groups[0].ID, secondStage.Groups[0].ID; got != want {
		t.Fatalf("staged group id = %d, want %d", got, want)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got, want := countRows(t, db, "review_groups"), 1; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}

	group, ok, err := st.LoadReviewGroup(ctx, firstStage.Groups[0].ID)
	if err != nil {
		t.Fatalf("load staged review group: %v", err)
	}
	if !ok {
		t.Fatal("staged review group not found")
	}
	if got, want := group.SourceName, "Source A manual ingest"; got != want {
		t.Fatalf("group source name = %q, want %q", got, want)
	}
	if got, want := group.SourceURL, "https://source-a.example.test/events.ics"; got != want {
		t.Fatalf("group source url = %q, want %q", got, want)
	}
}

func TestCreateReviewGroupsFromReportPersistsAuthoritativeGroupMetadata(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer st.Close()

	report := ingest.Report{
		Source:      ingest.DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{
			{
				URL: "https://calendar.example.test/live.ics",
				Candidates: []ingest.EventCandidate{
					{
						UID:      "shared-uid",
						Summary:  "UTC Show",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-01T19:00:00Z",
						EndAt:    "2026-05-01T22:00:00Z",
					},
					{
						UID:      "shared-uid",
						Summary:  "UTC Show duplicate",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-01T19:05:00Z",
						EndAt:    "2026-05-01T22:05:00Z",
					},
				},
			},
		},
	}

	stage, err := createReviewGroupsFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create review groups: %v", err)
	}
	if got, want := len(stage.Groups), 1; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}

	group, ok, err := st.LoadReviewGroup(ctx, stage.Groups[0].ID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if got, want := group.AuthoritativeSourceName, "Sidney & Matilda manual ingest"; got != want {
		t.Fatalf("authoritative source name = %q, want %q", got, want)
	}
	if got, want := group.AuthoritativeSourceURL, "https://calendar.example.test/live.ics"; got != want {
		t.Fatalf("authoritative source url = %q, want %q", got, want)
	}
	if got, want := group.AuthoritativeSourceEventKey, "shared-uid"; got != want {
		t.Fatalf("authoritative source event key = %q, want %q", got, want)
	}
}

func TestParseIngestArgsFlagCompatibility(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		args         []string
		wantUA       string
		wantContact  string
		wantStage    bool
		wantFixture  string
		wantImportID int64
		wantAll      bool
		wantErr      bool
	}{
		{name: "canonical user agent", args: []string{"-http-user-agent", "agent"}, wantUA: "agent"},
		{name: "alias user agent", args: []string{"-user-agent", "agent"}, wantUA: "agent"},
		{name: "canonical+alias user agent same", args: []string{"-http-user-agent", "agent", "-user-agent", "agent"}, wantUA: "agent"},
		{name: "canonical+alias user agent different", args: []string{"-http-user-agent", "agent-a", "-user-agent", "agent-b"}, wantErr: true},
		{name: "reordered user agent mismatch", args: []string{"-http-user-agent", "agent-a", "-user-agent", "agent-b", "-http-user-agent", "agent-a"}, wantErr: true},
		{name: "contact override", args: []string{"-contact", "ops@example.com"}, wantContact: "ops@example.com"},
		{name: "contact suppression", args: []string{"-contact", "none"}, wantContact: "none"},
		{name: "canonical stage groups", args: []string{"-stage-review-groups"}, wantStage: true},
		{name: "alias stage groups", args: []string{"-stage-review"}, wantStage: true},
		{name: "canonical+alias stage groups same", args: []string{"-stage-review-groups=true", "-stage-review=true"}, wantStage: true},
		{name: "canonical+alias stage groups different", args: []string{"-stage-review-groups=true", "-stage-review=false"}, wantErr: true},
		{name: "reordered stage groups mismatch", args: []string{"-stage-review-groups=true", "-stage-review=false", "-stage-review-groups=true"}, wantErr: true},
		{name: "canonical fixture", args: []string{"-review-ics-fixture", "fixture.ics"}, wantFixture: "fixture.ics"},
		{name: "alias fixture", args: []string{"-review-fixture", "fixture.ics"}, wantFixture: "fixture.ics"},
		{name: "canonical+alias fixture same", args: []string{"-review-ics-fixture", "fixture.ics", "-review-fixture", "fixture.ics"}, wantFixture: "fixture.ics"},
		{name: "canonical+alias fixture different", args: []string{"-review-ics-fixture", "one.ics", "-review-fixture", "two.ics"}, wantErr: true},
		{name: "reordered fixture mismatch", args: []string{"-review-ics-fixture", "one.ics", "-review-fixture", "two.ics", "-review-ics-fixture", "one.ics"}, wantErr: true},
		{name: "replay mode", args: []string{"-import-run-id", "42"}, wantImportID: 42},
		{name: "all sources mode", args: []string{"-all-sources"}, wantAll: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseIngestArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected parse error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parse args: %v", err)
			}
			if got := cfg.httpUserAgent; got != tc.wantUA {
				t.Fatalf("user agent = %q, want %q", got, tc.wantUA)
			}
			if got := cfg.contact; got != tc.wantContact {
				t.Fatalf("contact = %q, want %q", got, tc.wantContact)
			}
			if got := cfg.stageReviewGroups; got != tc.wantStage {
				t.Fatalf("stage review groups = %v, want %v", got, tc.wantStage)
			}
			if got := cfg.reviewICSFixture; got != tc.wantFixture {
				t.Fatalf("fixture = %q, want %q", got, tc.wantFixture)
			}
			if got := cfg.importRunID; got != tc.wantImportID {
				t.Fatalf("import run id = %d, want %d", got, tc.wantImportID)
			}
			if got := cfg.allSources; got != tc.wantAll {
				t.Fatalf("all sources = %v, want %v", got, tc.wantAll)
			}
		})
	}
}

func TestEffectiveHTTPUserAgentDefaultsFromGitEmail(t *testing.T) {
	originalLookup := lookupGitUserEmail
	defer func() {
		lookupGitUserEmail = originalLookup
	}()
	lookupGitUserEmail = func(context.Context) string {
		return "git-user@example.com"
	}

	got := effectiveHTTPUserAgent(context.Background(), ingestCommandConfig{})
	want := "sheffield-live ingest/1.0 (contact: git-user@example.com)"
	if got != want {
		t.Fatalf("user agent = %q, want %q", got, want)
	}
}

func TestEffectiveHTTPUserAgentContactOverrideAndSuppression(t *testing.T) {
	originalLookup := lookupGitUserEmail
	defer func() {
		lookupGitUserEmail = originalLookup
	}()
	lookupGitUserEmail = func(context.Context) string {
		return "git-user@example.com"
	}

	if got, want := effectiveHTTPUserAgent(context.Background(), ingestCommandConfig{contact: "ops@example.com"}), "sheffield-live ingest/1.0 (contact: ops@example.com)"; got != want {
		t.Fatalf("override user agent = %q, want %q", got, want)
	}
	for _, value := range []string{"none", "null", "false"} {
		if got, want := effectiveHTTPUserAgent(context.Background(), ingestCommandConfig{contact: value}), "sheffield-live ingest/1.0"; got != want {
			t.Fatalf("suppressed user agent for %q = %q, want %q", value, got, want)
		}
	}
}

func TestEffectiveHTTPUserAgentRespectsExplicitValue(t *testing.T) {
	if got, want := effectiveHTTPUserAgent(context.Background(), ingestCommandConfig{httpUserAgent: "custom-agent"}), "custom-agent"; got != want {
		t.Fatalf("user agent = %q, want %q", got, want)
	}
}

func TestParseIngestArgsRejectsFixtureReplayCombination(t *testing.T) {
	_, err := parseIngestArgs([]string{"-review-ics-fixture", "fixture.ics", "-import-run-id", "1"})
	if err == nil {
		t.Fatal("expected fixture/replay conflict")
	}
}

func TestParseIngestArgsRejectsAllSourcesSourceCombination(t *testing.T) {
	_, err := parseIngestArgs([]string{"-all-sources", "-source", ingest.LeadmillSource})
	if err == nil {
		t.Fatal("expected all-sources/source conflict")
	}
}

func TestParseIngestArgsRejectsAllSourcesReplayCombination(t *testing.T) {
	_, err := parseIngestArgs([]string{"-all-sources", "-import-run-id", "1"})
	if err == nil {
		t.Fatal("expected all-sources/replay conflict")
	}
}

func TestParseIngestArgsRejectsAllSourcesFixtureCombination(t *testing.T) {
	_, err := parseIngestArgs([]string{"-all-sources", "-review-ics-fixture", "fixture.ics"})
	if err == nil {
		t.Fatal("expected all-sources/fixture conflict")
	}
}

func TestParseIngestArgsRejectsRepairDescriptionConflicts(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "stage review groups", args: []string{"-repair-descriptions", "-stage-review-groups"}},
		{name: "all sources", args: []string{"-repair-descriptions", "-all-sources"}},
		{name: "fixture", args: []string{"-repair-descriptions", "-review-ics-fixture", "fixture.ics"}},
		{name: "unsupported source", args: []string{"-repair-descriptions", "-source", ingest.LeadmillSource}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseIngestArgs(tc.args); err == nil {
				t.Fatal("expected repair description flag conflict")
			}
		})
	}
}

func TestRunWithArgsRepairDescriptionsUpdatesOnlyDescriptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID, seedSlug := seedReplayRunForCLIRepairDescriptions(t, path)

	originalFetcher := newHTTPFetcher
	originalRunManual := runManualImport
	defer func() {
		newHTTPFetcher = originalFetcher
		runManualImport = originalRunManual
	}()

	newHTTPFetcher = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		t.Fatalf("newHTTPFetcher called unexpectedly with timeout %v and user agent %q", timeout, userAgent)
		return fakeFetcher{}, nil
	}
	runManualImport = func(_ context.Context, _ *sqlite.Store, _ ingest.Fetcher, _ *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
		t.Fatalf("runManualImport called unexpectedly for source %q", opts.Source)
		return ingest.Report{}, nil
	}

	db := openRawDB(t, path)
	before := loadEventRow(t, db, seedSlug)
	db.Close()

	var stdout bytes.Buffer
	if err := runWithArgs([]string{
		"-db", path,
		"-import-run-id", strconv.FormatInt(runID, 10),
		"-repair-descriptions",
	}, &stdout, io.Discard); err != nil {
		t.Fatalf("repair descriptions run: %v", err)
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode repair output keys: %v", err)
	}
	if _, ok := payload["review_stage"]; ok {
		t.Fatalf("repair output contains unexpected review_stage key: %s", stdout.Bytes())
	}
	if _, ok := payload["description_repair"]; !ok {
		t.Fatalf("repair output missing description_repair key: %s", stdout.Bytes())
	}
	if got, want := len(payload), 2; got != want {
		t.Fatalf("repair output keys = %d, want %d", got, want)
	}

	var got descriptionRepairRunReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode repair output: %v", err)
	}
	if got.Report.ImportRunID != runID {
		t.Fatalf("import run id = %d, want %d", got.Report.ImportRunID, runID)
	}
	if got.Report.Source != ingest.CafeNo9Source {
		t.Fatalf("report source = %q, want %q", got.Report.Source, ingest.CafeNo9Source)
	}
	if got.Report.SourceURL != "https://www.wegottickets.com/Cafe9" {
		t.Fatalf("report source url = %q, want Cafe No. 9 source url", got.Report.SourceURL)
	}
	if got.Report.Status != "succeeded" {
		t.Fatalf("report status = %q, want succeeded", got.Report.Status)
	}
	foundDescription := false
	for _, calendar := range got.Report.Calendars {
		for _, candidate := range calendar.Candidates {
			if candidate.UID == "https://www.wegottickets.com/event/700001" {
				foundDescription = true
				if candidate.Description != "With special support from Robbie Thompson" {
					t.Fatalf("replay candidate description = %q, want replayed description", candidate.Description)
				}
			}
		}
	}
	if !foundDescription {
		t.Fatal("replay output missing seeded Cafe No. 9 candidate")
	}
	if got.DescriptionRepair.Repaired != 1 {
		t.Fatalf("repaired = %d, want 1", got.DescriptionRepair.Repaired)
	}
	if len(got.DescriptionRepair.RepairedSlugs) != 1 || got.DescriptionRepair.RepairedSlugs[0] != seedSlug {
		t.Fatalf("repaired slugs = %#v, want [%q]", got.DescriptionRepair.RepairedSlugs, seedSlug)
	}

	db = openRawDB(t, path)
	defer db.Close()
	after := loadEventRow(t, db, seedSlug)
	if before.Description == after.Description {
		t.Fatalf("description = %q, want repaired description", after.Description)
	}
	if before != after {
		before.Description = after.Description
		if before != after {
			t.Fatalf("event row changed beyond description:\nbefore: %#v\nafter:  %#v", before, after)
		}
	}

	for _, table := range []string{"review_groups", "review_candidates", "review_draft_choices", "import_run_review_groups"} {
		if got := countRows(t, db, table); got != 0 {
			t.Fatalf("%s rows = %d, want 0", table, got)
		}
	}
}

func TestRunWithArgsReviewICSFixtureCreatesReviewGroupWithoutUserAgent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sheffield-live.db")
	fixturePath := filepath.Join("..", "..", "internal", "ingest", "testdata", "sidney.ics")

	var stdout bytes.Buffer
	if err := runWithArgs([]string{
		"-db", dbPath,
		"-review-ics-fixture", fixturePath,
		"-review-title", "Custom fixture title",
	}, &stdout, io.Discard); err != nil {
		t.Fatalf("fixture run: %v", err)
	}

	var got reviewFixtureReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode fixture output: %v", err)
	}
	if got.Fixture != fixturePath {
		t.Fatalf("fixture = %q, want %q", got.Fixture, fixturePath)
	}
	if got.GroupID == 0 {
		t.Fatal("group id = 0, want persisted review group")
	}
	if got.Candidates != 3 {
		t.Fatalf("candidates = %d, want 3", got.Candidates)
	}
	if len(got.Skips) != 4 {
		t.Fatalf("skips = %d, want 4", len(got.Skips))
	}
	if len(got.Errors) != 0 {
		t.Fatalf("errors = %#v, want none", got.Errors)
	}

	db := openRawDB(t, dbPath)
	defer db.Close()
	if got := countRows(t, db, "review_groups"); got != 1 {
		t.Fatalf("review groups = %d, want 1", got)
	}

	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	group, ok, err := st.LoadReviewGroup(context.Background(), got.GroupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if group.Title != "Custom fixture title" {
		t.Fatalf("group title = %q, want custom title", group.Title)
	}
	if group.SourceURL != "file:"+fixturePath {
		t.Fatalf("group source url = %q, want file fixture URL", group.SourceURL)
	}
	if got := len(group.Candidates); got != 3 {
		t.Fatalf("group candidates = %d, want 3", got)
	}
	if got, want := group.Candidates[0].VenueSlug, "sidney-and-matilda"; got != want {
		t.Fatalf("first candidate venue slug = %q, want %q", got, want)
	}
}

func TestRunWithArgsReviewICSFixtureUsesDefaultTitle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sheffield-live.db")
	fixturePath := filepath.Join("..", "..", "internal", "ingest", "testdata", "sidney.ics")

	var stdout bytes.Buffer
	if err := runWithArgs([]string{
		"-db", dbPath,
		"-review-ics-fixture", fixturePath,
	}, &stdout, io.Discard); err != nil {
		t.Fatalf("fixture run: %v", err)
	}

	var got reviewFixtureReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode fixture output: %v", err)
	}

	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	group, ok, err := st.LoadReviewGroup(context.Background(), got.GroupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if group.Title != "Fixture review: sidney.ics" {
		t.Fatalf("group title = %q, want default fixture title", group.Title)
	}
}

func TestRunWithArgsReplayDoesNotRequireUserAgent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID := seedReplayRunForCLI(t, path)

	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "-import-run-id", strconv.FormatInt(runID, 10), "-limit", "1", "-stage-review-groups"}, &stdout, io.Discard); err != nil {
		t.Fatalf("replay run: %v", err)
	}

	var got manualIngestReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode replay output: %v", err)
	}
	if got.Report.Limit != 1 {
		t.Fatalf("report limit = %d, want 1", got.Report.Limit)
	}
	if got.Report.ImportRunID != runID {
		t.Fatalf("import run id = %d, want %d", got.Report.ImportRunID, runID)
	}
	if got.Report.Status != "succeeded" {
		t.Fatalf("report status = %q, want succeeded", got.Report.Status)
	}
	if got := len(got.Report.Links); got != 1 {
		t.Fatalf("links = %d, want 1", got)
	}
	if got := got.ReviewStage.GroupsCreated; got != 1 {
		t.Fatalf("review groups created = %d, want 1", got)
	}
	if got := got.ReviewStage.AutoPromotedCount; got != 1 {
		t.Fatalf("auto promoted count = %d, want 1", got)
	}
	if got := got.ReviewStage.ReviewCandidateCount; got != 2 {
		t.Fatalf("review candidate count = %d, want 2", got)
	}
}

func TestRunWithArgsReplayYellowArchUsesStoredSourcePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID := seedReplayRunForCLIYellowArch(t, path)

	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "-import-run-id", strconv.FormatInt(runID, 10), "-limit", "1", "-stage-review-groups"}, &stdout, io.Discard); err != nil {
		t.Fatalf("replay run: %v", err)
	}

	var got manualIngestReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode replay output: %v", err)
	}
	if got.Report.Source != ingest.YellowArchSource {
		t.Fatalf("report source = %q, want %q", got.Report.Source, ingest.YellowArchSource)
	}
	if got.Report.SourceURL != "https://www.yellowarch.com/events/" {
		t.Fatalf("report source url = %q, want Yellow Arch events page", got.Report.SourceURL)
	}
	if got.Report.Limit != 1 {
		t.Fatalf("report limit = %d, want 1", got.Report.Limit)
	}
	if got := len(got.Report.Calendars); got != 1 {
		t.Fatalf("calendars = %d, want 1", got)
	}
	if got := len(got.Report.Calendars[0].Candidates); got != 1 {
		t.Fatalf("candidates = %d, want 1", got)
	}
	if got := got.ReviewStage.GroupsCreated; got != 0 {
		t.Fatalf("review groups created = %d, want 0", got)
	}
	if got := got.ReviewStage.AutoPromotedCount; got != 1 {
		t.Fatalf("auto promoted count = %d, want 1", got)
	}
	if got := len(got.ReviewStage.Groups); got != 0 {
		t.Fatalf("review groups = %d, want 0", got)
	}
	if got := len(got.ReviewStage.AutoPromoted); got != 1 {
		t.Fatalf("auto promoted groups = %d, want 1", got)
	}
	if got := got.ReviewStage.AutoPromoted[0].SourceURL; got != "https://www.yellowarch.com/events/" {
		t.Fatalf("auto promoted source url = %q, want Yellow Arch events page", got)
	}

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	if _, ok := st.EventBySlug(got.ReviewStage.AutoPromoted[0].EventSlug); !ok {
		t.Fatalf("published event %q not found", got.ReviewStage.AutoPromoted[0].EventSlug)
	}
}

func TestRunWithArgsReplayYellowArchReappliesLinkedAuthoritativeEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID := seedReplayRunForCLIYellowArch(t, path)

	runReplay := func() manualIngestReport {
		t.Helper()
		var stdout bytes.Buffer
		if err := runWithArgs([]string{"-db", path, "-import-run-id", strconv.FormatInt(runID, 10), "-limit", "1", "-stage-review-groups"}, &stdout, io.Discard); err != nil {
			t.Fatalf("replay run: %v", err)
		}
		var got manualIngestReport
		if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
			t.Fatalf("decode replay output: %v", err)
		}
		return got
	}

	first := runReplay()
	second := runReplay()

	if got := first.ReviewStage.AutoPromotedCount; got != 1 {
		t.Fatalf("first auto promoted count = %d, want 1", got)
	}
	if got := second.ReviewStage.AutoPromotedCount; got != 1 {
		t.Fatalf("second auto promoted count = %d, want 1", got)
	}
	if got := second.ReviewStage.GroupsCreated; got != 0 {
		t.Fatalf("second review groups created = %d, want 0", got)
	}
	if got := len(second.ReviewStage.Groups); got != 0 {
		t.Fatalf("second review groups = %d, want 0", got)
	}
	if got := len(second.ReviewStage.AutoPromoted); got != 1 {
		t.Fatalf("second auto promoted groups = %d, want 1", got)
	}
	if got, want := second.ReviewStage.AutoPromoted[0].EventSlug, first.ReviewStage.AutoPromoted[0].EventSlug; got != want {
		t.Fatalf("second event slug = %q, want %q", got, want)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got := countRows(t, db, "events"); got != 1 {
		t.Fatalf("events rows = %d, want 1", got)
	}
	if got := countRows(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
	if got := countRows(t, db, "event_source_links"); got != 1 {
		t.Fatalf("event_source_links rows = %d, want 1", got)
	}
}

func TestRunWithArgsReplayLeadmillUsesStoredSourcePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID := seedReplayRunForCLILeadmill(t, path)

	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "-import-run-id", strconv.FormatInt(runID, 10), "-limit", "20", "-stage-review-groups"}, &stdout, io.Discard); err != nil {
		t.Fatalf("replay run: %v", err)
	}

	var got manualIngestReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode replay output: %v", err)
	}
	if got.Report.Source != ingest.LeadmillSource {
		t.Fatalf("report source = %q, want %q", got.Report.Source, ingest.LeadmillSource)
	}
	if got.Report.SourceURL != "https://leadmill.co.uk/live/" {
		t.Fatalf("report source url = %q, want Leadmill live page", got.Report.SourceURL)
	}
	if got := len(got.Report.Links); got != 1 {
		t.Fatalf("links = %d, want 1", got)
	}
	if got := len(got.Report.Calendars); got != 1 {
		t.Fatalf("calendars = %d, want 1", got)
	}
	if got := len(got.Report.Calendars[0].Candidates); got != 1 {
		t.Fatalf("candidates = %d, want 1", got)
	}
	if got := got.ReviewStage.GroupsCreated; got != 0 {
		t.Fatalf("review groups created = %d, want 0", got)
	}
	if got := got.ReviewStage.AutoPromotedCount; got != 1 {
		t.Fatalf("auto promoted count = %d, want 1", got)
	}
	if got := len(got.ReviewStage.Groups); got != 0 {
		t.Fatalf("review groups = %d, want 0", got)
	}
	if got := got.ReviewStage.ReviewCandidateCount; got != 0 {
		t.Fatalf("review candidate count = %d, want 0", got)
	}
	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()

	if got := len(got.ReviewStage.AutoPromoted); got != 1 {
		t.Fatalf("auto promoted groups = %d, want 1", got)
	}
	event, ok := st.EventBySlug(got.ReviewStage.AutoPromoted[0].EventSlug)
	if !ok {
		t.Fatalf("published event %q not found", got.ReviewStage.AutoPromoted[0].EventSlug)
	}
	if got := event.VenueSlug; got != "leadmill" {
		t.Fatalf("venue slug = %q, want leadmill", got)
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for unknown end", event.End)
	}
}

func TestRunWithArgsBackfillsImageFocus(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	mediaRoot := filepath.Join(t.TempDir(), "media")
	imagePath := filepath.Join(mediaRoot, "events", "poster.png")
	if err := os.MkdirAll(filepath.Dir(imagePath), 0o755); err != nil {
		t.Fatalf("make media dir: %v", err)
	}
	if err := os.WriteFile(imagePath, focusFixturePNG(t), 0o644); err != nil {
		t.Fatalf("write fixture image: %v", err)
	}

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	if err := st.SaveImageAsset(ctx, ingest.ImageAsset{
		SourceURL:   "https://example.test/poster.png",
		PublicURL:   "/media/events/poster.png",
		StoragePath: "events/poster.png",
		ContentType: "image/png",
		FocusX:      50,
		FocusY:      50,
		CopiedAt:    time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("save image asset: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	t.Setenv("MEDIA_ROOT", mediaRoot)
	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "-backfill-image-focus"}, &stdout, io.Discard); err != nil {
		t.Fatalf("backfill image focus: %v", err)
	}

	var report imageFocusBackfillReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode backfill output: %v", err)
	}
	if report.Updated != 1 || report.Defaulted != 0 || report.MissingFiles != 0 || report.DecodeFailures != 0 {
		t.Fatalf("backfill report = %#v, want one clean update", report)
	}

	st, err = sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer st.Close()
	asset, ok, err := st.LoadImageAsset(ctx, "https://example.test/poster.png")
	if err != nil {
		t.Fatalf("load image asset: %v", err)
	}
	if !ok {
		t.Fatal("image asset not found")
	}
	if asset.FocusX <= 55 || asset.FocusY <= 55 {
		t.Fatalf("focus = %d,%d, want lower-right quadrant", asset.FocusX, asset.FocusY)
	}
}

func TestRunWithArgsBackfillsImageFocusDefaultsOversizedImages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	mediaRoot := filepath.Join(t.TempDir(), "media")
	imagePath := filepath.Join(mediaRoot, "events", "oversized.png")
	if err := os.MkdirAll(filepath.Dir(imagePath), 0o755); err != nil {
		t.Fatalf("make media dir: %v", err)
	}
	if err := os.WriteFile(imagePath, oversizedPNGHeader(t, 9000, 9000), 0o644); err != nil {
		t.Fatalf("write oversized image: %v", err)
	}

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	if err := st.SaveImageAsset(ctx, ingest.ImageAsset{
		SourceURL:   "https://example.test/oversized.png",
		PublicURL:   "/media/events/oversized.png",
		StoragePath: "events/oversized.png",
		ContentType: "image/png",
		FocusX:      25,
		FocusY:      75,
		CopiedAt:    time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("save image asset: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	t.Setenv("MEDIA_ROOT", mediaRoot)
	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "-backfill-image-focus"}, &stdout, io.Discard); err != nil {
		t.Fatalf("backfill image focus: %v", err)
	}

	var report imageFocusBackfillReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode backfill output: %v", err)
	}
	if report.Updated != 1 || report.Defaulted != 1 || report.MissingFiles != 0 || report.DecodeFailures != 0 {
		t.Fatalf("backfill report = %#v, want one defaulted oversized update", report)
	}

	st, err = sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer st.Close()
	asset, ok, err := st.LoadImageAsset(ctx, "https://example.test/oversized.png")
	if err != nil {
		t.Fatalf("load image asset: %v", err)
	}
	if !ok {
		t.Fatal("image asset not found")
	}
	if asset.FocusX != ingest.DefaultImageFocusX || asset.FocusY != ingest.DefaultImageFocusY {
		t.Fatalf("focus = %d,%d, want default", asset.FocusX, asset.FocusY)
	}
}

func TestRunWithArgsReplayFailureStillEmitsJSONAndSkipsReviewStaging(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID := seedReplayRunForCLIWithNoLinks(t, path)

	var stdout bytes.Buffer
	err := runWithArgs([]string{
		"-db", path,
		"-import-run-id", strconv.FormatInt(runID, 10),
		"-limit", "1",
		"-stage-review-groups",
	}, &stdout, io.Discard)
	if !errors.Is(err, ingest.ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}

	var got manualIngestReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode replay output: %v", err)
	}
	if got.Report.ImportRunID != runID {
		t.Fatalf("import run id = %d, want %d", got.Report.ImportRunID, runID)
	}
	if got.Report.Status != "failed" {
		t.Fatalf("report status = %q, want failed", got.Report.Status)
	}
	if len(got.Report.Errors) == 0 || !strings.Contains(got.Report.Errors[0], "no ICS links found") {
		t.Fatalf("report errors = %#v, want no ICS links failure", got.Report.Errors)
	}
	if !got.ReviewStage.Enabled {
		t.Fatal("review stage enabled = false, want true")
	}
	if got.ReviewStage.GroupsCreated != 0 {
		t.Fatalf("review groups created = %d, want 0", got.ReviewStage.GroupsCreated)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got := countRows(t, db, "review_groups"); got != 0 {
		t.Fatalf("review groups = %d, want 0", got)
	}
}

func TestRunWithArgsAllSourcesRunsInRegistryOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	var stdout bytes.Buffer

	originalFetcher := newHTTPFetcher
	originalRunManual := runManualImport
	defer func() {
		newHTTPFetcher = originalFetcher
		runManualImport = originalRunManual
	}()

	newHTTPFetcher = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		return fakeFetcher{}, nil
	}
	var order []string
	runManualImport = func(_ context.Context, _ *sqlite.Store, _ ingest.Fetcher, _ *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
		order = append(order, opts.Source)
		return ingest.Report{
			Source:      opts.Source,
			SourceURL:   "https://" + opts.Source + ".example.test/",
			ImportRunID: int64(len(order)),
			StartedAt:   "2026-04-24T10:00:00Z",
			FinishedAt:  "2026-04-24T10:01:00Z",
			Status:      "succeeded",
			Limit:       opts.Limit,
			Links:       []string{},
			Calendars:   []ingest.CalendarReport{},
			Totals:      ingest.ReportTotals{},
		}, nil
	}

	if err := runWithArgs([]string{"-db", path, "-all-sources", "-http-user-agent", "agent"}, &stdout, io.Discard); err != nil {
		t.Fatalf("all-sources run: %v", err)
	}

	if got, want := order, ingest.RegisteredSourceKeys(); !equalStrings(got, want) {
		t.Fatalf("run order = %#v, want %#v", got, want)
	}

	var got batchManualIngestReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode batch output: %v", err)
	}
	if gotCount, wantCount := len(got.Results), len(ingest.RegisteredSourceKeys()); gotCount != wantCount {
		t.Fatalf("results = %d, want %d", gotCount, wantCount)
	}
	for i, source := range ingest.RegisteredSourceKeys() {
		if got.Results[i].Source != source {
			t.Fatalf("result %d source = %q, want %q", i, got.Results[i].Source, source)
		}
	}
}

func TestRunWithArgsLogsToStderrAndKeepsStdoutJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	originalFetcher := newHTTPFetcher
	originalRunManual := runManualImport
	defer func() {
		newHTTPFetcher = originalFetcher
		runManualImport = originalRunManual
	}()

	newHTTPFetcher = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		return fakeFetcher{}, nil
	}
	runManualImport = func(_ context.Context, _ *sqlite.Store, _ ingest.Fetcher, _ *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
		return ingest.Report{
			Source:      opts.Source,
			SourceURL:   "https://" + opts.Source + ".example.test/",
			ImportRunID: 7,
			StartedAt:   "2026-04-24T10:00:00Z",
			FinishedAt:  "2026-04-24T10:01:00Z",
			Status:      "succeeded",
			Limit:       opts.Limit,
			Totals: ingest.ReportTotals{
				Links:      2,
				Snapshots:  3,
				Candidates: 4,
				Skips:      1,
			},
		}, nil
	}

	if err := runWithArgs([]string{"-db", path, "-http-user-agent", "agent"}, &stdout, &stderr); err != nil {
		t.Fatalf("runWithArgs: %v", err)
	}

	var got ingest.Report
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout is not JSON report: %v; output %q", err, stdout.String())
	}
	if got.ImportRunID != 7 {
		t.Fatalf("import run id = %d, want 7", got.ImportRunID)
	}
	if strings.Contains(stdout.String(), "ingest starting") || strings.Contains(stdout.String(), "ingest finished") {
		t.Fatalf("stdout contains logs: %q", stdout.String())
	}

	logs := stderr.String()
	for _, want := range []string{
		`msg="ingest starting"`,
		`msg="ingest finished"`,
		`mode=live`,
		`source=sidney-and-matilda`,
		`import_run_id=7`,
		`status=succeeded`,
		`candidates=4`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("stderr logs = %q, want %q", logs, want)
		}
	}
}

func TestRunWithArgsUsesDerivedDefaultUserAgent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	originalFetcher := newHTTPFetcher
	originalRunManual := runManualImport
	originalLookup := lookupGitUserEmail
	defer func() {
		newHTTPFetcher = originalFetcher
		runManualImport = originalRunManual
		lookupGitUserEmail = originalLookup
	}()

	lookupGitUserEmail = func(context.Context) string {
		return "git-user@example.com"
	}

	var gotUA string
	newHTTPFetcher = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		gotUA = userAgent
		return fakeFetcher{}, nil
	}
	runManualImport = func(_ context.Context, _ *sqlite.Store, _ ingest.Fetcher, _ *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
		return ingest.Report{
			Source:      opts.Source,
			SourceURL:   "https://" + opts.Source + ".example.test/",
			ImportRunID: 1,
			StartedAt:   "2026-04-24T10:00:00Z",
			FinishedAt:  "2026-04-24T10:01:00Z",
			Status:      "succeeded",
			Limit:       opts.Limit,
		}, nil
	}

	if err := runWithArgs([]string{"-db", path}, io.Discard, io.Discard); err != nil {
		t.Fatalf("runWithArgs: %v", err)
	}
	if got, want := gotUA, "sheffield-live ingest/1.0 (contact: git-user@example.com)"; got != want {
		t.Fatalf("user agent = %q, want %q", got, want)
	}
}

func TestRunWithArgsContactSuppressionRemovesContactFromDefaultUserAgent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	originalFetcher := newHTTPFetcher
	originalRunManual := runManualImport
	originalLookup := lookupGitUserEmail
	defer func() {
		newHTTPFetcher = originalFetcher
		runManualImport = originalRunManual
		lookupGitUserEmail = originalLookup
	}()

	lookupGitUserEmail = func(context.Context) string {
		return "git-user@example.com"
	}

	var gotUA string
	newHTTPFetcher = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		gotUA = userAgent
		return fakeFetcher{}, nil
	}
	runManualImport = func(_ context.Context, _ *sqlite.Store, _ ingest.Fetcher, _ *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
		return ingest.Report{
			Source:      opts.Source,
			SourceURL:   "https://" + opts.Source + ".example.test/",
			ImportRunID: 1,
			StartedAt:   "2026-04-24T10:00:00Z",
			FinishedAt:  "2026-04-24T10:01:00Z",
			Status:      "succeeded",
			Limit:       opts.Limit,
		}, nil
	}

	if err := runWithArgs([]string{"-db", path, "-contact", "none"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("runWithArgs: %v", err)
	}
	if got, want := gotUA, "sheffield-live ingest/1.0"; got != want {
		t.Fatalf("user agent = %q, want %q", got, want)
	}
}

func TestRunWithArgsAllSourcesContinuesAfterFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	var stdout bytes.Buffer

	originalFetcher := newHTTPFetcher
	originalRunManual := runManualImport
	defer func() {
		newHTTPFetcher = originalFetcher
		runManualImport = originalRunManual
	}()

	newHTTPFetcher = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		return fakeFetcher{}, nil
	}
	var order []string
	runManualImport = func(_ context.Context, _ *sqlite.Store, _ ingest.Fetcher, _ *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
		order = append(order, opts.Source)
		report := ingest.Report{
			Source:      opts.Source,
			SourceURL:   "https://" + opts.Source + ".example.test/",
			ImportRunID: int64(len(order)),
			StartedAt:   "2026-04-24T10:00:00Z",
			FinishedAt:  "2026-04-24T10:01:00Z",
			Status:      "succeeded",
			Limit:       opts.Limit,
			Links:       []string{},
			Calendars:   []ingest.CalendarReport{},
			Totals:      ingest.ReportTotals{},
		}
		if opts.Source == ingest.YellowArchSource {
			report.Status = "failed"
			report.Errors = []string{"yellow arch failed"}
			return report, ingest.ErrRunFailed
		}
		return report, nil
	}

	err := runWithArgs([]string{"-db", path, "-all-sources", "-http-user-agent", "agent"}, &stdout, io.Discard)
	if err == nil {
		t.Fatal("expected batch failure")
	}
	if got, want := order, ingest.RegisteredSourceKeys(); !equalStrings(got, want) {
		t.Fatalf("run order = %#v, want %#v", got, want)
	}

	var got batchManualIngestReport
	if decodeErr := json.Unmarshal(stdout.Bytes(), &got); decodeErr != nil {
		t.Fatalf("decode batch output: %v", decodeErr)
	}
	if got.Results[1].Source != ingest.YellowArchSource {
		t.Fatalf("failed result source = %q, want %q", got.Results[1].Source, ingest.YellowArchSource)
	}
	if got.Results[1].Error == "" {
		t.Fatal("failed result error = empty, want error")
	}
	if got.Results[len(got.Results)-1].Source != ingest.RegisteredSourceKeys()[len(ingest.RegisteredSourceKeys())-1] {
		t.Fatalf("last result source = %q, want %q", got.Results[len(got.Results)-1].Source, ingest.RegisteredSourceKeys()[len(ingest.RegisteredSourceKeys())-1])
	}
}

func TestRunWithArgsAllSourcesStagesEachSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	var stdout bytes.Buffer

	originalFetcher := newHTTPFetcher
	originalRunManual := runManualImport
	defer func() {
		newHTTPFetcher = originalFetcher
		runManualImport = originalRunManual
	}()

	newHTTPFetcher = func(timeout time.Duration, userAgent string) (ingest.Fetcher, error) {
		return fakeFetcher{}, nil
	}
	runManualImport = func(_ context.Context, _ *sqlite.Store, _ ingest.Fetcher, _ *ingest.Catalog, opts ingest.Options) (ingest.Report, error) {
		return ingest.Report{
			Source:      opts.Source,
			SourceURL:   "https://" + opts.Source + ".example.test/",
			ImportRunID: int64(len(opts.Source)),
			StartedAt:   "2026-04-24T10:00:00Z",
			FinishedAt:  "2026-04-24T10:01:00Z",
			Status:      "succeeded",
			Limit:       opts.Limit,
			Calendars: []ingest.CalendarReport{{
				URL: "https://" + opts.Source + ".example.test/calendar.ics",
				Candidates: []ingest.EventCandidate{{
					UID:      opts.Source + "-uid",
					Summary:  strings.ToUpper(opts.Source) + " Show",
					Location: "External Room",
					StartAt:  "2026-05-01T19:00:00Z",
					EndAt:    "2026-05-01T22:00:00Z",
				}},
			}},
			Links:  []string{"https://" + opts.Source + ".example.test/calendar.ics"},
			Totals: ingest.ReportTotals{Links: 1, Candidates: 1},
		}, nil
	}

	if err := runWithArgs([]string{"-db", path, "-all-sources", "-http-user-agent", "agent", "-stage-review-groups"}, &stdout, io.Discard); err != nil {
		t.Fatalf("all-sources staged run: %v", err)
	}

	var got batchManualIngestReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode batch output: %v", err)
	}
	if gotCount, wantCount := len(got.Results), len(ingest.RegisteredSourceKeys()); gotCount != wantCount {
		t.Fatalf("results = %d, want %d", gotCount, wantCount)
	}
	for _, result := range got.Results {
		if result.ReviewStage == nil {
			t.Fatalf("review stage missing for source %q", result.Source)
		}
		if result.ReviewStage.AutoPromotedCount != 1 {
			t.Fatalf("auto promoted count for %q = %d, want 1", result.Source, result.ReviewStage.AutoPromotedCount)
		}
		if result.ReviewStage.GroupsCreated != 0 {
			t.Fatalf("groups created for %q = %d, want 0", result.Source, result.ReviewStage.GroupsCreated)
		}
		if result.ReviewStage.ReviewCandidateCount != 0 {
			t.Fatalf("review candidate count for %q = %d, want 0", result.Source, result.ReviewStage.ReviewCandidateCount)
		}
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got := countRows(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
}

func TestReviewStageForReportSkipsFailedManualRun(t *testing.T) {
	st := &fakeReviewStageStore{results: []fakeReviewStageResult{{id: 101, created: true}}}

	stage, err := reviewStageForReport(context.Background(), st, testSourceCatalog(t), successfulManualReportForReviewStage(), errors.New("manual ingest failed"))
	if err != nil {
		t.Fatalf("review stage for failed run: %v", err)
	}
	if got, want := len(st.inputs), 0; got != want {
		t.Fatalf("created groups = %d, want %d", got, want)
	}
	if !stage.Enabled {
		t.Fatal("stage enabled = false, want true")
	}
	if stage.GroupsCreated != 0 || stage.CandidateCount != 0 {
		t.Fatalf("stage counts = groups %d candidates %d, want zero", stage.GroupsCreated, stage.CandidateCount)
	}
}

func TestCreateReviewGroupsFromReportReportsCreateError(t *testing.T) {
	st := &fakeReviewStageStore{err: errors.New("insert failed")}

	stage, err := createReviewGroupsFromReport(context.Background(), st, testSourceCatalog(t), successfulManualReportForReviewStage())
	if err == nil {
		t.Fatal("expected staging error")
	}
	if got, want := stage.GroupsCreated, 0; got != want {
		t.Fatalf("stage groups created = %d, want %d", got, want)
	}
	if got, want := len(stage.Errors), 1; got != want {
		t.Fatalf("stage errors = %d, want %d", got, want)
	}
	if !strings.Contains(stage.Errors[0], "insert failed") {
		t.Fatalf("stage error = %q, want insert failure", stage.Errors[0])
	}
}

type fakeReviewStageStore struct {
	results          []fakeReviewStageResult
	promotionResults []fakePromotionResult
	err              error
	inputs           []review.GroupInput
	promotedInputs   []review.GroupInput
}

type fakeFetcher struct{}

func (fakeFetcher) Fetch(context.Context, string) (ingest.FetchResult, error) {
	return ingest.FetchResult{}, nil
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

type fakeReviewStageResult struct {
	id                 int64
	created            bool
	autoResolved       bool
	autoResolvedResult string
	canonicalEventSlug string
}

type fakePromotionResult struct {
	eventSlug string
	promoted  bool
	err       error
}

func (s *fakeReviewStageStore) StageReviewGroup(_ context.Context, input review.GroupInput) (review.StageGroupResult, error) {
	s.inputs = append(s.inputs, input)
	if s.err != nil {
		return review.StageGroupResult{}, s.err
	}
	if len(s.results) == 0 {
		return review.StageGroupResult{ID: int64(len(s.inputs)), Created: true}, nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return review.StageGroupResult{
		ID:                 result.id,
		Created:            result.created,
		AutoResolved:       result.autoResolved,
		AutoResolvedResult: result.autoResolvedResult,
		CanonicalEventSlug: result.canonicalEventSlug,
	}, nil
}

func (s *fakeReviewStageStore) PromoteSingletonReviewGroupIfMissing(_ context.Context, input review.GroupInput) (string, bool, error) {
	s.promotedInputs = append(s.promotedInputs, input)
	if s.err != nil {
		return "", false, s.err
	}
	if len(s.promotionResults) == 0 {
		return "", false, nil
	}
	result := s.promotionResults[0]
	s.promotionResults = s.promotionResults[1:]
	return result.eventSlug, result.promoted, result.err
}

func successfulManualReportForReviewStage() ingest.Report {
	return successfulManualReportForReviewStageWithSource(ingest.DefaultSource, "https://www.sidneyandmatilda.com/")
}

func successfulManualReportForReviewStageWithSource(source, sourceURL string) ingest.Report {
	return ingest.Report{
		Source:      source,
		SourceURL:   sourceURL,
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{
			{
				URL: "https://calendar.example.test/one.ics",
				Candidates: []ingest.EventCandidate{
					{
						UID:      "duplicate",
						Summary:  "Duplicate one",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-01T19:00:00Z",
					},
					{
						UID:      "duplicate",
						Summary:  "Duplicate two",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-02T19:00:00Z",
					},
					{
						UID:      "singleton",
						Summary:  "Singleton",
						Location: "Sidney & Matilda",
						StartAt:  "2026-05-03T19:00:00Z",
					},
				},
			},
		},
	}
}

func seedReplayRunForCLI(t *testing.T, path string) int64 {
	t.Helper()

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	pageSourceID, err := st.EnsureSource(ctx, "Sidney & Matilda listings", "https://www.sidneyandmatilda.com/")
	if err != nil {
		t.Fatalf("ensure page source: %v", err)
	}
	icsSourceID, err := st.EnsureSource(ctx, "Sidney & Matilda Google Calendar ICS", "https://legacy.example.test/live.ics")
	if err != nil {
		t.Fatalf("ensure ICS source: %v", err)
	}

	runID, startedAt, err := st.CreateImportRun(ctx, "succeeded", "links=2 candidates=4 skips=0 errors=0")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}

	pagePayload := mustReplaySnapshotPayload(t, ingest.FetchResult{
		URL:         "https://www.sidneyandmatilda.com/",
		FinalURL:    "https://www.sidneyandmatilda.com/events/",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/html",
		Body:        []byte(`<a href="https://legacy.example.test/live-one.ics">Google Calendar ICS</a><a href="https://legacy.example.test/live-two.ics">Google Calendar ICS</a>`),
		CapturedAt:  startedAt.Add(time.Minute),
	}, nil)
	if _, _, err := st.CreateSnapshot(ctx, runID, &pageSourceID, startedAt.Add(time.Minute), pagePayload); err != nil {
		t.Fatalf("create page snapshot: %v", err)
	}

	firstICSPayload := mustReplaySnapshotPayload(t, ingest.FetchResult{
		URL:         "https://legacy.example.test/live-one.ics",
		FinalURL:    "https://legacy.example.test/live-one.ics",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/calendar",
		Body: []byte(strings.Join([]string{
			"BEGIN:VCALENDAR",
			"BEGIN:VEVENT",
			"UID: duplicate",
			"SUMMARY: Duplicate one",
			"LOCATION: Sidney & Matilda",
			"DTSTART:20260501T190000Z",
			"DTEND:20260501T210000Z",
			"END:VEVENT",
			"BEGIN:VEVENT",
			"UID: duplicate",
			"SUMMARY: Duplicate two",
			"LOCATION: Sidney & Matilda",
			"DTSTART:20260502T190000Z",
			"END:VEVENT",
			"BEGIN:VEVENT",
			"UID: singleton",
			"SUMMARY: Singleton",
			"LOCATION: Sidney & Matilda",
			"DTSTART:20260503T190000Z",
			"END:VEVENT",
			"END:VCALENDAR",
			"",
		}, "\n")),
		CapturedAt: startedAt.Add(2 * time.Minute),
	}, nil)
	if _, _, err := st.CreateSnapshot(ctx, runID, &icsSourceID, startedAt.Add(2*time.Minute), firstICSPayload); err != nil {
		t.Fatalf("create ICS snapshot: %v", err)
	}

	secondICSPayload := mustReplaySnapshotPayload(t, ingest.FetchResult{
		URL:         "https://legacy.example.test/live-two.ics",
		FinalURL:    "https://legacy.example.test/live-two.ics",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/calendar",
		Body: []byte(strings.Join([]string{
			"BEGIN:VCALENDAR",
			"BEGIN:VEVENT",
			"UID: second",
			"SUMMARY: Second",
			"LOCATION: Sidney & Matilda",
			"DTSTART:20260504T190000Z",
			"END:VEVENT",
			"END:VCALENDAR",
			"",
		}, "\n")),
		CapturedAt: startedAt.Add(3 * time.Minute),
	}, nil)
	if _, _, err := st.CreateSnapshot(ctx, runID, &icsSourceID, startedAt.Add(3*time.Minute), secondICSPayload); err != nil {
		t.Fatalf("create second ICS snapshot: %v", err)
	}

	if _, err := st.FinishImportRun(ctx, runID, "succeeded", "links=2 candidates=4 skips=0 errors=0"); err != nil {
		t.Fatalf("finish import run: %v", err)
	}

	return runID
}

func seedReplayRunForCLIWithNoLinks(t *testing.T, path string) int64 {
	t.Helper()

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	pageSourceID, err := st.EnsureSource(ctx, "Sidney & Matilda listings", "https://www.sidneyandmatilda.com/")
	if err != nil {
		t.Fatalf("ensure page source: %v", err)
	}

	runID, startedAt, err := st.CreateImportRun(ctx, "succeeded", "links=0 candidates=0 skips=0 errors=0")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}

	pagePayload := mustReplaySnapshotPayload(t, ingest.FetchResult{
		URL:         "https://www.sidneyandmatilda.com/",
		FinalURL:    "https://www.sidneyandmatilda.com/events/",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/html",
		Body:        []byte(`<a href="/calendar.ics">Other calendar</a>`),
		CapturedAt:  startedAt.Add(time.Minute),
	}, nil)
	if _, _, err := st.CreateSnapshot(ctx, runID, &pageSourceID, startedAt.Add(time.Minute), pagePayload); err != nil {
		t.Fatalf("create page snapshot: %v", err)
	}

	if _, err := st.FinishImportRun(ctx, runID, "succeeded", "links=0 candidates=0 skips=0 errors=0"); err != nil {
		t.Fatalf("finish import run: %v", err)
	}

	return runID
}

func seedReplayRunForCLIYellowArch(t *testing.T, path string) int64 {
	t.Helper()

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	pageSourceID, err := st.EnsureSource(ctx, "Yellow Arch listings", "https://www.yellowarch.com/events/")
	if err != nil {
		t.Fatalf("ensure page source: %v", err)
	}

	runID, startedAt, err := st.CreateImportRun(ctx, "succeeded", "links=0 candidates=2 skips=0 errors=0")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}

	pagePayload := mustReplaySnapshotPayload(t, ingest.FetchResult{
		URL:         "https://www.yellowarch.com/events/",
		FinalURL:    "https://www.yellowarch.com/events/",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/html",
		Body:        readFixture(t, filepath.Join("..", "..", "internal", "ingest", "testdata", "yellow_arch.html")),
		CapturedAt:  startedAt.Add(time.Minute),
	}, nil)
	if _, _, err := st.CreateSnapshot(ctx, runID, &pageSourceID, startedAt.Add(time.Minute), pagePayload); err != nil {
		t.Fatalf("create page snapshot: %v", err)
	}

	if _, err := st.FinishImportRun(ctx, runID, "succeeded", "links=0 candidates=2 skips=0 errors=0"); err != nil {
		t.Fatalf("finish import run: %v", err)
	}

	return runID
}

func seedReplayRunForCLILeadmill(t *testing.T, path string) int64 {
	t.Helper()

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	pageSourceID, err := st.EnsureSource(ctx, "The Leadmill listings", "https://leadmill.co.uk/live/")
	if err != nil {
		t.Fatalf("ensure page source: %v", err)
	}
	icsSourceID, err := st.EnsureSource(ctx, "The Leadmill iCal feed", "https://leadmill.co.uk/listings/?ical=1")
	if err != nil {
		t.Fatalf("ensure ICS source: %v", err)
	}

	runID, startedAt, err := st.CreateImportRun(ctx, "succeeded", "links=1 candidates=1 skips=1 errors=0")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}

	pagePayload := mustReplaySnapshotPayload(t, ingest.FetchResult{
		URL:         "https://leadmill.co.uk/live/",
		FinalURL:    "https://leadmill.co.uk/live/",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/html",
		Body:        []byte(`<link rel="alternate" type="text/calendar" href="https://leadmill.co.uk/listings/?ical=1">`),
		CapturedAt:  startedAt.Add(time.Minute),
	}, nil)
	if _, _, err := st.CreateSnapshot(ctx, runID, &pageSourceID, startedAt.Add(time.Minute), pagePayload); err != nil {
		t.Fatalf("create page snapshot: %v", err)
	}

	icsPayload := mustReplaySnapshotPayload(t, ingest.FetchResult{
		URL:         "https://leadmill.co.uk/listings/?ical=1",
		FinalURL:    "https://leadmill.co.uk/listings/?ical=1",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/calendar",
		Body: []byte(strings.Join([]string{
			"BEGIN:VCALENDAR",
			"BEGIN:VEVENT",
			"UID: leadmill-live",
			"SUMMARY: Leadmill Show",
			"LOCATION: The Leadmill, Sheffield",
			"CATEGORIES: Live",
			"DTSTART:20260501T190000Z",
			"END:VEVENT",
			"BEGIN:VEVENT",
			"UID: leadmill-club",
			"SUMMARY: Club Night",
			"LOCATION: The Leadmill, Sheffield",
			"CATEGORIES: Club",
			"DTSTART:20260502T190000Z",
			"END:VEVENT",
			"END:VCALENDAR",
			"",
		}, "\n")),
		CapturedAt: startedAt.Add(2 * time.Minute),
	}, nil)
	if _, _, err := st.CreateSnapshot(ctx, runID, &icsSourceID, startedAt.Add(2*time.Minute), icsPayload); err != nil {
		t.Fatalf("create ICS snapshot: %v", err)
	}

	if _, err := st.FinishImportRun(ctx, runID, "succeeded", "links=1 candidates=1 skips=1 errors=0"); err != nil {
		t.Fatalf("finish import run: %v", err)
	}

	return runID
}

func seedReplayRunForCLIRepairDescriptions(t *testing.T, path string) (int64, string) {
	t.Helper()

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	pageSourceID, err := st.EnsureSource(ctx, "Cafe No. 9 listings", "https://www.wegottickets.com/Cafe9")
	if err != nil {
		t.Fatalf("ensure page source: %v", err)
	}

	runID, startedAt, err := st.CreateImportRun(ctx, "succeeded", "links=0 candidates=3 skips=1 errors=0")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}

	pagePayload := mustReplaySnapshotPayload(t, ingest.FetchResult{
		URL:         "https://www.wegottickets.com/Cafe9",
		FinalURL:    "https://www.wegottickets.com/Cafe9",
		Status:      "200 OK",
		StatusCode:  200,
		ContentType: "text/html",
		Body:        readFixture(t, filepath.Join("..", "..", "internal", "ingest", "testdata", "cafe9_page.html")),
		CapturedAt:  startedAt.Add(time.Minute),
	}, nil)
	if _, _, err := st.CreateSnapshot(ctx, runID, &pageSourceID, startedAt.Add(time.Minute), pagePayload); err != nil {
		t.Fatalf("create page snapshot: %v", err)
	}

	if _, err := st.FinishImportRun(ctx, runID, "succeeded", "links=0 candidates=3 skips=1 errors=0"); err != nil {
		t.Fatalf("finish import run: %v", err)
	}

	seedSlug, promoted, err := st.PromoteSingletonReviewGroupIfMissing(ctx, review.GroupInput{
		Title:      "Cafe No. 9 singleton",
		SourceName: "Cafe No. 9 manual ingest",
		SourceURL:  "https://www.wegottickets.com/Cafe9",
		Candidates: []review.CandidateInput{{
			ExternalID:  "https://www.wegottickets.com/event/700001",
			Name:        "An evening with Ellie Gowers at Cafe No9",
			VenueSlug:   "cafe-no-9",
			StartAt:     "2026-05-10T19:30:00Z",
			Status:      "Listed",
			Description: "",
			SourceName:  "Cafe No. 9 manual ingest",
			SourceURL:   "https://www.wegottickets.com/event/700001",
		}},
	})
	if err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if !promoted {
		t.Fatal("seed promoted = false, want true")
	}

	return runID, seedSlug
}

func readFixture(t *testing.T, path string) []byte {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return raw
}

type eventRow struct {
	ID               int64
	Slug             string
	VenueID          int64
	SourceID         int64
	Name             string
	StartAt          string
	EndAt            sql.NullString
	Genre            string
	Status           string
	Description      string
	LastCheckedAt    string
	Origin           string
	PublicationState string
}

func loadEventRow(t *testing.T, db *sql.DB, slug string) eventRow {
	t.Helper()

	var row eventRow
	if err := db.QueryRow(`
		SELECT id, slug, venue_id, source_id, name, start_at, end_at, genre, status, description, last_checked_at, origin, publication_state
		FROM events
		WHERE slug = ?
	`, slug).Scan(&row.ID, &row.Slug, &row.VenueID, &row.SourceID, &row.Name, &row.StartAt, &row.EndAt, &row.Genre, &row.Status, &row.Description, &row.LastCheckedAt, &row.Origin, &row.PublicationState); err != nil {
		t.Fatalf("load event row %q: %v", slug, err)
	}
	return row
}

func openRawDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", "file://"+filepath.ToSlash(path)+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping raw db: %v", err)
	}
	return db
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()

	row := db.QueryRow("SELECT COUNT(*) FROM " + table)
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func focusFixturePNG(t *testing.T) []byte {
	t.Helper()

	img := image.NewRGBA(image.Rect(0, 0, 100, 100))
	for y := 0; y < 100; y++ {
		for x := 0; x < 100; x++ {
			img.Set(x, y, color.RGBA{R: 180, G: 180, B: 180, A: 255})
		}
	}
	for y := 70; y < 90; y++ {
		for x := 70; x < 90; x++ {
			img.Set(x, y, color.RGBA{A: 255})
		}
	}

	var body bytes.Buffer
	if err := png.Encode(&body, img); err != nil {
		t.Fatalf("encode focus fixture image: %v", err)
	}
	return body.Bytes()
}

func oversizedPNGHeader(t *testing.T, width, height uint32) []byte {
	t.Helper()

	var body bytes.Buffer
	body.Write([]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'})
	writePNGChunk(t, &body, "IHDR", func() []byte {
		data := make([]byte, 13)
		binary.BigEndian.PutUint32(data[0:4], width)
		binary.BigEndian.PutUint32(data[4:8], height)
		data[8] = 8
		data[9] = 2
		return data
	}())
	writePNGChunk(t, &body, "IEND", nil)
	return body.Bytes()
}

func writePNGChunk(t *testing.T, body *bytes.Buffer, chunkType string, data []byte) {
	t.Helper()

	if len(chunkType) != 4 {
		t.Fatalf("chunk type %q length = %d, want 4", chunkType, len(chunkType))
	}
	if err := binary.Write(body, binary.BigEndian, uint32(len(data))); err != nil {
		t.Fatalf("write chunk length: %v", err)
	}
	body.WriteString(chunkType)
	body.Write(data)
	crc := crc32.NewIEEE()
	_, _ = crc.Write([]byte(chunkType))
	_, _ = crc.Write(data)
	if err := binary.Write(body, binary.BigEndian, crc.Sum32()); err != nil {
		t.Fatalf("write chunk crc: %v", err)
	}
}

func mustReplaySnapshotPayload(t *testing.T, result ingest.FetchResult, mutate func(*ingest.SnapshotEnvelope)) string {
	t.Helper()

	envelope := ingest.NewSnapshotEnvelope(result)
	if mutate != nil {
		mutate(&envelope)
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal replay snapshot payload: %v", err)
	}
	return string(raw)
}
