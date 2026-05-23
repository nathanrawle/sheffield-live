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
	seedstore "sheffield-live/internal/store"
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

func TestCreateEventReviewClustersFromReportStagesEventReviewClusters(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{
		{id: 101, created: true},
		{id: 101, created: false},
		{id: 102, created: false},
	}}
	report := successfulManualReportForEventReviewClusters()

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("stage event-review clusters: %v", err)
	}

	if got, want := len(st.inputs), 3; got != want {
		t.Fatalf("created groups = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("stage event-review clusters created = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClustersReused, 1; got != want {
		t.Fatalf("stage event-review clusters reused = %d, want %d", got, want)
	}
	if got, want := stage.CandidateCount, 3; got != want {
		t.Fatalf("stage candidate count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 3; got != want {
		t.Fatalf("stage review candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.EventReviewClusters), 2; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClusters[0].ClusterID, int64(101); got != want {
		t.Fatalf("stage cluster ID = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClusters[0].CandidateCount, 2; got != want {
		t.Fatalf("first stage cluster candidates = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClusters[0].Result, "created"; got != want {
		t.Fatalf("first stage cluster result = %q, want %q", got, want)
	}
	if got, want := stage.EventReviewClusters[1].ClusterID, int64(102); got != want {
		t.Fatalf("second stage cluster ID = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClusters[1].CandidateCount, 1; got != want {
		t.Fatalf("second stage cluster candidates = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClusters[1].Result, "reused"; got != want {
		t.Fatalf("second stage cluster result = %q, want %q", got, want)
	}
	var firstPayload map[string]any
	if err := json.Unmarshal([]byte(st.inputs[0].Payload), &firstPayload); err != nil {
		t.Fatalf("decode first payload: %v", err)
	}
	if got, want := firstPayload["group_title"], "Duplicate review: Duplicate one"; got != want {
		t.Fatalf("first staged cluster title = %q, want %q", got, want)
	}
	var secondPayload map[string]any
	if err := json.Unmarshal([]byte(st.inputs[2].Payload), &secondPayload); err != nil {
		t.Fatalf("decode second payload: %v", err)
	}
	if got, want := secondPayload["group_title"], "New listing review: Singleton"; got != want {
		t.Fatalf("second staged cluster title = %q, want %q", got, want)
	}
}

func TestCreateEventReviewClustersFromReportReportsSupersededClusterIDsAndStagingKey(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{
		{id: 201, created: true, supersededClusterIDs: []int64{88, 77}},
		{id: 201, created: false},
	}}
	report := ingest.Report{
		Source:      ingest.DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []ingest.EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "Duplicate one",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T19:00:00Z",
				},
				{
					UID:      "shared-uid",
					Summary:  "Duplicate two",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-02T19:00:00Z",
				},
			},
		}},
	}

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}

	if got, want := len(st.inputs), 2; got != want {
		t.Fatalf("staged evidence inputs = %d, want %d", got, want)
	}
	if st.inputs[0].StagingKey == "" || st.inputs[0].StagingKeyVersion != 1 {
		t.Fatalf("first staging key = %q v%d, want populated key v1", st.inputs[0].StagingKey, st.inputs[0].StagingKeyVersion)
	}
	if got, want := st.inputs[1].StagingKey, st.inputs[0].StagingKey; got != want {
		t.Fatalf("second staging key = %q, want %q", got, want)
	}
	if got, want := len(stage.EventReviewClusters), 1; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClusters[0].SupersededClusterIDs, []int64{88, 77}; !equalInt64Slices(got, want) {
		t.Fatalf("superseded cluster ids = %#v, want %#v", got, want)
	}
}

func TestCreateEventReviewClustersFromReportReportsAutoResolvedClusters(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{
		{id: 301, created: true, clusterStatus: seedstore.EventReviewClusterStatusOpen},
		{id: 301, created: false, clusterStatus: seedstore.EventReviewClusterStatusOpen},
	}, finalizeResults: []*seedstore.EventReviewResolutionSummary{
		{
			ClusterID: 301,
			Status:    seedstore.EventReviewResolutionStatusResolved,
			AppliedAutoResolution: &seedstore.EventReviewResolutionAppliedAutoResolutionSummary{
				EventID:       9001,
				EventSlug:     "roots-night-yellow-arch-20260510183000",
				Result:        "canonical_exact_match",
				SourceID:      77,
				SourceName:    "Store test source",
				SourceURL:     "https://example.test/store-test",
				EvidenceCount: 2,
			},
		},
	}}
	report := ingest.Report{
		Source:      ingest.DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://calendar.example.test/auto-resolve.ics",
			Candidates: []ingest.EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "Roots Night",
					Location: "Yellow Arch Studios",
					StartAt:  "2026-05-10T18:30:00Z",
				},
				{
					UID:      "shared-uid",
					Summary:  "Roots Night",
					Location: "Yellow Arch Studios",
					StartAt:  "2026-05-10T18:30:00Z",
				},
			},
		}},
	}

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}
	if got, want := stage.EventReviewClustersAutoResolvedCount, 1; got != want {
		t.Fatalf("auto-resolved count = %d, want %d", got, want)
	}
	if got, want := len(stage.EventReviewClustersAutoResolved), 1; got != want {
		t.Fatalf("auto-resolved rows = %d, want %d", got, want)
	}
	row := stage.EventReviewClustersAutoResolved[0]
	if got, want := row.Result, "canonical_exact_match"; got != want {
		t.Fatalf("auto-resolved result = %q, want %q", got, want)
	}
	if got, want := row.CanonicalEventSlug, "roots-night-yellow-arch-20260510183000"; got != want {
		t.Fatalf("auto-resolved canonical slug = %q, want %q", got, want)
	}
	if got, want := row.CandidateCount, 2; got != want {
		t.Fatalf("auto-resolved candidate count = %d, want %d", got, want)
	}
	if got, want := row.ClusterID, int64(301); got != want {
		t.Fatalf("auto-resolved cluster id = %d, want %d", got, want)
	}
	if got, want := len(st.finalizedCalls), 1; got != want {
		t.Fatalf("finalized calls = %d, want %d", got, want)
	}
	if got, want := st.finalizedCalls[0].clusterID, int64(301); got != want {
		t.Fatalf("finalized cluster id = %d, want %d", got, want)
	}
}

func TestCreateEventReviewClustersFromReportSkipsFinalizationForTerminalReusedCluster(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{
		{id: 301, created: false, clusterStatus: seedstore.EventReviewClusterStatusResolved},
		{id: 301, created: false, clusterStatus: seedstore.EventReviewClusterStatusResolved},
	}}
	report := ingest.Report{
		Source:      ingest.DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []ingest.EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "Reuse one",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T19:00:00Z",
				},
				{
					UID:      "shared-uid",
					Summary:  "Reuse two",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-02T19:00:00Z",
				},
			},
		}},
	}

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}
	if got, want := len(stage.EventReviewClusters), 1; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
	}
	if got := len(st.finalizedCalls); got != 0 {
		t.Fatalf("finalize calls = %d, want 0", got)
	}
}

func TestCreateEventReviewClustersFromReportFinalizesOnlyOpenClusterInMixedResults(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{
		{id: 301, created: false, clusterStatus: seedstore.EventReviewClusterStatusResolved},
		{id: 302, created: true, clusterStatus: seedstore.EventReviewClusterStatusOpen},
	}}
	report := ingest.Report{
		Source:      ingest.DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://calendar.example.test/one.ics",
			Candidates: []ingest.EventCandidate{
				{
					UID:      "shared-uid",
					Summary:  "Mixed one",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-01T19:00:00Z",
				},
				{
					UID:      "shared-uid",
					Summary:  "Mixed two",
					Location: "Sidney & Matilda",
					StartAt:  "2026-05-02T19:00:00Z",
				},
			},
		}},
	}

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}
	if got, want := len(stage.EventReviewClusters), 1; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClusters[0].ClusterID, int64(302); got != want {
		t.Fatalf("stage cluster id = %d, want open cluster id %d", got, want)
	}
	if got, want := len(st.finalizedCalls), 1; got != want {
		t.Fatalf("finalize calls = %d, want %d", got, want)
	}
	if got, want := st.finalizedCalls[0].clusterID, int64(302); got != want {
		t.Fatalf("finalized cluster id = %d, want %d", got, want)
	}
	if got, want := st.finalizedCalls[0].evidenceIDs, []int64{302}; !equalInt64Slices(got, want) {
		t.Fatalf("finalized evidence ids = %#v, want %#v", got, want)
	}
}

func TestCreateEventReviewClustersFromReportAutoPromotesOwnedVenueSingleton(t *testing.T) {
	st := &fakeEventReviewClustersStore{
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

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
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
	if got, want := len(stage.EventReviewClusters), 0; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
	}
	if got, want := len(stage.AutoPromoted), 1; got != want {
		t.Fatalf("auto promoted = %d, want %d", got, want)
	}
	if got, want := stage.AutoPromoted[0].EventSlug, "live-late-junction-yellow-arch-20260510183000"; got != want {
		t.Fatalf("event slug = %q, want %q", got, want)
	}
	if got, want := len(st.inputs), 0; got != want {
		t.Fatalf("staged event-review clusters = %d, want %d", got, want)
	}
	if got, want := len(st.promotedInputs), 1; got != want {
		t.Fatalf("promoted inputs = %d, want %d", got, want)
	}
}

func TestCreateEventReviewClustersFromReportAutoPromotesCafeNo9SingletonWithoutEndTime(t *testing.T) {
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

	stage, err := createEventReviewClustersFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}
	if got, want := stage.AutoPromotedCount, 1; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 0; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.EventReviewClusters), 0; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
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

func TestCreateEventReviewClustersFromReportAutoPromotesJazzAtTheLescarSingleton(t *testing.T) {
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

	stage, err := createEventReviewClustersFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}
	if got, want := stage.AutoPromotedCount, 1; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 0; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.EventReviewClusters), 0; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
	}

	event, ok := st.EventBySlug(stage.AutoPromoted[0].EventSlug)
	if !ok {
		t.Fatalf("missing published event %q", stage.AutoPromoted[0].EventSlug)
	}
	if got, want := event.VenueSlug, "lescar"; got != want {
		t.Fatalf("venue slug = %q, want %q", got, want)
	}
}

func TestCreateEventReviewClustersFromReportAutoPromotesSingletonAtNewProvisionalVenue(t *testing.T) {
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

	stage, err := createEventReviewClustersFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}
	if got, want := stage.AutoPromotedCount, 1; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 0; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(stage.EventReviewClusters), 0; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
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

func TestCreateEventReviewClustersFromReportKeepsOffsiteLeadmillSingletonInReview(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{{id: 101, created: true}}}
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

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}

	if got, want := stage.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event-review clusters created = %d, want %d", got, want)
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
		t.Fatalf("staged event review evidence inputs = %d, want %d", got, want)
	}
}

func TestCreateEventReviewClustersFromReportStagesDuplicateJazzAtTheLescarGroup(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{{id: 301, created: true}}}
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

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}

	if got, want := stage.AutoPromotedCount, 0; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event-review clusters created = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 2; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(st.promotedInputs), 0; got != want {
		t.Fatalf("promoted inputs = %d, want %d", got, want)
	}
	if got, want := len(st.inputs), 2; got != want {
		t.Fatalf("staged event-review clusters = %d, want %d", got, want)
	}
}

func TestCreateEventReviewClustersFromReportKeepsWrongVenueJazzAtTheLescarSingletonInReview(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{{id: 302, created: true}}}
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

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}

	if got, want := stage.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event-review clusters created = %d, want %d", got, want)
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
		t.Fatalf("staged event-review clusters = %d, want %d", got, want)
	}
}

func TestCreateEventReviewClustersFromReportFallsBackToReviewWhenAutoPromoteSeesExistingCanonical(t *testing.T) {
	st := &fakeEventReviewClustersStore{
		results: []fakeEventReviewClustersResult{{id: 201, created: true}},
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

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}

	if got, want := stage.AutoPromotedCount, 0; got != want {
		t.Fatalf("auto promoted count = %d, want %d", got, want)
	}
	if got, want := stage.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("event-review clusters created = %d, want %d", got, want)
	}
	if got, want := stage.ReviewCandidateCount, 1; got != want {
		t.Fatalf("review candidate count = %d, want %d", got, want)
	}
	if got, want := len(st.inputs), 1; got != want {
		t.Fatalf("staged event-review clusters = %d, want %d", got, want)
	}
}

func TestCreateEventReviewClustersFromReportReusesExistingGroupWhenOnlySourceMetadataDiffers(t *testing.T) {
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
	importRunID, _, err := st.CreateImportRun(ctx, "succeeded", "event review staging test")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}

	firstReport := ingest.Report{
		Source:      "Source A",
		SourceURL:   "https://source-a.example.test/events.ics",
		ImportRunID: importRunID,
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
	firstStage, err := createEventReviewClustersFromReport(ctx, st, testSourceCatalog(t), firstReport)
	if err != nil {
		t.Fatalf("stage first report: %v", err)
	}
	if got, want := firstStage.EventReviewClustersCreated, 1; got != want {
		t.Fatalf("first stage event-review clusters created = %d, want %d", got, want)
	}
	if got, want := firstStage.EventReviewClustersReused, 0; got != want {
		t.Fatalf("first stage event-review clusters reused = %d, want %d", got, want)
	}
	if got, want := len(firstStage.EventReviewClusters), 1; got != want {
		t.Fatalf("first stage clusters = %d, want %d", got, want)
	}

	secondReport := ingest.Report{
		Source:      "Source B",
		SourceURL:   "https://source-b.example.test/events.ics",
		ImportRunID: importRunID,
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
	secondStage, err := createEventReviewClustersFromReport(ctx, st, testSourceCatalog(t), secondReport)
	if err != nil {
		t.Fatalf("stage second report: %v", err)
	}
	if got, want := secondStage.EventReviewClustersCreated, 0; got != want {
		t.Fatalf("second stage event-review clusters created = %d, want %d", got, want)
	}
	if got, want := secondStage.EventReviewClustersReused, 1; got != want {
		t.Fatalf("second stage event-review clusters reused = %d, want %d", got, want)
	}
	if got, want := len(secondStage.EventReviewClusters), 1; got != want {
		t.Fatalf("second stage clusters = %d, want %d", got, want)
	}
	if got, want := firstStage.EventReviewClusters[0].ClusterID, secondStage.EventReviewClusters[0].ClusterID; got != want {
		t.Fatalf("staged cluster id = %d, want %d", got, want)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got, want := countRows(t, db, "event_review_clusters"), 1; got != want {
		t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
	}
	if got, want := countRows(t, db, "event_review_evidence"), 4; got != want {
		t.Fatalf("event_review_evidence rows = %d, want %d", got, want)
	}
	if got, want := countRows(t, db, "import_run_event_review_clusters"), 1; got != want {
		t.Fatalf("import_run_event_review_clusters rows = %d, want %d", got, want)
	}
	if got, want := countRows(t, db, "review_groups"), 0; got != want {
		t.Fatalf("review_groups rows = %d, want %d", got, want)
	}
}

func TestCreateEventReviewClustersFromReportPersistsAuthoritativeGroupMetadata(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer st.Close()
	importRunID, _, err := st.CreateImportRun(ctx, "succeeded", "event review staging test")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}

	report := ingest.Report{
		Source:      ingest.DefaultSource,
		SourceURL:   "https://www.sidneyandmatilda.com/",
		ImportRunID: importRunID,
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

	stage, err := createEventReviewClustersFromReport(ctx, st, testSourceCatalog(t), report)
	if err != nil {
		t.Fatalf("create event-review clusters: %v", err)
	}
	if got, want := len(stage.EventReviewClusters), 1; got != want {
		t.Fatalf("event-review clusters = %d, want %d", got, want)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got, want := countRows(t, db, "event_review_clusters"), 1; got != want {
		t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
	}
	if got, want := countRows(t, db, "event_review_evidence"), 2; got != want {
		t.Fatalf("event_review_evidence rows = %d, want %d", got, want)
	}
	if got, want := countRows(t, db, "import_run_event_review_clusters"), 1; got != want {
		t.Fatalf("import_run_event_review_clusters rows = %d, want %d", got, want)
	}
	if got, want := countRows(t, db, "review_groups"), 0; got != want {
		t.Fatalf("review_groups rows = %d, want %d", got, want)
	}

	var payload string
	var sourceID int64
	if err := db.QueryRow(`
		SELECT payload, source_id
		FROM event_review_evidence
		ORDER BY id
		LIMIT 1
	`).Scan(&payload, &sourceID); err != nil {
		t.Fatalf("load event review evidence: %v", err)
	}

	var sourceName string
	var sourceURL string
	if err := db.QueryRow(`
		SELECT name, url
		FROM sources
		WHERE id = ?
	`, sourceID).Scan(&sourceName, &sourceURL); err != nil {
		t.Fatalf("load event review source: %v", err)
	}
	if got, want := sourceName, "Sidney & Matilda manual ingest"; got != want {
		t.Fatalf("source name = %q, want %q", got, want)
	}
	if got, want := sourceURL, "https://calendar.example.test/live.ics"; got != want {
		t.Fatalf("source url = %q, want %q", got, want)
	}

	var evidencePayload map[string]any
	if err := json.Unmarshal([]byte(payload), &evidencePayload); err != nil {
		t.Fatalf("decode evidence payload: %v", err)
	}
	if got, want := evidencePayload["source_authority"], "authoritative"; got != want {
		t.Fatalf("payload source authority = %q, want %q", got, want)
	}
	if got, want := evidencePayload["group_authoritative_source_name"], "Sidney & Matilda manual ingest"; got != want {
		t.Fatalf("payload authoritative source name = %q, want %q", got, want)
	}
	if got, want := evidencePayload["group_authoritative_source_url"], "https://calendar.example.test/live.ics"; got != want {
		t.Fatalf("payload authoritative source url = %q, want %q", got, want)
	}
	if got, want := evidencePayload["group_authoritative_source_event_key"], "uid:shared-uid"; got != want {
		t.Fatalf("payload authoritative source event key = %q, want %q", got, want)
	}
}

func TestParseIngestArgsCommandDispatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		args       []string
		wantCmd    ingestCommand
		wantFix    fixCommandKind
		wantSource string
		wantSet    bool
		wantAll    bool
		wantUA     string
		wantDB     string
		wantDryRun bool
		wantStage  bool
		wantTitle  bool
		wantDesc   bool
		wantReplay int64
		wantLatest bool
		wantErr    bool
	}{
		{name: "bare live all sources", args: nil, wantCmd: ingestCommandLive, wantAll: true, wantStage: true},
		{name: "single source live", args: []string{"-source", ingest.LeadmillSource}, wantCmd: ingestCommandLive, wantSource: ingest.LeadmillSource, wantSet: true, wantStage: true},
		{name: "live dry run", args: []string{"-dry-run"}, wantCmd: ingestCommandLive, wantAll: true, wantDryRun: true},
		{name: "user agent", args: []string{"-user-agent", "agent"}, wantCmd: ingestCommandLive, wantAll: true, wantUA: "agent", wantStage: true},
		{name: "replay latest", args: []string{"replay"}, wantCmd: ingestCommandReplay, wantLatest: true, wantStage: true},
		{name: "replay absolute", args: []string{"replay", "42"}, wantCmd: ingestCommandReplay, wantReplay: 42, wantStage: true},
		{name: "replay title repair", args: []string{"replay", "-titles", "42"}, wantCmd: ingestCommandReplay, wantReplay: 42, wantTitle: true},
		{name: "replay description repair dry run", args: []string{"replay", "-descriptions", "-dry-run", "42"}, wantCmd: ingestCommandReplay, wantReplay: 42, wantDesc: true, wantDryRun: true},
		{name: "global db replay", args: []string{"-db", "global.db", "replay", "42"}, wantCmd: ingestCommandReplay, wantReplay: 42, wantDB: "global.db", wantStage: true},
		{name: "global long db replay", args: []string{"--db", "global.db", "replay", "42"}, wantCmd: ingestCommandReplay, wantReplay: 42, wantDB: "global.db", wantStage: true},
		{name: "global long db equals fix", args: []string{"--db=global.db", "fix", "titles"}, wantCmd: ingestCommandFix, wantFix: fixCommandTitles, wantAll: true, wantDB: "global.db", wantTitle: true},
		{name: "matching global and local db", args: []string{"-db", "global.db", "replay", "-db", "global.db", "42"}, wantCmd: ingestCommandReplay, wantReplay: 42, wantDB: "global.db", wantStage: true},
		{name: "fix titles", args: []string{"fix", "titles"}, wantCmd: ingestCommandFix, wantFix: fixCommandTitles, wantAll: true, wantTitle: true},
		{name: "fix descriptions source", args: []string{"fix", "descriptions", "-source", ingest.CafeNo9Source}, wantCmd: ingestCommandFix, wantFix: fixCommandDescriptions, wantSource: ingest.CafeNo9Source, wantSet: true, wantDesc: true},
		{name: "fix historical duplicates", args: []string{"fix", "historical-duplicates", "-dry-run"}, wantCmd: ingestCommandFix, wantFix: fixCommandHistoricalDuplicates, wantDryRun: true},
		{name: "fix image focus", args: []string{"fix", "image-focus"}, wantCmd: ingestCommandFix, wantFix: fixCommandImageFocus},
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
			if got := cfg.command; got != tc.wantCmd {
				t.Fatalf("command = %q, want %q", got, tc.wantCmd)
			}
			if got := cfg.fixKind; got != tc.wantFix {
				t.Fatalf("fix kind = %q, want %q", got, tc.wantFix)
			}
			if got := cfg.source; got != tc.wantSource {
				t.Fatalf("source = %q, want %q", got, tc.wantSource)
			}
			if got := cfg.sourceSet; got != tc.wantSet {
				t.Fatalf("source set = %v, want %v", got, tc.wantSet)
			}
			if got := cfg.httpUserAgent; got != tc.wantUA {
				t.Fatalf("user agent = %q, want %q", got, tc.wantUA)
			}
			if got := cfg.dbPath; got != tc.wantDB {
				t.Fatalf("db path = %q, want %q", got, tc.wantDB)
			}
			if got := cfg.dryRun; got != tc.wantDryRun {
				t.Fatalf("dry run = %v, want %v", got, tc.wantDryRun)
			}
			if got := cfg.stageEventReviewClusters; got != tc.wantStage {
				t.Fatalf("stage event reviews = %v, want %v", got, tc.wantStage)
			}
			if got := cfg.repairEventTitles; got != tc.wantTitle {
				t.Fatalf("repair event titles = %v, want %v", got, tc.wantTitle)
			}
			if got := cfg.repairDescriptions; got != tc.wantDesc {
				t.Fatalf("repair descriptions = %v, want %v", got, tc.wantDesc)
			}
			if got := cfg.replayImportRunID; got != tc.wantReplay {
				t.Fatalf("replay import run id = %d, want %d", got, tc.wantReplay)
			}
			if got := cfg.replayUseLatest; got != tc.wantLatest {
				t.Fatalf("replay latest = %v, want %v", got, tc.wantLatest)
			}
			if got := cfg.allSources; got != tc.wantAll {
				t.Fatalf("all sources = %v, want %v", got, tc.wantAll)
			}
		})
	}
}

func TestParseIngestArgsRejectsRemovedAndMalformedCommands(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		args []string
	}{
		{name: "removed http user agent", args: []string{"-http-user-agent", "agent"}},
		{name: "removed all sources", args: []string{"-all-sources"}},
		{name: "removed stage event reviews", args: []string{"-stage-event-reviews"}},
		{name: "removed repair descriptions", args: []string{"-repair-descriptions"}},
		{name: "removed repair event titles", args: []string{"-repair-event-titles"}},
		{name: "removed backfill image focus", args: []string{"-backfill-image-focus"}},
		{name: "fix missing subcommand", args: []string{"fix"}},
		{name: "fix unknown subcommand", args: []string{"fix", "nope"}},
		{name: "replay flags after id", args: []string{"replay", "42", "-db", "path"}},
		{name: "replay negative id", args: []string{"replay", "-1"}},
		{name: "replay extra args", args: []string{"replay", "42", "43"}},
		{name: "replay repair conflict", args: []string{"replay", "-titles", "-descriptions"}},
		{name: "unsupported description source", args: []string{"fix", "descriptions", "-source", ingest.LeadmillSource}},
		{name: "conflicting global db", args: []string{"-db", "global.db", "replay", "-db", "local.db"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseIngestArgs(tc.args); err == nil {
				t.Fatal("expected parse error")
			}
		})
	}
}

func TestIngestModeReportsFixTitleRepair(t *testing.T) {
	cfg := ingestCommandConfig{
		command:           ingestCommandFix,
		fixKind:           fixCommandTitles,
		repairEventTitles: true,
	}
	if got, want := ingestMode(cfg), "title_repair_live"; got != want {
		t.Fatalf("ingest mode = %q, want %q", got, want)
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

func TestRunWithArgsRepairEventTitlesDryRunDoesNotMutate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sourceID, err := st.EnsureSource(context.Background(), "Yellow Arch manual ingest", "https://www.yellowarch.com/event/late-junction/")
	if err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	db := openRawDB(t, path)
	defer db.Close()
	dirty := "Late Junction - Yellow Arch Studios"
	startAt := "2026-05-10T18:30:00Z"
	dirtySlug := mustLiveEventSlugForCLI(t, dirty, "yellow-arch", startAt)
	insertCLIEvent(t, db, sourceID, dirtySlug, "yellow-arch", dirty, startAt)

	originalReplay := replayImportRun
	defer func() {
		replayImportRun = originalReplay
	}()
	replayImportRun = func(_ context.Context, _ *sqlite.Store, _ *ingest.Catalog, importRunID int64, _ ingest.ReplayOptions) (ingest.Report, error) {
		if importRunID != 42 {
			t.Fatalf("import run id = %d, want 42", importRunID)
		}
		return cliYellowArchTitleRepairReport(startAt, dirty), nil
	}

	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "replay", "-titles", "-dry-run", "42"}, &stdout, io.Discard); err != nil {
		t.Fatalf("run repair event titles: %v", err)
	}
	var got titleRepairRunReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if !got.TitleRepair.DryRun || got.TitleRepair.Applied {
		t.Fatalf("dry/apply = %v/%v, want true/false", got.TitleRepair.DryRun, got.TitleRepair.Applied)
	}
	if got.TitleRepair.Repaired != 1 || got.TitleRepair.Changes[0].Result != "would_repair" {
		t.Fatalf("title repair = %#v, want one dry-run repair", got.TitleRepair)
	}
	row := loadEventRow(t, db, dirtySlug)
	if row.Name != dirty {
		t.Fatalf("event name = %q, want unchanged %q", row.Name, dirty)
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
		"replay",
		"-descriptions",
		strconv.FormatInt(runID, 10),
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

func TestRunWithArgsFixTitlesContinuesAfterSourceFailure(t *testing.T) {
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
		report := cliEmptySucceededReport(opts.Source, int64(len(order)), opts.Limit)
		if opts.Source == ingest.YellowArchSource {
			report.Status = "failed"
			report.Errors = []string{"yellow arch title repair failed"}
			return report, ingest.ErrRunFailed
		}
		return report, nil
	}

	err := runWithArgs([]string{"fix", "titles", "-db", path, "-user-agent", "agent"}, &stdout, io.Discard)
	if err == nil {
		t.Fatal("expected fix titles batch failure")
	}
	if got, want := order, ingest.RegisteredSourceKeys(); !equalStrings(got, want) {
		t.Fatalf("run order = %#v, want %#v", got, want)
	}
	if strings.Contains(stdout.String(), "review_stage") {
		t.Fatalf("fix title output contains review_stage: %s", stdout.String())
	}

	var got batchManualIngestReport
	if decodeErr := json.Unmarshal(stdout.Bytes(), &got); decodeErr != nil {
		t.Fatalf("decode fix title output: %v", decodeErr)
	}
	if gotCount, wantCount := len(got.Results), len(ingest.RegisteredSourceKeys()); gotCount != wantCount {
		t.Fatalf("results = %d, want %d", gotCount, wantCount)
	}
	var sawFailure bool
	for i, result := range got.Results {
		if result.Source != ingest.RegisteredSourceKeys()[i] {
			t.Fatalf("result %d source = %q, want %q", i, result.Source, ingest.RegisteredSourceKeys()[i])
		}
		if result.Source == ingest.YellowArchSource {
			sawFailure = true
			if result.Error == "" {
				t.Fatal("failed title result error = empty, want error")
			}
			if result.TitleRepair != nil {
				t.Fatalf("failed title result repair = %#v, want nil", result.TitleRepair)
			}
			continue
		}
		if result.TitleRepair == nil {
			t.Fatalf("successful title result for %q missing title_repair", result.Source)
		}
	}
	if !sawFailure {
		t.Fatalf("fix title results missing failed source %q", ingest.YellowArchSource)
	}
}

func TestRunWithArgsFixDescriptionsContinuesAfterSourceFailure(t *testing.T) {
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
		report := cliEmptySucceededReport(opts.Source, int64(len(order)), opts.Limit)
		if opts.Source == ingest.CafeNo9Source {
			report.Status = "failed"
			report.Errors = []string{"cafe no. 9 description repair failed"}
			return report, ingest.ErrRunFailed
		}
		return report, nil
	}

	err := runWithArgs([]string{"fix", "descriptions", "-db", path, "-user-agent", "agent"}, &stdout, io.Discard)
	if err == nil {
		t.Fatal("expected fix descriptions batch failure")
	}
	if got, want := order, []string{ingest.DefaultSource, ingest.CafeNo9Source}; !equalStrings(got, want) {
		t.Fatalf("run order = %#v, want %#v", got, want)
	}
	if strings.Contains(stdout.String(), "review_stage") {
		t.Fatalf("fix description output contains review_stage: %s", stdout.String())
	}

	var got batchManualIngestReport
	if decodeErr := json.Unmarshal(stdout.Bytes(), &got); decodeErr != nil {
		t.Fatalf("decode fix description output: %v", decodeErr)
	}
	if gotCount, wantCount := len(got.Results), 2; gotCount != wantCount {
		t.Fatalf("results = %d, want %d", gotCount, wantCount)
	}
	if got.Results[0].Source != ingest.DefaultSource || got.Results[0].DescriptionRepair == nil || got.Results[0].Error != "" {
		t.Fatalf("first description result = %#v, want successful default source repair", got.Results[0])
	}
	if got.Results[1].Source != ingest.CafeNo9Source || got.Results[1].Error == "" || got.Results[1].DescriptionRepair != nil {
		t.Fatalf("second description result = %#v, want failed Cafe No. 9 result", got.Results[1])
	}
}

func TestRunWithArgsRepairHistoricalDuplicatesDryRunDoesNotMutate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	_, loserID, loserSlug := seedHistoricalDuplicateCLIRepairPair(t, path)

	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "fix", "historical-duplicates", "-dry-run"}, &stdout, io.Discard); err != nil {
		t.Fatalf("dry-run historical duplicate repair: %v", err)
	}

	var got historicalDuplicateRepairRunReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode historical duplicate dry-run output: %v", err)
	}
	repair := got.HistoricalDuplicateRepair
	if !repair.DryRun || repair.Applied {
		t.Fatalf("dry/apply = %v/%v, want true/false", repair.DryRun, repair.Applied)
	}
	if repair.WouldWithhold != 1 || repair.AutoWithheld != 0 {
		t.Fatalf("repair counts = %#v, want one would-withhold and no auto-withhold", repair)
	}

	db := openRawDB(t, path)
	defer db.Close()
	state, canonicalID, withheldReason, repairRunID := loadHistoricalDuplicateCLIState(t, db, loserSlug)
	if state != string(domain.PublicationStateProvisional) {
		t.Fatalf("publication state = %q, want provisional", state)
	}
	if canonicalID.Valid || strings.TrimSpace(withheldReason) != "" || repairRunID.Valid {
		t.Fatalf("dry-run mutated loser %d state: canonical=%v reason=%q repair_run=%v", loserID, canonicalID, withheldReason, repairRunID)
	}
}

func TestRunWithArgsRepairHistoricalDuplicatesAppliesByDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	targetID, _, loserSlug := seedHistoricalDuplicateCLIRepairPair(t, path)

	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "fix", "historical-duplicates"}, &stdout, io.Discard); err != nil {
		t.Fatalf("historical duplicate repair: %v", err)
	}

	var got historicalDuplicateRepairRunReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode historical duplicate output: %v", err)
	}
	repair := got.HistoricalDuplicateRepair
	if repair.DryRun || !repair.Applied {
		t.Fatalf("dry/apply = %v/%v, want false/true", repair.DryRun, repair.Applied)
	}
	if repair.AutoWithheld != 1 || repair.WouldWithhold != 0 || repair.RepairRunID == 0 {
		t.Fatalf("repair counts = %#v, want one applied auto-withhold with repair run", repair)
	}

	db := openRawDB(t, path)
	defer db.Close()
	state, canonicalID, withheldReason, repairRunID := loadHistoricalDuplicateCLIState(t, db, loserSlug)
	if state != string(domain.PublicationStateWithheld) {
		t.Fatalf("publication state = %q, want withheld", state)
	}
	if !canonicalID.Valid || canonicalID.Int64 != targetID {
		t.Fatalf("canonical id = %v, want %d", canonicalID, targetID)
	}
	if withheldReason != "historical duplicate listing" {
		t.Fatalf("withheld reason = %q, want historical duplicate listing", withheldReason)
	}
	if !repairRunID.Valid || repairRunID.Int64 != repair.RepairRunID {
		t.Fatalf("withheld repair run id = %v, want %d", repairRunID, repair.RepairRunID)
	}
}

func TestRunWithArgsReplayDoesNotRequireUserAgent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID := seedReplayRunForCLI(t, path)

	runReplay := func() (manualIngestReport, []byte) {
		t.Helper()
		var stdout bytes.Buffer
		if err := runWithArgs([]string{"-db", path, "replay", "-limit", "1", strconv.FormatInt(runID, 10)}, &stdout, io.Discard); err != nil {
			t.Fatalf("replay run: %v", err)
		}
		var got manualIngestReport
		if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
			t.Fatalf("decode replay output: %v", err)
		}
		return got, append([]byte(nil), stdout.Bytes()...)
	}

	first, firstRaw := runReplay()
	second, _ := runReplay()

	var firstPayload map[string]json.RawMessage
	if err := json.Unmarshal(firstRaw, &firstPayload); err != nil {
		t.Fatalf("decode replay payload: %v", err)
	}
	var firstStagePayload map[string]json.RawMessage
	if err := json.Unmarshal(firstPayload["review_stage"], &firstStagePayload); err != nil {
		t.Fatalf("decode replay stage payload: %v", err)
	}
	for _, key := range []string{"event_review_clusters_created", "event_review_clusters_reused", "event_review_clusters_auto_resolved_count", "event_review_clusters", "event_review_clusters_auto_resolved"} {
		if _, ok := firstStagePayload[key]; !ok {
			t.Fatalf("review stage missing %q key: %s", key, firstRaw)
		}
	}
	if _, ok := firstStagePayload["duplicate_auto_resolved"]; ok {
		t.Fatalf("review stage contains legacy duplicate_auto_resolved key: %s", firstRaw)
	}
	if _, ok := firstStagePayload["groups_created"]; ok {
		t.Fatalf("review stage contains legacy groups_created key: %s", firstRaw)
	}
	var firstClusters []map[string]json.RawMessage
	if err := json.Unmarshal(firstStagePayload["event_review_clusters"], &firstClusters); err != nil {
		t.Fatalf("decode replay clusters: %v", err)
	}
	var firstAutoResolved []map[string]json.RawMessage
	if err := json.Unmarshal(firstStagePayload["event_review_clusters_auto_resolved"], &firstAutoResolved); err != nil {
		t.Fatalf("decode replay auto-resolved rows: %v", err)
	}
	if len(firstClusters) != 1 {
		t.Fatalf("replay clusters = %d, want 1", len(firstClusters))
	}
	if len(firstAutoResolved) != 0 {
		t.Fatalf("replay auto-resolved rows = %d, want 0", len(firstAutoResolved))
	}
	if _, ok := firstClusters[0]["cluster_id"]; !ok {
		t.Fatalf("review cluster missing cluster_id key: %s", firstRaw)
	}
	if _, ok := firstClusters[0]["id"]; ok {
		t.Fatalf("review cluster contains legacy id key: %s", firstRaw)
	}
	if got := firstStagePayload["event_review_clusters_auto_resolved_count"]; string(got) != "0" {
		t.Fatalf("auto-resolved count = %s, want 0", got)
	}

	if first.Report.Limit != 1 || second.Report.Limit != 1 {
		t.Fatalf("report limits = %d, %d, want 1", first.Report.Limit, second.Report.Limit)
	}
	if first.Report.ImportRunID != runID || second.Report.ImportRunID != runID {
		t.Fatalf("import run ids = %d, %d, want %d", first.Report.ImportRunID, second.Report.ImportRunID, runID)
	}
	if first.Report.Status != "succeeded" || second.Report.Status != "succeeded" {
		t.Fatalf("report statuses = %q, %q, want succeeded", first.Report.Status, second.Report.Status)
	}
	if got := len(first.Report.Links); got != 1 {
		t.Fatalf("first links = %d, want 1", got)
	}
	if got := len(second.Report.Links); got != 1 {
		t.Fatalf("second links = %d, want 1", got)
	}
	if got := first.EventReviewClusters.EventReviewClustersCreated; got != 1 {
		t.Fatalf("first event-review clusters created = %d, want 1", got)
	}
	if got := second.EventReviewClusters.EventReviewClustersCreated; got != 0 {
		t.Fatalf("second event-review clusters created = %d, want 0", got)
	}
	if got := second.EventReviewClusters.EventReviewClustersReused; got != 1 {
		t.Fatalf("second event-review clusters reused = %d, want 1", got)
	}
	if got := first.EventReviewClusters.AutoPromotedCount; got != 1 {
		t.Fatalf("first auto promoted count = %d, want 1", got)
	}
	if got := second.EventReviewClusters.AutoPromotedCount; got != 1 {
		t.Fatalf("second auto promoted count = %d, want 1", got)
	}
	if got := first.EventReviewClusters.ReviewCandidateCount; got != 2 {
		t.Fatalf("first review candidate count = %d, want 2", got)
	}
	if got := second.EventReviewClusters.ReviewCandidateCount; got != 2 {
		t.Fatalf("second review candidate count = %d, want 2", got)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got := countRows(t, db, "event_review_evidence"); got != 2 {
		t.Fatalf("event_review_evidence rows = %d, want 2", got)
	}
	if got := countRows(t, db, "event_review_clusters"); got != 1 {
		t.Fatalf("event_review_clusters rows = %d, want 1", got)
	}
	if got := countRows(t, db, "import_run_event_review_clusters"); got != 1 {
		t.Fatalf("import_run_event_review_clusters rows = %d, want 1", got)
	}
	if got := countRows(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
}

func TestRunWithArgsReplayLatestFinishedFailedRunErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	runID, _, err := st.CreateImportRun(context.Background(), "running", "failed latest")
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}
	if _, err := st.FinishImportRun(context.Background(), runID, "failed", "failed latest"); err != nil {
		t.Fatalf("finish import run: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	var stdout bytes.Buffer
	err = runWithArgs([]string{"-db", path, "replay"}, &stdout, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("error = %v, want failed latest replay error", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestRunWithArgsReplayYellowArchUsesStoredSourcePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID := seedReplayRunForCLIYellowArch(t, path)

	var stdout bytes.Buffer
	if err := runWithArgs([]string{"-db", path, "replay", "-limit", "1", strconv.FormatInt(runID, 10)}, &stdout, io.Discard); err != nil {
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
	if got := got.EventReviewClusters.EventReviewClustersCreated; got != 0 {
		t.Fatalf("event-review clusters created = %d, want 0", got)
	}
	if got := got.EventReviewClusters.AutoPromotedCount; got != 1 {
		t.Fatalf("auto promoted count = %d, want 1", got)
	}
	if got := len(got.EventReviewClusters.EventReviewClusters); got != 0 {
		t.Fatalf("event-review clusters = %d, want 0", got)
	}
	if got := len(got.EventReviewClusters.AutoPromoted); got != 1 {
		t.Fatalf("auto promoted groups = %d, want 1", got)
	}
	if got := got.EventReviewClusters.AutoPromoted[0].SourceURL; got != "https://www.yellowarch.com/events/" {
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

	if _, ok := st.EventBySlug(got.EventReviewClusters.AutoPromoted[0].EventSlug); !ok {
		t.Fatalf("published event %q not found", got.EventReviewClusters.AutoPromoted[0].EventSlug)
	}
}

func TestRunWithArgsReplayYellowArchReappliesLinkedAuthoritativeEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	runID := seedReplayRunForCLIYellowArch(t, path)

	runReplay := func() manualIngestReport {
		t.Helper()
		var stdout bytes.Buffer
		if err := runWithArgs([]string{"-db", path, "replay", "-limit", "1", strconv.FormatInt(runID, 10)}, &stdout, io.Discard); err != nil {
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

	if got := first.EventReviewClusters.AutoPromotedCount; got != 1 {
		t.Fatalf("first auto promoted count = %d, want 1", got)
	}
	if got := second.EventReviewClusters.AutoPromotedCount; got != 1 {
		t.Fatalf("second auto promoted count = %d, want 1", got)
	}
	if got := second.EventReviewClusters.EventReviewClustersCreated; got != 0 {
		t.Fatalf("second event-review clusters created = %d, want 0", got)
	}
	if got := len(second.EventReviewClusters.EventReviewClusters); got != 0 {
		t.Fatalf("second event-review clusters = %d, want 0", got)
	}
	if got := len(second.EventReviewClusters.AutoPromoted); got != 1 {
		t.Fatalf("second auto promoted groups = %d, want 1", got)
	}
	if got, want := second.EventReviewClusters.AutoPromoted[0].EventSlug, first.EventReviewClusters.AutoPromoted[0].EventSlug; got != want {
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
	if err := runWithArgs([]string{"-db", path, "replay", "-limit", "20", strconv.FormatInt(runID, 10)}, &stdout, io.Discard); err != nil {
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
	if got := got.EventReviewClusters.EventReviewClustersCreated; got != 0 {
		t.Fatalf("event-review clusters created = %d, want 0", got)
	}
	if got := got.EventReviewClusters.AutoPromotedCount; got != 1 {
		t.Fatalf("auto promoted count = %d, want 1", got)
	}
	if got := len(got.EventReviewClusters.EventReviewClusters); got != 0 {
		t.Fatalf("event-review clusters = %d, want 0", got)
	}
	if got := got.EventReviewClusters.ReviewCandidateCount; got != 0 {
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

	if got := len(got.EventReviewClusters.AutoPromoted); got != 1 {
		t.Fatalf("auto promoted groups = %d, want 1", got)
	}
	event, ok := st.EventBySlug(got.EventReviewClusters.AutoPromoted[0].EventSlug)
	if !ok {
		t.Fatalf("published event %q not found", got.EventReviewClusters.AutoPromoted[0].EventSlug)
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
	if err := runWithArgs([]string{"-db", path, "fix", "image-focus"}, &stdout, io.Discard); err != nil {
		t.Fatalf("backfill image focus: %v", err)
	}

	var payload imageFocusRepairRunReport
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode backfill output: %v", err)
	}
	report := payload.ImageFocus
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

func TestRunWithArgsBackfillsImageFocusDryRunDoesNotMutate(t *testing.T) {
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
	if err := runWithArgs([]string{"-db", path, "fix", "image-focus", "-dry-run"}, &stdout, io.Discard); err != nil {
		t.Fatalf("dry-run backfill image focus: %v", err)
	}

	var payload imageFocusRepairRunReport
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode backfill output: %v", err)
	}
	report := payload.ImageFocus
	if !report.DryRun || report.Applied {
		t.Fatalf("dry/apply = %v/%v, want true/false", report.DryRun, report.Applied)
	}
	if report.Updated != 1 || report.Defaulted != 0 || report.MissingFiles != 0 || report.DecodeFailures != 0 {
		t.Fatalf("backfill report = %#v, want one clean dry-run update", report)
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
	if asset.FocusX != 50 || asset.FocusY != 50 {
		t.Fatalf("focus = %d,%d, want unchanged 50,50", asset.FocusX, asset.FocusY)
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
	if err := runWithArgs([]string{"-db", path, "fix", "image-focus"}, &stdout, io.Discard); err != nil {
		t.Fatalf("backfill image focus: %v", err)
	}

	var payload imageFocusRepairRunReport
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode backfill output: %v", err)
	}
	report := payload.ImageFocus
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
		"replay",
		"-limit", "1",
		strconv.FormatInt(runID, 10),
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
	if !got.EventReviewClusters.Enabled {
		t.Fatal("event-review clusters enabled = false, want true")
	}
	if got.EventReviewClusters.Applied {
		t.Fatal("event-review clusters applied = true, want false")
	}
	if len(got.EventReviewClusters.Errors) != 1 || !strings.Contains(got.EventReviewClusters.Errors[0], "skipped event review staging") {
		t.Fatalf("event-review cluster errors = %#v, want skipped staging reason", got.EventReviewClusters.Errors)
	}
	if got.EventReviewClusters.EventReviewClustersCreated != 0 {
		t.Fatalf("event-review clusters created = %d, want 0", got.EventReviewClusters.EventReviewClustersCreated)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got := countRows(t, db, "review_groups"); got != 0 {
		t.Fatalf("event-review clusters = %d, want 0", got)
	}
}

func TestRunWithArgsLiveFailureStillEmitsJSONAndSkipsReviewStaging(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	t.Setenv("MEDIA_ROOT", filepath.Join(t.TempDir(), "media"))
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
			ImportRunID: 17,
			StartedAt:   "2026-04-24T10:00:00Z",
			FinishedAt:  "2026-04-24T10:01:00Z",
			Status:      "failed",
			Limit:       opts.Limit,
			Errors:      []string{"manual ingest failed"},
			Calendars: []ingest.CalendarReport{{
				URL: "https://" + opts.Source + ".example.test/calendar.ics",
				Candidates: []ingest.EventCandidate{{
					UID:      "failed-live-show",
					Summary:  "Failed Live Show",
					Location: "External Room",
					StartAt:  "2026-05-01T19:00:00Z",
				}},
			}},
			Links:  []string{"https://" + opts.Source + ".example.test/calendar.ics"},
			Totals: ingest.ReportTotals{Links: 1, Candidates: 1, Errors: 1},
		}, ingest.ErrRunFailed
	}

	err := runWithArgs([]string{"-db", path, "-source", ingest.DefaultSource, "-user-agent", "agent"}, &stdout, io.Discard)
	if !errors.Is(err, ingest.ErrRunFailed) {
		t.Fatalf("error = %v, want ErrRunFailed", err)
	}

	var got manualIngestReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode live output: %v", err)
	}
	if got.Report.ImportRunID != 17 {
		t.Fatalf("import run id = %d, want 17", got.Report.ImportRunID)
	}
	if got.Report.Status != "failed" {
		t.Fatalf("report status = %q, want failed", got.Report.Status)
	}
	if !got.EventReviewClusters.Enabled {
		t.Fatal("event-review clusters enabled = false, want true")
	}
	if got.EventReviewClusters.Applied {
		t.Fatal("event-review clusters applied = true, want false")
	}
	if len(got.EventReviewClusters.Errors) != 1 || !strings.Contains(got.EventReviewClusters.Errors[0], "skipped event review staging") {
		t.Fatalf("event-review cluster errors = %#v, want skipped staging reason", got.EventReviewClusters.Errors)
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got := countRows(t, db, "review_groups"); got != 0 {
		t.Fatalf("event-review clusters = %d, want 0", got)
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

	if err := runWithArgs([]string{"-db", path, "-user-agent", "agent"}, &stdout, io.Discard); err != nil {
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

	if err := runWithArgs([]string{"-db", path, "-source", ingest.DefaultSource, "-user-agent", "agent"}, &stdout, &stderr); err != nil {
		t.Fatalf("runWithArgs: %v", err)
	}

	var got manualIngestReport
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout is not JSON report: %v; output %q", err, stdout.String())
	}
	if got.Report.ImportRunID != 7 {
		t.Fatalf("import run id = %d, want 7", got.Report.ImportRunID)
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

	err := runWithArgs([]string{"-db", path, "-user-agent", "agent"}, &stdout, io.Discard)
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
	failedResult := got.Results[1]
	if failedResult.Source != ingest.YellowArchSource {
		t.Fatalf("failed result source = %q, want %q", failedResult.Source, ingest.YellowArchSource)
	}
	if failedResult.Error == "" {
		t.Fatal("failed result error = empty, want error")
	}
	if failedResult.EventReviewClusters == nil {
		t.Fatal("failed result review stage = nil, want skipped review stage")
	}
	if !failedResult.EventReviewClusters.Enabled {
		t.Fatal("failed result review stage enabled = false, want true")
	}
	if failedResult.EventReviewClusters.Applied {
		t.Fatal("failed result review stage applied = true, want false")
	}
	if len(failedResult.EventReviewClusters.Errors) != 1 || !strings.Contains(failedResult.EventReviewClusters.Errors[0], "skipped event review staging") {
		t.Fatalf("failed result review stage errors = %#v, want skipped staging reason", failedResult.EventReviewClusters.Errors)
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

	if err := runWithArgs([]string{"-db", path, "-user-agent", "agent"}, &stdout, io.Discard); err != nil {
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
		if result.EventReviewClusters == nil {
			t.Fatalf("review stage missing for source %q", result.Source)
		}
		if !result.EventReviewClusters.Applied {
			t.Fatalf("review stage applied for source %q = false, want true", result.Source)
		}
		if result.EventReviewClusters.AutoPromotedCount != 1 {
			t.Fatalf("auto promoted count for %q = %d, want 1", result.Source, result.EventReviewClusters.AutoPromotedCount)
		}
		if result.EventReviewClusters.EventReviewClustersCreated != 0 {
			t.Fatalf("event-review clusters created for %q = %d, want 0", result.Source, result.EventReviewClusters.EventReviewClustersCreated)
		}
		if result.EventReviewClusters.ReviewCandidateCount != 0 {
			t.Fatalf("review candidate count for %q = %d, want 0", result.Source, result.EventReviewClusters.ReviewCandidateCount)
		}
		if result.EventReviewClusters.EventReviewClustersAutoResolvedCount != 0 {
			t.Fatalf("auto-resolved count for %q = %d, want 0", result.Source, result.EventReviewClusters.EventReviewClustersAutoResolvedCount)
		}
		if len(result.EventReviewClusters.EventReviewClustersAutoResolved) != 0 {
			t.Fatalf("auto-resolved rows for %q = %d, want 0", result.Source, len(result.EventReviewClusters.EventReviewClustersAutoResolved))
		}
	}

	db := openRawDB(t, path)
	defer db.Close()
	if got := countRows(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
}

func TestEventReviewClustersForReportSkipsFailedManualRun(t *testing.T) {
	st := &fakeEventReviewClustersStore{results: []fakeEventReviewClustersResult{{id: 101, created: true}}}

	stage, err := eventReviewClustersForReport(context.Background(), st, testSourceCatalog(t), successfulManualReportForEventReviewClusters(), errors.New("manual ingest failed"))
	if err != nil {
		t.Fatalf("review stage for failed run: %v", err)
	}
	if got, want := len(st.inputs), 0; got != want {
		t.Fatalf("created groups = %d, want %d", got, want)
	}
	if !stage.Enabled {
		t.Fatal("stage enabled = false, want true")
	}
	if stage.Applied {
		t.Fatal("stage applied = true, want false")
	}
	if len(stage.Errors) != 1 || !strings.Contains(stage.Errors[0], "skipped event review staging") {
		t.Fatalf("stage errors = %#v, want skipped staging reason", stage.Errors)
	}
	if stage.EventReviewClustersCreated != 0 || stage.CandidateCount != 0 {
		t.Fatalf("stage counts = groups %d candidates %d, want zero", stage.EventReviewClustersCreated, stage.CandidateCount)
	}
}

func TestCreateEventReviewClustersFromReportReportsCreateError(t *testing.T) {
	st := &fakeEventReviewClustersStore{err: errors.New("insert failed")}

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), successfulManualReportForEventReviewClusters())
	if err == nil {
		t.Fatal("expected staging error")
	}
	if got, want := stage.EventReviewClustersCreated, 0; got != want {
		t.Fatalf("stage event-review clusters created = %d, want %d", got, want)
	}
	if got, want := len(stage.Errors), 1; got != want {
		t.Fatalf("stage errors = %d, want %d", got, want)
	}
	if !strings.Contains(stage.Errors[0], "insert failed") {
		t.Fatalf("stage error = %q, want insert failure", stage.Errors[0])
	}
	if !strings.Contains(stage.Errors[0], "event-review cluster") {
		t.Fatalf("stage error = %q, want event-review cluster wording", stage.Errors[0])
	}
	if strings.Contains(stage.Errors[0], "review group") {
		t.Fatalf("stage error = %q, want no legacy review group wording", stage.Errors[0])
	}
}

func TestCreateEventReviewClustersFromReportReportsAutoPromoteErrorWithClusterWording(t *testing.T) {
	st := &fakeEventReviewClustersStore{
		promotionResults: []fakePromotionResult{{err: errors.New("promotion failed")}},
	}
	report := ingest.Report{
		Source:      ingest.YellowArchSource,
		SourceURL:   "https://www.yellowarch.com/events/",
		ImportRunID: 99,
		Status:      "succeeded",
		Calendars: []ingest.CalendarReport{{
			URL: "https://www.yellowarch.com/events/",
			Candidates: []ingest.EventCandidate{{
				UID:      "yellow-arch-promotion-error",
				Summary:  "Promotion Error",
				Location: "Yellow Arch Studios",
				StartAt:  "2026-05-10T18:30:00Z",
			}},
		}},
	}

	stage, err := createEventReviewClustersFromReport(context.Background(), st, testSourceCatalog(t), report)
	if err == nil {
		t.Fatal("expected staging error")
	}
	if got, want := len(stage.Errors), 1; got != want {
		t.Fatalf("stage errors = %d, want %d", got, want)
	}
	if !strings.Contains(stage.Errors[0], "promotion failed") {
		t.Fatalf("stage error = %q, want promotion failure", stage.Errors[0])
	}
	if !strings.Contains(stage.Errors[0], "event-review cluster") {
		t.Fatalf("stage error = %q, want event-review cluster wording", stage.Errors[0])
	}
	if strings.Contains(stage.Errors[0], "review group") {
		t.Fatalf("stage error = %q, want no legacy review group wording", stage.Errors[0])
	}
}

type fakeEventReviewClustersStore struct {
	results          []fakeEventReviewClustersResult
	promotionResults []fakePromotionResult
	finalizeResults  []*seedstore.EventReviewResolutionSummary
	err              error
	inputs           []seedstore.StageEventReviewEvidenceInput
	promotedInputs   []ingest.ReviewStageClusterInput
	finalizedCalls   []fakeFinalizeCall
	sourceIDs        map[string]int64
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

func equalInt64Slices(got, want []int64) bool {
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

type fakeEventReviewClustersResult struct {
	id                   int64
	created              bool
	autoResolved         bool
	autoResolvedResult   string
	canonicalEventSlug   string
	supersededClusterIDs []int64
	clusterStatus        seedstore.EventReviewClusterStatus
}

type fakePromotionResult struct {
	eventSlug string
	promoted  bool
	err       error
}

type fakeFinalizeCall struct {
	clusterID   int64
	evidenceIDs []int64
}

func (s *fakeEventReviewClustersStore) EnsureSource(_ context.Context, name, sourceURL string) (int64, error) {
	if s.sourceIDs == nil {
		s.sourceIDs = make(map[string]int64)
	}
	if s.err != nil {
		return 0, s.err
	}
	key := strings.TrimSpace(name) + "\x00" + strings.TrimSpace(sourceURL)
	if id, ok := s.sourceIDs[key]; ok {
		return id, nil
	}
	id := int64(len(s.sourceIDs) + 1)
	s.sourceIDs[key] = id
	return id, nil
}

func (s *fakeEventReviewClustersStore) StageEventReviewEvidence(_ context.Context, input seedstore.StageEventReviewEvidenceInput) (seedstore.StageEventReviewEvidenceResult, error) {
	s.inputs = append(s.inputs, input)
	if s.err != nil {
		return seedstore.StageEventReviewEvidenceResult{}, s.err
	}
	result := fakeEventReviewClustersResult{created: true}
	if len(s.results) > 0 {
		result = s.results[0]
		s.results = s.results[1:]
	}
	clusterID := result.id
	if clusterID == 0 {
		clusterID = int64(len(s.inputs))
	}
	return seedstore.StageEventReviewEvidenceResult{
		EvidenceID:           clusterID,
		ClusterID:            clusterID,
		ClusterStatus:        result.clusterStatus,
		Created:              result.created,
		Reused:               !result.created,
		Attached:             true,
		ClusterCreated:       result.created,
		ClusterReused:        !result.created,
		AutoResolved:         result.autoResolved,
		AutoResolvedResult:   result.autoResolvedResult,
		CanonicalEventSlug:   result.canonicalEventSlug,
		SupersededClusterIDs: append([]int64(nil), result.supersededClusterIDs...),
	}, nil
}

func (s *fakeEventReviewClustersStore) PromoteSingletonReviewClusterIfMissing(_ context.Context, input ingest.ReviewStageClusterInput) (string, bool, error) {
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

func (s *fakeEventReviewClustersStore) FinalizeOpenEventReviewClusterRestage(_ context.Context, clusterID int64, evidenceIDs []int64) (*seedstore.EventReviewResolutionSummary, error) {
	s.finalizedCalls = append(s.finalizedCalls, fakeFinalizeCall{clusterID: clusterID, evidenceIDs: append([]int64(nil), evidenceIDs...)})
	if s.err != nil {
		return nil, s.err
	}
	if len(s.finalizeResults) == 0 {
		return nil, nil
	}
	result := s.finalizeResults[0]
	s.finalizeResults = s.finalizeResults[1:]
	return result, nil
}

func successfulManualReportForEventReviewClusters() ingest.Report {
	return successfulManualReportForEventReviewClustersWithSource(ingest.DefaultSource, "https://www.sidneyandmatilda.com/")
}

func successfulManualReportForEventReviewClustersWithSource(source, sourceURL string) ingest.Report {
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

	seedSlug, promoted, err := st.PromoteSingletonReviewClusterIfMissing(ctx, ingest.ReviewStageClusterInput{
		ImportRunID: runID,
		Title:       "Cafe No. 9 singleton",
		SourceName:  "Cafe No. 9 manual ingest",
		SourceURL:   "https://www.wegottickets.com/event/700001",
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

func cliEmptySucceededReport(source string, importRunID int64, limit int) ingest.Report {
	return ingest.Report{
		Source:      source,
		SourceURL:   "https://" + source + ".example.test/",
		ImportRunID: importRunID,
		StartedAt:   "2026-04-24T10:00:00Z",
		FinishedAt:  "2026-04-24T10:01:00Z",
		Status:      "succeeded",
		Limit:       limit,
		Links:       []string{},
		Calendars:   []ingest.CalendarReport{},
		Totals:      ingest.ReportTotals{},
	}
}

func cliYellowArchTitleRepairReport(startAt, summary string) ingest.Report {
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

func mustLiveEventSlugForCLI(t *testing.T, name, venueSlug, startAt string) string {
	t.Helper()

	start, err := time.Parse(time.RFC3339, startAt)
	if err != nil {
		t.Fatalf("parse start: %v", err)
	}
	return "live-" + cliSlugFromText(name) + "-" + cliSlugFromText(venueSlug) + "-" + start.UTC().Format("20060102150405")
}

func cliSlugFromText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	wroteDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
			wroteDash = false
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
			wroteDash = false
		default:
			if builder.Len() > 0 && !wroteDash {
				builder.WriteByte('-')
				wroteDash = true
			}
		}
	}
	return strings.Trim(builder.String(), "-")
}

func insertCLIEvent(t *testing.T, db *sql.DB, sourceID int64, slug, venueSlug, name, startAt string) {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, venueSlug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue: %v", err)
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
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, name, startAt, "Test", "Listed", "Existing description.", "2026-05-01T10:00:00Z", string(domain.OriginLive), string(domain.PublicationStateReviewed)); err != nil {
		t.Fatalf("insert CLI event: %v", err)
	}
}

func insertHistoricalDuplicateCLIEvent(t *testing.T, db *sql.DB, sourceID int64, slug string, venueID int64, name, startAt string, publicationState string) int64 {
	t.Helper()

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
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, name, startAt, "Test", "Listed", "Historical duplicate repair", "2026-05-01T10:00:00Z", string(domain.OriginLive), publicationState)
	if err != nil {
		t.Fatalf("insert historical duplicate CLI event: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("historical duplicate CLI event id: %v", err)
	}
	return id
}

func seedHistoricalDuplicateCLIRepairPair(t *testing.T, path string) (int64, int64, string) {
	t.Helper()

	st, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	sourceID, err := st.EnsureSource(context.Background(), "CLI historical duplicate source", "https://example.test/historical-duplicates")
	if err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	db := openRawDB(t, path)
	defer db.Close()
	var venueID int64
	if err := db.QueryRow(`
		SELECT id
		FROM venues
		WHERE slug = ?
	`, "leadmill").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue: %v", err)
	}
	startAt := "2026-05-12T19:00:00Z"
	targetID := insertHistoricalDuplicateCLIEvent(t, db, sourceID, "cli-historical-duplicate-target", venueID, "CLI Historical Duplicate", startAt, string(domain.PublicationStateReviewed))
	loserSlug := "cli-historical-duplicate-loser"
	loserID := insertHistoricalDuplicateCLIEvent(t, db, sourceID, loserSlug, venueID, "CLI Historical Duplicate", startAt, string(domain.PublicationStateProvisional))
	return targetID, loserID, loserSlug
}

func loadHistoricalDuplicateCLIState(t *testing.T, db *sql.DB, slug string) (string, sql.NullInt64, string, sql.NullInt64) {
	t.Helper()

	var publicationState string
	var canonicalID sql.NullInt64
	var withheldReason string
	var repairRunID sql.NullInt64
	if err := db.QueryRow(`
		SELECT publication_state, canonical_event_id, COALESCE(withheld_reason, ''), withheld_repair_run_id
		FROM events
		WHERE slug = ?
	`, slug).Scan(&publicationState, &canonicalID, &withheldReason, &repairRunID); err != nil {
		t.Fatalf("load historical duplicate state %q: %v", slug, err)
	}
	return publicationState, canonicalID, withheldReason, repairRunID
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
