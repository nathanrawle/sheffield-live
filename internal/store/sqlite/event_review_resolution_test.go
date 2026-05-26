package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

type eventReviewResolutionSnapshotTest struct {
	ClusterID             int64  `json:"cluster_id"`
	ExpectedVersion       int    `json:"expected_version"`
	CurrentVersion        int    `json:"current_version"`
	CurrentStatus         string `json:"current_status"`
	TargetStatus          string `json:"target_status"`
	DiscardReason         string `json:"discard_reason,omitempty"`
	SupersededByClusterID int64  `json:"superseded_by_cluster_id,omitempty"`
	RecordedAt            string `json:"recorded_at"`
}

func TestDiscardEventReviewClusterWritesSnapshotAndLeavesLegacyTablesUntouched(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "discard-event-review-cluster", venueID, sourceID, domain.OriginLive)

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	beforeEvents := mustCount(t, db, "events")
	beforeSources := mustCount(t, db, "sources")
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")
	beforeObservations := mustCount(t, db, "event_source_attribute_observations")
	var beforePublicationState, beforeWithheldReason string
	if err := db.QueryRow(`
		SELECT publication_state, withheld_reason
		FROM events
		WHERE slug = ?
	`, "discard-event-review-cluster").Scan(&beforePublicationState, &beforeWithheldReason); err != nil {
		t.Fatalf("load baseline event state: %v", err)
	}

	if err := st.DiscardEventReviewCluster(context.Background(), seedstore.EventReviewDiscardInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       clusterID,
			ExpectedVersion: 1,
		},
		Reason: "  duplicate of another cluster  ",
	}); err != nil {
		t.Fatalf("discard cluster: %v", err)
	}

	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusDiscarded), 2, nil)

	var status, snapshot, discardReason string
	if err := db.QueryRow(`
		SELECT status, snapshot, discard_reason
		FROM event_review_resolutions
		WHERE cluster_id = ?
	`, clusterID).Scan(&status, &snapshot, &discardReason); err != nil {
		t.Fatalf("load discard resolution: %v", err)
	}
	if status != string(seedstore.EventReviewResolutionStatusDiscarded) {
		t.Fatalf("resolution status = %q, want %q", status, seedstore.EventReviewResolutionStatusDiscarded)
	}
	if discardReason != "duplicate of another cluster" {
		t.Fatalf("discard reason = %q, want %q", discardReason, "duplicate of another cluster")
	}

	var got eventReviewResolutionSnapshotTest
	if err := json.Unmarshal([]byte(snapshot), &got); err != nil {
		t.Fatalf("unmarshal resolution snapshot: %v", err)
	}
	if got.ClusterID != clusterID || got.ExpectedVersion != 1 || got.CurrentVersion != 1 || got.CurrentStatus != string(seedstore.EventReviewClusterStatusOpen) || got.TargetStatus != string(seedstore.EventReviewResolutionStatusDiscarded) || got.DiscardReason != "duplicate of another cluster" {
		t.Fatalf("discard snapshot = %#v", got)
	}

	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents)
	}
	if got := mustCount(t, db, "sources"); got != beforeSources {
		t.Fatalf("sources rows = %d, want %d", got, beforeSources)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}
	if got := mustCount(t, db, "event_source_attribute_observations"); got != beforeObservations {
		t.Fatalf("event_source_attribute_observations rows = %d, want %d", got, beforeObservations)
	}

	var publicationState, withheldReason string
	if err := db.QueryRow(`
		SELECT publication_state, withheld_reason
		FROM events
		WHERE slug = ?
	`, "discard-event-review-cluster").Scan(&publicationState, &withheldReason); err != nil {
		t.Fatalf("load event state: %v", err)
	}
	if publicationState != beforePublicationState || withheldReason != beforeWithheldReason {
		t.Fatalf("event state changed: publication_state=%q->%q withheld_reason=%q->%q", beforePublicationState, publicationState, beforeWithheldReason, withheldReason)
	}
}

func TestDiscardEventReviewClusterRejectsStaleVersionWithoutWrites(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")

	err := st.DiscardEventReviewCluster(context.Background(), seedstore.EventReviewDiscardInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       clusterID,
			ExpectedVersion: 2,
		},
		Reason: "stale version",
	})
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("discard stale version error = %v, want version mismatch", err)
	}

	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}
}

func TestSupersedeEventReviewClusterWritesSnapshotAndSetsSupersededByClusterID(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	supersedingClusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")

	if err := st.SupersedeEventReviewCluster(context.Background(), seedstore.EventReviewSupersedeInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       clusterID,
			ExpectedVersion: 1,
		},
		SupersededByClusterID: supersedingClusterID,
	}); err != nil {
		t.Fatalf("supersede cluster: %v", err)
	}

	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusSuperseded), 2, &supersedingClusterID)

	var status, snapshot string
	if err := db.QueryRow(`
		SELECT status, snapshot
		FROM event_review_resolutions
		WHERE cluster_id = ?
	`, clusterID).Scan(&status, &snapshot); err != nil {
		t.Fatalf("load supersede resolution: %v", err)
	}
	if status != string(seedstore.EventReviewResolutionStatusSuperseded) {
		t.Fatalf("resolution status = %q, want %q", status, seedstore.EventReviewResolutionStatusSuperseded)
	}

	var got eventReviewResolutionSnapshotTest
	if err := json.Unmarshal([]byte(snapshot), &got); err != nil {
		t.Fatalf("unmarshal supersede snapshot: %v", err)
	}
	if got.ClusterID != clusterID || got.ExpectedVersion != 1 || got.CurrentVersion != 1 || got.CurrentStatus != string(seedstore.EventReviewClusterStatusOpen) || got.TargetStatus != string(seedstore.EventReviewResolutionStatusSuperseded) || got.SupersededByClusterID != supersedingClusterID {
		t.Fatalf("supersede snapshot = %#v", got)
	}

	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}
}

func TestSupersedeEventReviewClusterRejectsStaleVersionWithoutWrites(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	supersedingClusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	beforeResolutions := mustCount(t, db, "event_review_resolutions")

	err := st.SupersedeEventReviewCluster(context.Background(), seedstore.EventReviewSupersedeInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       clusterID,
			ExpectedVersion: 2,
		},
		SupersededByClusterID: supersedingClusterID,
	})
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("supersede stale version error = %v, want version mismatch", err)
	}

	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
	}
}

type historicalDuplicateResolutionFixture struct {
	clusterID   int64
	canonicalID int64
	loserID     int64
	sourceID    int64
}

type titleRepairResolutionFixture struct {
	clusterID int64
	eventID   int64
	sourceID  int64
	venueID   int64
	oldTitle  string
	newTitle  string
	oldSlug   string
	newSlug   string
}

func TestResolveEventReviewClusterAppliesHistoricalDuplicateLiveActions(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedHistoricalDuplicateResolutionFixture(t, db)
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")
	beforeRepairRuns := mustCount(t, db, "repair_runs")

	if err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
		ClusterID:       fixture.clusterID,
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("resolve historical duplicate cluster: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)

	var status, snapshot string
	if err := db.QueryRow(`
		SELECT status, snapshot
		FROM event_review_resolutions
		WHERE cluster_id = ?
	`, fixture.clusterID).Scan(&status, &snapshot); err != nil {
		t.Fatalf("load resolution: %v", err)
	}
	if status != string(seedstore.EventReviewResolutionStatusResolved) {
		t.Fatalf("resolution status = %q, want %q", status, seedstore.EventReviewResolutionStatusResolved)
	}

	type appliedLiveActionSnapshot struct {
		EventID   int64                               `json:"event_id"`
		EventSlug string                              `json:"event_slug"`
		Action    seedstore.EventReviewLiveActionKind `json:"action"`
		Reason    string                              `json:"reason"`
	}
	var got struct {
		RepairRunID        *int64                      `json:"repair_run_id"`
		AppliedLiveActions []appliedLiveActionSnapshot `json:"applied_live_actions"`
	}
	if err := json.Unmarshal([]byte(snapshot), &got); err != nil {
		t.Fatalf("unmarshal resolution snapshot: %v", err)
	}
	if got.RepairRunID == nil {
		t.Fatal("repair run id missing from resolution snapshot")
	}
	if len(got.AppliedLiveActions) != 2 {
		t.Fatalf("applied live actions = %d, want 2", len(got.AppliedLiveActions))
	}
	if got.AppliedLiveActions[0].Action != seedstore.EventReviewLiveActionKindKeepSeparate || got.AppliedLiveActions[1].Action != seedstore.EventReviewLiveActionKindWithholdDuplicate {
		t.Fatalf("applied live actions = %#v", got.AppliedLiveActions)
	}

	var loserState, loserReason string
	var loserCanonical sql.NullInt64
	var loserRepairRun sql.NullInt64
	if err := db.QueryRow(`
		SELECT publication_state, withheld_reason, canonical_event_id, withheld_repair_run_id
		FROM events
		WHERE id = ?
	`, fixture.loserID).Scan(&loserState, &loserReason, &loserCanonical, &loserRepairRun); err != nil {
		t.Fatalf("load loser event: %v", err)
	}
	if loserState != string(domain.PublicationStateWithheld) {
		t.Fatalf("loser publication state = %q, want withheld", loserState)
	}
	if loserReason != "historical duplicate listing" {
		t.Fatalf("loser withheld reason = %q, want historical duplicate listing", loserReason)
	}
	if !loserCanonical.Valid || loserCanonical.Int64 != fixture.canonicalID {
		t.Fatalf("loser canonical event id = %v, want %d", loserCanonical, fixture.canonicalID)
	}
	if !loserRepairRun.Valid || loserRepairRun.Int64 != *got.RepairRunID {
		t.Fatalf("loser repair run id = %v, want %d", loserRepairRun, *got.RepairRunID)
	}

	var canonicalState string
	if err := db.QueryRow(`
		SELECT publication_state
		FROM events
		WHERE id = ?
	`, fixture.canonicalID).Scan(&canonicalState); err != nil {
		t.Fatalf("load canonical event: %v", err)
	}
	if canonicalState != string(domain.PublicationStateReviewed) {
		t.Fatalf("canonical publication state = %q, want reviewed", canonicalState)
	}

	if got := mustCount(t, db, "repair_runs"); got != beforeRepairRuns+1 {
		t.Fatalf("repair_runs rows = %d, want %d", got, beforeRepairRuns+1)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}
}

func TestResolveHistoricalDuplicateKeepSeparateRecordsSeparationsWithoutLiveMutation(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedHistoricalDuplicateResolutionFixture(t, db)
	beforeRepairRuns := mustCount(t, db, "repair_runs")

	if err := st.ResolveHistoricalDuplicateKeepSeparate(context.Background(), seedstore.EventReviewHistoricalDuplicateKeepSeparateInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		KeptEventIDs: []int64{fixture.canonicalID, fixture.loserID},
	}); err != nil {
		t.Fatalf("resolve historical duplicate keep separate: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	if got := mustCount(t, db, "repair_runs"); got != beforeRepairRuns {
		t.Fatalf("repair_runs rows = %d, want %d", got, beforeRepairRuns)
	}

	var canonicalState, loserState string
	if err := db.QueryRow(`SELECT publication_state FROM events WHERE id = ?`, fixture.canonicalID).Scan(&canonicalState); err != nil {
		t.Fatalf("load canonical state: %v", err)
	}
	if err := db.QueryRow(`SELECT publication_state FROM events WHERE id = ?`, fixture.loserID).Scan(&loserState); err != nil {
		t.Fatalf("load loser state: %v", err)
	}
	if canonicalState != string(domain.PublicationStateReviewed) || loserState != string(domain.PublicationStateReviewed) {
		t.Fatalf("event states = canonical %q loser %q, want both reviewed", canonicalState, loserState)
	}

	var separationCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_separations
		WHERE active = 1
			AND endpoint_a_key = ?
			AND endpoint_b_key = ?
	`, seedstore.EventReviewSeparationEventEndpointKey(fixture.canonicalID), seedstore.EventReviewSeparationEventEndpointKey(fixture.loserID)).Scan(&separationCount); err != nil {
		t.Fatalf("count event separations: %v", err)
	}
	if separationCount != 1 {
		t.Fatalf("active event-event separations = %d, want 1", separationCount)
	}

	var snapshot string
	if err := db.QueryRow(`SELECT snapshot FROM event_review_resolutions WHERE cluster_id = ?`, fixture.clusterID).Scan(&snapshot); err != nil {
		t.Fatalf("load resolution snapshot: %v", err)
	}
	var got struct {
		RepairRunID        *int64 `json:"repair_run_id"`
		AppliedLiveActions []struct {
			EventID int64 `json:"event_id"`
		} `json:"applied_live_actions"`
		AppliedSeparations            []map[string]any `json:"applied_separations"`
		AppliedHistoricalKeepSeparate struct {
			KeptEvents []struct {
				EventID   int64  `json:"event_id"`
				EventSlug string `json:"event_slug"`
			} `json:"kept_events"`
		} `json:"applied_historical_keep_separate"`
	}
	if err := json.Unmarshal([]byte(snapshot), &got); err != nil {
		t.Fatalf("unmarshal keep-separate snapshot: %v", err)
	}
	if got.RepairRunID != nil {
		t.Fatalf("repair run id = %v, want nil", *got.RepairRunID)
	}
	if len(got.AppliedLiveActions) != 0 {
		t.Fatalf("applied live actions = %#v, want none", got.AppliedLiveActions)
	}
	if len(got.AppliedSeparations) != 1 {
		t.Fatalf("applied separations = %d, want 1", len(got.AppliedSeparations))
	}
	if len(got.AppliedHistoricalKeepSeparate.KeptEvents) != 2 {
		t.Fatalf("kept events = %#v, want 2", got.AppliedHistoricalKeepSeparate.KeptEvents)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil || !ok {
		t.Fatalf("load resolved detail ok=%v err=%v", ok, err)
	}
	if detail.Resolution == nil || detail.Resolution.AppliedHistoricalKeepSeparate == nil || len(detail.Resolution.AppliedHistoricalKeepSeparate.KeptEvents) != 2 {
		t.Fatalf("resolution keep-separate summary = %#v", detail.Resolution)
	}
}

func TestResolveHistoricalDuplicateKeepSeparateUsesEvidenceEventsWhenNoCanonicalActionExists(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "historical-duplicate-first-false-positive", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "historical-duplicate-second-false-positive", venueID, sourceID, domain.OriginLive)
	firstID := mustEventIDBySlug(t, db, "historical-duplicate-first-false-positive")
	secondID := mustEventIDBySlug(t, db, "historical-duplicate-second-false-positive")
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := db.Exec(`UPDATE event_review_clusters SET conflict_type = ?, conflict_reason = ? WHERE id = ?`, "historical_duplicate", "multiple reviewed targets", clusterID); err != nil {
		t.Fatalf("seed conflict metadata: %v", err)
	}
	firstEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &firstID, "historical-duplicate-first-evidence", `{"role":"candidate"}`)
	secondEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &secondID, "historical-duplicate-second-evidence", `{"role":"candidate"}`)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, firstEvidenceID, true, time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC), nil, "first event")
	insertEventReviewClusterEvidenceOK(t, db, clusterID, secondEvidenceID, true, time.Date(2026, time.May, 15, 11, 1, 0, 0, time.UTC), nil, "second event")

	if err := st.ResolveHistoricalDuplicateKeepSeparate(context.Background(), seedstore.EventReviewHistoricalDuplicateKeepSeparateInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       clusterID,
			ExpectedVersion: 1,
		},
		KeptEventIDs: []int64{firstID, secondID},
	}); err != nil {
		t.Fatalf("resolve evidence-only historical duplicate keep separate: %v", err)
	}

	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	var separationCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_separations
		WHERE active = 1
			AND endpoint_a_key = ?
			AND endpoint_b_key = ?
	`, seedstore.EventReviewSeparationEventEndpointKey(firstID), seedstore.EventReviewSeparationEventEndpointKey(secondID)).Scan(&separationCount); err != nil {
		t.Fatalf("count event separations: %v", err)
	}
	if separationCount != 1 {
		t.Fatalf("active event-event separations = %d, want 1", separationCount)
	}
}

func TestResolveHistoricalDuplicateKeepSeparateRequiresSubmittedKeptEventSet(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedHistoricalDuplicateResolutionFixture(t, db)

	err := st.ResolveHistoricalDuplicateKeepSeparate(context.Background(), seedstore.EventReviewHistoricalDuplicateKeepSeparateInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "at least two kept event IDs") {
		t.Fatalf("resolve missing kept IDs error = %v, want kept ID requirement", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != 0 {
		t.Fatalf("event_review_resolutions rows = %d, want 0", got)
	}
}

func TestResolveHistoricalDuplicateWithActionsOverridesCanonicalAndWithholdsSelectedDuplicate(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedHistoricalDuplicateResolutionFixture(t, db)

	if err := st.ResolveHistoricalDuplicateWithActions(context.Background(), seedstore.EventReviewHistoricalDuplicateWithActionsInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		CanonicalEventID: fixture.loserID,
		Actions: []seedstore.EventReviewHistoricalDuplicateActionInput{
			{EventID: fixture.canonicalID, Action: seedstore.EventReviewLiveActionKindWithholdDuplicate},
			{EventID: fixture.loserID, Action: seedstore.EventReviewLiveActionKindKeepSeparate},
		},
	}); err != nil {
		t.Fatalf("resolve historical duplicate override actions: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	var clusterCanonical sql.NullInt64
	if err := db.QueryRow(`SELECT canonical_event_id FROM event_review_clusters WHERE id = ?`, fixture.clusterID).Scan(&clusterCanonical); err != nil {
		t.Fatalf("load cluster canonical: %v", err)
	}
	if !clusterCanonical.Valid || clusterCanonical.Int64 != fixture.loserID {
		t.Fatalf("cluster canonical = %v, want %d", clusterCanonical, fixture.loserID)
	}

	var oldCanonicalState string
	var oldCanonicalTarget sql.NullInt64
	if err := db.QueryRow(`SELECT publication_state, canonical_event_id FROM events WHERE id = ?`, fixture.canonicalID).Scan(&oldCanonicalState, &oldCanonicalTarget); err != nil {
		t.Fatalf("load old canonical event: %v", err)
	}
	if oldCanonicalState != string(domain.PublicationStateWithheld) || !oldCanonicalTarget.Valid || oldCanonicalTarget.Int64 != fixture.loserID {
		t.Fatalf("old canonical state=%q target=%v, want withheld to %d", oldCanonicalState, oldCanonicalTarget, fixture.loserID)
	}
	var survivorState string
	if err := db.QueryRow(`SELECT publication_state FROM events WHERE id = ?`, fixture.loserID).Scan(&survivorState); err != nil {
		t.Fatalf("load survivor state: %v", err)
	}
	if survivorState != string(domain.PublicationStateReviewed) {
		t.Fatalf("survivor state = %q, want reviewed", survivorState)
	}

	var snapshot string
	if err := db.QueryRow(`SELECT snapshot FROM event_review_resolutions WHERE cluster_id = ?`, fixture.clusterID).Scan(&snapshot); err != nil {
		t.Fatalf("load resolution snapshot: %v", err)
	}
	var got struct {
		RepairRunID        *int64 `json:"repair_run_id"`
		CanonicalEventID   *int64 `json:"canonical_event_id"`
		AppliedLiveActions []struct {
			EventID int64                               `json:"event_id"`
			Action  seedstore.EventReviewLiveActionKind `json:"action"`
		} `json:"applied_live_actions"`
	}
	if err := json.Unmarshal([]byte(snapshot), &got); err != nil {
		t.Fatalf("unmarshal override snapshot: %v", err)
	}
	if got.RepairRunID == nil {
		t.Fatal("repair run id missing from override snapshot")
	}
	if got.CanonicalEventID == nil || *got.CanonicalEventID != fixture.loserID {
		t.Fatalf("snapshot canonical = %v, want %d", got.CanonicalEventID, fixture.loserID)
	}
	if len(got.AppliedLiveActions) != 2 || got.AppliedLiveActions[0].Action != seedstore.EventReviewLiveActionKindWithholdDuplicate || got.AppliedLiveActions[1].Action != seedstore.EventReviewLiveActionKindKeepSeparate {
		t.Fatalf("applied live actions = %#v", got.AppliedLiveActions)
	}
}

func TestResolveHistoricalDuplicateWithActionsAllKeepDelegatesToKeepSeparate(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedHistoricalDuplicateResolutionFixture(t, db)

	if err := st.ResolveHistoricalDuplicateWithActions(context.Background(), seedstore.EventReviewHistoricalDuplicateWithActionsInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		Actions: []seedstore.EventReviewHistoricalDuplicateActionInput{
			{EventID: fixture.canonicalID, Action: seedstore.EventReviewLiveActionKindKeepSeparate},
			{EventID: fixture.loserID, Action: seedstore.EventReviewLiveActionKindKeepSeparate},
		},
	}); err != nil {
		t.Fatalf("resolve historical duplicate all-keep override: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	var snapshot string
	if err := db.QueryRow(`SELECT snapshot FROM event_review_resolutions WHERE cluster_id = ?`, fixture.clusterID).Scan(&snapshot); err != nil {
		t.Fatalf("load resolution snapshot: %v", err)
	}
	var got struct {
		RepairRunID                   *int64 `json:"repair_run_id"`
		AppliedHistoricalKeepSeparate *struct {
			KeptEvents []struct {
				EventID int64 `json:"event_id"`
			} `json:"kept_events"`
		} `json:"applied_historical_keep_separate"`
	}
	if err := json.Unmarshal([]byte(snapshot), &got); err != nil {
		t.Fatalf("unmarshal all-keep snapshot: %v", err)
	}
	if got.RepairRunID != nil {
		t.Fatalf("repair run id = %v, want nil", *got.RepairRunID)
	}
	if got.AppliedHistoricalKeepSeparate == nil || len(got.AppliedHistoricalKeepSeparate.KeptEvents) != 2 {
		t.Fatalf("historical keep-separate snapshot = %#v", got.AppliedHistoricalKeepSeparate)
	}
}

func TestResolveHistoricalDuplicateWithActionsRejectsInvalidActionSets(t *testing.T) {
	tests := []struct {
		name    string
		input   func(fixture historicalDuplicateResolutionFixture) seedstore.EventReviewHistoricalDuplicateWithActionsInput
		wantErr string
	}{
		{
			name: "missing canonical for withhold",
			input: func(fixture historicalDuplicateResolutionFixture) seedstore.EventReviewHistoricalDuplicateWithActionsInput {
				return seedstore.EventReviewHistoricalDuplicateWithActionsInput{
					Actions: []seedstore.EventReviewHistoricalDuplicateActionInput{
						{EventID: fixture.canonicalID, Action: seedstore.EventReviewLiveActionKindKeepSeparate},
						{EventID: fixture.loserID, Action: seedstore.EventReviewLiveActionKindWithholdDuplicate},
					},
				}
			},
			wantErr: "requires a canonical event",
		},
		{
			name: "unknown event",
			input: func(fixture historicalDuplicateResolutionFixture) seedstore.EventReviewHistoricalDuplicateWithActionsInput {
				return seedstore.EventReviewHistoricalDuplicateWithActionsInput{
					CanonicalEventID: fixture.canonicalID,
					Actions: []seedstore.EventReviewHistoricalDuplicateActionInput{
						{EventID: fixture.canonicalID, Action: seedstore.EventReviewLiveActionKindKeepSeparate},
						{EventID: fixture.loserID + 999, Action: seedstore.EventReviewLiveActionKindWithholdDuplicate},
					},
				}
			},
			wantErr: "submitted action events do not match",
		},
		{
			name: "canonical withhold action",
			input: func(fixture historicalDuplicateResolutionFixture) seedstore.EventReviewHistoricalDuplicateWithActionsInput {
				return seedstore.EventReviewHistoricalDuplicateWithActionsInput{
					CanonicalEventID: fixture.canonicalID,
					Actions: []seedstore.EventReviewHistoricalDuplicateActionInput{
						{EventID: fixture.canonicalID, Action: seedstore.EventReviewLiveActionKindWithholdDuplicate},
						{EventID: fixture.loserID, Action: seedstore.EventReviewLiveActionKindKeepSeparate},
					},
				}
			},
			wantErr: "must be keep_separate",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, db := openEventReviewSchemaStore(t)
			defer db.Close()

			st := mustStoreFromDB(t, db)
			fixture := seedHistoricalDuplicateResolutionFixture(t, db)
			input := tc.input(fixture)
			input.EventReviewResolutionInput = seedstore.EventReviewResolutionInput{ClusterID: fixture.clusterID, ExpectedVersion: 1}
			err := st.ResolveHistoricalDuplicateWithActions(context.Background(), input)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("resolve override error = %v, want %q", err, tc.wantErr)
			}
			assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
			if got := mustCount(t, db, "event_review_resolutions"); got != 0 {
				t.Fatalf("event_review_resolutions rows = %d, want 0", got)
			}
		})
	}
}

func TestResolveHistoricalDuplicateWithActionsPreflightsSourceLinkGuardBeforeDetach(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedHistoricalDuplicateResolutionFixture(t, db)
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			source_id,
			event_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, fixture.sourceID, fixture.loserID, "uid:override-source-conflict", 1, "2026-05-12T09:00:00Z", "2026-05-12T09:00:00Z"); err != nil {
		t.Fatalf("insert loser source link: %v", err)
	}

	err := st.ResolveHistoricalDuplicateWithActions(context.Background(), seedstore.EventReviewHistoricalDuplicateWithActionsInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		CanonicalEventID: fixture.canonicalID,
		Actions: []seedstore.EventReviewHistoricalDuplicateActionInput{
			{EventID: fixture.canonicalID, Action: seedstore.EventReviewLiveActionKindKeepSeparate},
			{EventID: fixture.loserID, Action: seedstore.EventReviewLiveActionKindWithholdDuplicate},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "authoritative source identity does not resolve to canonical") {
		t.Fatalf("resolve override error = %v, want source-link guard", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != 0 {
		t.Fatalf("event_review_resolutions rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair_runs rows = %d, want 0", got)
	}
	var linkCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM event_source_links WHERE event_id = ?`, fixture.loserID).Scan(&linkCount); err != nil {
		t.Fatalf("count loser source links: %v", err)
	}
	if linkCount != 1 {
		t.Fatalf("loser source links = %d, want 1", linkCount)
	}
	var loserState string
	var loserCanonical sql.NullInt64
	if err := db.QueryRow(`SELECT publication_state, canonical_event_id FROM events WHERE id = ?`, fixture.loserID).Scan(&loserState, &loserCanonical); err != nil {
		t.Fatalf("load loser state: %v", err)
	}
	if loserState == string(domain.PublicationStateWithheld) || loserCanonical.Valid {
		t.Fatalf("loser state=%q canonical=%v, want not withheld with no canonical", loserState, loserCanonical)
	}
}

func TestResolveEventReviewClusterAppliesTitleRepairAndRefreshesExactIdentity(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedTitleRepairResolutionFixture(t, db)
	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	beforeRepairRuns := mustCount(t, db, "repair_runs")
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")

	if err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
		ClusterID:       fixture.clusterID,
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("resolve title repair cluster: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved title repair cluster: %v", err)
	}
	if !ok {
		t.Fatal("load resolved title repair cluster ok = false")
	}
	if detail.Resolution == nil {
		t.Fatal("resolved title repair cluster missing resolution summary")
	}
	if detail.Resolution.Status != seedstore.EventReviewResolutionStatusResolved {
		t.Fatalf("resolution status = %q, want %q", detail.Resolution.Status, seedstore.EventReviewResolutionStatusResolved)
	}
	if detail.Resolution.RepairRunID != nil {
		t.Fatalf("title repair resolution repair run id = %v, want nil", detail.Resolution.RepairRunID)
	}
	if detail.Resolution.AppliedTitleRepair == nil {
		t.Fatal("title repair resolution missing applied title repair summary")
	}
	if got := detail.Resolution.AppliedTitleRepair; got.EventID != fixture.eventID || got.OldTitle != fixture.oldTitle || got.NewTitle != fixture.newTitle || got.OldSlug != fixture.oldSlug || got.NewSlug != fixture.newSlug {
		t.Fatalf("applied title repair = %#v", got)
	}

	var resolvedTitle, resolvedSlug string
	if err := db.QueryRow(`
		SELECT name, slug
		FROM events
		WHERE id = ?
	`, fixture.eventID).Scan(&resolvedTitle, &resolvedSlug); err != nil {
		t.Fatalf("load resolved event: %v", err)
	}
	if resolvedTitle != fixture.newTitle || resolvedSlug != fixture.newSlug {
		t.Fatalf("resolved event = (%q, %q), want (%q, %q)", resolvedTitle, resolvedSlug, fixture.newTitle, fixture.newSlug)
	}

	rows := mustExactIdentityRowsByEvent(t, db, fixture.eventID)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("exact identity rows after title repair = %d, want %d", got, want)
	}
	if rows[0].Active != 0 || rows[1].Active != 1 {
		t.Fatalf("exact identity rows after title repair = %#v", rows)
	}

	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions+1 {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions+1)
	}
	if got := mustCount(t, db, "repair_runs"); got != beforeRepairRuns {
		t.Fatalf("repair_runs rows = %d, want %d", got, beforeRepairRuns)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}
}

func TestResolveTitleRepairSlugConflictMergesDuplicateIntoSlugOwner(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedTitleRepairResolutionFixture(t, db)
	conflictID := mustInsertExactIdentityEvent(t, db, fixture.newSlug, "Clean duplicate title", fixture.venueID, fixture.sourceID, time.Date(2026, time.May, 18, 19, 0, 0, 0, time.UTC), time.Date(2026, time.May, 18, 21, 0, 0, 0, time.UTC), time.Date(2026, time.May, 18, 9, 5, 0, 0, time.UTC), domain.OriginLive)

	if err := st.ResolveTitleRepairSlugConflict(context.Background(), seedstore.EventReviewTitleRepairSlugConflictInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		Mode:                     seedstore.EventReviewTitleRepairSlugConflictModeMergeDuplicate,
		OriginalCanonicalEventID: fixture.eventID,
		SlugConflictEventID:      conflictID,
		DraftTitle:               fixture.newTitle,
		DraftSlug:                fixture.newSlug,
	}); err != nil {
		t.Fatalf("resolve title repair slug conflict merge: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	var canonicalID int64
	if err := db.QueryRow(`SELECT canonical_event_id FROM event_review_clusters WHERE id = ?`, fixture.clusterID).Scan(&canonicalID); err != nil {
		t.Fatalf("load cluster canonical id: %v", err)
	}
	if canonicalID != conflictID {
		t.Fatalf("cluster canonical event id = %d, want surviving conflict event %d", canonicalID, conflictID)
	}
	var oldCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE id = ?`, fixture.eventID).Scan(&oldCount); err != nil {
		t.Fatalf("count old canonical event rows: %v", err)
	}
	if oldCount != 0 {
		t.Fatalf("old canonical event rows = %d, want 0", oldCount)
	}
	var title, slug string
	if err := db.QueryRow(`SELECT name, slug FROM events WHERE id = ?`, conflictID).Scan(&title, &slug); err != nil {
		t.Fatalf("load surviving event: %v", err)
	}
	if title != fixture.newTitle || slug != fixture.newSlug {
		t.Fatalf("surviving event = (%q, %q), want (%q, %q)", title, slug, fixture.newTitle, fixture.newSlug)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved title slug conflict cluster: %v", err)
	}
	if !ok {
		t.Fatal("load resolved title slug conflict cluster ok = false")
	}
	if detail.Resolution == nil || detail.Resolution.AppliedTitleSlugConflict == nil {
		t.Fatalf("resolved title slug conflict summary = %#v", detail.Resolution)
	}
	applied := detail.Resolution.AppliedTitleSlugConflict
	if applied.Mode != seedstore.EventReviewTitleRepairSlugConflictModeMergeDuplicate || applied.OldCanonicalEventID != fixture.eventID || applied.SlugConflictEventID != conflictID || applied.SurvivingEventID != conflictID {
		t.Fatalf("applied title slug conflict = %#v", applied)
	}
}

func TestResolveTitleRepairSlugConflictKeepsSeparateWithoutTitleChange(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedTitleRepairResolutionFixture(t, db)
	conflictID := mustInsertExactIdentityEvent(t, db, fixture.newSlug, "Distinct event title", fixture.venueID, fixture.sourceID, time.Date(2026, time.May, 19, 19, 0, 0, 0, time.UTC), time.Date(2026, time.May, 19, 21, 0, 0, 0, time.UTC), time.Date(2026, time.May, 18, 9, 5, 0, 0, time.UTC), domain.OriginLive)

	if err := st.ResolveTitleRepairSlugConflict(context.Background(), seedstore.EventReviewTitleRepairSlugConflictInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		Mode:                     seedstore.EventReviewTitleRepairSlugConflictModeKeepSeparateNoChange,
		OriginalCanonicalEventID: fixture.eventID,
		SlugConflictEventID:      conflictID,
		DraftTitle:               fixture.newTitle,
		DraftSlug:                fixture.newSlug,
	}); err != nil {
		t.Fatalf("resolve title repair slug conflict keep separate: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	var canonicalID int64
	if err := db.QueryRow(`SELECT canonical_event_id FROM event_review_clusters WHERE id = ?`, fixture.clusterID).Scan(&canonicalID); err != nil {
		t.Fatalf("load cluster canonical id: %v", err)
	}
	if canonicalID != fixture.eventID {
		t.Fatalf("cluster canonical event id = %d, want original event %d", canonicalID, fixture.eventID)
	}
	var originalTitle, originalSlug, conflictTitle string
	if err := db.QueryRow(`SELECT name, slug FROM events WHERE id = ?`, fixture.eventID).Scan(&originalTitle, &originalSlug); err != nil {
		t.Fatalf("load original event: %v", err)
	}
	if err := db.QueryRow(`SELECT name FROM events WHERE id = ?`, conflictID).Scan(&conflictTitle); err != nil {
		t.Fatalf("load conflict event: %v", err)
	}
	if originalTitle != fixture.oldTitle || originalSlug != fixture.oldSlug || conflictTitle != "Distinct event title" {
		t.Fatalf("events mutated unexpectedly: original=(%q,%q) conflict=%q", originalTitle, originalSlug, conflictTitle)
	}
	var separationCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_separations
		WHERE active = 1
			AND ((endpoint_a_key = ? AND endpoint_b_key = ?) OR (endpoint_a_key = ? AND endpoint_b_key = ?))
	`, seedstore.EventReviewSeparationEventEndpointKey(fixture.eventID), seedstore.EventReviewSeparationEventEndpointKey(conflictID), seedstore.EventReviewSeparationEventEndpointKey(conflictID), seedstore.EventReviewSeparationEventEndpointKey(fixture.eventID)).Scan(&separationCount); err != nil {
		t.Fatalf("load separation count: %v", err)
	}
	if separationCount != 1 {
		t.Fatalf("separation count = %d, want 1", separationCount)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved title slug conflict cluster: %v", err)
	}
	if !ok {
		t.Fatal("load resolved title slug conflict cluster ok = false")
	}
	if detail.Resolution == nil || detail.Resolution.AppliedTitleSlugConflict == nil {
		t.Fatalf("resolved title slug conflict summary = %#v", detail.Resolution)
	}
	applied := detail.Resolution.AppliedTitleSlugConflict
	if applied.Mode != seedstore.EventReviewTitleRepairSlugConflictModeKeepSeparateNoChange || applied.OldCanonicalEventID != fixture.eventID || applied.SlugConflictEventID != conflictID || applied.SurvivingEventID != 0 {
		t.Fatalf("applied title slug conflict = %#v", applied)
	}
	if len(detail.Resolution.AppliedSeparations) != 1 {
		t.Fatalf("applied separations = %#v, want 1", detail.Resolution.AppliedSeparations)
	}
}

func TestResolveEventReviewClusterRejectsUnsupportedTitleRepairStates(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(t *testing.T, db *sql.DB, fixture titleRepairResolutionFixture)
		wantErr string
	}{
		{
			name: "stale version",
			mutate: func(t *testing.T, db *sql.DB, fixture titleRepairResolutionFixture) {
				_ = fixture
			},
			wantErr: "version",
		},
		{
			name: "missing draft name",
			mutate: func(t *testing.T, db *sql.DB, fixture titleRepairResolutionFixture) {
				if _, err := db.Exec(`DELETE FROM event_review_draft_choices WHERE cluster_id = ? AND field_name = ?`, fixture.clusterID, "name"); err != nil {
					t.Fatalf("delete draft name: %v", err)
				}
			},
			wantErr: "draft title is required",
		},
		{
			name: "missing draft slug",
			mutate: func(t *testing.T, db *sql.DB, fixture titleRepairResolutionFixture) {
				if _, err := db.Exec(`DELETE FROM event_review_draft_choices WHERE cluster_id = ? AND field_name = ?`, fixture.clusterID, "slug"); err != nil {
					t.Fatalf("delete draft slug: %v", err)
				}
			},
			wantErr: "draft slug is required",
		},
		{
			name: "missing canonical",
			mutate: func(t *testing.T, db *sql.DB, fixture titleRepairResolutionFixture) {
				if _, err := db.Exec(`UPDATE event_review_clusters SET canonical_event_id = NULL WHERE id = ?`, fixture.clusterID); err != nil {
					t.Fatalf("clear canonical id: %v", err)
				}
			},
			wantErr: "canonical event is missing",
		},
		{
			name: "withheld canonical",
			mutate: func(t *testing.T, db *sql.DB, fixture titleRepairResolutionFixture) {
				if _, err := db.Exec(`UPDATE events SET publication_state = ? WHERE id = ?`, string(domain.PublicationStateWithheld), fixture.eventID); err != nil {
					t.Fatalf("withhold canonical: %v", err)
				}
			},
			wantErr: "current event is not live/non-withheld",
		},
		{
			name: "slug conflict",
			mutate: func(t *testing.T, db *sql.DB, fixture titleRepairResolutionFixture) {
				mustInsertExactIdentityEvent(t, db, fixture.newSlug, "Conflicting event", fixture.venueID, fixture.sourceID, time.Date(2026, time.May, 18, 19, 0, 0, 0, time.UTC), time.Date(2026, time.May, 18, 21, 0, 0, 0, time.UTC), time.Date(2026, time.May, 18, 9, 5, 0, 0, time.UTC), domain.OriginLive)
			},
			wantErr: "target slug already belongs to another live event",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, db := openEventReviewSchemaStore(t)
			defer db.Close()

			st := mustStoreFromDB(t, db)
			fixture := seedTitleRepairResolutionFixture(t, db)
			beforeResolutions := mustCount(t, db, "event_review_resolutions")
			beforeRepairRuns := mustCount(t, db, "repair_runs")
			beforeReviewGroups := mustCount(t, db, "review_groups")
			beforeReviewCandidates := mustCount(t, db, "review_candidates")
			tc.mutate(t, db, fixture)

			expectedVersion := 1
			if tc.name == "stale version" {
				expectedVersion = 2
			}
			err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
				ClusterID:       fixture.clusterID,
				ExpectedVersion: expectedVersion,
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("resolve error = %v, want %q", err, tc.wantErr)
			}

			assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
			if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
				t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
			}
			if got := mustCount(t, db, "repair_runs"); got != beforeRepairRuns {
				t.Fatalf("repair_runs rows = %d, want %d", got, beforeRepairRuns)
			}
			if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
				t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
			}
			if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
				t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
			}
		})
	}
}

func TestResolveEventReviewClusterRejectsUnsupportedHistoricalDuplicateStates(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(t *testing.T, db *sql.DB, fixture historicalDuplicateResolutionFixture)
		wantErr string
	}{
		{
			name: "stale version",
			mutate: func(t *testing.T, db *sql.DB, fixture historicalDuplicateResolutionFixture) {
				_ = fixture
			},
			wantErr: "version",
		},
		{
			name: "unsupported conflict type",
			mutate: func(t *testing.T, db *sql.DB, fixture historicalDuplicateResolutionFixture) {
				if _, err := db.Exec(`UPDATE event_review_clusters SET conflict_type = ? WHERE id = ?`, "other_conflict", fixture.clusterID); err != nil {
					t.Fatalf("update conflict type: %v", err)
				}
			},
			wantErr: "not supported",
		},
		{
			name: "missing canonical",
			mutate: func(t *testing.T, db *sql.DB, fixture historicalDuplicateResolutionFixture) {
				if _, err := db.Exec(`UPDATE event_review_clusters SET canonical_event_id = NULL WHERE id = ?`, fixture.clusterID); err != nil {
					t.Fatalf("clear canonical id: %v", err)
				}
			},
			wantErr: "requires a canonical event",
		},
		{
			name: "missing live actions",
			mutate: func(t *testing.T, db *sql.DB, fixture historicalDuplicateResolutionFixture) {
				if _, err := db.Exec(`DELETE FROM event_review_live_actions WHERE cluster_id = ?`, fixture.clusterID); err != nil {
					t.Fatalf("delete live actions: %v", err)
				}
			},
			wantErr: "no stored live actions",
		},
		{
			name: "missing withhold",
			mutate: func(t *testing.T, db *sql.DB, fixture historicalDuplicateResolutionFixture) {
				if _, err := db.Exec(`DELETE FROM event_review_live_actions WHERE cluster_id = ? AND event_id = ?`, fixture.clusterID, fixture.loserID); err != nil {
					t.Fatalf("delete withhold action: %v", err)
				}
			},
			wantErr: "at least one withhold_duplicate",
		},
		{
			name: "mismatched keep",
			mutate: func(t *testing.T, db *sql.DB, fixture historicalDuplicateResolutionFixture) {
				if _, err := db.Exec(`DELETE FROM event_review_live_actions WHERE cluster_id = ?`, fixture.clusterID); err != nil {
					t.Fatalf("clear live actions: %v", err)
				}
				if _, err := db.Exec(`
					INSERT INTO event_review_live_actions (
						cluster_id,
						event_id,
						action,
						reason,
						created_at,
						updated_at
					) VALUES (?, ?, ?, ?, ?, ?)
				`, fixture.clusterID, fixture.loserID, string(seedstore.EventReviewLiveActionKindKeepSeparate), "wrong keep", "2026-05-15T11:00:00Z", "2026-05-15T11:00:00Z"); err != nil {
					t.Fatalf("insert mismatched keep: %v", err)
				}
			},
			wantErr: "keep_separate action must target canonical event",
		},
		{
			name: "unsupported action",
			mutate: func(t *testing.T, db *sql.DB, fixture historicalDuplicateResolutionFixture) {
				setIgnoreCheckConstraints(t, db, true)
				defer setIgnoreCheckConstraints(t, db, false)
				if _, err := db.Exec(`
					UPDATE event_review_live_actions
					SET action = ?
					WHERE cluster_id = ? AND event_id = ?
				`, "auto_resolved", fixture.clusterID, fixture.loserID); err != nil {
					t.Fatalf("update unsupported action: %v", err)
				}
			},
			wantErr: "unsupported live action",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, db := openEventReviewSchemaStore(t)
			defer db.Close()
			st := mustStoreFromDB(t, db)
			fixture := seedHistoricalDuplicateResolutionFixture(t, db)
			beforeResolutions := mustCount(t, db, "event_review_resolutions")
			beforeRepairRuns := mustCount(t, db, "repair_runs")
			beforeReviewGroups := mustCount(t, db, "review_groups")
			beforeReviewCandidates := mustCount(t, db, "review_candidates")
			tc.mutate(t, db, fixture)

			expectedVersion := 1
			if tc.name == "stale version" {
				expectedVersion = 2
			}
			err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
				ClusterID:       fixture.clusterID,
				ExpectedVersion: expectedVersion,
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("resolve error = %v, want %q", err, tc.wantErr)
			}

			assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
			if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
				t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
			}
			if got := mustCount(t, db, "repair_runs"); got != beforeRepairRuns {
				t.Fatalf("repair_runs rows = %d, want %d", got, beforeRepairRuns)
			}
			if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
				t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
			}
			if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
				t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
			}
		})
	}
}

func seedHistoricalDuplicateResolutionFixture(t *testing.T, db *sql.DB) historicalDuplicateResolutionFixture {
	t.Helper()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "historical-duplicate-canonical", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "historical-duplicate-loser", venueID, sourceID, domain.OriginLive)

	canonicalID := mustEventIDBySlug(t, db, "historical-duplicate-canonical")
	loserID := mustEventIDBySlug(t, db, "historical-duplicate-loser")
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalID)
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET conflict_type = ?, conflict_reason = ?
		WHERE id = ?
	`, "historical_duplicate", "historical duplicate review", clusterID); err != nil {
		t.Fatalf("seed conflict metadata: %v", err)
	}
	if _, err := insertEventReviewLiveAction(t, db, clusterID, canonicalID, string(seedstore.EventReviewLiveActionKindKeepSeparate), "keep canonical", time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed canonical live action: %v", err)
	}
	if _, err := insertEventReviewLiveAction(t, db, clusterID, loserID, string(seedstore.EventReviewLiveActionKindWithholdDuplicate), "withhold loser", time.Date(2026, time.May, 15, 11, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed withhold live action: %v", err)
	}
	return historicalDuplicateResolutionFixture{
		clusterID:   clusterID,
		canonicalID: canonicalID,
		loserID:     loserID,
		sourceID:    sourceID,
	}
}

type importReviewResolutionFixture struct {
	clusterID    int64
	evidenceID   int64
	sourceID     int64
	venueID      int64
	title        string
	venueText    string
	start        time.Time
	end          time.Time
	externalID   string
	sourceURL    string
	calendarURL  string
	sourceName   string
	expectedSlug string
}

func TestResolveEventReviewClusterAppliesImportReviewNewListing(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")

	if err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
		ClusterID:       fixture.clusterID,
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("resolve import review cluster: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved import review cluster: %v", err)
	}
	if !ok {
		t.Fatal("load resolved import review cluster ok = false")
	}
	if detail.Resolution == nil {
		t.Fatal("resolved import review cluster missing resolution summary")
	}
	if detail.Resolution.AppliedImportListing == nil {
		t.Fatal("resolved import review cluster missing applied import listing")
	}
	if got := detail.Resolution.AppliedImportListing; got.Title != fixture.title || got.VenueName != "The Leadmill" || got.VenueSlug != "leadmill" || got.SourceID != fixture.sourceID || got.SourceName != fixture.sourceName || got.SourceURL != fixture.sourceURL || got.EvidenceID == 0 {
		t.Fatalf("applied import listing = %#v", got)
	}
	var eventID int64
	if err := db.QueryRow(`
		SELECT canonical_event_id
		FROM event_review_clusters
		WHERE id = ?
	`, fixture.clusterID).Scan(&eventID); err != nil {
		t.Fatalf("load cluster canonical event id: %v", err)
	}
	if eventID <= 0 {
		t.Fatalf("canonical event id = %d, want positive", eventID)
	}

	var officialListingURL, calendarURL, origin, publicationState string
	if err := db.QueryRow(`
		SELECT official_listing_url, calendar_url, origin, publication_state
		FROM events
		WHERE id = ?
	`, eventID).Scan(&officialListingURL, &calendarURL, &origin, &publicationState); err != nil {
		t.Fatalf("load imported event row: %v", err)
	}
	if officialListingURL != fixture.sourceURL {
		t.Fatalf("official_listing_url = %q, want %q", officialListingURL, fixture.sourceURL)
	}
	if calendarURL != fixture.calendarURL {
		t.Fatalf("calendar_url = %q, want %q", calendarURL, fixture.calendarURL)
	}
	if origin != string(domain.OriginLive) {
		t.Fatalf("origin = %q, want %q", origin, domain.OriginLive)
	}
	if publicationState != string(domain.PublicationStateReviewed) {
		t.Fatalf("publication_state = %q, want %q", publicationState, domain.PublicationStateReviewed)
	}

	var evidenceEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, detail.Evidence[0].EvidenceID).Scan(&evidenceEventID); err != nil {
		t.Fatalf("load evidence event id: %v", err)
	}
	if !evidenceEventID.Valid || evidenceEventID.Int64 != eventID {
		t.Fatalf("evidence event id = %v, want %d", evidenceEventID, eventID)
	}

	rows := mustExactIdentityRowsByEvent(t, db, eventID)
	if len(rows) == 0 || rows[len(rows)-1].Active != 1 {
		t.Fatalf("exact identity rows after import listing = %#v", rows)
	}

	publishedSourceKey, ok := ingest.SourceIdentityKey(fixture.externalID)
	if !ok {
		t.Fatalf("source identity key for %q was rejected", fixture.externalID)
	}
	var publishedSourceLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_links
		WHERE event_id = ? AND source_id = ? AND source_event_key = ?
	`, eventID, fixture.sourceID, publishedSourceKey).Scan(&publishedSourceLinkCount); err != nil {
		t.Fatalf("load published source link count: %v", err)
	}
	if publishedSourceLinkCount != 1 {
		t.Fatalf("published source link count = %d, want 1", publishedSourceLinkCount)
	}

	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions+1 {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions+1)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}
}

func TestResolveEventReviewClusterAppliesSelectedImportReviewNewListingFromMultipleEvidenceRows(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	beforeEvents := mustCount(t, db, "events")
	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")

	selectedKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-import-review-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-import-review-key")
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, selectedKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert selected evidence identity: %v", err)
	}
	if _, err := insertEventReviewSourceIdentityChoice(t, db, fixture.clusterID, fixture.sourceID, "selected-import-review-key", true, "preferred selected candidate", time.Date(2026, time.May, 15, 9, 40, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert selected source identity choice: %v", err)
	}
	unselectedKeyID := insertEventReviewIdentityKeyOK(t, db, "unselected-import-review-hash", seedstore.EventReviewIdentityKeyKindSource, "unselected-import-review-key")
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, unselectedKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert unselected evidence identity: %v", err)
	}
	if _, err := insertEventReviewSourceIdentityChoice(t, db, fixture.clusterID, fixture.sourceID, "unselected-import-review-key", false, "not selected", time.Date(2026, time.May, 15, 9, 41, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert unselected source identity choice: %v", err)
	}
	unselectedLinkedEventID := mustInsertExactIdentityEvent(t, db, "selected-import-review-unselected-link", "Linked Unselected Import Listing", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), time.Date(2026, time.May, 15, 9, 35, 0, 0, time.UTC), domain.OriginLive)
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			event_id,
			source_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 0, ?, ?)
	`, unselectedLinkedEventID, fixture.sourceID, "unselected-import-review-key", "2026-05-15T09:35:00Z", "2026-05-15T09:35:00Z"); err != nil {
		t.Fatalf("insert unselected linked source identity: %v", err)
	}
	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load selected multi-evidence readiness before resolve: %v", err)
	}
	if !ok || detail.ImportReadiness == nil {
		t.Fatal("selected multi-evidence readiness missing before resolve")
	}
	unselectedTarget := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, unselectedLinkedEventID, seedstore.EventReviewImportTargetBasisSourceIdentity)
	if !hasString(unselectedTarget.BlockingReasons, "source identity is not selected") {
		t.Fatalf("unselected source target blockers = %#v, want source identity not selected", unselectedTarget.BlockingReasons)
	}
	if err := st.AcceptEventReviewSupportingSource(context.Background(), seedstore.EventReviewAcceptSupportingSourceInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:         fixture.evidenceID,
		TargetEventID:      unselectedLinkedEventID,
		TargetBasis:        seedstore.EventReviewImportTargetBasisSourceIdentity,
		SourceIdentityKeys: []string{"unselected-import-review-key"},
	}); err == nil || !strings.Contains(err.Error(), "not selected for evidence") {
		t.Fatalf("accept supporting with unselected source key error = %v, want not selected", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)

	extraPayload := importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), "Unselected Import Listing", fixture.venueText, "", fixture.start.Add(1*time.Hour), fixture.end.Add(1*time.Hour), "import-review-extra")
	extraEvidenceID := insertEventReviewEvidenceOK(t, db, fixture.sourceID, nil, "import-review-extra-"+strconv.FormatInt(fixture.clusterID, 10), extraPayload)
	insertEventReviewClusterEvidenceOK(t, db, fixture.clusterID, extraEvidenceID, true, fixture.start.Add(10*time.Minute), nil, "extra evidence")
	extraTargetID := mustInsertExactIdentityEvent(t, db, "unselected-evidence-existing-target", "Unselected Import Listing", fixture.venueID, fixture.sourceID, fixture.start.Add(1*time.Hour), fixture.end.Add(1*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	extraExactKey := "unselected-evidence-existing-exact-key"
	if _, err := db.Exec(`
		INSERT INTO event_exact_identities (
			event_id,
			identity_key,
			key_version,
			venue_slug,
			utc_start_at,
			clean_title,
			active,
			created_at,
			updated_at,
			deactivated_at,
			deactivated_reason,
			repair_run_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, extraTargetID, extraExactKey, exactIdentityKeyVersion, "leadmill", formatRFC3339UTC(fixture.start.Add(1*time.Hour)), "Unselected Import Listing", 1, "2026-05-15T09:45:00Z", "2026-05-15T09:45:00Z", nil, "", nil); err != nil {
		t.Fatalf("insert unselected evidence exact target identity: %v", err)
	}
	extraExactKeyID := insertEventReviewIdentityKeyOK(t, db, "unselected-evidence-existing-exact-hash", seedstore.EventReviewIdentityKeyKindExact, extraExactKey)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, extraEvidenceID, extraExactKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert unselected evidence exact key: %v", err)
	}
	detail, ok, err = st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load selected multi-evidence readiness with unselected existing target: %v", err)
	}
	if !ok || detail.ImportReadiness == nil {
		t.Fatal("selected multi-evidence readiness with unselected target missing")
	}
	extraTarget := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, extraEvidenceID, extraTargetID, seedstore.EventReviewImportTargetBasisExactIdentity)
	if !hasString(extraTarget.BlockingReasons, "evidence is not selected by source identity choices") {
		t.Fatalf("unselected evidence target blockers = %#v, want selected-evidence blocker", extraTarget.BlockingReasons)
	}
	if err := st.AcceptEventReviewSupportingSource(context.Background(), seedstore.EventReviewAcceptSupportingSourceInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:    extraEvidenceID,
		TargetEventID: extraTargetID,
		TargetBasis:   seedstore.EventReviewImportTargetBasisExactIdentity,
	}); err == nil || !strings.Contains(err.Error(), "not selected by source identity choices") {
		t.Fatalf("accept supporting with unselected evidence error = %v, want not selected", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)

	if err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
		ClusterID:       fixture.clusterID,
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("resolve selected multi-evidence cluster: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)

	detail, ok, err = st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved selected multi-evidence cluster: %v", err)
	}
	if !ok || detail.Resolution == nil || detail.Resolution.AppliedImportListing == nil {
		t.Fatal("resolved selected multi-evidence cluster missing applied import listing")
	}
	if got := detail.Resolution.AppliedImportListing; got.EvidenceID != fixture.evidenceID || got.Title != fixture.title || got.VenueName != "The Leadmill" || got.VenueSlug != "leadmill" || got.SourceID != fixture.sourceID || got.SourceName != fixture.sourceName || got.SourceURL != fixture.sourceURL || got.EventID <= 0 {
		t.Fatalf("selected applied import listing = %#v", got)
	}

	var eventID int64
	if err := db.QueryRow(`
		SELECT canonical_event_id
		FROM event_review_clusters
		WHERE id = ?
	`, fixture.clusterID).Scan(&eventID); err != nil {
		t.Fatalf("load selected canonical event id: %v", err)
	}
	if eventID <= 0 {
		t.Fatalf("canonical event id = %d, want positive", eventID)
	}

	var officialListingURL, publicationState string
	if err := db.QueryRow(`
		SELECT official_listing_url, publication_state
		FROM events
		WHERE id = ?
	`, eventID).Scan(&officialListingURL, &publicationState); err != nil {
		t.Fatalf("load selected imported event row: %v", err)
	}
	if officialListingURL != fixture.sourceURL {
		t.Fatalf("official_listing_url = %q, want %q", officialListingURL, fixture.sourceURL)
	}
	if publicationState != string(domain.PublicationStateReviewed) {
		t.Fatalf("publication_state = %q, want %q", publicationState, domain.PublicationStateReviewed)
	}

	var selectedEvidenceEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, fixture.evidenceID).Scan(&selectedEvidenceEventID); err != nil {
		t.Fatalf("load selected evidence event id: %v", err)
	}
	if !selectedEvidenceEventID.Valid || selectedEvidenceEventID.Int64 != eventID {
		t.Fatalf("selected evidence event id = %v, want %d", selectedEvidenceEventID, eventID)
	}

	var otherEvidenceEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, extraEvidenceID).Scan(&otherEvidenceEventID); err != nil {
		t.Fatalf("load extra evidence event id: %v", err)
	}
	if otherEvidenceEventID.Valid {
		t.Fatalf("extra evidence event id = %v, want NULL", otherEvidenceEventID)
	}

	payloadExternalKey, ok := ingest.SourceIdentityKey(fixture.externalID)
	if !ok {
		t.Fatalf("payload external id key for %q was rejected", fixture.externalID)
	}
	for _, tc := range []struct {
		name string
		key  string
		want int
	}{
		{name: "selected", key: "selected-import-review-key", want: 1},
		{name: "unselected", key: "unselected-import-review-key", want: 0},
		{name: "payload external", key: payloadExternalKey, want: 0},
	} {
		var got int
		if err := db.QueryRow(`
			SELECT COUNT(*)
			FROM event_source_links
			WHERE event_id = ? AND source_id = ? AND source_event_key = ?
		`, eventID, fixture.sourceID, tc.key).Scan(&got); err != nil {
			t.Fatalf("load %s source link count: %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s source link count = %d, want %d", tc.name, got, tc.want)
		}
	}

	if payloadSourceURLKey, ok := ingest.SourceIdentityKey(fixture.sourceURL); ok {
		var got int
		if err := db.QueryRow(`
			SELECT COUNT(*)
			FROM event_source_links
			WHERE event_id = ? AND source_id = ? AND source_event_key = ?
		`, eventID, fixture.sourceID, payloadSourceURLKey).Scan(&got); err != nil {
			t.Fatalf("load payload source url source link count: %v", err)
		}
		if got != 0 {
			t.Fatalf("payload source url source link count = %d, want 0", got)
		}
	}

	if got := mustCount(t, db, "events"); got != beforeEvents+3 {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents+3)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions+1 {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions+1)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}
}

func TestResolveEventReviewClusterSkipsMalformedNonSelectedSecondaryEvidence(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	beforeEvents := mustCount(t, db, "events")

	selectedKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-import-review-secondary-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-import-review-secondary-key")
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, selectedKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert selected evidence identity: %v", err)
	}
	if _, err := insertEventReviewSourceIdentityChoice(t, db, fixture.clusterID, fixture.sourceID, "selected-import-review-secondary-key", true, "preferred selected candidate", time.Date(2026, time.May, 15, 9, 40, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert selected source identity choice: %v", err)
	}

	malformedEvidenceID := insertEventReviewEvidenceOK(t, db, fixture.sourceID, nil, "import-review-malformed-secondary-"+strconv.FormatInt(fixture.clusterID, 10), "{bad json")
	insertEventReviewClusterEvidenceOK(t, db, fixture.clusterID, malformedEvidenceID, true, fixture.start.Add(10*time.Minute), nil, "malformed secondary evidence")

	if err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
		ClusterID:       fixture.clusterID,
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("resolve selected multi-evidence cluster with malformed secondary: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	if got := mustCount(t, db, "events"); got != beforeEvents+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents+1)
	}
	var selectedEventID sql.NullInt64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, fixture.evidenceID).Scan(&selectedEventID); err != nil {
		t.Fatalf("load selected evidence event id: %v", err)
	}
	if !selectedEventID.Valid {
		t.Fatal("selected evidence event id was not set")
	}
	var malformedEventID sql.NullInt64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, malformedEvidenceID).Scan(&malformedEventID); err != nil {
		t.Fatalf("load malformed evidence event id: %v", err)
	}
	if malformedEventID.Valid {
		t.Fatalf("malformed secondary evidence event id = %v, want NULL", malformedEventID)
	}
}

func TestAcceptEventReviewSupportingSourceAppliesExactIdentityTarget(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	beforeEvents := mustCount(t, db, "events")
	beforeResolutions := mustCount(t, db, "event_review_resolutions")

	targetID := mustInsertExactIdentityEvent(t, db, fixture.expectedSlug, fixture.title, fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?
		WHERE id = ?
	`, string(domain.PublicationStateProvisional), targetID); err != nil {
		t.Fatalf("mark target provisional: %v", err)
	}
	exactKey := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", fixture.start, normalizeExactIdentityCleanTitle(fixture.title))
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin exact identity tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(context.Background(), tx, targetID, domain.Event{
		Slug:             fixture.expectedSlug,
		Name:             fixture.title,
		VenueSlug:        "leadmill",
		Start:            fixture.start,
		Origin:           domain.OriginLive,
		LastChecked:      fixture.start.Add(-24 * time.Hour),
		PublicationState: domain.PublicationStateProvisional,
	}, 0, fixture.start.Add(-24*time.Hour)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("ensure exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact identity tx: %v", err)
	}
	exactKeyID := insertEventReviewIdentityKeyOK(t, db, "supporting-exact-target-hash", seedstore.EventReviewIdentityKeyKindExact, exactKey)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, exactKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert exact evidence identity: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load open import review cluster: %v", err)
	}
	if !ok || detail.ImportReadiness == nil {
		t.Fatal("open import review cluster missing readiness")
	}
	var exactTarget *seedstore.EventReviewImportExistingEventTarget
	for i := range detail.ImportReadiness.ExistingEventTargets {
		target := &detail.ImportReadiness.ExistingEventTargets[i]
		if target.EventID == targetID && target.TargetBasis == seedstore.EventReviewImportTargetBasisExactIdentity {
			exactTarget = target
			break
		}
	}
	if exactTarget == nil || len(exactTarget.ExactIdentityKeys) != 1 || exactTarget.ExactIdentityKeys[0] != exactKey || len(exactTarget.BlockingReasons) != 0 {
		t.Fatalf("exact existing event target = %#v; all targets = %#v", exactTarget, detail.ImportReadiness.ExistingEventTargets)
	}
	if detail.ImportReadiness.NewListingScope {
		t.Fatalf("new listing scope = true, want blocked when exact existing target is available")
	}
	if !hasString(detail.ImportReadiness.BlockingReasons, "candidate resolves to existing live event") {
		t.Fatalf("new-listing blockers = %#v, want existing-event blocker", detail.ImportReadiness.BlockingReasons)
	}

	if err := st.AcceptEventReviewSupportingSource(context.Background(), seedstore.EventReviewAcceptSupportingSourceInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:         fixture.evidenceID,
		TargetEventID:      targetID,
		TargetBasis:        seedstore.EventReviewImportTargetBasisExactIdentity,
		SourceIdentityKeys: []string{"tampered-source-key"},
	}); err == nil || !strings.Contains(err.Error(), "not observed for evidence") {
		t.Fatalf("accept supporting with tampered source key error = %v, want not observed", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows after tampered key = %d, want %d", got, beforeResolutions)
	}

	if err := st.AcceptEventReviewSupportingSource(context.Background(), seedstore.EventReviewAcceptSupportingSourceInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:    fixture.evidenceID,
		TargetEventID: targetID,
		TargetBasis:   seedstore.EventReviewImportTargetBasisExactIdentity,
	}); err != nil {
		t.Fatalf("accept supporting source: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	var canonicalEventID int64
	if err := db.QueryRow(`
		SELECT canonical_event_id
		FROM event_review_clusters
		WHERE id = ?
	`, fixture.clusterID).Scan(&canonicalEventID); err != nil {
		t.Fatalf("load supporting cluster canonical event id: %v", err)
	}
	if canonicalEventID != targetID {
		t.Fatalf("canonical event id = %d, want %d", canonicalEventID, targetID)
	}
	if got := mustCount(t, db, "events"); got != beforeEvents+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents+1)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions+1 {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions+1)
	}

	var evidenceEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, fixture.evidenceID).Scan(&evidenceEventID); err != nil {
		t.Fatalf("load supporting evidence event id: %v", err)
	}
	if !evidenceEventID.Valid || evidenceEventID.Int64 != targetID {
		t.Fatalf("supporting evidence event id = %v, want %d", evidenceEventID, targetID)
	}

	var publicationState string
	if err := db.QueryRow(`
		SELECT publication_state
		FROM events
		WHERE id = ?
	`, targetID).Scan(&publicationState); err != nil {
		t.Fatalf("load target publication state: %v", err)
	}
	if publicationState != string(domain.PublicationStateReviewed) {
		t.Fatalf("target publication_state = %q, want %q", publicationState, domain.PublicationStateReviewed)
	}

	publishedSourceKey, ok := ingest.SourceIdentityKey(fixture.externalID)
	if !ok {
		t.Fatalf("source identity key for %q was rejected", fixture.externalID)
	}
	var sourceLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_links
		WHERE event_id = ? AND source_id = ? AND source_event_key = ?
	`, targetID, fixture.sourceID, publishedSourceKey).Scan(&sourceLinkCount); err != nil {
		t.Fatalf("load supporting source link count: %v", err)
	}
	if sourceLinkCount != 1 {
		t.Fatalf("supporting source link count = %d, want 1", sourceLinkCount)
	}

	detail, ok, err = st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved supporting source cluster: %v", err)
	}
	if !ok || detail.Resolution == nil || detail.Resolution.AppliedSupportingSource == nil {
		t.Fatal("resolved supporting source cluster missing applied supporting source")
	}
	if got := detail.Resolution.AppliedSupportingSource; got.EventID != targetID || got.EvidenceID != fixture.evidenceID || got.TargetBasis != seedstore.EventReviewImportTargetBasisExactIdentity || !got.PromotedReview {
		t.Fatalf("applied supporting source = %#v", got)
	}
	if detail.Resolution.AppliedImportListing != nil {
		t.Fatalf("applied import listing = %#v, want nil", detail.Resolution.AppliedImportListing)
	}
}

func TestAcceptEventReviewSupportingSourceResolvesSourceLinkThroughCanonicalTarget(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	canonicalID := mustInsertExactIdentityEvent(t, db, "source-link-canonical-target", "Canonical Source Target", fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
	rawLinkedID := mustInsertExactIdentityEvent(t, db, "source-link-withheld-raw", "Withheld Raw Source Target", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?,
			canonical_event_id = ?
		WHERE id = ?
	`, string(domain.PublicationStateWithheld), canonicalID, rawLinkedID); err != nil {
		t.Fatalf("withhold raw linked event: %v", err)
	}
	sourceKey, ok := ingest.SourceIdentityKey(fixture.externalID)
	if !ok {
		t.Fatalf("source identity key for %q was rejected", fixture.externalID)
	}
	sourceKeyID := insertEventReviewIdentityKeyOK(t, db, "canonical-source-link-key-hash", seedstore.EventReviewIdentityKeyKindSource, sourceKey)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, sourceKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert source identity evidence key: %v", err)
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
	`, rawLinkedID, fixture.sourceID, sourceKey, formatRFC3339UTC(fixture.start.Add(-24*time.Hour)), formatRFC3339UTC(fixture.start.Add(-24*time.Hour))); err != nil {
		t.Fatalf("insert raw source link: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load source canonical target cluster: %v", err)
	}
	if !ok || detail.ImportReadiness == nil {
		t.Fatal("source canonical target cluster missing readiness")
	}
	target := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, canonicalID, seedstore.EventReviewImportTargetBasisSourceIdentity)
	if target.RawLinkedEventID == nil || *target.RawLinkedEventID != rawLinkedID || target.RawLinkedEventSlug != "source-link-withheld-raw" || target.RawLinkedPublicationState != string(domain.PublicationStateWithheld) || !target.ResolvedFromWithheld {
		t.Fatalf("source canonical readiness target = %#v", target)
	}
	if len(target.BlockingReasons) != 0 {
		t.Fatalf("source canonical target blockers = %#v", target.BlockingReasons)
	}

	if err := st.AcceptEventReviewSupportingSource(context.Background(), seedstore.EventReviewAcceptSupportingSourceInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:         fixture.evidenceID,
		TargetEventID:      canonicalID,
		TargetBasis:        seedstore.EventReviewImportTargetBasisSourceIdentity,
		SourceIdentityKeys: []string{sourceKey},
	}); err != nil {
		t.Fatalf("accept source canonical supporting source: %v", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
}

func TestImportReviewReadinessResolvesNonLiveSourceLinkThroughCanonicalTarget(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	canonicalID := mustInsertExactIdentityEvent(t, db, "non-live-source-link-canonical-target", "Non-live Canonical Source Target", fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
	rawLinkedID := mustInsertExactIdentityEvent(t, db, "non-live-source-link-raw", "Non-live Raw Source Target", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginSeed)
	if _, err := db.Exec(`
		UPDATE events
		SET canonical_event_id = ?
		WHERE id = ?
	`, canonicalID, rawLinkedID); err != nil {
		t.Fatalf("set non-live raw canonical: %v", err)
	}
	sourceKey, ok := ingest.SourceIdentityKey("non-live-source-link")
	if !ok {
		t.Fatal("source identity key for non-live-source-link was rejected")
	}
	sourceKeyID := insertEventReviewIdentityKeyOK(t, db, "non-live-source-link-key-hash", seedstore.EventReviewIdentityKeyKindSource, sourceKey)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, sourceKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert non-live source identity evidence key: %v", err)
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
	`, rawLinkedID, fixture.sourceID, sourceKey, formatRFC3339UTC(fixture.start.Add(-24*time.Hour)), formatRFC3339UTC(fixture.start.Add(-24*time.Hour))); err != nil {
		t.Fatalf("insert non-live source link: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load non-live source canonical target cluster: %v", err)
	}
	if !ok || detail.ImportReadiness == nil {
		t.Fatal("non-live source canonical target cluster missing readiness")
	}
	target := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, canonicalID, seedstore.EventReviewImportTargetBasisSourceIdentity)
	if target.RawLinkedEventID == nil || *target.RawLinkedEventID != rawLinkedID || target.RawLinkedPublicationState != string(domain.PublicationStateReviewed) || !target.ResolvedFromWithheld {
		t.Fatalf("non-live source canonical readiness target = %#v", target)
	}
	if len(target.BlockingReasons) != 0 {
		t.Fatalf("non-live source canonical target blockers = %#v", target.BlockingReasons)
	}
}

func TestImportReviewReadinessBlocksUnresolvedRawSourceLinkTargets(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	targetID := mustInsertExactIdentityEvent(t, db, fixture.expectedSlug, fixture.title, fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
	rawLinkedID := mustInsertExactIdentityEvent(t, db, "unresolved-raw-source-link", "Unresolved Raw Source Link", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginSeed)
	sourceKey, ok := ingest.SourceIdentityKey(fixture.externalID)
	if !ok {
		t.Fatalf("source identity key for %q was rejected", fixture.externalID)
	}
	sourceKeyID := insertEventReviewIdentityKeyOK(t, db, "unresolved-raw-source-link-key-hash", seedstore.EventReviewIdentityKeyKindSource, sourceKey)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, sourceKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert unresolved source identity evidence key: %v", err)
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
	`, rawLinkedID, fixture.sourceID, sourceKey, formatRFC3339UTC(fixture.start.Add(-24*time.Hour)), formatRFC3339UTC(fixture.start.Add(-24*time.Hour))); err != nil {
		t.Fatalf("insert unresolved raw source link: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load unresolved source link cluster: %v", err)
	}
	if !ok || detail.ImportReadiness == nil {
		t.Fatal("unresolved source link cluster missing readiness")
	}
	slugTarget := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, targetID, seedstore.EventReviewImportTargetBasisSlug)
	if !hasString(slugTarget.BlockingReasons, eventReviewImportUnresolvedSourceLinkBlocker) {
		t.Fatalf("slug target blockers = %#v, want unresolved source-link blocker", slugTarget.BlockingReasons)
	}
	for _, target := range detail.ImportReadiness.ExistingEventTargets {
		if target.EvidenceID == fixture.evidenceID && !hasString(target.BlockingReasons, eventReviewImportUnresolvedSourceLinkBlocker) {
			t.Fatalf("target %#v missing unresolved source-link blocker", target)
		}
	}
}

func TestAcceptEventReviewSupportingSourceAppliesNearTitleTarget(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	incomingTitle := "Jane Doe + The Openers"
	targetTitle := "Jane Doe"
	if _, err := db.Exec(`
		UPDATE event_review_evidence
		SET payload = ?
		WHERE id = ?
	`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), incomingTitle, fixture.venueText, "", fixture.start, fixture.end, "near-title-same-event"), fixture.evidenceID); err != nil {
		t.Fatalf("update near-title payload: %v", err)
	}
	targetID := mustInsertExactIdentityEvent(t, db, "near-title-same-event-target", targetTitle, fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load near-title import review cluster: %v", err)
	}
	if !ok || detail.ImportReadiness == nil || len(detail.ImportReadiness.ExistingEventTargets) != 1 {
		t.Fatalf("near-title targets = %#v", detail.ImportReadiness)
	}
	if target := detail.ImportReadiness.ExistingEventTargets[0]; target.EventID != targetID || target.TargetBasis != seedstore.EventReviewImportTargetBasisNearTitle {
		t.Fatalf("near-title target = %#v", target)
	}

	if err := st.AcceptEventReviewSupportingSource(context.Background(), seedstore.EventReviewAcceptSupportingSourceInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:    fixture.evidenceID,
		TargetEventID: targetID,
		TargetBasis:   seedstore.EventReviewImportTargetBasisNearTitle,
	}); err != nil {
		t.Fatalf("accept near-title supporting source: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	var evidenceEventID sql.NullInt64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, fixture.evidenceID).Scan(&evidenceEventID); err != nil {
		t.Fatalf("load near-title evidence event id: %v", err)
	}
	if !evidenceEventID.Valid || evidenceEventID.Int64 != targetID {
		t.Fatalf("near-title evidence event id = %v, want %d", evidenceEventID, targetID)
	}
	detail, ok, err = st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved near-title supporting cluster: %v", err)
	}
	if !ok || detail.Resolution == nil || detail.Resolution.AppliedSupportingSource == nil {
		t.Fatal("resolved near-title supporting cluster missing applied supporting source")
	}
	if got := detail.Resolution.AppliedSupportingSource; got.EventID != targetID || got.TargetBasis != seedstore.EventReviewImportTargetBasisNearTitle {
		t.Fatalf("near-title applied supporting source = %#v", got)
	}
}

func TestImportReviewReadinessExposesSlugAndExactTitleTargets(t *testing.T) {
	t.Run("slug", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		st := mustStoreFromDB(t, db)
		fixture := seedImportReviewResolutionFixture(t, db)
		targetID := mustInsertExactIdentityEvent(t, db, fixture.expectedSlug, "Slug Owner", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
		if err != nil {
			t.Fatalf("load slug target cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("slug target cluster missing readiness")
		}
		target := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, targetID, seedstore.EventReviewImportTargetBasisSlug)
		if len(target.BlockingReasons) != 0 {
			t.Fatalf("slug target blockers = %#v", target.BlockingReasons)
		}
	})

	t.Run("exact title venue start", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		st := mustStoreFromDB(t, db)
		fixture := seedImportReviewResolutionFixture(t, db)
		targetID := mustInsertExactIdentityEvent(t, db, "manual-exact-title-target", fixture.title, fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
		detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
		if err != nil {
			t.Fatalf("load exact title target cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("exact title target cluster missing readiness")
		}
		target := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, targetID, seedstore.EventReviewImportTargetBasisExactTitleVenueStart)
		if len(target.BlockingReasons) != 0 {
			t.Fatalf("exact title target blockers = %#v", target.BlockingReasons)
		}
	})

	t.Run("hard target blocks on near-title disagreement", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		st := mustStoreFromDB(t, db)
		fixture := seedImportReviewResolutionFixture(t, db)
		incomingTitle := "Jane Doe + The Openers"
		if _, err := db.Exec(`
			UPDATE event_review_evidence
			SET payload = ?
			WHERE id = ?
		`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), incomingTitle, fixture.venueText, "", fixture.start, fixture.end, "near-title-hidden-hard-target"), fixture.evidenceID); err != nil {
			t.Fatalf("update near-title hard-target payload: %v", err)
		}
		slug, err := buildLiveEventSlug(incomingTitle, "leadmill", fixture.start)
		if err != nil {
			t.Fatalf("build hard target slug: %v", err)
		}
		hardTargetID := mustInsertExactIdentityEvent(t, db, slug, "Slug Owner", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
		nearTargetID := mustInsertExactIdentityEvent(t, db, "near-title-hidden-hard-target", "Jane Doe", fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)

		detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
		if err != nil {
			t.Fatalf("load near-title hard-target cluster: %v", err)
		}
		if !ok || detail.ImportReadiness == nil {
			t.Fatal("near-title hard-target readiness missing")
		}
		target := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, hardTargetID, seedstore.EventReviewImportTargetBasisSlug)
		if !hasString(target.BlockingReasons, "near-title target disagrees with hard target") {
			t.Fatalf("hard target blockers = %#v, want near-title disagreement", target.BlockingReasons)
		}
		for _, target := range detail.ImportReadiness.ExistingEventTargets {
			if target.EventID == nearTargetID && target.TargetBasis == seedstore.EventReviewImportTargetBasisNearTitle {
				t.Fatalf("near-title target should remain hidden while hard targets exist: %#v", target)
			}
		}
	})
}

func TestImportReviewReadinessBlocksDisagreeingHardTargets(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	exactTargetID := mustInsertExactIdentityEvent(t, db, "disagreeing-exact-target", fixture.title, fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
	sourceTargetID := mustInsertExactIdentityEvent(t, db, "disagreeing-source-target", "Disagreeing Source Target", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	exactKey, err := buildImportReviewExactIdentityKey(domain.Event{
		Slug:             fixture.expectedSlug,
		Name:             fixture.title,
		VenueSlug:        "leadmill",
		Start:            fixture.start,
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateReviewed,
	})
	if err != nil {
		t.Fatalf("build exact key: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin exact identity tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(context.Background(), tx, exactTargetID, domain.Event{
		Slug:             "disagreeing-exact-target",
		Name:             fixture.title,
		VenueSlug:        "leadmill",
		Start:            fixture.start,
		Origin:           domain.OriginLive,
		LastChecked:      fixture.start.Add(-24 * time.Hour),
		PublicationState: domain.PublicationStateReviewed,
	}, 0, fixture.start.Add(-24*time.Hour)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("ensure exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact identity tx: %v", err)
	}
	exactKeyID := insertEventReviewIdentityKeyOK(t, db, "disagreeing-exact-key-hash", seedstore.EventReviewIdentityKeyKindExact, exactKey)
	sourceKeyID := insertEventReviewIdentityKeyOK(t, db, "disagreeing-source-key-hash", seedstore.EventReviewIdentityKeyKindSource, fixture.externalID)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, exactKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert exact identity key: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, sourceKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert source identity key: %v", err)
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
	`, sourceTargetID, fixture.sourceID, fixture.externalID, formatRFC3339UTC(fixture.start.Add(-24*time.Hour)), formatRFC3339UTC(fixture.start.Add(-24*time.Hour))); err != nil {
		t.Fatalf("insert source link: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load disagreeing targets cluster: %v", err)
	}
	if !ok || detail.ImportReadiness == nil {
		t.Fatal("disagreeing targets cluster missing readiness")
	}
	exactTarget := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, exactTargetID, seedstore.EventReviewImportTargetBasisExactIdentity)
	sourceTarget := mustImportExistingTarget(t, detail.ImportReadiness.ExistingEventTargets, fixture.evidenceID, sourceTargetID, seedstore.EventReviewImportTargetBasisSourceIdentity)
	if !hasString(exactTarget.BlockingReasons, "hard target signals disagree") || !hasString(sourceTarget.BlockingReasons, "hard target signals disagree") {
		t.Fatalf("target blockers exact=%#v source=%#v", exactTarget.BlockingReasons, sourceTarget.BlockingReasons)
	}
}

func TestAcceptEventReviewSupportingSourceRejectsStagedHardTargetDisagreement(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	slugTargetID := mustInsertExactIdentityEvent(t, db, fixture.expectedSlug, "Slug Target", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	exactTargetID := mustInsertExactIdentityEvent(t, db, "resolver-disagreeing-exact-target", fixture.title, fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
	exactKey, err := buildImportReviewExactIdentityKey(domain.Event{
		Slug:             fixture.expectedSlug,
		Name:             fixture.title,
		VenueSlug:        "leadmill",
		Start:            fixture.start,
		Origin:           domain.OriginLive,
		PublicationState: domain.PublicationStateReviewed,
	})
	if err != nil {
		t.Fatalf("build exact key: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin exact identity tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(context.Background(), tx, exactTargetID, domain.Event{
		Slug:             "resolver-disagreeing-exact-target",
		Name:             fixture.title,
		VenueSlug:        "leadmill",
		Start:            fixture.start,
		Origin:           domain.OriginLive,
		LastChecked:      fixture.start.Add(-24 * time.Hour),
		PublicationState: domain.PublicationStateReviewed,
	}, 0, fixture.start.Add(-24*time.Hour)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("ensure exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact identity tx: %v", err)
	}
	exactKeyID := insertEventReviewIdentityKeyOK(t, db, "resolver-disagreeing-exact-key-hash", seedstore.EventReviewIdentityKeyKindExact, exactKey)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, exactKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert exact identity key: %v", err)
	}

	if err := st.AcceptEventReviewSupportingSource(context.Background(), seedstore.EventReviewAcceptSupportingSourceInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:    fixture.evidenceID,
		TargetEventID: slugTargetID,
		TargetBasis:   seedstore.EventReviewImportTargetBasisSlug,
	}); err == nil || !strings.Contains(err.Error(), "staged exact identity") {
		t.Fatalf("accept supporting with staged exact disagreement error = %v, want staged exact identity", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
}

func TestResolveEventReviewImportSeparateAndInsertCreatesNearTitleSeparations(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	incomingTitle := "Jane Doe + The Openers"
	targetTitle := "Jane Doe"
	if _, err := db.Exec(`
		UPDATE event_review_evidence
		SET payload = ?
		WHERE id = ?
	`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), incomingTitle, fixture.venueText, "", fixture.start, fixture.end, "near-title-separate-event"), fixture.evidenceID); err != nil {
		t.Fatalf("update near-title separate payload: %v", err)
	}
	targetID := mustInsertExactIdentityEvent(t, db, "near-title-separate-target", targetTitle, fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
	beforeEvents := mustCount(t, db, "events")
	beforeSeparations := mustCount(t, db, "event_review_separations")

	if err := st.ResolveEventReviewImportSeparateAndInsert(context.Background(), seedstore.EventReviewImportSeparateAndInsertInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:       fixture.evidenceID,
		NearTitleEventID: targetID,
	}); err != nil {
		t.Fatalf("resolve near-title separate import: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	if got := mustCount(t, db, "events"); got != beforeEvents+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents+1)
	}
	if got := mustCount(t, db, "event_review_separations"); got != beforeSeparations+2 {
		t.Fatalf("event_review_separations rows = %d, want %d", got, beforeSeparations+2)
	}

	var newEventID int64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, fixture.evidenceID).Scan(&newEventID); err != nil {
		t.Fatalf("load near-title separate evidence event id: %v", err)
	}
	if newEventID <= 0 || newEventID == targetID {
		t.Fatalf("near-title separate new event id = %d, target %d", newEventID, targetID)
	}
	var canonicalEventID int64
	if err := db.QueryRow(`SELECT canonical_event_id FROM event_review_clusters WHERE id = ?`, fixture.clusterID).Scan(&canonicalEventID); err != nil {
		t.Fatalf("load near-title separate canonical event id: %v", err)
	}
	if canonicalEventID != newEventID {
		t.Fatalf("canonical event id = %d, want %d", canonicalEventID, newEventID)
	}

	for _, tc := range []struct {
		name string
		a    string
		b    string
	}{
		{name: "event evidence", a: seedstore.EventReviewSeparationEventEndpointKey(targetID), b: eventReviewSeparationEndpointKeyEvidence("import-review-" + strconv.FormatInt(fixture.clusterID, 10))},
		{name: "event event", a: seedstore.EventReviewSeparationEventEndpointKey(targetID), b: seedstore.EventReviewSeparationEventEndpointKey(newEventID)},
	} {
		a, b := tc.a, tc.b
		if a > b {
			a, b = b, a
		}
		var count int
		if err := db.QueryRow(`
			SELECT COUNT(*)
			FROM event_review_separations
			WHERE active = 1 AND endpoint_a_key = ? AND endpoint_b_key = ?
		`, a, b).Scan(&count); err != nil {
			t.Fatalf("load %s separation count: %v", tc.name, err)
		}
		if count != 1 {
			t.Fatalf("%s separation count = %d, want 1", tc.name, count)
		}
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved near-title separate cluster: %v", err)
	}
	if !ok || detail.Resolution == nil || detail.Resolution.AppliedImportListing == nil {
		t.Fatal("resolved near-title separate cluster missing applied import listing")
	}
	if got := detail.Resolution.AppliedImportListing; got.EventID != newEventID || got.EvidenceID != fixture.evidenceID || got.Title != incomingTitle {
		t.Fatalf("near-title applied import listing = %#v", got)
	}
	if len(detail.Resolution.AppliedSeparations) != 2 {
		t.Fatalf("applied separations = %#v, want 2", detail.Resolution.AppliedSeparations)
	}
}

func TestImportReviewNearTitleReadinessSkipsSeparatedTarget(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	incomingTitle := "Jane Doe + The Openers"
	if _, err := db.Exec(`
		UPDATE event_review_evidence
		SET payload = ?
		WHERE id = ?
	`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), incomingTitle, fixture.venueText, "", fixture.start, fixture.end, "near-title-separated-readiness"), fixture.evidenceID); err != nil {
		t.Fatalf("update separated near-title payload: %v", err)
	}
	targetID := mustInsertExactIdentityEvent(t, db, "near-title-separated-target", "Jane Doe", fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
	if _, err := insertEventReviewSeparation(t, db,
		seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(targetID),
			EventID: int64Ptr(targetID),
		},
		seedstore.EventReviewSeparationEndpoint{
			Kind:       seedstore.EventReviewSeparationEndpointKindEvidence,
			Key:        eventReviewSeparationEndpointKeyEvidence(fmt.Sprintf("import-review-%d", fixture.clusterID)),
			EvidenceID: int64Ptr(fixture.evidenceID),
		},
		true,
		"near-title false positive",
		fixture.start.Add(-time.Hour),
		fixture.start.Add(-time.Hour),
	); err != nil {
		t.Fatalf("insert near-title separation: %v", err)
	}

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load separated near-title cluster: %v", err)
	}
	if !ok || detail.ImportReadiness == nil {
		t.Fatal("separated near-title cluster missing readiness")
	}
	for _, target := range detail.ImportReadiness.ExistingEventTargets {
		if target.TargetBasis == seedstore.EventReviewImportTargetBasisNearTitle && target.EventID == targetID {
			t.Fatalf("near-title target should be suppressed by separation: %#v", target)
		}
	}
}

func TestTerminalReplayDoesNotMatchSeparatedOldNearTitleTarget(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	venueID := lookupStoreVenueID(t, db, "leadmill")
	sourceID := insertStoreTestSource(t, db)
	oldTargetID := mustInsertExactIdentityEvent(t, db, "terminal-replay-old-near-target", "Old Near Target", venueID, sourceID, time.Date(2026, time.May, 22, 19, 0, 0, 0, time.UTC), time.Date(2026, time.May, 22, 21, 0, 0, 0, time.UTC), time.Date(2026, time.May, 21, 19, 0, 0, 0, time.UTC), domain.OriginLive)
	newEventID := mustInsertExactIdentityEvent(t, db, "terminal-replay-new-import", "New Import", venueID, sourceID, time.Date(2026, time.May, 22, 20, 0, 0, 0, time.UTC), time.Date(2026, time.May, 22, 22, 0, 0, 0, time.UTC), time.Date(2026, time.May, 21, 20, 0, 0, 0, time.UTC), domain.OriginLive)
	cluster := seedstore.EventReviewCluster{ID: 999, Status: seedstore.EventReviewClusterStatusResolved}
	resolution := &seedstore.EventReviewResolutionSummary{
		AppliedImportListing: &seedstore.EventReviewResolutionAppliedImportListingSummary{EventID: newEventID},
		AppliedSeparations: []seedstore.EventReviewResolutionAppliedSeparationSummary{{
			EndpointAKey: seedstore.EventReviewSeparationEventEndpointKey(oldTargetID),
			EndpointBKey: seedstore.EventReviewSeparationEventEndpointKey(newEventID),
		}},
	}
	if matched, err := terminalEvidenceOutcomeMatchesInputTx(context.Background(), db, cluster, resolution, seedstore.StageEventReviewEvidenceInput{EventID: int64Ptr(oldTargetID)}); err != nil {
		t.Fatalf("terminal replay old target: %v", err)
	} else if matched {
		t.Fatal("terminal replay matched old separated near-title target, want false")
	}
	if matched, err := terminalEvidenceOutcomeMatchesInputTx(context.Background(), db, cluster, resolution, seedstore.StageEventReviewEvidenceInput{EventID: int64Ptr(newEventID)}); err != nil {
		t.Fatalf("terminal replay new target: %v", err)
	} else if !matched {
		t.Fatal("terminal replay did not match resolved imported event")
	}
}

func TestTerminalReplayMatchesTitleSlugConflictEventIDs(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	oldCanonicalID := int64(701)
	slugConflictID := int64(702)
	survivingID := slugConflictID
	cluster := seedstore.EventReviewCluster{ID: 1001, Status: seedstore.EventReviewClusterStatusResolved}
	resolution := &seedstore.EventReviewResolutionSummary{
		AppliedTitleSlugConflict: &seedstore.EventReviewResolutionAppliedTitleSlugConflictSummary{
			Mode:                seedstore.EventReviewTitleRepairSlugConflictModeMergeDuplicate,
			OldCanonicalEventID: oldCanonicalID,
			SlugConflictEventID: slugConflictID,
			SurvivingEventID:    survivingID,
		},
	}
	for _, eventID := range []int64{oldCanonicalID, slugConflictID, survivingID} {
		if matched, err := terminalEvidenceOutcomeMatchesInputTx(context.Background(), db, cluster, resolution, seedstore.StageEventReviewEvidenceInput{EventID: int64Ptr(eventID)}); err != nil {
			t.Fatalf("terminal replay title slug conflict event %d: %v", eventID, err)
		} else if !matched {
			t.Fatalf("terminal replay did not match title slug conflict event %d", eventID)
		}
	}
	if matched, err := terminalEvidenceOutcomeMatchesInputTx(context.Background(), db, cluster, resolution, seedstore.StageEventReviewEvidenceInput{EventID: int64Ptr(799)}); err != nil {
		t.Fatalf("terminal replay unrelated title slug conflict event: %v", err)
	} else if matched {
		t.Fatal("terminal replay matched unrelated title slug conflict event")
	}
}

func TestTerminalReplayMatchesHistoricalDuplicateKeepSeparateEventIDs(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	firstID := int64(801)
	secondID := int64(802)
	cluster := seedstore.EventReviewCluster{ID: 1002, Status: seedstore.EventReviewClusterStatusResolved}
	resolution := &seedstore.EventReviewResolutionSummary{
		AppliedHistoricalKeepSeparate: &seedstore.EventReviewResolutionAppliedHistoricalKeepSeparateSummary{
			KeptEvents: []seedstore.EventReviewResolutionKeptHistoricalDuplicateEventSummary{
				{EventID: firstID, EventSlug: "historical-duplicate-first"},
				{EventID: secondID, EventSlug: "historical-duplicate-second"},
			},
		},
	}
	for _, eventID := range []int64{firstID, secondID} {
		if matched, err := terminalEvidenceOutcomeMatchesInputTx(context.Background(), db, cluster, resolution, seedstore.StageEventReviewEvidenceInput{EventID: int64Ptr(eventID)}); err != nil {
			t.Fatalf("terminal replay historical duplicate keep-separate event %d: %v", eventID, err)
		} else if !matched {
			t.Fatalf("terminal replay did not match historical duplicate keep-separate event %d", eventID)
		}
	}
	if matched, err := terminalEvidenceOutcomeMatchesInputTx(context.Background(), db, cluster, resolution, seedstore.StageEventReviewEvidenceInput{EventID: int64Ptr(899)}); err != nil {
		t.Fatalf("terminal replay unrelated historical duplicate keep-separate event: %v", err)
	} else if matched {
		t.Fatal("terminal replay matched unrelated historical duplicate keep-separate event")
	}
}

func TestResolveEventReviewImportAuthoritativeInsertsNewEvent(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	if _, err := db.Exec(`
		UPDATE event_review_evidence
		SET payload = ?
		WHERE id = ?
	`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthorityAuthoritative), "Authoritative Insert", fixture.venueText, "", fixture.start, fixture.end, "authoritative-insert"), fixture.evidenceID); err != nil {
		t.Fatalf("update authoritative payload: %v", err)
	}
	beforeEvents := mustCount(t, db, "events")

	if err := st.ResolveEventReviewImportAuthoritative(context.Background(), seedstore.EventReviewImportAuthoritativeInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID: fixture.evidenceID,
	}); err != nil {
		t.Fatalf("resolve authoritative insert: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)
	if got := mustCount(t, db, "events"); got != beforeEvents+1 {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents+1)
	}
	var newEventID int64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, fixture.evidenceID).Scan(&newEventID); err != nil {
		t.Fatalf("load authoritative evidence event id: %v", err)
	}
	if newEventID <= 0 {
		t.Fatalf("authoritative evidence event id = %d, want positive", newEventID)
	}
	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved authoritative insert cluster: %v", err)
	}
	if !ok || detail.Resolution == nil || detail.Resolution.AppliedAuthoritativeImport == nil {
		t.Fatal("resolved authoritative cluster missing applied authoritative import")
	}
	if got := detail.Resolution.AppliedAuthoritativeImport; got.EventID != newEventID || got.EvidenceID != fixture.evidenceID || got.Result != "inserted" {
		t.Fatalf("applied authoritative import = %#v", got)
	}
}

func TestResolveEventReviewImportAuthoritativeUsesSourceIdentityBeforeTitleMatching(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	incomingTitle := "Authoritative Source Wins"
	if _, err := db.Exec(`
		UPDATE event_review_evidence
		SET payload = ?
		WHERE id = ?
	`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthorityAuthoritative), incomingTitle, fixture.venueText, "", fixture.start, fixture.end, fixture.externalID), fixture.evidenceID); err != nil {
		t.Fatalf("update authoritative source payload: %v", err)
	}
	targetID := mustInsertExactIdentityEvent(t, db, "authoritative-source-linked-target", "Legacy Source Target", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	sourceKey, ok := ingest.SourceIdentityKey(fixture.externalID)
	if !ok {
		t.Fatalf("source identity key for %q was rejected", fixture.externalID)
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
	`, targetID, fixture.sourceID, sourceKey, formatRFC3339UTC(fixture.start.Add(-24*time.Hour)), formatRFC3339UTC(fixture.start.Add(-24*time.Hour))); err != nil {
		t.Fatalf("insert authoritative source link: %v", err)
	}
	beforeEvents := mustCount(t, db, "events")

	if err := st.ResolveEventReviewImportAuthoritative(context.Background(), seedstore.EventReviewImportAuthoritativeInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:            fixture.evidenceID,
		ExpectedTargetEventID: targetID,
		SourceIdentityKeys:    []string{sourceKey},
	}); err != nil {
		t.Fatalf("resolve authoritative source update: %v", err)
	}

	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents)
	}
	var title string
	var startText string
	if err := db.QueryRow(`SELECT name, start_at FROM events WHERE id = ?`, targetID).Scan(&title, &startText); err != nil {
		t.Fatalf("load authoritative target event: %v", err)
	}
	if title != incomingTitle || startText != formatRFC3339UTC(fixture.start) {
		t.Fatalf("target event title/start = %q %q, want %q %q", title, startText, incomingTitle, formatRFC3339UTC(fixture.start))
	}
	var evidenceEventID int64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, fixture.evidenceID).Scan(&evidenceEventID); err != nil {
		t.Fatalf("load authoritative update evidence event id: %v", err)
	}
	if evidenceEventID != targetID {
		t.Fatalf("authoritative evidence event id = %d, want %d", evidenceEventID, targetID)
	}
	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
	if err != nil {
		t.Fatalf("load resolved authoritative update cluster: %v", err)
	}
	if !ok || detail.Resolution == nil || detail.Resolution.AppliedAuthoritativeImport == nil {
		t.Fatal("resolved authoritative update cluster missing applied authoritative import")
	}
	if got := detail.Resolution.AppliedAuthoritativeImport; got.EventID != targetID || got.Result != "updated" {
		t.Fatalf("applied authoritative update = %#v", got)
	}
}

func TestResolveEventReviewImportAuthoritativeRejectsAmbiguousSourceIdentity(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	if _, err := db.Exec(`
		UPDATE event_review_evidence
		SET payload = ?
		WHERE id = ?
	`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthorityAuthoritative), "Authoritative Ambiguous", fixture.venueText, "", fixture.start, fixture.end, "authoritative-ambiguous"), fixture.evidenceID); err != nil {
		t.Fatalf("update authoritative ambiguous payload: %v", err)
	}
	targetAID := mustInsertExactIdentityEvent(t, db, "authoritative-ambiguous-a", "Authoritative Ambiguous A", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	targetBID := mustInsertExactIdentityEvent(t, db, "authoritative-ambiguous-b", "Authoritative Ambiguous B", fixture.venueID, fixture.sourceID, fixture.start.Add(4*time.Hour), fixture.end.Add(4*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	sourceKeyA := "uid:authoritative-ambiguous-a"
	sourceKeyB := "uid:authoritative-ambiguous-b"
	for _, row := range []struct {
		eventID int64
		key     string
	}{
		{eventID: targetAID, key: sourceKeyA},
		{eventID: targetBID, key: sourceKeyB},
	} {
		if _, err := db.Exec(`
			INSERT INTO event_source_links (
				event_id,
				source_id,
				source_event_key,
				is_authoritative,
				created_at,
				updated_at
			) VALUES (?, ?, ?, 1, ?, ?)
		`, row.eventID, fixture.sourceID, row.key, formatRFC3339UTC(fixture.start.Add(-24*time.Hour)), formatRFC3339UTC(fixture.start.Add(-24*time.Hour))); err != nil {
			t.Fatalf("insert ambiguous authoritative source link: %v", err)
		}
	}
	beforeEvents := mustCount(t, db, "events")

	err := st.ResolveEventReviewImportAuthoritative(context.Background(), seedstore.EventReviewImportAuthoritativeInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:         fixture.evidenceID,
		SourceIdentityKeys: []string{sourceKeyA, sourceKeyB},
	})
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("resolve ambiguous authoritative source error = %v, want ambiguous", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents)
	}
	var evidenceEventID sql.NullInt64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, fixture.evidenceID).Scan(&evidenceEventID); err != nil {
		t.Fatalf("load ambiguous authoritative evidence event id: %v", err)
	}
	if evidenceEventID.Valid {
		t.Fatalf("ambiguous authoritative evidence event id = %d, want NULL", evidenceEventID.Int64)
	}
}

func TestResolveEventReviewImportAuthoritativeRejectsExpectedTargetMismatchWithoutCommit(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	incomingTitle := "Authoritative Mismatch Incoming"
	if _, err := db.Exec(`
		UPDATE event_review_evidence
		SET payload = ?
		WHERE id = ?
	`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthorityAuthoritative), incomingTitle, fixture.venueText, "", fixture.start, fixture.end, "authoritative-expected-mismatch"), fixture.evidenceID); err != nil {
		t.Fatalf("update authoritative mismatch payload: %v", err)
	}
	sourceLinkedTargetID := mustInsertExactIdentityEvent(t, db, "authoritative-mismatch-linked-target", "Authoritative Mismatch Linked", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	expectedTargetID := mustInsertExactIdentityEvent(t, db, "authoritative-mismatch-expected-target", "Authoritative Mismatch Expected", fixture.venueID, fixture.sourceID, fixture.start.Add(4*time.Hour), fixture.end.Add(4*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
	sourceKey, ok := ingest.SourceIdentityKey("authoritative-expected-mismatch")
	if !ok {
		t.Fatal("source identity key for authoritative-expected-mismatch was rejected")
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
	`, sourceLinkedTargetID, fixture.sourceID, sourceKey, formatRFC3339UTC(fixture.start.Add(-24*time.Hour)), formatRFC3339UTC(fixture.start.Add(-24*time.Hour))); err != nil {
		t.Fatalf("insert mismatch authoritative source link: %v", err)
	}
	beforeEvents := mustCount(t, db, "events")

	err := st.ResolveEventReviewImportAuthoritative(context.Background(), seedstore.EventReviewImportAuthoritativeInput{
		EventReviewResolutionInput: seedstore.EventReviewResolutionInput{
			ClusterID:       fixture.clusterID,
			ExpectedVersion: 1,
		},
		EvidenceID:            fixture.evidenceID,
		ExpectedTargetEventID: expectedTargetID,
		SourceIdentityKeys:    []string{sourceKey},
	})
	if err == nil || !strings.Contains(err.Error(), "does not match expected event") {
		t.Fatalf("resolve authoritative target mismatch error = %v, want expected mismatch", err)
	}
	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents)
	}
	var linkedTitle string
	if err := db.QueryRow(`SELECT name FROM events WHERE id = ?`, sourceLinkedTargetID).Scan(&linkedTitle); err != nil {
		t.Fatalf("load linked target title after mismatch: %v", err)
	}
	if linkedTitle != "Authoritative Mismatch Linked" {
		t.Fatalf("linked target title after mismatch = %q, want rollback", linkedTitle)
	}
	var evidenceEventID sql.NullInt64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, fixture.evidenceID).Scan(&evidenceEventID); err != nil {
		t.Fatalf("load mismatch authoritative evidence event id: %v", err)
	}
	if evidenceEventID.Valid {
		t.Fatalf("mismatch authoritative evidence event id = %d, want NULL", evidenceEventID.Int64)
	}
}

func TestResolveEventReviewClusterRejectsSelectedImportReviewWhenEvidenceUpdateIsPreempted(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	st := mustStoreFromDB(t, db)
	fixture := seedImportReviewResolutionFixture(t, db)
	beforeEvents := mustCount(t, db, "events")
	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeReviewCandidates := mustCount(t, db, "review_candidates")

	selectedKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-import-review-preempted-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-import-review-preempted-key")
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, selectedKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("insert selected evidence identity: %v", err)
	}
	if _, err := insertEventReviewSourceIdentityChoice(t, db, fixture.clusterID, fixture.sourceID, "selected-import-review-preempted-key", true, "preferred selected candidate", time.Date(2026, time.May, 15, 9, 40, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert selected source identity choice: %v", err)
	}

	extraPayload := importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), "Preempted Import Listing", fixture.venueText, "", fixture.start.Add(1*time.Hour), fixture.end.Add(1*time.Hour), "import-review-preempted-extra")
	extraEvidenceID := insertEventReviewEvidenceOK(t, db, fixture.sourceID, nil, "import-review-preempted-extra-"+strconv.FormatInt(fixture.clusterID, 10), extraPayload)
	insertEventReviewClusterEvidenceOK(t, db, fixture.clusterID, extraEvidenceID, true, fixture.start.Add(10*time.Minute), nil, "preempted extra evidence")

	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TRIGGER test_event_review_evidence_preempted_update
		AFTER INSERT ON events
		BEGIN
			UPDATE event_review_evidence
			SET event_id = NEW.id,
				updated_at = '2026-05-15T10:00:00Z'
			WHERE id = %d;
		END;
	`, fixture.evidenceID)); err != nil {
		t.Fatalf("create preempted evidence trigger: %v", err)
	}
	defer func() {
		if _, err := db.Exec(`DROP TRIGGER IF EXISTS test_event_review_evidence_preempted_update`); err != nil {
			t.Fatalf("drop preempted evidence trigger: %v", err)
		}
	}()

	err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
		ClusterID:       fixture.clusterID,
		ExpectedVersion: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "selected evidence update was rejected") {
		t.Fatalf("resolve preempted selected multi-evidence cluster error = %v, want selected evidence update rejection", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "events"); got != beforeEvents {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
		t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
	}

	var selectedEvidenceEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, fixture.evidenceID).Scan(&selectedEvidenceEventID); err != nil {
		t.Fatalf("load preempted selected evidence event id: %v", err)
	}
	if selectedEvidenceEventID.Valid {
		t.Fatalf("selected evidence event id = %v, want NULL", selectedEvidenceEventID)
	}
}

func TestResolveEventReviewClusterRejectsUnsupportedImportReviewStates(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture)
		wantErr string
	}{
		{
			name: "invalid candidate time range",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				if _, err := db.Exec(`
					UPDATE event_review_evidence
					SET payload = ?
					WHERE id = (SELECT evidence_id FROM event_review_cluster_evidence WHERE cluster_id = ? AND active = 1 LIMIT 1)
				`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), fixture.title, fixture.venueText, "", fixture.start, fixture.start, fixture.externalID), fixture.clusterID); err != nil {
					t.Fatalf("update invalid candidate time payload: %v", err)
				}
			},
			wantErr: "end time must be later than start time",
		},
		{
			name: "stale version",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				_ = fixture
			},
			wantErr: "version",
		},
		{
			name: "multi evidence",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				evidenceID := insertEventReviewEvidenceOK(t, db, fixture.sourceID, nil, "import-review-extra", importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, "supporting", "Extra Listing", fixture.venueText, "", fixture.start, fixture.end, "external-extra"))
				insertEventReviewClusterEvidenceOK(t, db, fixture.clusterID, evidenceID, true, fixture.start.Add(10*time.Minute), nil, "extra evidence")
			},
			wantErr: "no selected source identity choices",
		},
		{
			name: "selected choices span multiple candidates",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				evidenceAID := fixture.evidenceID
				evidenceBID := insertEventReviewEvidenceOK(t, db, fixture.sourceID, nil, "import-review-selected-b", importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), "Selected Choice B", fixture.venueText, "", fixture.start.Add(30*time.Minute), fixture.end.Add(30*time.Minute), "external-selected-b"))
				insertEventReviewClusterEvidenceOK(t, db, fixture.clusterID, evidenceBID, true, fixture.start.Add(10*time.Minute), nil, "selected evidence b")

				selectedAKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-choice-a-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-choice-a")
				selectedBKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-choice-b-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-choice-b")
				if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceAID, selectedAKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
					t.Fatalf("insert selected choice a identity: %v", err)
				}
				if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceBID, selectedBKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
					t.Fatalf("insert selected choice b identity: %v", err)
				}
				if _, err := insertEventReviewSourceIdentityChoice(t, db, fixture.clusterID, fixture.sourceID, "selected-choice-a", true, "selected a", time.Date(2026, time.May, 15, 9, 40, 0, 0, time.UTC)); err != nil {
					t.Fatalf("insert selected choice a: %v", err)
				}
				if _, err := insertEventReviewSourceIdentityChoice(t, db, fixture.clusterID, fixture.sourceID, "selected-choice-b", true, "selected b", time.Date(2026, time.May, 15, 9, 41, 0, 0, time.UTC)); err != nil {
					t.Fatalf("insert selected choice b: %v", err)
				}
			},
			wantErr: "selected source identity choices span multiple candidates",
		},
		{
			name: "selected identity linked conflict",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				extraEvidenceID := insertEventReviewEvidenceOK(t, db, fixture.sourceID, nil, "import-review-linked-extra", importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), "Linked Conflict Extra", fixture.venueText, "", fixture.start.Add(45*time.Minute), fixture.end.Add(45*time.Minute), "external-linked-extra"))
				insertEventReviewClusterEvidenceOK(t, db, fixture.clusterID, extraEvidenceID, true, fixture.start.Add(10*time.Minute), nil, "linked conflict extra evidence")

				selectedExactTitle := normalizeExactIdentityCleanTitle(fixture.title)
				selectedExactKey := buildExactIdentityKey(exactIdentityKeyVersion, "leadmill", fixture.start, selectedExactTitle)
				selectedExactKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-linked-exact-hash", seedstore.EventReviewIdentityKeyKindExact, selectedExactKey)
				selectedSourceKeyID := insertEventReviewIdentityKeyOK(t, db, "selected-linked-source-hash", seedstore.EventReviewIdentityKeyKindSource, "selected-linked-source")
				if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, selectedExactKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
					t.Fatalf("insert selected exact identity: %v", err)
				}
				if _, err := insertEventReviewEvidenceIdentityKey(t, db, fixture.evidenceID, selectedSourceKeyID, &fixture.sourceID, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
					t.Fatalf("insert selected source identity: %v", err)
				}

				linkedEventID := mustInsertExactIdentityEvent(t, db, "selected-linked-conflict", fixture.title, fixture.venueID, fixture.sourceID, fixture.start, fixture.end, time.Date(2026, time.May, 15, 9, 25, 0, 0, time.UTC), domain.OriginLive)
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatalf("begin exact identity tx: %v", err)
				}
				if err := ensureActiveExactIdentityTx(context.Background(), tx, linkedEventID, domain.Event{
					Slug:             "selected-linked-conflict",
					Name:             fixture.title,
					VenueSlug:        "leadmill",
					Start:            fixture.start,
					Origin:           domain.OriginLive,
					LastChecked:      time.Date(2026, time.May, 15, 9, 25, 0, 0, time.UTC),
					PublicationState: domain.PublicationStateReviewed,
				}, 0, time.Date(2026, time.May, 15, 9, 25, 0, 0, time.UTC)); err != nil {
					_ = tx.Rollback()
					t.Fatalf("ensure exact identity: %v", err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit exact identity tx: %v", err)
				}
				if _, err := db.Exec(`
					INSERT INTO event_source_links (
						event_id,
						source_id,
						source_event_key,
						is_authoritative,
						created_at,
						updated_at
					) VALUES (?, ?, ?, 0, ?, ?)
				`, linkedEventID, fixture.sourceID, "selected-linked-source", "2026-05-15T09:25:00Z", "2026-05-15T09:25:00Z"); err != nil {
					t.Fatalf("insert selected source link conflict: %v", err)
				}

				if _, err := insertEventReviewSourceIdentityChoice(t, db, fixture.clusterID, fixture.sourceID, "selected-linked-source", true, "selected linked source", time.Date(2026, time.May, 15, 9, 42, 0, 0, time.UTC)); err != nil {
					t.Fatalf("insert selected linked choice: %v", err)
				}
			},
			wantErr: "selected candidate exact identity already links to live event",
		},
		{
			name: "malformed payload",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				if _, err := db.Exec(`UPDATE event_review_evidence SET payload = ? WHERE id = (SELECT evidence_id FROM event_review_cluster_evidence WHERE cluster_id = ? AND active = 1 LIMIT 1)`, "{bad payload", fixture.clusterID); err != nil {
					t.Fatalf("update malformed payload: %v", err)
				}
			},
			wantErr: "payload could not be parsed",
		},
		{
			name: "missing venue",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				if _, err := db.Exec(`
					UPDATE event_review_evidence
					SET payload = ?
					WHERE id = (SELECT evidence_id FROM event_review_cluster_evidence WHERE cluster_id = ? AND active = 1 LIMIT 1)
				`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), "Missing Venue", "", "", fixture.start, fixture.end, fixture.externalID), fixture.clusterID); err != nil {
					t.Fatalf("update missing venue payload: %v", err)
				}
			},
			wantErr: "candidate venue is required",
		},
		{
			name: "authoritative candidate",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				if _, err := db.Exec(`
					UPDATE event_review_evidence
					SET payload = ?
					WHERE id = (SELECT evidence_id FROM event_review_cluster_evidence WHERE cluster_id = ? AND active = 1 LIMIT 1)
				`, importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthorityAuthoritative), fixture.title, fixture.venueText, "", fixture.start, fixture.end, fixture.externalID), fixture.clusterID); err != nil {
					t.Fatalf("update authoritative payload: %v", err)
				}
			},
			wantErr: "requires a supporting candidate",
		},
		{
			name: "slug conflict",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				mustInsertExactIdentityEvent(t, db, fixture.expectedSlug, "Conflicting listing", fixture.venueID, fixture.sourceID, fixture.start, fixture.end, fixture.start.Add(-24*time.Hour), domain.OriginLive)
			},
			wantErr: "event slug",
		},
		{
			name: "source identity conflict",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				conflictID := mustInsertExactIdentityEvent(t, db, "import-review-source-conflict", "Source Conflict", fixture.venueID, fixture.sourceID, fixture.start.Add(2*time.Hour), fixture.end.Add(2*time.Hour), fixture.start.Add(-24*time.Hour), domain.OriginLive)
				sourceCtx := reviewSourceIdentityContextForCandidateInput(reviewSourceIdentitySupporting, fixture.sourceName, fixture.sourceURL, "", "", "", review.CandidateInput{
					ExternalID:  fixture.externalID,
					Name:        "Source Conflict",
					VenueSlug:   "leadmill",
					StartAt:     formatRFC3339UTC(fixture.start.Add(2 * time.Hour)),
					SourceName:  fixture.sourceName,
					SourceURL:   fixture.sourceURL,
					CalendarURL: fixture.calendarURL,
				}, "test_source_identity_conflict")
				nowText := formatRFC3339UTC(time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC))
				for _, key := range sourceCtx.Identities.LookupKeys() {
					if _, err := db.Exec(`
						INSERT INTO event_source_links (
							event_id,
							source_id,
							source_event_key,
							is_authoritative,
							created_at,
							updated_at
						) VALUES (?, ?, ?, 0, ?, ?)
					`, conflictID, fixture.sourceID, key, nowText, nowText); err != nil {
						t.Fatalf("insert source identity link: %v", err)
					}
				}
			},
			wantErr: "source identities",
		},
		{
			name: "near match conflict",
			mutate: func(t *testing.T, db *sql.DB, fixture importReviewResolutionFixture) {
				mustInsertExactIdentityEvent(t, db, "import-review-near-match", fixture.title, fixture.venueID, fixture.sourceID, fixture.start.Add(30*time.Minute), fixture.end.Add(30*time.Minute), fixture.start.Add(-24*time.Hour), domain.OriginLive)
			},
			wantErr: "too closely",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, db := openEventReviewSchemaStore(t)
			defer db.Close()

			st := mustStoreFromDB(t, db)
			fixture := seedImportReviewResolutionFixture(t, db)
			beforeResolutions := mustCount(t, db, "event_review_resolutions")
			beforeRepairRuns := mustCount(t, db, "repair_runs")
			beforeReviewGroups := mustCount(t, db, "review_groups")
			beforeReviewCandidates := mustCount(t, db, "review_candidates")
			tc.mutate(t, db, fixture)

			expectedVersion := 1
			if tc.name == "stale version" {
				expectedVersion = 2
			}
			err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
				ClusterID:       fixture.clusterID,
				ExpectedVersion: expectedVersion,
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("resolve error = %v, want %q", err, tc.wantErr)
			}

			assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
			if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
				t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
			}
			if got := mustCount(t, db, "repair_runs"); got != beforeRepairRuns {
				t.Fatalf("repair_runs rows = %d, want %d", got, beforeRepairRuns)
			}
			if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
				t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
			}
			if got := mustCount(t, db, "review_candidates"); got != beforeReviewCandidates {
				t.Fatalf("review_candidates rows = %d, want %d", got, beforeReviewCandidates)
			}
		})
	}
}

func seedImportReviewResolutionFixture(t *testing.T, db *sql.DB) importReviewResolutionFixture {
	t.Helper()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	start := time.Date(2026, time.May, 22, 19, 0, 0, 0, time.UTC)
	end := time.Date(2026, time.May, 22, 21, 0, 0, 0, time.UTC)
	sourceName := "Store test source"
	sourceURL := "https://source.example.test/listing"
	calendarURL := "https://source.example.test/calendar.ics"
	title := "Accept New Listing"
	venueText := "Leadmill"
	externalID := "import-review-external"
	expectedSlug, err := buildLiveEventSlug(title, "leadmill", start)
	if err != nil {
		t.Fatalf("build expected slug: %v", err)
	}
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET conflict_type = ?, conflict_reason = ?
		WHERE id = ?
	`, seedstore.EventReviewConflictTypeImportReview, seedstore.EventReviewConflictReasonIngestCandidate, clusterID); err != nil {
		t.Fatalf("seed import review conflict metadata: %v", err)
	}
	payload := importReviewResolutionPayload(t, sourceName, sourceURL, calendarURL, string(seedstore.SourceAuthoritySupporting), title, venueText, "", start, end, externalID)
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "import-review-"+strconv.FormatInt(clusterID, 10), payload)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, evidenceID, true, start.Add(5*time.Minute), nil, "import review evidence")

	return importReviewResolutionFixture{
		clusterID:    clusterID,
		evidenceID:   evidenceID,
		sourceID:     sourceID,
		venueID:      venueID,
		title:        title,
		venueText:    venueText,
		start:        start,
		end:          end,
		externalID:   externalID,
		sourceURL:    sourceURL,
		calendarURL:  calendarURL,
		sourceName:   sourceName,
		expectedSlug: expectedSlug,
	}
}

func importReviewResolutionPayload(t *testing.T, sourceName, sourceURL, calendarURL, sourceAuthority, title, venueText, venueSlug string, start, end time.Time, externalID string) string {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"source_authority":      sourceAuthority,
		"source_name":           sourceName,
		"source_url":            sourceURL,
		"calendar_url":          calendarURL,
		"candidate_external_id": externalID,
		"candidate_title":       title,
		"candidate_venue_slug":  venueSlug,
		"candidate_venue_text":  venueText,
		"candidate_start_at":    formatRFC3339UTC(start),
		"candidate_end_at":      formatRFC3339UTC(end),
	})
	if err != nil {
		t.Fatalf("marshal import review payload: %v", err)
	}
	return string(payload)
}

func mustImportExistingTarget(t *testing.T, targets []seedstore.EventReviewImportExistingEventTarget, evidenceID, eventID int64, basis seedstore.EventReviewImportTargetBasis) seedstore.EventReviewImportExistingEventTarget {
	t.Helper()
	for _, target := range targets {
		if target.EvidenceID == evidenceID && target.EventID == eventID && target.TargetBasis == basis {
			return target
		}
	}
	t.Fatalf("target evidence=%d event=%d basis=%s not found in %#v", evidenceID, eventID, basis, targets)
	return seedstore.EventReviewImportExistingEventTarget{}
}

func seedTitleRepairResolutionFixture(t *testing.T, db *sql.DB) titleRepairResolutionFixture {
	t.Helper()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	start := time.Date(2026, time.May, 18, 19, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	lastChecked := time.Date(2026, time.May, 18, 9, 0, 0, 0, time.UTC)
	oldSlug := "title-repair-current"
	oldTitle := "Legacy Event"
	eventID := mustInsertExactIdentityEvent(t, db, oldSlug, oldTitle, venueID, sourceID, start, end, lastChecked, domain.OriginLive)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin exact identity seed tx: %v", err)
	}
	if err := ensureActiveExactIdentityTx(context.Background(), tx, eventID, domain.Event{
		Slug:             oldSlug,
		Name:             oldTitle,
		VenueSlug:        "leadmill",
		Start:            start,
		Origin:           domain.OriginLive,
		LastChecked:      lastChecked,
		PublicationState: domain.PublicationStateReviewed,
	}, 0, lastChecked); err != nil {
		t.Fatalf("ensure exact identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact identity seed tx: %v", err)
	}

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &eventID)
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET conflict_type = ?, conflict_reason = ?
		WHERE id = ?
	`, eventTitleRepairConflictType, eventTitleRepairConflictReasonSupportingCleanTitle, clusterID); err != nil {
		t.Fatalf("seed title repair conflict metadata: %v", err)
	}
	newTitle := "Updated Title"
	newSlug := "title-repair-current-renamed"
	if _, err := insertEventReviewDraftChoice(t, db, clusterID, "name", seedstore.EventReviewChoiceKindManual, nil, nil, newTitle, time.Date(2026, time.May, 18, 10, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed draft title: %v", err)
	}
	if _, err := insertEventReviewDraftChoice(t, db, clusterID, "slug", seedstore.EventReviewChoiceKindManual, nil, nil, newSlug, time.Date(2026, time.May, 18, 10, 2, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed draft slug: %v", err)
	}

	return titleRepairResolutionFixture{
		clusterID: clusterID,
		eventID:   eventID,
		sourceID:  sourceID,
		venueID:   venueID,
		oldTitle:  oldTitle,
		newTitle:  newTitle,
		oldSlug:   oldSlug,
		newSlug:   newSlug,
	}
}

func TestLoadEventReviewClusterIncludesAppliedAutoResolutionSummary(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "resolution-auto-summary-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "resolution-auto-summary-leadmill-20260510190000")

	identityKey := "resolution-auto-summary"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 45, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert auto-summary identity link: %v", err)
	}

	runID := mustCreateImportRun(t, st, "resolution auto summary")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "resolution-auto-summary-fingerprint",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"resolution-auto-summary",
			"candidate_title":"Legacy Event",
			"candidate_venue_slug":"leadmill",
			"candidate_start_at":"2026-05-10T19:00:00Z",
			"candidate_end_at":"2026-05-10T22:00:00Z",
			"candidate_genre":"Indie",
			"candidate_status":"Listed",
			"candidate_description":"Legacy event",
			"calendar_url":"https://example.test/calendar.ics"
		}`,
		ExactIdentityKeys: []string{identityKey},
		StagingKey:        eventReviewTestStagingKey("resolution-auto-summary-fingerprint"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage auto-summary evidence: %v", err)
	}
	if result.AutoResolved {
		t.Fatalf("auto-summary stage result unexpectedly auto-resolved: %#v", result)
	}

	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID})
	if err != nil {
		t.Fatalf("finalize auto-summary cluster: %v", err)
	}
	if resolution == nil || resolution.AppliedAutoResolution == nil {
		t.Fatal("auto-summary cluster missing applied auto-resolution summary")
	}
	if resolution.AppliedAutoResolution.EventID != canonicalEventID || resolution.AppliedAutoResolution.Result != "canonical_exact_match" || resolution.AppliedAutoResolution.EvidenceCount != 1 || resolution.AppliedAutoResolution.SourceID != sourceID || resolution.AppliedAutoResolution.SourceName != "Store test source" || resolution.AppliedAutoResolution.SourceURL != "https://example.test/store-test" {
		t.Fatalf("applied auto-resolution summary = %#v", resolution.AppliedAutoResolution)
	}

	detail, ok, err := st.LoadEventReviewCluster(ctx, clusterID)
	if err != nil {
		t.Fatalf("load auto-summary cluster: %v", err)
	}
	if !ok || detail.Resolution == nil {
		t.Fatal("auto-summary cluster missing resolution summary")
	}
	if detail.Resolution.Status != seedstore.EventReviewResolutionStatusResolved {
		t.Fatalf("resolution status = %q, want %q", detail.Resolution.Status, seedstore.EventReviewResolutionStatusResolved)
	}
	if detail.Resolution.AppliedAutoResolution == nil {
		t.Fatal("resolution missing applied auto-resolution summary")
	}
	applied := detail.Resolution.AppliedAutoResolution
	if applied.EventID != canonicalEventID || applied.Result != "canonical_exact_match" || applied.EvidenceCount != 1 || applied.SourceID != sourceID || applied.SourceName != "Store test source" || applied.SourceURL != "https://example.test/store-test" {
		t.Fatalf("applied auto-resolution summary = %#v", applied)
	}
	if detail.Summary.CanonicalEventID == nil || *detail.Summary.CanonicalEventID != canonicalEventID {
		t.Fatalf("cluster canonical_event_id = %#v, want %d", detail.Summary.CanonicalEventID, canonicalEventID)
	}
}

func assertEventReviewClusterState(t *testing.T, db *sql.DB, clusterID int64, wantStatus string, wantVersion int, wantSupersededBy *int64) {
	t.Helper()

	var status string
	var version int
	var supersededBy sql.NullInt64
	if err := db.QueryRow(`
		SELECT status, version, superseded_by_cluster_id
		FROM event_review_clusters
		WHERE id = ?
	`, clusterID).Scan(&status, &version, &supersededBy); err != nil {
		t.Fatalf("load cluster %d: %v", clusterID, err)
	}
	if status != wantStatus {
		t.Fatalf("cluster %d status = %q, want %q", clusterID, status, wantStatus)
	}
	if version != wantVersion {
		t.Fatalf("cluster %d version = %d, want %d", clusterID, version, wantVersion)
	}
	if wantSupersededBy == nil {
		if supersededBy.Valid {
			t.Fatalf("cluster %d superseded_by_cluster_id = %d, want NULL", clusterID, supersededBy.Int64)
		}
		return
	}
	if !supersededBy.Valid || supersededBy.Int64 != *wantSupersededBy {
		t.Fatalf("cluster %d superseded_by_cluster_id = %v, want %d", clusterID, supersededBy, *wantSupersededBy)
	}
}
