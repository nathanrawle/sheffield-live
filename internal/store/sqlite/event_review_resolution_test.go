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
			name: "unsupported conflict reason",
			mutate: func(t *testing.T, db *sql.DB, fixture titleRepairResolutionFixture) {
				if _, err := db.Exec(`UPDATE event_review_clusters SET conflict_reason = ? WHERE id = ?`, "authoritative_slug_conflict", fixture.clusterID); err != nil {
					t.Fatalf("update conflict reason: %v", err)
				}
			},
			wantErr: "authoritative slug conflict resolution is not implemented",
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

	extraPayload := importReviewResolutionPayload(t, fixture.sourceName, fixture.sourceURL, fixture.calendarURL, string(seedstore.SourceAuthoritySupporting), "Unselected Import Listing", fixture.venueText, "", fixture.start.Add(1*time.Hour), fixture.end.Add(1*time.Hour), "import-review-extra")
	extraEvidenceID := insertEventReviewEvidenceOK(t, db, fixture.sourceID, nil, "import-review-extra-"+strconv.FormatInt(fixture.clusterID, 10), extraPayload)
	insertEventReviewClusterEvidenceOK(t, db, fixture.clusterID, extraEvidenceID, true, fixture.start.Add(10*time.Minute), nil, "extra evidence")

	if err := st.ResolveEventReviewCluster(context.Background(), seedstore.EventReviewResolutionInput{
		ClusterID:       fixture.clusterID,
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("resolve selected multi-evidence cluster: %v", err)
	}

	assertEventReviewClusterState(t, db, fixture.clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)

	detail, ok, err := st.LoadEventReviewCluster(context.Background(), fixture.clusterID)
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

	if got := mustCount(t, db, "events"); got != beforeEvents+2 {
		t.Fatalf("events rows = %d, want %d", got, beforeEvents+2)
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
