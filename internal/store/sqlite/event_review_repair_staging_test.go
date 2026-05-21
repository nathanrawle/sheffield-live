package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	seedstore "sheffield-live/internal/store"
)

func TestEventReviewClusterStagingKeyMigrationFoundation(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	for _, column := range []string{"staging_key", "staging_key_version"} {
		ok, err := columnExists(context.Background(), db, "event_review_clusters", column)
		if err != nil {
			t.Fatalf("check column %s: %v", column, err)
		}
		if !ok {
			t.Fatalf("column event_review_clusters.%s does not exist", column)
		}
	}

	var indexSQL string
	if err := db.QueryRow(`
		SELECT sql
		FROM sqlite_master
		WHERE type = 'index'
			AND name = ?
	`, "idx_event_review_clusters_staging_key_version").Scan(&indexSQL); err != nil {
		t.Fatalf("lookup staging key index: %v", err)
	}
	if !strings.Contains(indexSQL, "UNIQUE INDEX") || !strings.Contains(indexSQL, "WHERE staging_key IS NOT NULL") {
		t.Fatalf("index SQL = %q, want unique partial index on staging_key", indexSQL)
	}

	rows, err := db.Query(`PRAGMA index_list(event_review_clusters)`)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan index row: %v", err)
		}
		if name == "idx_event_review_clusters_staging_key_version" {
			found = true
			if unique != 1 {
				t.Fatalf("index unique flag = %d, want 1", unique)
			}
			if partial != 1 {
				t.Fatalf("index partial flag = %d, want 1", partial)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index rows: %v", err)
	}
	if !found {
		t.Fatal("missing idx_event_review_clusters_staging_key_version")
	}
}

func TestEventReviewClusterStagingKeyTriggersRejectInvalidPairsAndImmutability(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	if _, err := insertEventReviewClusterWithStagingKey(db, string(seedstore.EventReviewClusterStatusOpen), nil, 1, nil, nil, nil, "", ""); err == nil || !strings.Contains(err.Error(), "invalid event review cluster staging key") {
		t.Fatalf("insert null staging key with version error = %v, want staging key trigger", err)
	}
	if _, err := insertEventReviewClusterWithStagingKey(db, string(seedstore.EventReviewClusterStatusOpen), strPtr(" repair-key "), 1, nil, nil, nil, "", ""); err == nil || !strings.Contains(err.Error(), "invalid event review cluster staging key") {
		t.Fatalf("insert untrimmed staging key error = %v, want staging key trigger", err)
	}

	clusterID := insertEventReviewClusterWithStagingKeyOK(t, db, string(seedstore.EventReviewClusterStatusOpen), strPtr("repair-key"), 1, nil, nil, nil, "", "")
	if clusterID <= 0 {
		t.Fatal("missing cluster id")
	}
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET staging_key_version = ?
		WHERE id = ?
	`, 2, clusterID); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("update staging key version error = %v, want immutable trigger", err)
	}
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET staging_key = ?
		WHERE id = ?
	`, "repair-key-v2", clusterID); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("update staging key error = %v, want immutable trigger", err)
	}
}

func TestStageRepairEventReviewClusterCreatesReusesAndKeepsReviewGroupsEmpty(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceID := insertStoreTestSource(t, db)
	runID := int64(71)
	insertRepairRunFixture(t, db, runID)
	firstEventID := lookupOrInsertTestEvent(t, db, "repair-cluster-first-action")
	secondEventID := lookupOrInsertTestEvent(t, db, "repair-cluster-second-action")

	input := seedstore.StageRepairEventReviewClusterInput{
		RunRef: seedstore.EventReviewRunRef{
			Kind: seedstore.EventReviewRunKindRepair,
			ID:   runID,
		},
		StagingKey:        "repair-cluster-key",
		StagingKeyVersion: 1,
		ConflictType:      "duplicate_event",
		ConflictReason:    "same title and start time",
		CanonicalEventID:  int64Ptr(firstEventID),
		Evidence: []seedstore.StageRepairEventReviewEvidenceInput{
			{
				RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
				SourceID:            sourceID,
				SourceName:          "Fixture source",
				SourceURL:           "https://example.test/repair-a",
				SourceAuthority:     seedstore.SourceAuthoritySupporting,
				EventID:             int64Ptr(firstEventID),
				EvidenceFingerprint: "repair-evidence-a",
				Payload:             `{"payload":"a"}`,
				SourceIdentityKeys:  []string{"repair-source-a", "repair-source-a"},
				ExactIdentityKeys:   []string{"repair-exact-a"},
				WeakEvidence:        true,
				WeakEvidenceReason:  "weak supporting evidence",
			},
			{
				RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
				SourceID:            sourceID,
				SourceName:          "Fixture source",
				SourceURL:           "https://example.test/repair-b",
				SourceAuthority:     seedstore.SourceAuthoritySupporting,
				EventID:             int64Ptr(secondEventID),
				EvidenceFingerprint: "repair-evidence-b",
				Payload:             `{"payload":"b"}`,
				SourceIdentityKeys:  []string{"repair-source-b"},
				ExactIdentityKeys:   []string{"repair-exact-b"},
			},
		},
		CanonicalChoices: []seedstore.EventReviewChoiceInput{
			{FieldName: "canonical_title", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: "Canonical Title"},
			{FieldName: "canonical_reason", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: "Preferred canonical"},
		},
		DraftChoices: []seedstore.EventReviewChoiceInput{
			{FieldName: "draft_title", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: "Draft Title"},
			{FieldName: "draft_reason", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: "Needs cleanup"},
		},
		LiveActions: []seedstore.EventReviewLiveActionInput{
			{EventID: firstEventID, Action: seedstore.EventReviewLiveActionKindWithholdDuplicate, Reason: "withhold duplicate"},
			{EventID: secondEventID, Action: seedstore.EventReviewLiveActionKindKeepSeparate, Reason: "keep separate"},
		},
	}

	first, err := st.StageRepairEventReviewCluster(ctx, input)
	if err != nil {
		t.Fatalf("stage repair cluster: %v", err)
	}
	if !first.Created || first.Reused || first.TerminalReused {
		t.Fatalf("first result = %#v, want created open cluster", first)
	}
	if first.Status != seedstore.EventReviewClusterStatusOpen {
		t.Fatalf("status = %q, want open", first.Status)
	}
	if got, want := len(first.EvidenceIDs), 2; got != want {
		t.Fatalf("evidence IDs = %#v, want %d", first.EvidenceIDs, want)
	}

	var stagingKey string
	var stagingKeyVersion int
	var canonicalEventID sql.NullInt64
	var conflictType, conflictReason string
	if err := db.QueryRow(`
		SELECT staging_key, staging_key_version, canonical_event_id, conflict_type, conflict_reason
		FROM event_review_clusters
		WHERE id = ?
	`, first.ClusterID).Scan(&stagingKey, &stagingKeyVersion, &canonicalEventID, &conflictType, &conflictReason); err != nil {
		t.Fatalf("load repair cluster: %v", err)
	}
	if stagingKey != input.StagingKey || stagingKeyVersion != input.StagingKeyVersion {
		t.Fatalf("staging key = (%q, %d), want (%q, %d)", stagingKey, stagingKeyVersion, input.StagingKey, input.StagingKeyVersion)
	}
	if !canonicalEventID.Valid || canonicalEventID.Int64 != firstEventID {
		t.Fatalf("canonical event id = %#v, want %d", canonicalEventID, firstEventID)
	}
	if conflictType != input.ConflictType || conflictReason != input.ConflictReason {
		t.Fatalf("metadata = (%q, %q), want (%q, %q)", conflictType, conflictReason, input.ConflictType, input.ConflictReason)
	}

	if got := mustCount(t, db, "event_review_evidence"); got != 2 {
		t.Fatalf("event_review_evidence rows = %d, want 2", got)
	}
	if got := mustCount(t, db, "event_review_identity_keys"); got != 4 {
		t.Fatalf("event_review_identity_keys rows = %d, want 4", got)
	}
	if got := mustCount(t, db, "event_review_evidence_identity_keys"); got != 4 {
		t.Fatalf("event_review_evidence_identity_keys rows = %d, want 4", got)
	}
	if got := mustCount(t, db, "event_review_cluster_identity_keys"); got != 4 {
		t.Fatalf("event_review_cluster_identity_keys rows = %d, want 4", got)
	}
	if got := mustCount(t, db, "event_review_canonical_choices"); got != 2 {
		t.Fatalf("event_review_canonical_choices rows = %d, want 2", got)
	}
	if got := mustCount(t, db, "event_review_draft_choices"); got != 2 {
		t.Fatalf("event_review_draft_choices rows = %d, want 2", got)
	}
	if got := mustCount(t, db, "event_review_live_actions"); got != 2 {
		t.Fatalf("event_review_live_actions rows = %d, want 2", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
	var repairLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM repair_run_event_review_clusters
		WHERE repair_run_id = ?
			AND cluster_id = ?
	`, runID, first.ClusterID).Scan(&repairLinkCount); err != nil {
		t.Fatalf("count repair run links: %v", err)
	}
	if repairLinkCount != 1 {
		t.Fatalf("repair run cluster links = %d, want 1", repairLinkCount)
	}

	second, err := st.StageRepairEventReviewCluster(ctx, input)
	if err != nil {
		t.Fatalf("stage repair cluster again: %v", err)
	}
	if second.ClusterID != first.ClusterID {
		t.Fatalf("cluster id = %d, want %d", second.ClusterID, first.ClusterID)
	}
	if !second.Reused || second.Created || second.TerminalReused {
		t.Fatalf("second result = %#v, want reused open cluster", second)
	}
	if got, want := len(second.EvidenceIDs), len(first.EvidenceIDs); got != want {
		t.Fatalf("second evidence IDs = %#v, want len %d", second.EvidenceIDs, want)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != 2 {
		t.Fatalf("event_review_evidence rows = %d, want 2 after reuse", got)
	}
	if got := mustCount(t, db, "event_review_identity_keys"); got != 4 {
		t.Fatalf("event_review_identity_keys rows = %d, want 4 after reuse", got)
	}
	if got := mustCount(t, db, "event_review_evidence_identity_keys"); got != 4 {
		t.Fatalf("event_review_evidence_identity_keys rows = %d, want 4 after reuse", got)
	}
	if got := mustCount(t, db, "event_review_cluster_identity_keys"); got != 4 {
		t.Fatalf("event_review_cluster_identity_keys rows = %d, want 4 after reuse", got)
	}
	if got := mustCount(t, db, "event_review_canonical_choices"); got != 2 {
		t.Fatalf("event_review_canonical_choices rows = %d, want 2 after reuse", got)
	}
	if got := mustCount(t, db, "event_review_draft_choices"); got != 2 {
		t.Fatalf("event_review_draft_choices rows = %d, want 2 after reuse", got)
	}
	if got := mustCount(t, db, "event_review_live_actions"); got != 2 {
		t.Fatalf("event_review_live_actions rows = %d, want 2 after reuse", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0 after reuse", got)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM repair_run_event_review_clusters
		WHERE repair_run_id = ?
			AND cluster_id = ?
	`, runID, first.ClusterID).Scan(&repairLinkCount); err != nil {
		t.Fatalf("count repair run links after reuse: %v", err)
	}
	if repairLinkCount != 1 {
		t.Fatalf("repair run cluster links after reuse = %d, want 1", repairLinkCount)
	}
}

func TestStageRepairEventReviewClusterRestageReplacesOmittedRows(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceID := insertStoreTestSource(t, db)
	runID := int64(72)
	insertRepairRunFixture(t, db, runID)
	firstEventID := lookupOrInsertTestEvent(t, db, "repair-restage-first")
	secondEventID := lookupOrInsertTestEvent(t, db, "repair-restage-second")

	first, err := st.StageRepairEventReviewCluster(ctx, seedstore.StageRepairEventReviewClusterInput{
		RunRef:            seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
		StagingKey:        "repair-restage-key",
		StagingKeyVersion: 1,
		ConflictType:      "duplicate_event",
		ConflictReason:    "first pass",
		Evidence: []seedstore.StageRepairEventReviewEvidenceInput{
			{
				RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
				SourceID:            sourceID,
				SourceName:          "Fixture source",
				SourceURL:           "https://example.test/restage-a",
				SourceAuthority:     seedstore.SourceAuthoritySupporting,
				EventID:             int64Ptr(firstEventID),
				EvidenceFingerprint: "restage-shared-fingerprint",
				Payload:             `{"payload":"a"}`,
				SourceIdentityKeys:  []string{"restage-source-a"},
				ExactIdentityKeys:   []string{"restage-exact-a"},
			},
		},
		CanonicalChoices: []seedstore.EventReviewChoiceInput{
			{FieldName: "canonical_title", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: "Canonical A"},
		},
		DraftChoices: []seedstore.EventReviewChoiceInput{
			{FieldName: "draft_title", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: "Draft A"},
		},
		LiveActions: []seedstore.EventReviewLiveActionInput{
			{EventID: firstEventID, Action: seedstore.EventReviewLiveActionKindWithholdDuplicate, Reason: "withhold A"},
		},
	})
	if err != nil {
		t.Fatalf("stage first repair cluster: %v", err)
	}

	second, err := st.StageRepairEventReviewCluster(ctx, seedstore.StageRepairEventReviewClusterInput{
		RunRef:            seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
		StagingKey:        "repair-restage-key",
		StagingKeyVersion: 1,
		ConflictType:      "duplicate_event",
		ConflictReason:    "second pass",
		Evidence: []seedstore.StageRepairEventReviewEvidenceInput{
			{
				RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
				SourceID:            sourceID,
				SourceName:          "Fixture source",
				SourceURL:           "https://example.test/restage-b",
				SourceAuthority:     seedstore.SourceAuthoritySupporting,
				EventID:             int64Ptr(secondEventID),
				EvidenceFingerprint: "restage-shared-fingerprint",
				Payload:             `{"payload":"b"}`,
				SourceIdentityKeys:  []string{"restage-source-b"},
				ExactIdentityKeys:   []string{"restage-exact-b"},
			},
		},
	})
	if err != nil {
		t.Fatalf("stage second repair cluster: %v", err)
	}
	if second.ClusterID != first.ClusterID {
		t.Fatalf("cluster id = %d, want %d", second.ClusterID, first.ClusterID)
	}

	var activeEvidenceCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, first.ClusterID).Scan(&activeEvidenceCount); err != nil {
		t.Fatalf("count active evidence links: %v", err)
	}
	if activeEvidenceCount != 1 {
		t.Fatalf("active evidence links = %d, want 1", activeEvidenceCount)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != 2 {
		t.Fatalf("event_review_evidence rows = %d, want 2", got)
	}
	var activeEvidenceFingerprint string
	if err := db.QueryRow(`
		SELECT e.evidence_fingerprint
		FROM event_review_cluster_evidence ce
		JOIN event_review_evidence e ON e.id = ce.evidence_id
		WHERE ce.cluster_id = ?
			AND ce.active = 1
	`, first.ClusterID).Scan(&activeEvidenceFingerprint); err != nil {
		t.Fatalf("load active evidence fingerprint: %v", err)
	}
	if activeEvidenceFingerprint == "restage-shared-fingerprint" {
		t.Fatal("active evidence fingerprint should be version-scoped, got original fingerprint")
	}

	var activeIdentityCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_identity_keys
		WHERE cluster_id = ?
			AND active = 1
	`, first.ClusterID).Scan(&activeIdentityCount); err != nil {
		t.Fatalf("count active identity links: %v", err)
	}
	if activeIdentityCount != 2 {
		t.Fatalf("active identity links = %d, want 2", activeIdentityCount)
	}

	if got := mustCount(t, db, "event_review_canonical_choices"); got != 0 {
		t.Fatalf("canonical choices rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_review_draft_choices"); got != 0 {
		t.Fatalf("draft choices rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_review_live_actions"); got != 0 {
		t.Fatalf("live actions rows = %d, want 0", got)
	}

	var liveActionCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_live_actions
		WHERE cluster_id = ?
	`, first.ClusterID).Scan(&liveActionCount); err != nil {
		t.Fatalf("count live actions: %v", err)
	}
	if liveActionCount != 0 {
		t.Fatalf("cluster live actions = %d, want 0", liveActionCount)
	}

	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
}

func TestStageRepairEventReviewClusterSameKeyDifferentVersionCreatesDistinctCluster(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceID := insertStoreTestSource(t, db)
	runOne := int64(81)
	runTwo := int64(82)
	insertRepairRunFixture(t, db, runOne)
	insertRepairRunFixture(t, db, runTwo)

	input := seedstore.StageRepairEventReviewClusterInput{
		StagingKey:     "repair-cluster-key",
		ConflictType:   "duplicate_event",
		ConflictReason: "same start time",
		Evidence: []seedstore.StageRepairEventReviewEvidenceInput{
			{
				RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runOne},
				SourceID:            sourceID,
				EvidenceFingerprint: "repair-version-a",
			},
		},
	}

	firstInput := input
	firstInput.RunRef = seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runOne}
	firstInput.StagingKeyVersion = 1
	first, err := st.StageRepairEventReviewCluster(ctx, firstInput)
	if err != nil {
		t.Fatalf("stage version 1 cluster: %v", err)
	}
	secondInput := input
	secondInput.RunRef = seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runTwo}
	secondInput.StagingKeyVersion = 2
	secondInput.Evidence[0].RunRef = seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runTwo}
	second, err := st.StageRepairEventReviewCluster(ctx, secondInput)
	if err != nil {
		t.Fatalf("stage version 2 cluster: %v", err)
	}

	if first.ClusterID == second.ClusterID {
		t.Fatalf("cluster ids matched: %d", first.ClusterID)
	}
	if got, want := mustCount(t, db, "event_review_clusters"), 2; got != want {
		t.Fatalf("event_review_clusters rows = %d, want %d", got, want)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != 2 {
		t.Fatalf("event_review_evidence rows = %d, want 2", got)
	}
	var activeClusterEvidenceCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id IN (?, ?)
			AND active = 1
	`, first.ClusterID, second.ClusterID).Scan(&activeClusterEvidenceCount); err != nil {
		t.Fatalf("count active cluster evidence rows: %v", err)
	}
	if activeClusterEvidenceCount != 2 {
		t.Fatalf("active cluster evidence rows = %d, want 2", activeClusterEvidenceCount)
	}
}

func TestStageRepairEventReviewClusterTerminalExactReuseLinksTerminalCluster(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceID := insertStoreTestSource(t, db)
	runID := int64(90)
	insertRepairRunFixture(t, db, runID)
	eventID := lookupOrInsertTestEvent(t, db, "repair-terminal-exact")

	for _, status := range []seedstore.EventReviewClusterStatus{
		seedstore.EventReviewClusterStatusResolved,
		seedstore.EventReviewClusterStatusDiscarded,
		seedstore.EventReviewClusterStatusSuperseded,
	} {
		t.Run(string(status), func(t *testing.T) {
			terminalKey := "repair-terminal-exact-" + string(status)
			repairKey := terminalKey + "-repair"
			var supersededBy *int64
			if status == seedstore.EventReviewClusterStatusSuperseded {
				targetClusterID := insertEventReviewClusterWithStagingKeyOK(t, db, string(seedstore.EventReviewClusterStatusOpen), strPtr(terminalKey+"-target"), 1, nil, nil, nil, "seed-target", "seed target")
				supersededBy = int64Ptr(targetClusterID)
			}
			clusterID := insertEventReviewClusterWithStagingKeyOK(t, db, string(status), strPtr(terminalKey), 1, supersededBy, nil, int64Ptr(eventID), "seed-conflict-type", "seed conflict reason")
			discardReason := ""
			if status == seedstore.EventReviewClusterStatusDiscarded {
				discardReason = "terminal discard reason"
			}
			insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatus(status), `{"cluster":"terminal"}`, discardReason)
			repairInputFingerprint := "terminal-exact-" + string(status)
			repairFingerprint := repairEventReviewEvidenceFingerprint(repairKey, 1, seedstore.StageRepairEventReviewEvidenceInput{
				SourceID:            sourceID,
				SourceName:          "Terminal exact source",
				SourceURL:           "https://example.test/exact",
				SourceAuthority:     seedstore.SourceAuthoritySupporting,
				EventID:             int64Ptr(eventID),
				EvidenceFingerprint: repairInputFingerprint,
			})
			evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, int64Ptr(eventID), repairFingerprint, `{"payload":"seed"}`)
			if _, err := insertEventReviewClusterEvidence(t, db, clusterID, evidenceID, true, time.Date(2026, time.May, 15, 12, 0, 0, 0, time.UTC), nil, "seed evidence"); err != nil {
				t.Fatalf("insert terminal cluster evidence: %v", err)
			}
			identityKeyID := insertEventReviewIdentityKeyOK(t, db, "terminal-exact-identity-"+string(status), seedstore.EventReviewIdentityKeyKindExact, "terminal-exact-identity-"+string(status))
			if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 12, 5, 0, 0, time.UTC), nil); err != nil {
				t.Fatalf("insert terminal cluster identity key: %v", err)
			}
			if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, identityKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
				t.Fatalf("insert terminal evidence identity key: %v", err)
			}
			repairClusterID := insertEventReviewClusterWithStagingKeyOK(t, db, string(seedstore.EventReviewClusterStatusOpen), strPtr(repairKey), 1, nil, nil, nil, "repair-seed-conflict-type", "repair-seed conflict reason")

			beforeClusters := mustCount(t, db, "event_review_clusters")
			beforeEvidence := mustCount(t, db, "event_review_evidence")

			result, err := st.StageRepairEventReviewCluster(ctx, seedstore.StageRepairEventReviewClusterInput{
				RunRef:            seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
				StagingKey:        repairKey,
				StagingKeyVersion: 1,
				ConflictType:      "new-conflict-type",
				ConflictReason:    "new conflict reason",
				CanonicalEventID:  int64Ptr(eventID),
				Evidence: []seedstore.StageRepairEventReviewEvidenceInput{
					{
						RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
						SourceID:            sourceID,
						SourceName:          "Terminal exact source",
						SourceURL:           "https://example.test/exact",
						SourceAuthority:     seedstore.SourceAuthoritySupporting,
						EventID:             int64Ptr(eventID),
						EvidenceFingerprint: repairInputFingerprint,
						Payload:             `{"payload":"seed"}`,
						ExactIdentityKeys:   []string{"terminal-exact-identity-" + string(status)},
					},
				},
			})
			if err != nil {
				t.Fatalf("stage terminal exact cluster: %v", err)
			}
			if !result.TerminalReused || result.Created || result.Reused {
				t.Fatalf("terminal exact result = %#v, want terminal reuse only", result)
			}
			if result.ClusterID != clusterID {
				t.Fatalf("result cluster id = %d, want terminal cluster %d", result.ClusterID, clusterID)
			}
			if len(result.EvidenceIDs) != 1 || result.EvidenceIDs[0] != evidenceID {
				t.Fatalf("evidence ids = %#v, want terminal evidence %d", result.EvidenceIDs, evidenceID)
			}
			if result.ClusterID == repairClusterID {
				t.Fatalf("result cluster id = %d, want terminal cluster %d, not repair cluster %d", result.ClusterID, clusterID, repairClusterID)
			}
			if got := mustCount(t, db, "event_review_clusters"); got != beforeClusters {
				t.Fatalf("event_review_clusters rows = %d, want %d", got, beforeClusters)
			}
			if got := mustCount(t, db, "event_review_evidence"); got != beforeEvidence {
				t.Fatalf("event_review_evidence rows = %d, want %d", got, beforeEvidence)
			}
			var repairLinkCount int
			if err := db.QueryRow(`
				SELECT COUNT(*)
				FROM repair_run_event_review_clusters
				WHERE repair_run_id = ?
					AND cluster_id = ?
			`, runID, result.ClusterID).Scan(&repairLinkCount); err != nil {
				t.Fatalf("count repair run links: %v", err)
			}
			if repairLinkCount != 1 {
				t.Fatalf("repair run cluster links = %d, want 1", repairLinkCount)
			}
			if err := db.QueryRow(`
				SELECT COUNT(*)
				FROM repair_run_event_review_clusters
				WHERE repair_run_id = ?
					AND cluster_id = ?
			`, runID, repairClusterID).Scan(&repairLinkCount); err != nil {
				t.Fatalf("count repair staging cluster links: %v", err)
			}
			if repairLinkCount != 0 {
				t.Fatalf("repair staging cluster links = %d, want 0", repairLinkCount)
			}
		})
	}
}

func TestStageRepairEventReviewClusterTerminalSuccessorLinksSuccessorCluster(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceID := insertStoreTestSource(t, db)
	runID := int64(91)
	insertRepairRunFixture(t, db, runID)
	firstEventID := lookupOrInsertTestEvent(t, db, "repair-terminal-first")
	secondEventID := lookupOrInsertTestEvent(t, db, "repair-terminal-second")

	for _, status := range []seedstore.EventReviewClusterStatus{
		seedstore.EventReviewClusterStatusResolved,
		seedstore.EventReviewClusterStatusDiscarded,
		seedstore.EventReviewClusterStatusSuperseded,
	} {
		t.Run(string(status), func(t *testing.T) {
			clusterKey := "repair-terminal-successor-" + string(status)
			repairKey := clusterKey + "-repair"
			var supersededBy *int64
			if status == seedstore.EventReviewClusterStatusSuperseded {
				targetClusterID := insertEventReviewClusterWithStagingKeyOK(t, db, string(seedstore.EventReviewClusterStatusOpen), strPtr(clusterKey+"-target"), 1, nil, nil, nil, "seed-target", "seed target")
				supersededBy = int64Ptr(targetClusterID)
			}
			clusterID := insertEventReviewClusterWithStagingKeyOK(t, db, string(status), strPtr(clusterKey), 1, supersededBy, nil, int64Ptr(firstEventID), "seed-conflict-type", "seed conflict reason")
			discardReason := ""
			if status == seedstore.EventReviewClusterStatusDiscarded {
				discardReason = "terminal discard reason"
			}
			insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatus(status), `{"cluster":"terminal"}`, discardReason)
			repairInputFingerprint := "terminal-evidence-" + string(status)
			repairFingerprint := repairEventReviewEvidenceFingerprint(repairKey, 1, seedstore.StageRepairEventReviewEvidenceInput{
				SourceID:            sourceID,
				SourceName:          "Mutated source",
				SourceURL:           "https://example.test/mutated",
				SourceAuthority:     seedstore.SourceAuthoritySupporting,
				EventID:             int64Ptr(firstEventID),
				EvidenceFingerprint: repairInputFingerprint,
			})
			evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, int64Ptr(firstEventID), repairFingerprint, `{"payload":"seed"}`)
			if _, err := insertEventReviewClusterEvidence(t, db, clusterID, evidenceID, true, time.Date(2026, time.May, 15, 12, 0, 0, 0, time.UTC), nil, "seed evidence"); err != nil {
				t.Fatalf("insert terminal cluster evidence: %v", err)
			}
			identityKeyID := insertEventReviewIdentityKeyOK(t, db, "terminal-identity-"+string(status), seedstore.EventReviewIdentityKeyKindExact, "terminal-identity-"+string(status))
			if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 12, 5, 0, 0, time.UTC), nil); err != nil {
				t.Fatalf("insert terminal cluster identity key: %v", err)
			}
			if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, identityKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
				t.Fatalf("insert terminal evidence identity key: %v", err)
			}
			if _, err := insertEventReviewCanonicalChoice(t, db, clusterID, "canonical_field", seedstore.EventReviewChoiceKindManual, nil, nil, "seed canonical", time.Date(2026, time.May, 15, 12, 10, 0, 0, time.UTC)); err != nil {
				t.Fatalf("insert terminal canonical choice: %v", err)
			}
			if _, err := insertEventReviewDraftChoice(t, db, clusterID, "draft_field", seedstore.EventReviewChoiceKindManual, nil, nil, "seed draft", time.Date(2026, time.May, 15, 12, 11, 0, 0, time.UTC)); err != nil {
				t.Fatalf("insert terminal draft choice: %v", err)
			}
			if _, err := insertEventReviewLiveAction(t, db, clusterID, firstEventID, string(seedstore.EventReviewLiveActionKindWithholdDuplicate), "seed live action", time.Date(2026, time.May, 15, 12, 12, 0, 0, time.UTC)); err != nil {
				t.Fatalf("insert terminal live action: %v", err)
			}
			repairClusterID := insertEventReviewClusterWithStagingKeyOK(t, db, string(seedstore.EventReviewClusterStatusOpen), strPtr(repairKey), 1, nil, nil, nil, "repair-seed-conflict-type", "repair-seed conflict reason")

			beforeEvidence := mustCount(t, db, "event_review_evidence")
			beforeCanonicalChoices := mustCount(t, db, "event_review_canonical_choices")
			beforeDraftChoices := mustCount(t, db, "event_review_draft_choices")
			beforeLiveActions := mustCount(t, db, "event_review_live_actions")
			beforeClusterCount := mustCount(t, db, "event_review_clusters")
			beforeClusterEvidence := mustCount(t, db, "event_review_cluster_evidence")
			beforeRepairProvenance := mustCount(t, db, "repair_run_event_review_evidence")

			result, err := st.StageRepairEventReviewCluster(ctx, seedstore.StageRepairEventReviewClusterInput{
				RunRef:            seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
				StagingKey:        repairKey,
				StagingKeyVersion: 1,
				ConflictType:      "new-conflict-type",
				ConflictReason:    "new conflict reason",
				CanonicalEventID:  int64Ptr(secondEventID),
				Evidence: []seedstore.StageRepairEventReviewEvidenceInput{
					{
						RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: runID},
						SourceID:            sourceID,
						SourceName:          "Mutated source",
						SourceURL:           "https://example.test/mutated",
						SourceAuthority:     seedstore.SourceAuthoritySupporting,
						EventID:             int64Ptr(firstEventID),
						EvidenceFingerprint: repairInputFingerprint,
						Payload:             `{"payload":"mutated"}`,
						ExactIdentityKeys:   []string{"terminal-identity-" + string(status)},
					},
				},
				CanonicalChoices: []seedstore.EventReviewChoiceInput{
					{FieldName: "canonical_field", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: "seed canonical"},
				},
				DraftChoices: []seedstore.EventReviewChoiceInput{
					{FieldName: "draft_field", ChoiceKind: seedstore.EventReviewChoiceKindManual, Value: "seed draft"},
				},
				LiveActions: []seedstore.EventReviewLiveActionInput{
					{EventID: firstEventID, Action: seedstore.EventReviewLiveActionKindWithholdDuplicate, Reason: "seed live action"},
				},
			})
			if err != nil {
				t.Fatalf("stage terminal cluster: %v", err)
			}
			if !result.TerminalReused || result.Created || result.Reused {
				t.Fatalf("terminal result = %#v, want terminal reuse only", result)
			}
			if result.Status != seedstore.EventReviewClusterStatusOpen {
				t.Fatalf("result status = %q, want open successor", result.Status)
			}
			if result.ClusterID == clusterID {
				t.Fatalf("result cluster id = %d, want open successor distinct from terminal cluster %d", result.ClusterID, clusterID)
			}

			if len(result.EvidenceIDs) != 1 {
				t.Fatalf("evidence ids = %#v, want single revised evidence id", result.EvidenceIDs)
			}
			if got := mustCount(t, db, "event_review_clusters"); got != beforeClusterCount+1 {
				t.Fatalf("event_review_clusters rows = %d, want %d", got, beforeClusterCount+1)
			}
			if got := mustCount(t, db, "event_review_evidence"); got != beforeEvidence+1 {
				t.Fatalf("event_review_evidence rows = %d, want %d", got, beforeEvidence+1)
			}
			if got := mustCount(t, db, "repair_run_event_review_evidence"); got != beforeRepairProvenance+1 {
				t.Fatalf("repair_run_event_review_evidence rows = %d, want %d", got, beforeRepairProvenance+1)
			}
			if got := mustCount(t, db, "event_review_canonical_choices"); got != beforeCanonicalChoices {
				t.Fatalf("event_review_canonical_choices rows = %d, want %d", got, beforeCanonicalChoices)
			}
			if got := mustCount(t, db, "event_review_draft_choices"); got != beforeDraftChoices {
				t.Fatalf("event_review_draft_choices rows = %d, want %d", got, beforeDraftChoices)
			}
			if got := mustCount(t, db, "event_review_live_actions"); got != beforeLiveActions {
				t.Fatalf("event_review_live_actions rows = %d, want %d", got, beforeLiveActions)
			}
			if result.EvidenceIDs[0] == evidenceID {
				t.Fatalf("evidence ids = %#v, want revised successor evidence", result.EvidenceIDs)
			}
			if result.ClusterID == repairClusterID {
				t.Fatalf("result cluster id = %d, want successor cluster not repair cluster %d", result.ClusterID, repairClusterID)
			}
			if got := mustCount(t, db, "event_review_cluster_evidence"); got != beforeClusterEvidence+1 {
				t.Fatalf("event_review_cluster_evidence rows = %d, want %d", got, beforeClusterEvidence+1)
			}
			successorKey := terminalSuccessorStagingKey(clusterID, int64Ptr(clusterID), terminalEvidenceIdentityHashesForInput(seedstore.StageEventReviewEvidenceInput{
				ExactIdentityKeys: []string{"terminal-identity-" + string(status)},
				StagingKey:        eventReviewTestStagingKey(""),
				StagingKeyVersion: 1,
			}), "seed-conflict-type", "seed conflict reason")
			var successorClusterID sql.NullInt64
			if err := db.QueryRow(`
				SELECT id
				FROM event_review_clusters
				WHERE staging_key = ?
					AND staging_key_version = 1
			`, successorKey).Scan(&successorClusterID); err != nil {
				t.Fatalf("load successor cluster: %v", err)
			}
			if !successorClusterID.Valid {
				t.Fatal("successor cluster id is invalid")
			}
			if result.ClusterID != successorClusterID.Int64 {
				t.Fatalf("result cluster id = %d, want successor %d", result.ClusterID, successorClusterID.Int64)
			}
			var successorStatus string
			var successorPrevious sql.NullInt64
			if err := db.QueryRow(`
				SELECT status, previous_cluster_id
				FROM event_review_clusters
				WHERE id = ?
			`, successorClusterID.Int64).Scan(&successorStatus, &successorPrevious); err != nil {
				t.Fatalf("load successor cluster metadata: %v", err)
			}
			if successorStatus != string(seedstore.EventReviewClusterStatusOpen) {
				t.Fatalf("successor status = %q, want open", successorStatus)
			}
			if !successorPrevious.Valid || successorPrevious.Int64 != clusterID {
				t.Fatalf("successor previous cluster id = %#v, want %d", successorPrevious, clusterID)
			}

			var canonicalEventID sql.NullInt64
			var conflictType, conflictReason string
			if err := db.QueryRow(`
				SELECT canonical_event_id, conflict_type, conflict_reason
				FROM event_review_clusters
				WHERE id = ?
			`, clusterID).Scan(&canonicalEventID, &conflictType, &conflictReason); err != nil {
				t.Fatalf("load terminal cluster metadata: %v", err)
			}
			if !canonicalEventID.Valid || canonicalEventID.Int64 != firstEventID {
				t.Fatalf("canonical event id = %#v, want %d", canonicalEventID, firstEventID)
			}
			if conflictType != "seed-conflict-type" {
				t.Fatalf("conflict type = %q, want unchanged seed value", conflictType)
			}
			if conflictReason != "seed conflict reason" {
				t.Fatalf("conflict reason = %q, want unchanged seed value", conflictReason)
			}
			var repairLinkCount int
			if err := db.QueryRow(`
				SELECT COUNT(*)
				FROM repair_run_event_review_clusters
				WHERE repair_run_id = ?
					AND cluster_id = ?
			`, runID, result.ClusterID).Scan(&repairLinkCount); err != nil {
				t.Fatalf("count repair run successor links: %v", err)
			}
			if repairLinkCount != 1 {
				t.Fatalf("repair run successor links = %d, want 1", repairLinkCount)
			}
			if err := db.QueryRow(`
				SELECT COUNT(*)
				FROM repair_run_event_review_clusters
				WHERE repair_run_id = ?
					AND cluster_id = ?
			`, runID, repairClusterID).Scan(&repairLinkCount); err != nil {
				t.Fatalf("count repair staging cluster links: %v", err)
			}
			if repairLinkCount != 0 {
				t.Fatalf("repair staging cluster links = %d, want 0", repairLinkCount)
			}
			if err := db.QueryRow(`
				SELECT COUNT(*)
				FROM repair_run_event_review_clusters
				WHERE repair_run_id = ?
					AND cluster_id = ?
			`, runID, clusterID).Scan(&repairLinkCount); err != nil {
				t.Fatalf("count repair run terminal links: %v", err)
			}
			if repairLinkCount != 0 {
				t.Fatalf("repair run terminal links = %d, want 0", repairLinkCount)
			}
		})
	}
}

func TestStageRepairEventReviewClusterRejectsImportRunRef(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceID := insertStoreTestSource(t, db)
	runID := mustCreateImportRun(t, st, "import run ref rejection")
	_, err := st.StageRepairEventReviewCluster(ctx, seedstore.StageRepairEventReviewClusterInput{
		RunRef:            seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		StagingKey:        "repair-import-reject",
		StagingKeyVersion: 1,
		Evidence: []seedstore.StageRepairEventReviewEvidenceInput{
			{
				SourceID:            sourceID,
				EvidenceFingerprint: "repair-import-reject-fingerprint",
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "repair run ref") {
		t.Fatalf("stage repair cluster error = %v, want repair run ref rejection", err)
	}
}

func TestStageEventReviewEvidenceRejectsMissingStagingKey(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sourceID := insertStoreTestSource(t, db)
	runID := mustCreateImportRun(t, st, "normal staging key check")
	base := seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/normal",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "normal-staging-evidence",
		Payload:             `{"payload":"normal"}`,
	}
	if _, err := st.StageEventReviewEvidence(ctx, base); err == nil || !strings.Contains(err.Error(), "staging key is required") {
		t.Fatalf("stage evidence error = %v, want missing staging key", err)
	}
	base.StagingKey = eventReviewTestStagingKey(base.EvidenceFingerprint)
	if _, err := st.StageEventReviewEvidence(ctx, base); err == nil || !strings.Contains(err.Error(), "staging key version is required") {
		t.Fatalf("stage evidence error = %v, want missing staging key version", err)
	}
}

func insertEventReviewClusterWithStagingKey(db *sql.DB, status string, stagingKey *string, stagingKeyVersion int, supersededBy, previous, canonical *int64, conflictType, conflictReason string) (int64, error) {
	var stagingKeyValue any
	if stagingKey != nil {
		stagingKeyValue = *stagingKey
	}
	res, err := db.Exec(`
		INSERT INTO event_review_clusters (
			status,
			version,
			staging_key,
			staging_key_version,
			superseded_by_cluster_id,
			previous_cluster_id,
			canonical_event_id,
			conflict_type,
			conflict_reason,
			created_at,
			updated_at
		) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, status, stagingKeyValue, stagingKeyVersion, supersededBy, previous, canonical, conflictType, conflictReason, "2026-05-15T10:00:00Z", "2026-05-15T10:00:00Z")
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func insertEventReviewClusterWithStagingKeyOK(t *testing.T, db *sql.DB, status string, stagingKey *string, stagingKeyVersion int, supersededBy, previous, canonical *int64, conflictType, conflictReason string) int64 {
	t.Helper()

	id, err := insertEventReviewClusterWithStagingKey(db, status, stagingKey, stagingKeyVersion, supersededBy, previous, canonical, conflictType, conflictReason)
	if err != nil {
		t.Fatalf("insert cluster %q: %v", status, err)
	}
	return id
}

func strPtr(value string) *string {
	return &value
}
