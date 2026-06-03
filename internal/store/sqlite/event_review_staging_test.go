package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	seedstore "sheffield-live/internal/store"
)

func eventReviewTestStagingKey(fingerprint string) string {
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return "test-stage:empty"
	}
	return "test-stage:" + fingerprint
}

func TestStageEventReviewEvidenceFingerprintIdempotencyAndRunLinks(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	runOne := mustCreateImportRun(t, st, "stage event review one")
	runTwo := mustCreateImportRun(t, st, "stage event review two")

	input := seedstore.StageEventReviewEvidenceInput{
		RunRef: seedstore.EventReviewRunRef{
			Kind: seedstore.EventReviewRunKindImport,
			ID:   runOne,
		},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/source",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "fingerprint-idempotent",
		Payload:             `{"payload":"one"}`,
		SourceIdentityKeys:  []string{"source-identity"},
		ExactIdentityKeys:   []string{"exact-identity"},
		StagingKey:          eventReviewTestStagingKey("fingerprint-idempotent"),
		StagingKeyVersion:   1,
	}

	first, err := st.StageEventReviewEvidence(ctx, input)
	if err != nil {
		t.Fatalf("stage first evidence: %v", err)
	}
	if !first.Created || !first.ClusterCreated || !first.Attached {
		t.Fatalf("first result = %#v, want created cluster attach", first)
	}

	input.RunRef.ID = runTwo
	second, err := st.StageEventReviewEvidence(ctx, input)
	if err != nil {
		t.Fatalf("stage duplicate evidence: %v", err)
	}
	if second.EvidenceID != first.EvidenceID {
		t.Fatalf("evidence id = %d, want %d", second.EvidenceID, first.EvidenceID)
	}
	if second.ClusterID != first.ClusterID {
		t.Fatalf("cluster id = %d, want %d", second.ClusterID, first.ClusterID)
	}
	if !second.Reused || second.Created || second.Attached {
		t.Fatalf("second result = %#v, want reused evidence and run-link-only reuse", second)
	}

	if got := mustCount(t, db, "event_review_evidence"); got != 1 {
		t.Fatalf("event_review_evidence rows = %d, want 1", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
	var linkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM import_run_event_review_clusters
		WHERE cluster_id = ?
	`, first.ClusterID).Scan(&linkCount); err != nil {
		t.Fatalf("count import run links: %v", err)
	}
	if linkCount != 2 {
		t.Fatalf("import run cluster links = %d, want 2", linkCount)
	}
}

func TestStageEventReviewEvidencePersistsImportConflictMetadata(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	runID := mustCreateImportRun(t, st, "import conflict metadata")

	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/import-metadata",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "import-conflict-metadata",
		Payload:             `{"payload":"import"}`,
		SourceIdentityKeys:  []string{"import-identity"},
		StagingKey:          eventReviewTestStagingKey("import-conflict-metadata"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage import evidence: %v", err)
	}

	var conflictType, conflictReason string
	if err := db.QueryRow(`
		SELECT conflict_type, conflict_reason
		FROM event_review_clusters
		WHERE id = ?
	`, result.ClusterID).Scan(&conflictType, &conflictReason); err != nil {
		t.Fatalf("load import cluster metadata: %v", err)
	}
	if conflictType != seedstore.EventReviewConflictTypeImportReview {
		t.Fatalf("conflict_type = %q, want %q", conflictType, seedstore.EventReviewConflictTypeImportReview)
	}
	if conflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		t.Fatalf("conflict_reason = %q, want %q", conflictReason, seedstore.EventReviewConflictReasonIngestCandidate)
	}
}

func TestStageEventReviewEvidenceRecordsImportClusterObservations(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	runID := mustCreateImportRun(t, st, "import cluster observations")

	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Import Source",
		SourceURL:           "https://example.test/import-review-source",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "import-cluster-observations",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Import Source",
			"source_url":"",
			"candidate_external_id":"external-1",
			"candidate_title":"Import Review Title",
			"candidate_venue_slug":"the-hall",
			"candidate_venue_text":"The Hall",
			"candidate_room_text":"Main Hall",
			"candidate_start_at":"2026-05-15T20:00:00Z",
			"candidate_end_at":"2026-05-15T22:00:00Z",
			"candidate_genre":"indie",
			"candidate_status":"confirmed",
			"candidate_description":"A staged import review",
			"calendar_url":"https://calendar.example.test/import-review.ics",
			"provenance":"fixture"
		}`,
		SourceIdentityKeys: []string{"source-identity"},
		StagingKey:         eventReviewTestStagingKey("import-cluster-observations"),
		StagingKeyVersion:  1,
	})
	if err != nil {
		t.Fatalf("stage import evidence: %v", err)
	}
	if !result.ClusterCreated || !result.Attached {
		t.Fatalf("result = %#v, want created cluster attach", result)
	}

	rows := mustLoadEventReviewClusterObservations(t, db, result.ClusterID)
	if got := len(rows); got != 10 {
		t.Fatalf("cluster observation rows = %d, want 10", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}

	expectRow := func(fieldName, raw, normalized string) {
		t.Helper()
		row, ok := rows[fieldName]
		if !ok {
			t.Fatalf("missing observation for field %q", fieldName)
		}
		if row.TargetKind != string(seedstore.ObservationTargetKindEventReviewCluster) {
			t.Fatalf("field %q target_kind = %q, want %q", fieldName, row.TargetKind, seedstore.ObservationTargetKindEventReviewCluster)
		}
		if row.EventID.Valid {
			t.Fatalf("field %q event_id = %d, want NULL", fieldName, row.EventID.Int64)
		}
		if row.ReviewGroupID.Valid {
			t.Fatalf("field %q review_group_id = %d, want NULL", fieldName, row.ReviewGroupID.Int64)
		}
		if !row.EventReviewClusterID.Valid || row.EventReviewClusterID.Int64 != result.ClusterID {
			t.Fatalf("field %q event_review_cluster_id = %#v, want %d", fieldName, row.EventReviewClusterID, result.ClusterID)
		}
		if row.Outcome != string(seedstore.ObservationOutcomeStagedForReview) {
			t.Fatalf("field %q outcome = %q, want %q", fieldName, row.Outcome, seedstore.ObservationOutcomeStagedForReview)
		}
		if row.SourceAuthority != string(seedstore.SourceAuthoritySupporting) {
			t.Fatalf("field %q source_authority = %q, want %q", fieldName, row.SourceAuthority, seedstore.SourceAuthoritySupporting)
		}
		if row.IncomingRaw != raw {
			t.Fatalf("field %q incoming_raw = %q, want %q", fieldName, row.IncomingRaw, raw)
		}
		if row.IncomingNormalized != normalized {
			t.Fatalf("field %q incoming_normalized = %q, want %q", fieldName, row.IncomingNormalized, normalized)
		}
	}

	expectRow("name", "Import Review Title", "import review title")
	expectRow("venue_slug", "the-hall", "the-hall")
	expectRow("room_text", "Main Hall", "Main Hall")
	expectRow("start_at", "2026-05-15T20:00:00Z", "2026-05-15T20:00:00Z")
	expectRow("end_at", "2026-05-15T22:00:00Z", "2026-05-15T22:00:00Z")
	expectRow("status", "confirmed", "confirmed")
	expectRow("genre", "indie", "indie")
	expectRow("description", "A staged import review", "A staged import review")
	expectRow("official_listing_url", "https://example.test/import-review-source", "https://example.test/import-review-source")
	expectRow("calendar_url", "https://calendar.example.test/import-review.ics", "https://calendar.example.test/import-review.ics")
}

func TestStageEventReviewEvidenceImportClusterObservationsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	runID := mustCreateImportRun(t, st, "import cluster observations idempotent")
	input := seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Import Source",
		SourceURL:           "https://example.test/import-review-source",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "import-cluster-observations-idempotent",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Import Source",
			"candidate_external_id":"external-1",
			"candidate_title":"Import Review Title",
			"candidate_venue_slug":"the-hall",
			"candidate_room_text":"Main Hall",
			"candidate_start_at":"2026-05-15T20:00:00Z",
			"candidate_end_at":"2026-05-15T22:00:00Z",
			"candidate_genre":"indie",
			"candidate_status":"confirmed",
			"candidate_description":"A staged import review",
			"calendar_url":"https://calendar.example.test/import-review.ics"
		}`,
		SourceIdentityKeys: []string{"source-identity"},
		StagingKey:         eventReviewTestStagingKey("import-cluster-observations-idempotent"),
		StagingKeyVersion:  1,
	}

	first, err := st.StageEventReviewEvidence(ctx, input)
	if err != nil {
		t.Fatalf("stage first evidence: %v", err)
	}
	second, err := st.StageEventReviewEvidence(ctx, input)
	if err != nil {
		t.Fatalf("stage second evidence: %v", err)
	}
	if second.EvidenceID != first.EvidenceID {
		t.Fatalf("evidence id = %d, want %d", second.EvidenceID, first.EvidenceID)
	}
	if second.ClusterID != first.ClusterID {
		t.Fatalf("cluster id = %d, want %d", second.ClusterID, first.ClusterID)
	}
	if got := len(mustLoadEventReviewClusterObservations(t, db, first.ClusterID)); got != 10 {
		t.Fatalf("cluster observation rows = %d, want 10", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
}

func TestStageEventReviewEvidenceMalformedImportPayloadSkipsClusterObservations(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	runID := mustCreateImportRun(t, st, "malformed import cluster observations")

	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Import Source",
		SourceURL:           "https://example.test/import-review-source",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "malformed-import-cluster-observations",
		Payload:             "{bad payload",
		SourceIdentityKeys:  []string{"source-identity"},
		StagingKey:          eventReviewTestStagingKey("malformed-import-cluster-observations"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage malformed import evidence: %v", err)
	}
	if !result.ClusterCreated || !result.Attached {
		t.Fatalf("result = %#v, want created cluster attach", result)
	}

	if got := len(mustLoadEventReviewClusterObservations(t, db, result.ClusterID)); got != 0 {
		t.Fatalf("cluster observation rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
}

func TestStageEventReviewEvidenceRepairRunSkipsClusterObservations(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	repairRunID := int64(41)
	insertRepairRunFixture(t, db, repairRunID)

	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindRepair, ID: repairRunID},
		SourceID:            sourceID,
		SourceName:          "Repair Source",
		SourceURL:           "https://example.test/repair-review-source",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "repair-cluster-observations",
		Payload: `{
			"source_authority":"supporting",
			"candidate_external_id":"external-1",
			"candidate_title":"Repair Review Title",
			"candidate_venue_slug":"the-hall",
			"candidate_start_at":"2026-05-15T20:00:00Z",
			"calendar_url":"https://calendar.example.test/repair-review.ics"
		}`,
		SourceIdentityKeys: []string{"repair-source-identity"},
		StagingKey:         eventReviewTestStagingKey("repair-cluster-observations"),
		StagingKeyVersion:  1,
	})
	if err != nil {
		t.Fatalf("stage repair evidence: %v", err)
	}
	if !result.ClusterCreated || !result.Attached {
		t.Fatalf("result = %#v, want created cluster attach", result)
	}

	if got := len(mustLoadEventReviewClusterObservations(t, db, result.ClusterID)); got != 0 {
		t.Fatalf("cluster observation rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
}

func TestStageEventReviewEvidenceBackfillsBlankOpenClusterConflictMetadata(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	key := "backfill-open-cluster"
	keyHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, key)
	keyID := insertEventReviewIdentityKeyOK(t, db, keyHash, seedstore.EventReviewIdentityKeyKindExact, key)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, keyID, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster identity: %v", err)
	}
	runID := mustCreateImportRun(t, st, "backfill conflict metadata")

	if _, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/backfill-metadata",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "backfill-conflict-metadata",
		Payload:             `{"payload":"backfill"}`,
		ExactIdentityKeys:   []string{key},
		StagingKey:          eventReviewTestStagingKey("backfill-conflict-metadata"),
		StagingKeyVersion:   1,
	}); err != nil {
		t.Fatalf("stage backfill evidence: %v", err)
	}

	var conflictType, conflictReason string
	if err := db.QueryRow(`
		SELECT conflict_type, conflict_reason
		FROM event_review_clusters
		WHERE id = ?
	`, clusterID).Scan(&conflictType, &conflictReason); err != nil {
		t.Fatalf("load backfilled cluster metadata: %v", err)
	}
	if conflictType != seedstore.EventReviewConflictTypeImportReview {
		t.Fatalf("conflict_type = %q, want %q", conflictType, seedstore.EventReviewConflictTypeImportReview)
	}
	if conflictReason != seedstore.EventReviewConflictReasonIngestCandidate {
		t.Fatalf("conflict_reason = %q, want %q", conflictReason, seedstore.EventReviewConflictReasonIngestCandidate)
	}
}

func TestStageEventReviewEvidencePreservesExistingOpenClusterConflictMetadata(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	key := "preserve-open-cluster"
	keyHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, key)
	keyID := insertEventReviewIdentityKeyOK(t, db, keyHash, seedstore.EventReviewIdentityKeyKindExact, key)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET conflict_type = ?,
			conflict_reason = ?
		WHERE id = ?
	`, "existing-type", "existing reason", clusterID); err != nil {
		t.Fatalf("seed cluster metadata: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, keyID, true, time.Date(2026, time.May, 15, 10, 15, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster identity: %v", err)
	}
	runID := mustCreateImportRun(t, st, "preserve conflict metadata")

	if _, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/preserve-metadata",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "preserve-conflict-metadata",
		Payload:             `{"payload":"preserve"}`,
		ExactIdentityKeys:   []string{key},
		StagingKey:          eventReviewTestStagingKey("preserve-conflict-metadata"),
		StagingKeyVersion:   1,
	}); err != nil {
		t.Fatalf("stage preserve evidence: %v", err)
	}

	var conflictType, conflictReason string
	if err := db.QueryRow(`
		SELECT conflict_type, conflict_reason
		FROM event_review_clusters
		WHERE id = ?
	`, clusterID).Scan(&conflictType, &conflictReason); err != nil {
		t.Fatalf("load preserved cluster metadata: %v", err)
	}
	if conflictType != "existing-type" {
		t.Fatalf("conflict_type = %q, want %q", conflictType, "existing-type")
	}
	if conflictReason != "existing reason" {
		t.Fatalf("conflict_reason = %q, want %q", conflictReason, "existing reason")
	}
}

func TestStageEventReviewEvidenceLinksRepairRunToRepairTable(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	repairRunID := int64(41)
	insertRepairRunFixture(t, db, repairRunID)

	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef: seedstore.EventReviewRunRef{
			Kind: seedstore.EventReviewRunKindRepair,
			ID:   repairRunID,
		},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/repair",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "repair-fingerprint",
		Payload:             `{"payload":"repair"}`,
		StagingKey:          eventReviewTestStagingKey("repair-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage repair evidence: %v", err)
	}
	if !result.ClusterCreated || !result.Attached {
		t.Fatalf("repair result = %#v, want created cluster attach", result)
	}

	if got := mustCount(t, db, "import_run_event_review_clusters"); got != 0 {
		t.Fatalf("import_run_event_review_clusters rows = %d, want 0", got)
	}
	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM repair_run_event_review_clusters
		WHERE repair_run_id = ?
	`, repairRunID).Scan(&count); err != nil {
		t.Fatalf("count repair run links: %v", err)
	}
	if count != 1 {
		t.Fatalf("repair run cluster links = %d, want 1", count)
	}
}

func TestStageEventReviewEvidenceSupportsMultipleSourceAndExactIdentityKeys(t *testing.T) {
	ctx := context.Background()
	st, _ := openEventReviewSchemaStore(t)
	defer st.Close()

	sourceID := insertStoreTestSource(t, st.db)
	runID := mustCreateImportRun(t, st, "multiple identities")

	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef: seedstore.EventReviewRunRef{
			Kind: seedstore.EventReviewRunKindImport,
			ID:   runID,
		},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/multi",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "multi-identity-fingerprint",
		Payload:             `{"payload":"multi"}`,
		SourceIdentityKeys:  []string{"source-1", "source-2"},
		ExactIdentityKeys:   []string{"exact-1", "exact-2"},
		StagingKey:          eventReviewTestStagingKey("multi-identity-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage evidence: %v", err)
	}

	if got := mustCount(t, st.db, "event_review_identity_keys"); got != 4 {
		t.Fatalf("event_review_identity_keys rows = %d, want 4", got)
	}
	if got := mustCount(t, st.db, "event_review_evidence_identity_keys"); got != 4 {
		t.Fatalf("event_review_evidence_identity_keys rows = %d, want 4", got)
	}
	var activeClusterLinks int
	if err := st.db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_identity_keys
		WHERE cluster_id = ?
			AND active = 1
	`, result.ClusterID).Scan(&activeClusterLinks); err != nil {
		t.Fatalf("count active cluster identity links: %v", err)
	}
	if activeClusterLinks != 4 {
		t.Fatalf("active cluster identity links = %d, want 4", activeClusterLinks)
	}
}

func TestStageEventReviewEvidenceIdentityOverlapAttachesToOpenCluster(t *testing.T) {
	t.Run("source_identity", func(t *testing.T) {
		stageOverlapAttachesToOpenCluster(t, seedstore.EventReviewIdentityKeyKindSource, []string{"source-match"}, nil)
	})
	t.Run("exact_identity", func(t *testing.T) {
		stageOverlapAttachesToOpenCluster(t, seedstore.EventReviewIdentityKeyKindExact, nil, []string{"exact-match"})
	})
}

func TestStageEventReviewEvidenceNoIdentityEvidenceDoesNotClusterByPayload(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	runOne := mustCreateImportRun(t, st, "no-identity-one")
	runTwo := mustCreateImportRun(t, st, "no-identity-two")

	first, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runOne},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/no-identity",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "payload-only-one",
		Payload:             `{"title":"payload only"}`,
		WeakEvidence:        true,
		WeakEvidenceReason:  "payload only",
		StagingKey:          eventReviewTestStagingKey("payload-only-one"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage first weak evidence: %v", err)
	}

	second, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runTwo},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/no-identity",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "payload-only-two",
		Payload:             `{"title":"payload only"}`,
		WeakEvidence:        true,
		WeakEvidenceReason:  "payload only",
		StagingKey:          eventReviewTestStagingKey("payload-only-two"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage second weak evidence: %v", err)
	}

	if first.ClusterID == second.ClusterID {
		t.Fatalf("clusters matched by payload only: %d", first.ClusterID)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 2 {
		t.Fatalf("event_review_clusters rows = %d, want 2", got)
	}
}

func TestStageEventReviewEvidenceMergeBridgeSupersedesLoser(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	identityKey := "merge-bridge"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)

	clusterA := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	clusterB := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)

	evidenceA := insertEventReviewEvidenceOK(t, db, sourceID, nil, "merge-bridge-a", `{"payload":"a"}`)
	evidenceB := insertEventReviewEvidenceOK(t, db, sourceID, nil, "merge-bridge-b", `{"payload":"b"}`)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceA, identityKeyID, int64Ptr(sourceID), seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("link evidence A identity: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceB, identityKeyID, int64Ptr(sourceID), seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("link evidence B identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterA, identityKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("link cluster A identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterB, identityKeyID, true, time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("link cluster B identity: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, clusterA, evidenceA, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil, "seed A"); err != nil {
		t.Fatalf("link cluster A evidence: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, clusterB, evidenceB, true, time.Date(2026, time.May, 15, 10, 12, 0, 0, time.UTC), nil, "seed B"); err != nil {
		t.Fatalf("link cluster B evidence: %v", err)
	}

	runID := mustCreateImportRun(t, st, "merge bridge")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/merge-bridge",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "merge-bridge-new",
		Payload:             `{"payload":"merge"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("merge-bridge-new"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage merge bridge evidence: %v", err)
	}
	if result.ClusterID != clusterA {
		t.Fatalf("survivor cluster = %d, want %d", result.ClusterID, clusterA)
	}
	if len(result.MergedClusterIDs) != 1 || result.MergedClusterIDs[0] != clusterB {
		t.Fatalf("merged cluster ids = %#v, want [%d]", result.MergedClusterIDs, clusterB)
	}
	if len(result.SupersededClusterIDs) != 1 || result.SupersededClusterIDs[0] != clusterB {
		t.Fatalf("superseded cluster ids = %#v, want [%d]", result.SupersededClusterIDs, clusterB)
	}

	assertEventReviewClusterState(t, db, clusterA, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	assertEventReviewClusterState(t, db, clusterB, string(seedstore.EventReviewClusterStatusSuperseded), 2, &clusterA)

	var status string
	if err := db.QueryRow(`
		SELECT status
		FROM event_review_resolutions
		WHERE cluster_id = ?
	`, clusterB).Scan(&status); err != nil {
		t.Fatalf("load supersede resolution: %v", err)
	}
	if status != string(seedstore.EventReviewResolutionStatusSuperseded) {
		t.Fatalf("resolution status = %q, want %q", status, seedstore.EventReviewResolutionStatusSuperseded)
	}

	var activeIdentityLinks int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_identity_keys
		WHERE cluster_id = ?
			AND active = 1
	`, clusterB).Scan(&activeIdentityLinks); err != nil {
		t.Fatalf("count cluster B active identity links: %v", err)
	}
	if activeIdentityLinks != 0 {
		t.Fatalf("cluster B active identity links = %d, want 0", activeIdentityLinks)
	}
	var activeEvidenceLinks int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, clusterB).Scan(&activeEvidenceLinks); err != nil {
		t.Fatalf("count cluster B active evidence links: %v", err)
	}
	if activeEvidenceLinks != 0 {
		t.Fatalf("cluster B active evidence links = %d, want 0", activeEvidenceLinks)
	}
}

func TestStageEventReviewEvidencePrefersStagingKeyClusterAndPrunesStaleChoices(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	exactIdentityKey := "staging-key-match"
	exactIdentityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, exactIdentityKey)
	exactIdentityKeyID := insertEventReviewIdentityKeyOK(t, db, exactIdentityHash, seedstore.EventReviewIdentityKeyKindExact, exactIdentityKey)
	clusterA := int64(5)
	insertEventReviewClusterAtID(t, db, clusterA, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	clusterB := insertEventReviewClusterWithStagingKeyOK(t, db, string(seedstore.EventReviewClusterStatusOpen), strPtr("staging-key-cluster"), 1, nil, nil, nil, "", "")
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterA, exactIdentityKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster A exact identity: %v", err)
	}
	staleSourceIdentityKey := "stale-source-key"
	staleSourceIdentityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindSource, eventReviewIdentityKeyVersion, staleSourceIdentityKey)
	staleSourceIdentityKeyID := insertEventReviewIdentityKeyOK(t, db, staleSourceIdentityHash, seedstore.EventReviewIdentityKeyKindSource, staleSourceIdentityKey)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterA, staleSourceIdentityKeyID, true, time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster A stale source identity: %v", err)
	}
	staleEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "stale-evidence-choice", `{"payload":"stale"}`)
	if _, err := insertEventReviewClusterEvidence(t, db, clusterA, staleEvidenceID, true, time.Date(2026, time.May, 15, 10, 2, 0, 0, time.UTC), nil, "stale evidence"); err != nil {
		t.Fatalf("insert cluster A stale evidence: %v", err)
	}
	staleEventID := lookupOrInsertTestEvent(t, db, "stale-event-choice")
	if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterA, sourceID, staleSourceIdentityKey, true, "stale source identity", time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert stale source identity choice: %v", err)
	}
	if _, err := insertEventReviewCanonicalChoice(t, db, clusterA, "canonical_event_choice", seedstore.EventReviewChoiceKindEvent, int64Ptr(staleEventID), nil, "stale event choice", time.Date(2026, time.May, 15, 10, 6, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert stale event choice: %v", err)
	}
	if _, err := insertEventReviewCanonicalChoice(t, db, clusterA, "canonical_evidence_choice", seedstore.EventReviewChoiceKindEvidence, nil, int64Ptr(staleEvidenceID), "stale evidence choice", time.Date(2026, time.May, 15, 10, 7, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert stale evidence choice: %v", err)
	}
	if _, err := insertEventReviewDraftChoice(t, db, clusterA, "draft_manual_choice", seedstore.EventReviewChoiceKindManual, nil, nil, "stale manual choice", time.Date(2026, time.May, 15, 10, 8, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert stale manual choice: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterB, exactIdentityKeyID, true, time.Date(2026, time.May, 15, 10, 9, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster B exact identity: %v", err)
	}

	runID := mustCreateImportRun(t, st, "staging key cluster preference")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/staging-key-cluster",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		StagingKey:          "staging-key-cluster",
		StagingKeyVersion:   1,
		EvidenceFingerprint: "staging-key-cluster-fingerprint",
		Payload:             `{"payload":"staging-key-cluster"}`,
		SourceIdentityKeys:  []string{"active-source-key"},
		ExactIdentityKeys:   []string{exactIdentityKey},
	})
	if err != nil {
		t.Fatalf("stage evidence: %v", err)
	}
	if result.ClusterID != clusterA {
		t.Fatalf("survivor cluster = %d, want %d", result.ClusterID, clusterA)
	}
	if _, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID}); err != nil {
		t.Fatalf("finalize cluster restage: %v", err)
	}
	if len(result.MergedClusterIDs) != 1 || result.MergedClusterIDs[0] != clusterB {
		t.Fatalf("merged cluster ids = %#v, want [%d]", result.MergedClusterIDs, clusterB)
	}
	if len(result.SupersededClusterIDs) != 1 || result.SupersededClusterIDs[0] != clusterB {
		t.Fatalf("superseded cluster ids = %#v, want [%d]", result.SupersededClusterIDs, clusterB)
	}

	assertEventReviewClusterState(t, db, clusterA, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	assertEventReviewClusterState(t, db, clusterB, string(seedstore.EventReviewClusterStatusSuperseded), 2, int64Ptr(clusterA))

	var sourceIdentityChoiceCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_source_identity_choices
		WHERE cluster_id = ?
	`, clusterA).Scan(&sourceIdentityChoiceCount); err != nil {
		t.Fatalf("count source identity choices: %v", err)
	}
	if sourceIdentityChoiceCount != 0 {
		t.Fatalf("source identity choice rows for cluster %d = %d, want 0", clusterA, sourceIdentityChoiceCount)
	}

	var canonicalCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_canonical_choices
		WHERE cluster_id = ?
			AND field_name IN (?, ?)
	`, clusterA, "canonical_event_choice", "canonical_evidence_choice").Scan(&canonicalCount); err != nil {
		t.Fatalf("count canonical choices: %v", err)
	}
	if canonicalCount != 0 {
		t.Fatalf("canonical choice rows for cluster %d = %d, want 0", clusterA, canonicalCount)
	}

	var draftCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_draft_choices
		WHERE cluster_id = ?
			AND field_name = ?
	`, clusterA, "draft_manual_choice").Scan(&draftCount); err != nil {
		t.Fatalf("count draft choices: %v", err)
	}
	if draftCount != 0 {
		t.Fatalf("draft choice rows for cluster %d = %d, want 0", clusterA, draftCount)
	}

	var staleEvidenceActive int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND evidence_id = ?
			AND active = 1
	`, clusterA, staleEvidenceID).Scan(&staleEvidenceActive); err != nil {
		t.Fatalf("count stale active evidence links: %v", err)
	}
	if staleEvidenceActive != 0 {
		t.Fatalf("stale evidence active links = %d, want 0", staleEvidenceActive)
	}

	var staleIdentityActive int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_identity_keys
		WHERE cluster_id = ?
			AND identity_key_id = ?
			AND active = 1
	`, clusterA, staleSourceIdentityKeyID).Scan(&staleIdentityActive); err != nil {
		t.Fatalf("count stale active identity links: %v", err)
	}
	if staleIdentityActive != 0 {
		t.Fatalf("stale identity active links = %d, want 0", staleIdentityActive)
	}
}

func TestStageEventReviewEvidencePreservesCanonicalEventChoicesDuringPrune(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	clusterID := insertEventReviewClusterWithStagingKeyOK(t, db, string(seedstore.EventReviewClusterStatusOpen), strPtr("canonical-prune"), 1, nil, nil, nil, "", "")
	canonicalEventID := lookupOrInsertTestEvent(t, db, "canonical-choice-event")
	staleEventID := lookupOrInsertTestEvent(t, db, "stale-choice-event")
	exactIdentityKeyHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, "canonical-prune-key")
	exactIdentityKeyID := insertEventReviewIdentityKeyOK(t, db, exactIdentityKeyHash, seedstore.EventReviewIdentityKeyKindExact, "canonical-prune-key")
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, exactIdentityKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert canonical cluster identity: %v", err)
	}
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET canonical_event_id = ?
		WHERE id = ?
	`, canonicalEventID, clusterID); err != nil {
		t.Fatalf("set canonical event id: %v", err)
	}
	if _, err := insertEventReviewCanonicalChoice(t, db, clusterID, "canonical_event_choice", seedstore.EventReviewChoiceKindEvent, int64Ptr(canonicalEventID), nil, "keep canonical", time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert canonical event choice: %v", err)
	}
	if _, err := insertEventReviewCanonicalChoice(t, db, clusterID, "stale_event_choice", seedstore.EventReviewChoiceKindEvent, int64Ptr(staleEventID), nil, "remove stale", time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert stale event choice: %v", err)
	}

	runID := mustCreateImportRun(t, st, "canonical choice prune")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/canonical-choice",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		StagingKey:          "canonical-prune",
		StagingKeyVersion:   1,
		EvidenceFingerprint: "canonical-prune-fingerprint",
		Payload:             `{"payload":"canonical"}`,
		ExactIdentityKeys:   []string{"canonical-prune-key"},
	})
	if err != nil {
		t.Fatalf("stage canonical prune evidence: %v", err)
	}
	if result.ClusterID != clusterID {
		t.Fatalf("cluster id = %d, want %d", result.ClusterID, clusterID)
	}
	if _, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID}); err != nil {
		t.Fatalf("finalize canonical prune cluster: %v", err)
	}

	var canonicalCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_canonical_choices
		WHERE cluster_id = ?
			AND field_name = ?
	`, clusterID, "canonical_event_choice").Scan(&canonicalCount); err != nil {
		t.Fatalf("count canonical choice: %v", err)
	}
	if canonicalCount != 1 {
		t.Fatalf("canonical event choice rows = %d, want 1", canonicalCount)
	}
	var staleCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_canonical_choices
		WHERE cluster_id = ?
			AND field_name = ?
	`, clusterID, "stale_event_choice").Scan(&staleCount); err != nil {
		t.Fatalf("count stale event choice: %v", err)
	}
	if staleCount != 0 {
		t.Fatalf("stale event choice rows = %d, want 0", staleCount)
	}
}

func TestStageEventReviewEvidencePrefersLowestOpenClusterAndMovesActiveEvidence(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	identityKey := "lowest-survivor"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	insertEventReviewClusterAtID(t, db, 5, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	insertEventReviewClusterAtID(t, db, 10, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)

	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "lowest-survivor-fingerprint", `{"payload":"seed"}`)
	if _, err := insertEventReviewClusterEvidence(t, db, 10, evidenceID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil, "seed active"); err != nil {
		t.Fatalf("insert active evidence link: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, 5, identityKeyID, true, time.Date(2026, time.May, 15, 10, 1, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster 5 identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, 10, identityKeyID, true, time.Date(2026, time.May, 15, 10, 2, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster 10 identity: %v", err)
	}

	runID := mustCreateImportRun(t, st, "lowest survivor")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/lowest-survivor",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "lowest-survivor-fingerprint",
		Payload:             `{"payload":"restaged"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("lowest-survivor-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage lowest survivor evidence: %v", err)
	}
	if result.ClusterID != 5 {
		t.Fatalf("survivor cluster = %d, want 5", result.ClusterID)
	}
	if len(result.MergedClusterIDs) != 1 || result.MergedClusterIDs[0] != 10 {
		t.Fatalf("merged cluster ids = %#v, want [10]", result.MergedClusterIDs)
	}
	if len(result.SupersededClusterIDs) != 1 || result.SupersededClusterIDs[0] != 10 {
		t.Fatalf("superseded cluster ids = %#v, want [10]", result.SupersededClusterIDs)
	}

	assertEventReviewClusterState(t, db, 5, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	assertEventReviewClusterState(t, db, 10, string(seedstore.EventReviewClusterStatusSuperseded), 2, int64Ptr(5))

	var activeEvidenceCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, 5).Scan(&activeEvidenceCount); err != nil {
		t.Fatalf("count cluster 5 active evidence: %v", err)
	}
	if activeEvidenceCount != 1 {
		t.Fatalf("cluster 5 active evidence links = %d, want 1", activeEvidenceCount)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, 10).Scan(&activeEvidenceCount); err != nil {
		t.Fatalf("count cluster 10 active evidence: %v", err)
	}
	if activeEvidenceCount != 0 {
		t.Fatalf("cluster 10 active evidence links = %d, want 0", activeEvidenceCount)
	}
}

func TestStageEventReviewEvidenceFillsEventIDAfterSafeRestageAndBlocksLaterSeparatedEvent(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "duplicate-event-a", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "duplicate-event-b", venueID, sourceID, domain.OriginLive)
	eventA := lookupEventIDBySlug(t, db, "duplicate-event-a")
	eventB := lookupEventIDBySlug(t, db, "duplicate-event-b")

	runOne := mustCreateImportRun(t, st, "duplicate-fingerprint-first")
	first, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runOne},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/duplicate",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "duplicate-fingerprint",
		Payload:             `{"payload":"first"}`,
		ExactIdentityKeys:   []string{"duplicate-identity"},
		StagingKey:          eventReviewTestStagingKey("duplicate-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage first duplicate evidence: %v", err)
	}
	var storedEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, first.EvidenceID).Scan(&storedEventID); err != nil {
		t.Fatalf("load first stored event id: %v", err)
	}
	if storedEventID.Valid {
		t.Fatalf("stored event_id after first stage = %#v, want null", storedEventID)
	}

	runTwo := mustCreateImportRun(t, st, "duplicate-fingerprint-second")
	second, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runTwo},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/duplicate",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EventID:             &eventA,
		EvidenceFingerprint: "duplicate-fingerprint",
		Payload:             `{"payload":"second"}`,
		ExactIdentityKeys:   []string{"duplicate-identity"},
		StagingKey:          eventReviewTestStagingKey("duplicate-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage safe duplicate evidence: %v", err)
	}
	if second.ClusterID != first.ClusterID {
		t.Fatalf("second cluster id = %d, want %d", second.ClusterID, first.ClusterID)
	}

	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, first.EvidenceID).Scan(&storedEventID); err != nil {
		t.Fatalf("load stored event id after fill: %v", err)
	}
	if !storedEventID.Valid || storedEventID.Int64 != eventA {
		t.Fatalf("stored event_id after safe fill = %#v, want %d", storedEventID, eventA)
	}

	endpointA := seedstore.EventReviewSeparationEndpoint{
		Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
		Key:     seedstore.EventReviewSeparationEventEndpointKey(eventA),
		EventID: int64Ptr(eventA),
	}
	endpointB := seedstore.EventReviewSeparationEndpoint{
		Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
		Key:     seedstore.EventReviewSeparationEventEndpointKey(eventB),
		EventID: int64Ptr(eventB),
	}
	if _, err := insertEventReviewSeparation(t, db, endpointA, endpointB, true, "block duplicate restage", time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert event separation: %v", err)
	}

	runThree := mustCreateImportRun(t, st, "duplicate-fingerprint-third")
	if _, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runThree},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/duplicate",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EventID:             &eventB,
		EvidenceFingerprint: "duplicate-fingerprint",
		Payload:             `{"payload":"third"}`,
		ExactIdentityKeys:   []string{"duplicate-identity"},
		StagingKey:          eventReviewTestStagingKey("duplicate-fingerprint"),
		StagingKeyVersion:   1,
	}); err == nil || !strings.Contains(err.Error(), "conflicting proposed endpoints") {
		t.Fatalf("stage duplicate restage error = %v, want separation conflict", err)
	}

	if got := mustCount(t, db, "event_review_clusters"); got != 1 {
		t.Fatalf("event_review_clusters rows = %d, want 1", got)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != 1 {
		t.Fatalf("event_review_evidence rows = %d, want 1", got)
	}
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_review_evidence
		WHERE id = ?
	`, first.EvidenceID).Scan(&storedEventID); err != nil {
		t.Fatalf("load stored event id: %v", err)
	}
	if !storedEventID.Valid || storedEventID.Int64 != eventA {
		t.Fatalf("stored event_id = %#v, want %d", storedEventID, eventA)
	}
}

func TestStageEventReviewEvidenceSkipsLowerSurvivorWhenActiveHigherClusterConflicts(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	identityA := "handoff-a"
	identityB := "handoff-b"
	hashA := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityA)
	hashB := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityB)
	identityAID := insertEventReviewIdentityKeyOK(t, db, hashA, seedstore.EventReviewIdentityKeyKindExact, identityA)
	identityBID := insertEventReviewIdentityKeyOK(t, db, hashB, seedstore.EventReviewIdentityKeyKindExact, identityB)

	insertEventReviewClusterAtID(t, db, 5, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	insertEventReviewClusterAtID(t, db, 10, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := insertEventReviewClusterIdentityKey(t, db, 5, identityAID, true, time.Date(2026, time.May, 15, 10, 40, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster 5 identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, 10, identityBID, true, time.Date(2026, time.May, 15, 10, 41, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert cluster 10 identity: %v", err)
	}

	runOne := mustCreateImportRun(t, st, "handoff-seed")
	seedResult, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runOne},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/handoff",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "handoff-fingerprint",
		Payload:             `{"payload":"seed"}`,
		ExactIdentityKeys:   []string{identityB},
		StagingKey:          eventReviewTestStagingKey("handoff-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage seed handoff evidence: %v", err)
	}
	if seedResult.ClusterID != 10 {
		t.Fatalf("seed cluster id = %d, want 10", seedResult.ClusterID)
	}

	endpointA := seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           EventReviewSeparationEndpointKeyIdentity(hashA),
		IdentityKeyID: int64Ptr(identityAID),
	}
	endpointB := seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           EventReviewSeparationEndpointKeyIdentity(hashB),
		IdentityKeyID: int64Ptr(identityBID),
	}
	if endpointA.Key > endpointB.Key {
		endpointA, endpointB = endpointB, endpointA
	}
	if _, err := insertEventReviewSeparation(t, db, endpointA, endpointB, true, "block survivor handoff", time.Date(2026, time.May, 15, 10, 42, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 42, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert handoff separation: %v", err)
	}

	runTwo := mustCreateImportRun(t, st, "handoff-restage")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runTwo},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/handoff",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "handoff-fingerprint",
		Payload:             `{"payload":"restage"}`,
		ExactIdentityKeys:   []string{identityA},
		StagingKey:          eventReviewTestStagingKey("handoff-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err == nil {
		t.Fatalf("expected survivor handoff conflict, got result %#v", result)
	}
	if !result.RetryableConflict {
		t.Fatalf("retryable conflict flag = %v, want true", result.RetryableConflict)
	}

	assertEventReviewClusterState(t, db, 5, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	assertEventReviewClusterState(t, db, 10, string(seedstore.EventReviewClusterStatusOpen), 1, nil)

	var activeEvidenceCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, 10).Scan(&activeEvidenceCount); err != nil {
		t.Fatalf("count cluster 10 active evidence: %v", err)
	}
	if activeEvidenceCount != 1 {
		t.Fatalf("cluster 10 active evidence = %d, want 1", activeEvidenceCount)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, 5).Scan(&activeEvidenceCount); err != nil {
		t.Fatalf("count cluster 5 active evidence: %v", err)
	}
	if activeEvidenceCount != 0 {
		t.Fatalf("cluster 5 active evidence = %d, want 0", activeEvidenceCount)
	}
}

func TestStageEventReviewEvidenceRejectsSeparatedProposedEndpointsBeforeClusterCreation(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "proposed-separation-event", venueID, sourceID, domain.OriginLive)
	eventID := lookupEventIDBySlug(t, db, "proposed-separation-event")
	identityKey := "proposed-separation-identity"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	endpointA := seedstore.EventReviewSeparationEndpoint{
		Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
		Key:     seedstore.EventReviewSeparationEventEndpointKey(eventID),
		EventID: int64Ptr(eventID),
	}
	endpointB := seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           EventReviewSeparationEndpointKeyIdentity(identityHash),
		IdentityKeyID: int64Ptr(identityKeyID),
	}
	if _, err := insertEventReviewSeparation(t, db, endpointA, endpointB, true, "block proposed endpoints", time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert proposed separation: %v", err)
	}

	runID := mustCreateImportRun(t, st, "proposed separation")
	if _, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/proposed-separation",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EventID:             &eventID,
		EvidenceFingerprint: "proposed-separation-fingerprint",
		Payload:             `{"payload":"blocked"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("proposed-separation-fingerprint"),
		StagingKeyVersion:   1,
	}); err == nil {
		t.Fatal("expected proposed endpoint separation conflict")
	}

	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event_review_clusters rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != 0 {
		t.Fatalf("event_review_evidence rows = %d, want 0", got)
	}
}

func TestStageEventReviewEvidenceReusesExistingTerminalClusterForSameFingerprint(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	resolvedCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, nil, nil)
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET conflict_type = ?,
			conflict_reason = ?
		WHERE id = ?
	`, "terminal-conflict", "terminal conflict reason", resolvedCluster); err != nil {
		t.Fatalf("seed terminal metadata: %v", err)
	}
	insertEventReviewResolutionOK(t, db, resolvedCluster, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")
	identityKey := "terminal-open-match"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	openCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := insertEventReviewClusterIdentityKey(t, db, openCluster, identityKeyID, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert open cluster identity: %v", err)
	}
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "terminal-reuse-fingerprint", `{"payload":"terminal"}`)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, identityKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert terminal evidence identity: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, resolvedCluster, evidenceID, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil, "seed terminal"); err != nil {
		t.Fatalf("insert terminal evidence link: %v", err)
	}
	var beforePayload, beforeUpdatedAt string
	var beforeEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT payload, event_id, updated_at
		FROM event_review_evidence
		WHERE id = ?
	`, evidenceID).Scan(&beforePayload, &beforeEventID, &beforeUpdatedAt); err != nil {
		t.Fatalf("load terminal evidence before replay: %v", err)
	}
	var beforeEvidenceIdentityLinks int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_evidence_identity_keys
		WHERE evidence_id = ?
	`, evidenceID).Scan(&beforeEvidenceIdentityLinks); err != nil {
		t.Fatalf("count terminal evidence identity links before replay: %v", err)
	}

	runID := mustCreateImportRun(t, st, "terminal reuse")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/terminal-reuse",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "terminal-reuse-fingerprint",
		Payload:             `{"payload":"terminal"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("terminal-reuse-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage terminal reuse evidence: %v", err)
	}
	if result.ClusterID != resolvedCluster {
		t.Fatalf("cluster id = %d, want %d", result.ClusterID, resolvedCluster)
	}
	if result.ClusterCreated {
		t.Fatalf("terminal exact replay unexpectedly created a cluster: %#v", result)
	}
	if !result.ClusterReused || !result.Reused || result.Created {
		t.Fatalf("terminal exact replay result = %#v, want reused cluster/evidence without creating a new evidence row", result)
	}
	if result.EvidenceID != evidenceID {
		t.Fatalf("evidence id = %d, want terminal evidence %d", result.EvidenceID, evidenceID)
	}

	if got := mustCount(t, db, "event_review_clusters"); got != 2 {
		t.Fatalf("event_review_clusters rows = %d, want 2", got)
	}
	var linkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM import_run_event_review_clusters
		WHERE cluster_id = ?
	`, resolvedCluster).Scan(&linkCount); err != nil {
		t.Fatalf("count terminal run links: %v", err)
	}
	if linkCount != 1 {
		t.Fatalf("terminal cluster run links = %d, want 1", linkCount)
	}
	if got := mustCount(t, db, "import_run_event_review_evidence"); got != 1 {
		t.Fatalf("import_run_event_review_evidence rows = %d, want 1", got)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, resolvedCluster).Scan(&linkCount); err != nil {
		t.Fatalf("count terminal cluster evidence links: %v", err)
	}
	if linkCount != 1 {
		t.Fatalf("terminal cluster evidence links = %d, want 1", linkCount)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, openCluster).Scan(&linkCount); err != nil {
		t.Fatalf("count open cluster evidence links: %v", err)
	}
	if linkCount != 0 {
		t.Fatalf("open cluster evidence links = %d, want 0", linkCount)
	}
	var afterPayload, afterUpdatedAt string
	var afterEventID sql.NullInt64
	if err := db.QueryRow(`
		SELECT payload, event_id, updated_at
		FROM event_review_evidence
		WHERE id = ?
	`, evidenceID).Scan(&afterPayload, &afterEventID, &afterUpdatedAt); err != nil {
		t.Fatalf("load terminal evidence after replay: %v", err)
	}
	if afterPayload != beforePayload || afterEventID.Valid != beforeEventID.Valid || afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("terminal evidence mutated: before payload/event/updated=(%q,%#v,%q), after=(%q,%#v,%q)", beforePayload, beforeEventID, beforeUpdatedAt, afterPayload, afterEventID, afterUpdatedAt)
	}
	var afterEvidenceIdentityLinks int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_evidence_identity_keys
		WHERE evidence_id = ?
	`, evidenceID).Scan(&afterEvidenceIdentityLinks); err != nil {
		t.Fatalf("count terminal evidence identity links after replay: %v", err)
	}
	if afterEvidenceIdentityLinks != beforeEvidenceIdentityLinks {
		t.Fatalf("terminal evidence identity links = %d, want %d", afterEvidenceIdentityLinks, beforeEvidenceIdentityLinks)
	}
	var conflictType, conflictReason string
	if err := db.QueryRow(`
		SELECT conflict_type, conflict_reason
		FROM event_review_clusters
		WHERE id = ?
	`, resolvedCluster).Scan(&conflictType, &conflictReason); err != nil {
		t.Fatalf("load terminal cluster metadata: %v", err)
	}
	if conflictType != "terminal-conflict" {
		t.Fatalf("terminal conflict_type = %q, want %q", conflictType, "terminal-conflict")
	}
	if conflictReason != "terminal conflict reason" {
		t.Fatalf("terminal conflict_reason = %q, want %q", conflictReason, "terminal conflict reason")
	}
}

func TestStageEventReviewEvidenceAutoResolvesCanonicalExactMatchAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "live-legacy-event-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "live-legacy-event-leadmill-20260510190000")
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?
		WHERE id = ?
	`, string(domain.PublicationStateProvisional), canonicalEventID); err != nil {
		t.Fatalf("mark canonical event provisional: %v", err)
	}

	identityKey := "canonical-exact-match"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert canonical identity link: %v", err)
	}

	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	beforeReviewGroups := mustCount(t, db, "review_groups")

	runID := mustCreateImportRun(t, st, "canonical exact match")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "canonical-exact-match-fingerprint",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"canonical-exact-match",
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
		StagingKey:        eventReviewTestStagingKey("canonical-exact-match-fingerprint"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage canonical exact evidence: %v", err)
	}
	if result.AutoResolved {
		t.Fatalf("canonical exact stage result unexpectedly auto-resolved: %#v", result)
	}
	if result.ClusterID != clusterID {
		t.Fatalf("cluster id = %d, want %d", result.ClusterID, clusterID)
	}
	secondarySourceID := insertStoreNamedSource(t, db, "Secondary canonical source", "https://secondary.example.test/listing")
	secondaryEvidenceID := insertEventReviewEvidenceOK(t, db, secondarySourceID, nil, "canonical-exact-secondary-fingerprint", `{
		"source_authority":"supporting",
		"source_name":"Secondary canonical source",
		"source_url":"https://secondary.example.test/listing",
		"candidate_external_id":"canonical-exact-match-secondary",
		"candidate_title":"Legacy Event",
		"candidate_venue_slug":"leadmill",
		"candidate_start_at":"2026-05-10T19:00:00Z",
		"candidate_end_at":"2026-05-10T22:00:00Z",
		"candidate_genre":"Indie",
		"candidate_status":"Listed",
		"candidate_description":"Legacy event",
		"calendar_url":"https://secondary.example.test/calendar.ics"
	}`)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, secondaryEvidenceID, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil, "secondary canonical exact evidence")
	beforeObservations := mustCount(t, db, "event_source_attribute_observations")
	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID, secondaryEvidenceID})
	if err != nil {
		t.Fatalf("finalize canonical exact cluster: %v", err)
	}
	if resolution == nil || resolution.AppliedAutoResolution == nil {
		t.Fatal("finalized canonical exact cluster missing applied auto-resolution summary")
	}
	if resolution.AppliedAutoResolution.Result != "canonical_exact_match" || resolution.AppliedAutoResolution.EventID != canonicalEventID || resolution.AppliedAutoResolution.EventSlug != "live-legacy-event-leadmill-20260510190000" || resolution.AppliedAutoResolution.SourceID != sourceID || resolution.AppliedAutoResolution.SourceName != "Store test source" || resolution.AppliedAutoResolution.SourceURL != "https://example.test/store-test" || resolution.AppliedAutoResolution.EvidenceCount != 2 {
		t.Fatalf("canonical exact finalized auto-resolution = %#v", resolution.AppliedAutoResolution)
	}
	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)

	detail, ok, err := st.LoadEventReviewCluster(ctx, clusterID)
	if err != nil {
		t.Fatalf("load canonical exact cluster: %v", err)
	}
	if !ok || detail.Resolution == nil || detail.Resolution.AppliedAutoResolution == nil {
		t.Fatal("resolved canonical exact cluster missing applied auto-resolution summary")
	}
	applied := detail.Resolution.AppliedAutoResolution
	if applied.Result != "canonical_exact_match" || applied.EventID != canonicalEventID || applied.EventSlug != "live-legacy-event-leadmill-20260510190000" || applied.SourceID != sourceID || applied.SourceName != "Store test source" || applied.SourceURL != "https://example.test/store-test" || applied.EvidenceCount != 2 {
		t.Fatalf("canonical exact applied auto-resolution = %#v", applied)
	}
	if detail.Summary.CanonicalEventID == nil || *detail.Summary.CanonicalEventID != canonicalEventID {
		t.Fatalf("canonical exact summary canonical_event_id = %#v, want %d", detail.Summary.CanonicalEventID, canonicalEventID)
	}
	for _, evidenceID := range []int64{result.EvidenceID, secondaryEvidenceID} {
		var linkedEventID sql.NullInt64
		if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, evidenceID).Scan(&linkedEventID); err != nil {
			t.Fatalf("load canonical exact evidence event_id: %v", err)
		}
		if !linkedEventID.Valid || linkedEventID.Int64 != canonicalEventID {
			t.Fatalf("evidence %d event_id = %#v, want %d", evidenceID, linkedEventID, canonicalEventID)
		}
	}
	var publicationState string
	if err := db.QueryRow(`SELECT publication_state FROM events WHERE id = ?`, canonicalEventID).Scan(&publicationState); err != nil {
		t.Fatalf("load canonical exact publication state: %v", err)
	}
	if publicationState != string(domain.PublicationStateReviewed) {
		t.Fatalf("canonical exact publication state = %q, want %q", publicationState, domain.PublicationStateReviewed)
	}
	for _, tc := range []struct {
		sourceID int64
		key      string
	}{
		{sourceID: sourceID, key: "uid:canonical-exact-match"},
		{sourceID: secondarySourceID, key: "uid:canonical-exact-match-secondary"},
	} {
		var linkedEventID int64
		var authoritative int
		if err := db.QueryRow(`
			SELECT event_id, is_authoritative
			FROM event_source_links
			WHERE source_id = ? AND source_event_key = ?
		`, tc.sourceID, tc.key).Scan(&linkedEventID, &authoritative); err != nil {
			t.Fatalf("load canonical exact source link %q: %v", tc.key, err)
		}
		if linkedEventID != canonicalEventID || authoritative != 0 {
			t.Fatalf("source link %q = event %d authoritative %d, want event %d supporting", tc.key, linkedEventID, authoritative, canonicalEventID)
		}
	}
	var secondaryInfoRows int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_secondary_source_info
		WHERE event_id = ?
			AND source_id = ?
			AND info_type IN ('genre', 'description')
	`, canonicalEventID, secondarySourceID).Scan(&secondaryInfoRows); err != nil {
		t.Fatalf("count canonical exact secondary info: %v", err)
	}
	if secondaryInfoRows != 2 {
		t.Fatalf("canonical exact secondary info rows = %d, want 2", secondaryInfoRows)
	}
	if got := mustCount(t, db, "event_source_attribute_observations"); got <= beforeObservations {
		t.Fatalf("event_source_attribute_observations = %d, want greater than %d", got, beforeObservations)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions+1 {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions+1)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}

	replayRunID := mustCreateImportRun(t, st, "canonical exact replay")
	replay, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: replayRunID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "canonical-exact-match-fingerprint",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"canonical-exact-match",
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
		StagingKey:        eventReviewTestStagingKey("canonical-exact-match-fingerprint"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("replay canonical exact evidence: %v", err)
	}
	if replay.ClusterID != clusterID || replay.EvidenceID != result.EvidenceID || !replay.ClusterReused || replay.ClusterCreated || replay.Created {
		t.Fatalf("canonical exact replay result = %#v", replay)
	}
	if !replay.AutoResolved || replay.AutoResolvedResult != "canonical_exact_match" || replay.CanonicalEventSlug != "live-legacy-event-leadmill-20260510190000" {
		t.Fatalf("canonical exact replay result = %#v, want terminal auto-resolution metadata", replay)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions+1 {
		t.Fatalf("event_review_resolutions rows after replay = %d, want %d", got, beforeResolutions+1)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows after replay = %d, want %d", got, beforeReviewGroups)
	}
}

func TestCanonicalExactSupportingProvenanceSkipsAmbiguousSourceLinkWithoutPartialWrites(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "ambiguous-canonical-exact-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "ambiguous-canonical-exact-leadmill-20260510190000")
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?
		WHERE id = ?
	`, string(domain.PublicationStateProvisional), canonicalEventID); err != nil {
		t.Fatalf("mark ambiguous canonical event provisional: %v", err)
	}
	insertLegacyEvent(t, db, "ambiguous-canonical-exact-other-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	otherEventID := lookupEventIDBySlug(t, db, "ambiguous-canonical-exact-other-leadmill-20260510190000")
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			event_id,
			source_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 0, ?, ?)
	`, otherEventID, sourceID, "uid:ambiguous-canonical-exact", "2026-05-15T09:00:00Z", "2026-05-15T09:00:00Z"); err != nil {
		t.Fatalf("insert ambiguous canonical exact source link: %v", err)
	}

	identityKey := "ambiguous-canonical-exact"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert ambiguous canonical identity link: %v", err)
	}

	runID := mustCreateImportRun(t, st, "ambiguous canonical exact")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "ambiguous-canonical-exact-fingerprint",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"ambiguous-canonical-exact",
			"candidate_title":"Legacy Event",
			"candidate_venue_slug":"leadmill",
			"candidate_start_at":"2026-05-10T19:00:00Z",
			"candidate_end_at":"2026-05-10T22:00:00Z",
			"candidate_genre":"Indie",
			"candidate_status":"Listed",
			"candidate_description":"Legacy event"
		}`,
		ExactIdentityKeys: []string{identityKey},
		StagingKey:        eventReviewTestStagingKey("ambiguous-canonical-exact-fingerprint"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage ambiguous canonical exact evidence: %v", err)
	}
	beforeObservations := mustCount(t, db, "event_source_attribute_observations")
	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID})
	if err != nil {
		t.Fatalf("finalize ambiguous canonical exact cluster: %v", err)
	}
	if resolution != nil {
		t.Fatalf("ambiguous canonical exact resolution = %#v, want nil", resolution)
	}
	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	var evidenceEventID sql.NullInt64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, result.EvidenceID).Scan(&evidenceEventID); err != nil {
		t.Fatalf("load ambiguous canonical exact evidence event_id: %v", err)
	}
	if evidenceEventID.Valid {
		t.Fatalf("ambiguous canonical exact evidence event_id = %d, want NULL", evidenceEventID.Int64)
	}
	var publicationState string
	if err := db.QueryRow(`SELECT publication_state FROM events WHERE id = ?`, canonicalEventID).Scan(&publicationState); err != nil {
		t.Fatalf("load ambiguous canonical exact publication state: %v", err)
	}
	if publicationState != string(domain.PublicationStateProvisional) {
		t.Fatalf("ambiguous canonical exact publication state = %q, want %q", publicationState, domain.PublicationStateProvisional)
	}
	var linkedEventID int64
	if err := db.QueryRow(`
		SELECT event_id
		FROM event_source_links
		WHERE source_id = ? AND source_event_key = ?
	`, sourceID, "uid:ambiguous-canonical-exact").Scan(&linkedEventID); err != nil {
		t.Fatalf("load ambiguous canonical exact source link: %v", err)
	}
	if linkedEventID != otherEventID {
		t.Fatalf("ambiguous canonical exact source link event_id = %d, want %d", linkedEventID, otherEventID)
	}
	if got := mustCount(t, db, "event_source_attribute_observations"); got != beforeObservations {
		t.Fatalf("event_source_attribute_observations = %d, want %d", got, beforeObservations)
	}
	var secondaryInfoRows int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_secondary_source_info
		WHERE event_id = ?
	`, canonicalEventID).Scan(&secondaryInfoRows); err != nil {
		t.Fatalf("count ambiguous canonical exact secondary info: %v", err)
	}
	if secondaryInfoRows != 0 {
		t.Fatalf("ambiguous canonical exact secondary info rows = %d, want 0", secondaryInfoRows)
	}
}

func TestCanonicalExactAuthoritativeDoesNotHarvestSupportingProvenance(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	authoritativeSourceID := insertStoreNamedSource(t, db, "Authoritative canonical source", "https://authoritative.example.test/listing")
	supportingSourceID := insertStoreNamedSource(t, db, "Supporting canonical source", "https://supporting.example.test/listing")
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "authoritative-canonical-exact-leadmill-20260510190000", venueID, authoritativeSourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "authoritative-canonical-exact-leadmill-20260510190000")
	if _, err := db.Exec(`
		UPDATE events
		SET genre = ?
		WHERE id = ?
	`, "Indie", canonicalEventID); err != nil {
		t.Fatalf("seed authoritative canonical genre: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_genres (
			event_id,
			name,
			rank,
			score,
			mention_count,
			earliest_position,
			created_at,
			updated_at
		) VALUES (?, ?, 1, 1.0, 1, 0, ?, ?)
	`, canonicalEventID, "Existing genre", "2026-05-15T09:00:00Z", "2026-05-15T09:00:00Z"); err != nil {
		t.Fatalf("seed authoritative canonical event genre row: %v", err)
	}

	identityKey := "authoritative-canonical-exact"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert authoritative canonical identity link: %v", err)
	}

	runID := mustCreateImportRun(t, st, "authoritative canonical exact")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            authoritativeSourceID,
		SourceName:          "Authoritative canonical source",
		SourceURL:           "https://authoritative.example.test/listing",
		SourceAuthority:     seedstore.SourceAuthorityAuthoritative,
		EvidenceFingerprint: "authoritative-canonical-exact-fingerprint",
		Payload: `{
			"source_authority":"authoritative",
			"source_name":"Authoritative canonical source",
			"source_url":"https://authoritative.example.test/listing",
			"candidate_external_id":"authoritative-canonical-exact",
			"candidate_title":"Legacy Event",
			"candidate_venue_slug":"leadmill",
			"candidate_start_at":"2026-05-10T19:00:00Z",
			"candidate_end_at":"2026-05-10T22:00:00Z",
			"candidate_genre":"Indie",
			"candidate_status":"Listed",
			"candidate_description":"Legacy event"
		}`,
		ExactIdentityKeys: []string{identityKey},
		StagingKey:        eventReviewTestStagingKey("authoritative-canonical-exact-fingerprint"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage authoritative canonical exact evidence: %v", err)
	}
	supportingEvidenceID := insertEventReviewEvidenceOK(t, db, supportingSourceID, nil, "authoritative-canonical-supporting-fingerprint", `{
		"source_authority":"supporting",
		"source_name":"Supporting canonical source",
		"source_url":"https://supporting.example.test/listing",
		"candidate_external_id":"authoritative-canonical-supporting",
		"candidate_title":"Legacy Event",
		"candidate_venue_slug":"leadmill",
		"candidate_start_at":"2026-05-10T19:00:00Z",
		"candidate_end_at":"2026-05-10T22:00:00Z",
		"candidate_genre":"Indie",
		"candidate_status":"Listed",
		"candidate_description":"Legacy event"
	}`)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, supportingEvidenceID, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil, "supporting evidence in authoritative canonical exact cluster")

	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID, supportingEvidenceID})
	if err != nil {
		t.Fatalf("finalize authoritative canonical exact cluster: %v", err)
	}
	if resolution == nil || resolution.AppliedAutoResolution == nil || resolution.AppliedAutoResolution.Result != "canonical_exact_match" {
		t.Fatalf("authoritative canonical exact resolution = %#v", resolution)
	}
	for _, evidenceID := range []int64{result.EvidenceID, supportingEvidenceID} {
		var linkedEventID sql.NullInt64
		if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, evidenceID).Scan(&linkedEventID); err != nil {
			t.Fatalf("load authoritative canonical exact evidence event_id: %v", err)
		}
		if !linkedEventID.Valid || linkedEventID.Int64 != canonicalEventID {
			t.Fatalf("evidence %d event_id = %#v, want %d", evidenceID, linkedEventID, canonicalEventID)
		}
	}
	var supportingLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_links
		WHERE source_id = ? AND source_event_key = ?
	`, supportingSourceID, "uid:authoritative-canonical-supporting").Scan(&supportingLinkCount); err != nil {
		t.Fatalf("count supporting source links in authoritative canonical exact cluster: %v", err)
	}
	if supportingLinkCount != 0 {
		t.Fatalf("supporting source links in authoritative canonical exact cluster = %d, want 0", supportingLinkCount)
	}
	var supportingSecondaryInfoRows int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_secondary_source_info
		WHERE event_id = ? AND source_id = ?
	`, canonicalEventID, supportingSourceID).Scan(&supportingSecondaryInfoRows); err != nil {
		t.Fatalf("count supporting secondary info in authoritative canonical exact cluster: %v", err)
	}
	if supportingSecondaryInfoRows != 0 {
		t.Fatalf("supporting secondary info rows in authoritative canonical exact cluster = %d, want 0", supportingSecondaryInfoRows)
	}
	var supportingObservationRows int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_attribute_observations
		WHERE source_id = ?
	`, supportingSourceID).Scan(&supportingObservationRows); err != nil {
		t.Fatalf("count supporting observations in authoritative canonical exact cluster: %v", err)
	}
	if supportingObservationRows != 0 {
		t.Fatalf("supporting observations in authoritative canonical exact cluster = %d, want 0", supportingObservationRows)
	}
	var genreText string
	if err := db.QueryRow(`SELECT genre FROM events WHERE id = ?`, canonicalEventID).Scan(&genreText); err != nil {
		t.Fatalf("load authoritative canonical genre: %v", err)
	}
	if genreText != "Indie" {
		t.Fatalf("authoritative canonical genre = %q, want unchanged", genreText)
	}
	var eventGenreRows int
	var eventGenreName string
	if err := db.QueryRow(`
		SELECT COUNT(*), COALESCE(MAX(name), '')
		FROM event_genres
		WHERE event_id = ?
	`, canonicalEventID).Scan(&eventGenreRows, &eventGenreName); err != nil {
		t.Fatalf("load authoritative canonical event genres: %v", err)
	}
	if eventGenreRows != 1 || eventGenreName != "Existing genre" {
		t.Fatalf("authoritative canonical event_genres = %d/%q, want existing row unchanged", eventGenreRows, eventGenreName)
	}
}

func TestCanonicalExactBlocksConflictingEvidenceEventID(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "conflicting-evidence-canonical-exact-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "conflicting-evidence-canonical-exact-leadmill-20260510190000")
	insertLegacyEvent(t, db, "conflicting-evidence-other-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	otherEventID := lookupEventIDBySlug(t, db, "conflicting-evidence-other-leadmill-20260510190000")

	identityKey := "conflicting-evidence-canonical-exact"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert conflicting evidence canonical identity link: %v", err)
	}
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, &otherEventID, "conflicting-evidence-canonical-exact-fingerprint", `{
		"source_authority":"supporting",
		"source_name":"Store test source",
		"source_url":"https://example.test/store-test",
		"candidate_external_id":"conflicting-evidence-canonical-exact",
		"candidate_title":"Legacy Event",
		"candidate_venue_slug":"leadmill",
		"candidate_start_at":"2026-05-10T19:00:00Z",
		"candidate_end_at":"2026-05-10T22:00:00Z",
		"candidate_genre":"Indie",
		"candidate_status":"Listed",
		"candidate_description":"Legacy event"
	}`)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, evidenceID, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil, "conflicting evidence canonical exact")

	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, clusterID, []int64{evidenceID})
	if err != nil {
		t.Fatalf("finalize conflicting evidence canonical exact cluster: %v", err)
	}
	if resolution != nil {
		t.Fatalf("conflicting evidence canonical exact resolution = %#v, want nil", resolution)
	}
	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	var linkedEventID sql.NullInt64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, evidenceID).Scan(&linkedEventID); err != nil {
		t.Fatalf("load conflicting evidence event_id: %v", err)
	}
	if !linkedEventID.Valid || linkedEventID.Int64 != otherEventID {
		t.Fatalf("conflicting evidence event_id = %#v, want %d", linkedEventID, otherEventID)
	}
}

func TestCanonicalExactAuthoritativeBlocksConflictingEvidenceEventIDWithoutPartialWrites(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreNamedSource(t, db, "Authoritative conflict source", "https://authoritative-conflict.example.test/listing")
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "authoritative-conflicting-evidence-canonical-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "authoritative-conflicting-evidence-canonical-leadmill-20260510190000")
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?
		WHERE id = ?
	`, string(domain.PublicationStateProvisional), canonicalEventID); err != nil {
		t.Fatalf("mark authoritative conflicting canonical event provisional: %v", err)
	}
	insertLegacyEvent(t, db, "authoritative-conflicting-evidence-other-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	otherEventID := lookupEventIDBySlug(t, db, "authoritative-conflicting-evidence-other-leadmill-20260510190000")

	identityKey := "authoritative-conflicting-evidence"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert authoritative conflicting identity link: %v", err)
	}

	runID := mustCreateImportRun(t, st, "authoritative conflicting canonical exact")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Authoritative conflict source",
		SourceURL:           "https://authoritative-conflict.example.test/listing",
		SourceAuthority:     seedstore.SourceAuthorityAuthoritative,
		EventID:             &otherEventID,
		EvidenceFingerprint: "authoritative-conflicting-evidence-fingerprint",
		Payload: `{
			"source_authority":"authoritative",
			"source_name":"Authoritative conflict source",
			"source_url":"https://authoritative-conflict.example.test/listing",
			"candidate_external_id":"authoritative-conflicting-evidence",
			"candidate_title":"Legacy Event",
			"candidate_venue_slug":"leadmill",
			"candidate_start_at":"2026-05-10T19:00:00Z",
			"candidate_end_at":"2026-05-10T22:00:00Z",
			"candidate_genre":"Indie",
			"candidate_status":"Listed",
			"candidate_description":"Legacy event"
		}`,
		ExactIdentityKeys: []string{identityKey},
		StagingKey:        eventReviewTestStagingKey("authoritative-conflicting-evidence-fingerprint"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage authoritative conflicting canonical exact evidence: %v", err)
	}
	beforeObservations := mustCount(t, db, "event_source_attribute_observations")
	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID})
	if err != nil {
		t.Fatalf("finalize authoritative conflicting canonical exact cluster: %v", err)
	}
	if resolution != nil {
		t.Fatalf("authoritative conflicting canonical exact resolution = %#v, want nil", resolution)
	}
	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	var publicationState string
	if err := db.QueryRow(`SELECT publication_state FROM events WHERE id = ?`, canonicalEventID).Scan(&publicationState); err != nil {
		t.Fatalf("load authoritative conflicting canonical publication state: %v", err)
	}
	if publicationState != string(domain.PublicationStateProvisional) {
		t.Fatalf("authoritative conflicting canonical publication state = %q, want %q", publicationState, domain.PublicationStateProvisional)
	}
	var linkedEventID sql.NullInt64
	if err := db.QueryRow(`SELECT event_id FROM event_review_evidence WHERE id = ?`, result.EvidenceID).Scan(&linkedEventID); err != nil {
		t.Fatalf("load authoritative conflicting evidence event_id: %v", err)
	}
	if !linkedEventID.Valid || linkedEventID.Int64 != otherEventID {
		t.Fatalf("authoritative conflicting evidence event_id = %#v, want %d", linkedEventID, otherEventID)
	}
	var sourceLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_links
		WHERE source_id = ? AND source_event_key = ?
	`, sourceID, "uid:authoritative-conflicting-evidence").Scan(&sourceLinkCount); err != nil {
		t.Fatalf("count authoritative conflicting source links: %v", err)
	}
	if sourceLinkCount != 0 {
		t.Fatalf("authoritative conflicting source links = %d, want 0", sourceLinkCount)
	}
	if got := mustCount(t, db, "event_source_attribute_observations"); got != beforeObservations {
		t.Fatalf("event_source_attribute_observations = %d, want %d", got, beforeObservations)
	}
}

func TestStageEventReviewEvidenceDoesNotAutoResolveCanonicalFieldMismatch(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "canonical-mismatch-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "canonical-mismatch-leadmill-20260510190000")

	identityKey := "canonical-mismatch"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert canonical mismatch identity link: %v", err)
	}

	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	runID := mustCreateImportRun(t, st, "canonical mismatch")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "canonical-mismatch-fingerprint",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"canonical-mismatch",
			"candidate_title":"Legacy Event",
			"candidate_venue_slug":"leadmill",
			"candidate_start_at":"2026-05-10T19:00:00Z",
			"candidate_end_at":"2026-05-10T23:00:00Z",
			"candidate_genre":"Indie",
			"candidate_status":"Listed",
			"candidate_description":"Different description",
			"calendar_url":"https://example.test/calendar.ics"
		}`,
		ExactIdentityKeys: []string{identityKey},
		StagingKey:        eventReviewTestStagingKey("canonical-mismatch-fingerprint"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage canonical mismatch evidence: %v", err)
	}
	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID})
	if err != nil {
		t.Fatalf("finalize canonical mismatch cluster: %v", err)
	}
	if resolution != nil {
		t.Fatalf("canonical mismatch auto-resolution = %#v, want nil", resolution)
	}
	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
	}
}

func TestStageEventReviewEvidenceDoesNotAutoResolveCanonicalExactMatchWithInferredStart(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "canonical-inferred-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "canonical-inferred-leadmill-20260510190000")

	identityKey := "canonical-inferred"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, &canonicalEventID)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 5, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert canonical inferred identity link: %v", err)
	}

	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	runID := mustCreateImportRun(t, st, "canonical inferred")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "canonical-inferred-fingerprint",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"canonical-inferred",
			"candidate_title":"Legacy Event",
			"candidate_venue_slug":"leadmill",
			"candidate_start_at":"2026-05-10T19:00:00Z",
			"candidate_start_at_inferred":true,
			"candidate_start_at_basis":"source fallback 20:00 Europe/London",
			"candidate_end_at":"2026-05-10T22:00:00Z",
			"candidate_genre":"Indie",
			"candidate_status":"Listed",
			"candidate_description":"Legacy event",
			"calendar_url":"https://example.test/calendar.ics"
		}`,
		ExactIdentityKeys: []string{identityKey},
		StagingKey:        eventReviewTestStagingKey("canonical-inferred-fingerprint"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage canonical inferred evidence: %v", err)
	}
	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{result.EvidenceID})
	if err != nil {
		t.Fatalf("finalize canonical inferred cluster: %v", err)
	}
	if resolution != nil {
		t.Fatalf("canonical inferred auto-resolution = %#v, want nil", resolution)
	}
	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
	}
}

func TestStageEventReviewEvidenceAutoResolvesUnanimousDuplicateWithAuthoritativeCandidate(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "live-unanimous-duplicate-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)

	identityKey := "unanimous-duplicate-match"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 15, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert unanimous identity link: %v", err)
	}
	firstEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "unanimous-duplicate-evidence-1", `{
		"source_authority":"supporting",
		"source_name":"Store test source",
		"source_url":"https://example.test/store-test",
		"candidate_external_id":"unanimous-duplicate",
		"candidate_title":"Legacy Event",
		"candidate_venue_slug":"leadmill",
		"candidate_start_at":"2026-05-10T19:00:00Z",
		"candidate_end_at":"2026-05-10T22:00:00Z",
		"candidate_genre":"Indie",
		"candidate_status":"Listed",
		"candidate_description":"Legacy event",
		"calendar_url":"https://example.test/calendar.ics"
	}`)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, firstEvidenceID, true, time.Date(2026, time.May, 15, 10, 16, 0, 0, time.UTC), nil, "seed evidence")

	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	beforeReviewGroups := mustCount(t, db, "review_groups")

	runID := mustCreateImportRun(t, st, "unanimous duplicate")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthorityAuthoritative,
		EvidenceFingerprint: "unanimous-duplicate-evidence-2",
		Payload: `{
			"source_authority":"authoritative",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"unanimous-duplicate",
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
		StagingKey:        eventReviewTestStagingKey("unanimous-duplicate-evidence-2"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage unanimous duplicate evidence: %v", err)
	}
	if result.AutoResolved {
		t.Fatalf("unanimous duplicate stage result unexpectedly auto-resolved: %#v", result)
	}
	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{firstEvidenceID, result.EvidenceID})
	if err != nil {
		t.Fatalf("finalize unanimous duplicate cluster: %v", err)
	}
	if resolution == nil || resolution.AppliedAutoResolution == nil {
		t.Fatal("finalized unanimous duplicate cluster missing applied auto-resolution summary")
	}
	if resolution.AppliedAutoResolution.Result != "unanimous_duplicate" || resolution.AppliedAutoResolution.EventSlug != "live-unanimous-duplicate-leadmill-20260510190000" || resolution.AppliedAutoResolution.SourceID != sourceID || resolution.AppliedAutoResolution.SourceName != "Store test source" || resolution.AppliedAutoResolution.SourceURL != "https://example.test/store-test" || resolution.AppliedAutoResolution.EvidenceCount != 2 {
		t.Fatalf("unanimous duplicate finalized auto-resolution = %#v", resolution.AppliedAutoResolution)
	}
	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusResolved), 2, nil)

	detail, ok, err := st.LoadEventReviewCluster(ctx, clusterID)
	if err != nil {
		t.Fatalf("load unanimous duplicate cluster: %v", err)
	}
	if !ok || detail.Resolution == nil || detail.Resolution.AppliedAutoResolution == nil {
		t.Fatal("resolved unanimous duplicate cluster missing applied auto-resolution summary")
	}
	applied := detail.Resolution.AppliedAutoResolution
	if applied.Result != "unanimous_duplicate" || applied.EventSlug != "live-unanimous-duplicate-leadmill-20260510190000" || applied.SourceID != sourceID || applied.SourceName != "Store test source" || applied.SourceURL != "https://example.test/store-test" || applied.EvidenceCount != 2 {
		t.Fatalf("unanimous duplicate applied auto-resolution = %#v", applied)
	}
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions+1 {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions+1)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}

	replayRunID := mustCreateImportRun(t, st, "unanimous duplicate replay")
	replay, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: replayRunID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthorityAuthoritative,
		EvidenceFingerprint: "unanimous-duplicate-evidence-2",
		Payload: `{
			"source_authority":"authoritative",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"unanimous-duplicate",
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
		StagingKey:        eventReviewTestStagingKey("unanimous-duplicate-evidence-2"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("replay unanimous duplicate evidence: %v", err)
	}
	if !replay.AutoResolved || replay.AutoResolvedResult != "unanimous_duplicate" || replay.CanonicalEventSlug != "live-unanimous-duplicate-leadmill-20260510190000" {
		t.Fatalf("unanimous duplicate replay result = %#v, want terminal auto-resolution metadata", replay)
	}
}

func TestStageEventReviewEvidenceDoesNotAutoResolveUnanimousDuplicateWithInferredStart(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "inferred-unanimous-duplicate-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)

	identityKey := "inferred-unanimous-duplicate-match"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 15, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert inferred unanimous identity link: %v", err)
	}
	firstEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "inferred-unanimous-duplicate-evidence-1", `{
		"source_authority":"supporting",
		"source_name":"Store test source",
		"source_url":"https://example.test/store-test",
		"candidate_external_id":"inferred-unanimous-duplicate",
		"candidate_title":"Legacy Event",
		"candidate_venue_slug":"leadmill",
		"candidate_start_at":"2026-05-10T19:00:00Z",
		"candidate_end_at":"2026-05-10T22:00:00Z",
		"candidate_genre":"Indie",
		"candidate_status":"Listed",
		"candidate_description":"Legacy event",
		"calendar_url":"https://example.test/calendar.ics"
	}`)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, firstEvidenceID, true, time.Date(2026, time.May, 15, 10, 16, 0, 0, time.UTC), nil, "seed evidence")

	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	runID := mustCreateImportRun(t, st, "inferred unanimous duplicate")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthorityAuthoritative,
		EvidenceFingerprint: "inferred-unanimous-duplicate-evidence-2",
		Payload: `{
			"source_authority":"authoritative",
			"source_name":"Store test source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"inferred-unanimous-duplicate",
			"candidate_title":"Legacy Event",
			"candidate_venue_slug":"leadmill",
			"candidate_start_at":"2026-05-10T19:00:00Z",
			"candidate_start_at_inferred":true,
			"candidate_start_at_basis":"source fallback 20:00 Europe/London",
			"candidate_end_at":"2026-05-10T22:00:00Z",
			"candidate_genre":"Indie",
			"candidate_status":"Listed",
			"candidate_description":"Legacy event",
			"calendar_url":"https://example.test/calendar.ics"
		}`,
		ExactIdentityKeys: []string{identityKey},
		StagingKey:        eventReviewTestStagingKey("inferred-unanimous-duplicate-evidence-2"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage inferred unanimous duplicate evidence: %v", err)
	}
	resolution, err := st.FinalizeOpenEventReviewClusterRestage(ctx, result.ClusterID, []int64{firstEvidenceID, result.EvidenceID})
	if err != nil {
		t.Fatalf("finalize inferred unanimous duplicate cluster: %v", err)
	}
	if resolution != nil {
		t.Fatalf("inferred unanimous duplicate auto-resolution = %#v, want nil", resolution)
	}
	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
	}
}

func TestStageEventReviewEvidenceRollsBackAutoResolutionWritesOnFailure(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "rollback-auto-resolution-leadmill-20260510190000", venueID, sourceID, domain.OriginLive)
	eventID := lookupEventIDBySlug(t, db, "rollback-auto-resolution-leadmill-20260510190000")

	identityKey := "rollback-auto-resolution"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterID, identityKeyID, true, time.Date(2026, time.May, 15, 10, 25, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert rollback identity link: %v", err)
	}
	firstEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "rollback-auto-resolution-evidence-1", `{
		"source_authority":"supporting",
		"source_name":"Store test source",
		"source_url":"https://example.test/store-test",
		"candidate_external_id":"rollback-auto-resolution",
		"candidate_title":"Legacy Event",
		"candidate_venue_slug":"leadmill",
		"candidate_start_at":"2026-05-10T19:00:00Z",
		"candidate_end_at":"2026-05-10T22:00:00Z",
		"candidate_genre":"Indie",
		"candidate_status":"Listed",
		"candidate_description":"Legacy event",
		"calendar_url":"https://example.test/calendar.ics"
	}`)
	insertEventReviewClusterEvidenceOK(t, db, clusterID, firstEvidenceID, true, time.Date(2026, time.May, 15, 10, 26, 0, 0, time.UTC), nil, "seed evidence")
	if _, err := db.Exec(`
		CREATE TRIGGER abort_event_review_auto_resolution
		AFTER INSERT ON event_review_resolutions
		WHEN NEW.snapshot LIKE '%Rollback Source%'
		BEGIN
			SELECT RAISE(ABORT, 'forced event review auto-resolution failure');
		END;
	`); err != nil {
		t.Fatalf("create rollback trigger: %v", err)
	}
	defer func() {
		_, _ = db.Exec(`DROP TRIGGER abort_event_review_auto_resolution`)
	}()

	beforeResolutions := mustCount(t, db, "event_review_resolutions")
	beforeReviewGroups := mustCount(t, db, "review_groups")
	beforeEvidence := mustCount(t, db, "event_review_evidence")
	beforeClusterEvidence := mustCount(t, db, "event_review_cluster_evidence")

	runID := mustCreateImportRun(t, st, "rollback auto resolution")
	staged, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Store test source",
		SourceURL:           "https://example.test/store-test",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "rollback-auto-resolution-evidence-2",
		Payload: `{
			"source_authority":"supporting",
			"source_name":"Rollback Source",
			"source_url":"https://example.test/store-test",
			"candidate_external_id":"rollback-auto-resolution",
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
		StagingKey:        eventReviewTestStagingKey("rollback-auto-resolution-evidence-2"),
		StagingKeyVersion: 1,
	})
	if err != nil {
		t.Fatalf("stage rollback evidence: %v", err)
	}
	if staged.AutoResolved {
		t.Fatalf("stage rollback evidence unexpectedly auto-resolved: %#v", staged)
	}

	afterStageObservations := mustCount(t, db, "event_source_attribute_observations")

	if _, err := st.FinalizeOpenEventReviewClusterRestage(ctx, clusterID, []int64{firstEvidenceID, staged.EvidenceID}); err == nil || !strings.Contains(err.Error(), "forced event review auto-resolution failure") {
		t.Fatalf("rollback finalize error = %v, want forced auto-resolution failure", err)
	}

	assertEventReviewClusterState(t, db, clusterID, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	if got := mustCount(t, db, "event_review_resolutions"); got != beforeResolutions {
		t.Fatalf("event_review_resolutions rows = %d, want %d", got, beforeResolutions)
	}
	if got := mustCount(t, db, "review_groups"); got != beforeReviewGroups {
		t.Fatalf("review_groups rows = %d, want %d", got, beforeReviewGroups)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != beforeEvidence+1 {
		t.Fatalf("event_review_evidence rows = %d, want %d", got, beforeEvidence+1)
	}
	if got := mustCount(t, db, "event_review_cluster_evidence"); got != beforeClusterEvidence+1 {
		t.Fatalf("event_review_cluster_evidence rows = %d, want %d", got, beforeClusterEvidence+1)
	}
	if got := mustCount(t, db, "event_source_attribute_observations"); got != afterStageObservations {
		t.Fatalf("event_source_attribute_observations rows = %d, want %d", got, afterStageObservations)
	}
	var publicationState string
	if err := db.QueryRow(`
		SELECT publication_state
		FROM events
		WHERE id = ?
	`, eventID).Scan(&publicationState); err != nil {
		t.Fatalf("load rollback event publication state: %v", err)
	}
	if publicationState != string(domain.PublicationStateReviewed) {
		t.Fatalf("rollback event publication_state = %q, want %q", publicationState, domain.PublicationStateReviewed)
	}
}

func TestStageEventReviewEvidenceFindsTerminalClusterViaProvenanceOnlyEvidence(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	resolvedCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, nil, nil)
	insertEventReviewResolutionOK(t, db, resolvedCluster, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")

	identityKey := "terminal-provenance-only"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "terminal-provenance-only-fingerprint", `{"payload":"terminal"}`)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, identityKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert terminal evidence identity: %v", err)
	}

	provenanceRunID := mustCreateImportRun(t, st, "terminal provenance seed")
	if _, err := db.Exec(`
		INSERT INTO import_run_event_review_evidence (
			import_run_id,
			cluster_id,
			evidence_id,
			linked_at,
			link_reason
		) VALUES (?, ?, ?, ?, ?)
	`, provenanceRunID, resolvedCluster, evidenceID, "2026-05-15T10:10:00Z", "seed provenance"); err != nil {
		t.Fatalf("seed terminal provenance: %v", err)
	}

	runID := mustCreateImportRun(t, st, "terminal provenance lookup")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/terminal-provenance",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "terminal-provenance-only-fingerprint-fresh",
		Payload:             `{"payload":"terminal"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("terminal-provenance-only-fingerprint-fresh"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage terminal provenance lookup evidence: %v", err)
	}
	if result.ClusterID != resolvedCluster {
		t.Fatalf("cluster id = %d, want %d", result.ClusterID, resolvedCluster)
	}
	if result.ClusterCreated || !result.ClusterReused || !result.Created || result.Reused {
		t.Fatalf("terminal provenance lookup result = %#v, want fresh evidence on terminal cluster without active-link mutation", result)
	}
	if result.EvidenceID == evidenceID {
		t.Fatalf("evidence id = %d, want fresh evidence different from %d", result.EvidenceID, evidenceID)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != 2 {
		t.Fatalf("event_review_evidence rows = %d, want 2", got)
	}
	if got := mustCount(t, db, "event_review_cluster_evidence"); got != 0 {
		t.Fatalf("event_review_cluster_evidence rows = %d, want 0 active links", got)
	}
	if got := mustCount(t, db, "import_run_event_review_evidence"); got != 2 {
		t.Fatalf("import_run_event_review_evidence rows = %d, want 2", got)
	}
}

func TestStageEventReviewEvidenceRevisesTerminalFingerprintForChangedImmutableMaterial(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	resolvedCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, nil, nil)
	insertEventReviewResolutionOK(t, db, resolvedCluster, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")

	identityKey := "terminal-revision"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "terminal-revision-fingerprint", `{"payload":"seed"}`)
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceID, identityKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleExact); err != nil {
		t.Fatalf("insert terminal evidence identity: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, resolvedCluster, evidenceID, true, time.Date(2026, time.May, 15, 10, 10, 0, 0, time.UTC), nil, "seed terminal"); err != nil {
		t.Fatalf("insert terminal evidence link: %v", err)
	}

	runID := mustCreateImportRun(t, st, "terminal revision")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/terminal-revision",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "terminal-revision-fingerprint",
		Payload:             `{"payload":"mutated"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("terminal-revision-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage terminal revision evidence: %v", err)
	}
	if result.EvidenceID == evidenceID {
		t.Fatalf("evidence id = %d, want revised successor different from seed evidence %d", result.EvidenceID, evidenceID)
	}
	if !result.ClusterCreated || result.ClusterReused || !result.Created || result.Reused || !result.Attached {
		t.Fatalf("terminal revision result = %#v, want created open successor cluster with new evidence", result)
	}
	if result.ClusterID == resolvedCluster {
		t.Fatalf("cluster id = %d, want open successor distinct from terminal cluster %d", result.ClusterID, resolvedCluster)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 2 {
		t.Fatalf("event_review_clusters rows = %d, want 2", got)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != 2 {
		t.Fatalf("event_review_evidence rows = %d, want 2", got)
	}
	var revisedFingerprint string
	if err := db.QueryRow(`
		SELECT evidence_fingerprint
		FROM event_review_evidence
		WHERE id = ?
	`, result.EvidenceID).Scan(&revisedFingerprint); err != nil {
		t.Fatalf("load revised evidence fingerprint: %v", err)
	}
	expectedPrefix := "event-review-evidence-revision:v1:terminal-revision-fingerprint:"
	if !strings.HasPrefix(revisedFingerprint, expectedPrefix) {
		t.Fatalf("revised fingerprint = %q, want prefix %q", revisedFingerprint, expectedPrefix)
	}
	var activeLinkCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, resolvedCluster).Scan(&activeLinkCount); err != nil {
		t.Fatalf("count terminal active evidence links: %v", err)
	}
	if activeLinkCount != 1 {
		t.Fatalf("terminal active evidence links = %d, want 1", activeLinkCount)
	}
	successorKey := terminalSuccessorStagingKey(resolvedCluster, int64Ptr(resolvedCluster), terminalEvidenceIdentityHashesForInput(seedstore.StageEventReviewEvidenceInput{
		SourceIdentityKeys: []string{},
		ExactIdentityKeys:  []string{identityKey},
		StagingKey:         eventReviewTestStagingKey(""),
		StagingKeyVersion:  1,
	}), "", "")
	var successorClusterID int64
	if err := db.QueryRow(`
		SELECT id
		FROM event_review_clusters
		WHERE staging_key = ?
			AND staging_key_version = 1
	`, successorKey).Scan(&successorClusterID); err != nil {
		t.Fatalf("load successor cluster: %v", err)
	}
	if successorClusterID != result.ClusterID {
		t.Fatalf("successor cluster id = %d, want %d", successorClusterID, result.ClusterID)
	}
	if got := mustCount(t, db, "event_review_cluster_evidence"); got != 2 {
		t.Fatalf("event_review_cluster_evidence rows = %d, want 2", got)
	}
	if got := mustCount(t, db, "import_run_event_review_evidence"); got != 1 {
		t.Fatalf("import_run_event_review_evidence rows = %d, want 1", got)
	}

	secondRunID := mustCreateImportRun(t, st, "terminal revision replay")
	second, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: secondRunID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/terminal-revision",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "terminal-revision-fingerprint",
		Payload:             `{"payload":"mutated"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("terminal-revision-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage terminal revision replay: %v", err)
	}
	if second.ClusterID != result.ClusterID {
		t.Fatalf("second cluster id = %d, want %d", second.ClusterID, result.ClusterID)
	}
	if second.EvidenceID != result.EvidenceID {
		t.Fatalf("second evidence id = %d, want %d", second.EvidenceID, result.EvidenceID)
	}
	if second.ClusterCreated || !second.ClusterReused || second.Created || !second.Reused {
		t.Fatalf("second terminal revision result = %#v, want successor reuse and evidence reuse", second)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 2 {
		t.Fatalf("event_review_clusters rows after replay = %d, want 2", got)
	}
}

func TestStageEventReviewEvidenceCarriesResolvedLineageParentOntoOpenSurvivor(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	identityKey := "resolved-lineage-open"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "resolved-lineage-open-canonical", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "resolved-lineage-open-canonical")

	openCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	resolvedCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, nil, nil)
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET canonical_event_id = ?,
			conflict_type = ?,
			conflict_reason = ?
		WHERE id = ?
	`, canonicalEventID, "resolved-conflict", "resolved conflict reason", resolvedCluster); err != nil {
		t.Fatalf("seed resolved lineage metadata: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, openCluster, identityKeyID, true, time.Date(2026, time.May, 15, 10, 20, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert open cluster identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, resolvedCluster, identityKeyID, true, time.Date(2026, time.May, 15, 10, 25, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert resolved cluster identity: %v", err)
	}
	insertEventReviewResolutionOK(t, db, resolvedCluster, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")

	runID := mustCreateImportRun(t, st, "open survivor lineage")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/open-survivor-lineage",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "open-survivor-lineage-fingerprint",
		Payload:             `{"payload":"lineage"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("open-survivor-lineage-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage open survivor lineage evidence: %v", err)
	}
	if result.ClusterID != openCluster {
		t.Fatalf("cluster id = %d, want %d", result.ClusterID, openCluster)
	}

	var previousClusterID sql.NullInt64
	if err := db.QueryRow(`
		SELECT previous_cluster_id
		FROM event_review_clusters
		WHERE id = ?
	`, openCluster).Scan(&previousClusterID); err != nil {
		t.Fatalf("load open cluster previous_cluster_id: %v", err)
	}
	if !previousClusterID.Valid || previousClusterID.Int64 != resolvedCluster {
		t.Fatalf("previous_cluster_id = %#v, want %d", previousClusterID, resolvedCluster)
	}
	var canonical sql.NullInt64
	var conflictType, conflictReason string
	if err := db.QueryRow(`
		SELECT canonical_event_id, conflict_type, conflict_reason
		FROM event_review_clusters
		WHERE id = ?
	`, openCluster).Scan(&canonical, &conflictType, &conflictReason); err != nil {
		t.Fatalf("load open cluster lineage metadata: %v", err)
	}
	if !canonical.Valid || canonical.Int64 != canonicalEventID {
		t.Fatalf("canonical_event_id = %#v, want %d", canonical, canonicalEventID)
	}
	if conflictType != "resolved-conflict" {
		t.Fatalf("conflict_type = %q, want %q", conflictType, "resolved-conflict")
	}
	if conflictReason != "resolved conflict reason" {
		t.Fatalf("conflict_reason = %q, want %q", conflictReason, "resolved conflict reason")
	}
}

func TestStageEventReviewEvidenceSkipsConflictingMergeCandidateWithoutContaminatingLaterChecks(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	survivorIdentity := "merge-survivor"
	skippedIdentity := "merge-skip"
	laterIdentity := "merge-later"
	survivorHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, survivorIdentity)
	skippedHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, skippedIdentity)
	laterHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, laterIdentity)
	survivorKeyID := insertEventReviewIdentityKeyOK(t, db, survivorHash, seedstore.EventReviewIdentityKeyKindExact, survivorIdentity)
	skippedKeyID := insertEventReviewIdentityKeyOK(t, db, skippedHash, seedstore.EventReviewIdentityKeyKindExact, skippedIdentity)
	laterKeyID := insertEventReviewIdentityKeyOK(t, db, laterHash, seedstore.EventReviewIdentityKeyKindExact, laterIdentity)

	survivorCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	skippedCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	laterCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := insertEventReviewClusterIdentityKey(t, db, survivorCluster, survivorKeyID, true, time.Date(2026, time.May, 15, 10, 30, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert survivor identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, skippedCluster, skippedKeyID, true, time.Date(2026, time.May, 15, 10, 31, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert skipped identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, skippedCluster, survivorKeyID, true, time.Date(2026, time.May, 15, 10, 31, 30, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert skipped survivor identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, laterCluster, laterKeyID, true, time.Date(2026, time.May, 15, 10, 32, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert later identity: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, laterCluster, survivorKeyID, true, time.Date(2026, time.May, 15, 10, 32, 30, 0, time.UTC), nil); err != nil {
		t.Fatalf("insert later survivor identity: %v", err)
	}

	skippedEndpoint := seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           EventReviewSeparationEndpointKeyIdentity(skippedHash),
		IdentityKeyID: int64Ptr(skippedKeyID),
	}
	survivorEndpoint := seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           EventReviewSeparationEndpointKeyIdentity(survivorHash),
		IdentityKeyID: int64Ptr(survivorKeyID),
	}
	laterEndpoint := seedstore.EventReviewSeparationEndpoint{
		Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
		Key:           EventReviewSeparationEndpointKeyIdentity(laterHash),
		IdentityKeyID: int64Ptr(laterKeyID),
	}
	if skippedEndpoint.Key > survivorEndpoint.Key {
		skippedEndpoint, survivorEndpoint = survivorEndpoint, skippedEndpoint
	}
	if _, err := insertEventReviewSeparation(t, db, skippedEndpoint, survivorEndpoint, true, "skip candidate", time.Date(2026, time.May, 15, 10, 33, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 33, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert survivor-skip separation: %v", err)
	}
	if laterEndpoint.Key > skippedEndpoint.Key {
		laterEndpoint, skippedEndpoint = skippedEndpoint, laterEndpoint
	}
	if _, err := insertEventReviewSeparation(t, db, laterEndpoint, skippedEndpoint, true, "later candidate", time.Date(2026, time.May, 15, 10, 34, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 34, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert skip-later separation: %v", err)
	}

	runID := mustCreateImportRun(t, st, "merge contamination")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/merge-contamination",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "merge-contamination-fingerprint",
		Payload:             `{"payload":"merge"}`,
		ExactIdentityKeys:   []string{survivorIdentity},
		StagingKey:          eventReviewTestStagingKey("merge-contamination-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage merge contamination evidence: %v", err)
	}
	if result.ClusterID != survivorCluster {
		t.Fatalf("survivor cluster = %d, want %d", result.ClusterID, survivorCluster)
	}
	if len(result.MergedClusterIDs) != 1 || result.MergedClusterIDs[0] != laterCluster {
		t.Fatalf("merged cluster ids = %#v, want [%d]", result.MergedClusterIDs, laterCluster)
	}
	if len(result.SkippedClusterIDs) != 1 || result.SkippedClusterIDs[0] != skippedCluster {
		t.Fatalf("skipped cluster ids = %#v, want [%d]", result.SkippedClusterIDs, skippedCluster)
	}
	assertEventReviewClusterState(t, db, survivorCluster, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	assertEventReviewClusterState(t, db, skippedCluster, string(seedstore.EventReviewClusterStatusOpen), 1, nil)
	assertEventReviewClusterState(t, db, laterCluster, string(seedstore.EventReviewClusterStatusSuperseded), 2, int64Ptr(survivorCluster))
}

func TestStageEventReviewEvidenceSeparationsBlockAttachAndMerge(t *testing.T) {
	t.Run("evidence", func(t *testing.T) {
		stageSeparationBlocksAttach(t, separationBlockEvidence)
	})
	t.Run("identity", func(t *testing.T) {
		stageSeparationBlocksAttach(t, separationBlockIdentity)
	})
	t.Run("event", func(t *testing.T) {
		stageSeparationBlocksAttach(t, separationBlockEvent)
	})
}

func TestStageEventReviewEvidenceResolvedClusterCreatesLineageCluster(t *testing.T) {
	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "resolved-lineage-canonical", venueID, sourceID, domain.OriginLive)
	canonicalEventID := lookupEventIDBySlug(t, db, "resolved-lineage-canonical")

	identityKey := "resolved-lineage"
	identityHash := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, identityKey)
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, identityHash, seedstore.EventReviewIdentityKeyKindExact, identityKey)
	resolvedCluster := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, nil, &canonicalEventID)
	if _, err := db.Exec(`
		UPDATE event_review_clusters
		SET conflict_type = ?,
			conflict_reason = ?
		WHERE id = ?
	`, "resolved-lineage-conflict", "resolved lineage conflict reason", resolvedCluster); err != nil {
		t.Fatalf("seed resolved lineage metadata: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, resolvedCluster, identityKeyID, true, time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("link resolved cluster identity: %v", err)
	}
	if _, err := insertEventReviewResolution(t, db, resolvedCluster, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, ""); err != nil {
		t.Fatalf("insert resolved cluster resolution: %v", err)
	}

	runID := mustCreateImportRun(t, st, "resolved lineage")
	result, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runID},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/resolved-lineage",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		ConflictType:        seedstore.EventReviewConflictTypeImportReview,
		ConflictReason:      seedstore.EventReviewConflictReasonIngestCandidate,
		EvidenceFingerprint: "resolved-lineage-fingerprint",
		Payload:             `{"payload":"resolved"}`,
		ExactIdentityKeys:   []string{identityKey},
		StagingKey:          eventReviewTestStagingKey("resolved-lineage-fingerprint"),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage resolved lineage evidence: %v", err)
	}
	if result.ClusterCreated != true || result.ClusterID == resolvedCluster {
		t.Fatalf("resolved lineage result = %#v, want new cluster", result)
	}
	assertEventReviewClusterState(t, db, resolvedCluster, string(seedstore.EventReviewClusterStatusResolved), 1, nil)

	var previousClusterID sql.NullInt64
	var canonical sql.NullInt64
	var conflictType, conflictReason string
	if err := db.QueryRow(`
		SELECT previous_cluster_id, canonical_event_id, conflict_type, conflict_reason
		FROM event_review_clusters
		WHERE id = ?
	`, result.ClusterID).Scan(&previousClusterID, &canonical, &conflictType, &conflictReason); err != nil {
		t.Fatalf("load lineage cluster metadata: %v", err)
	}
	if !previousClusterID.Valid || previousClusterID.Int64 != resolvedCluster {
		t.Fatalf("previous_cluster_id = %#v, want %d", previousClusterID, resolvedCluster)
	}
	if !canonical.Valid || canonical.Int64 != canonicalEventID {
		t.Fatalf("canonical_event_id = %#v, want %d", canonical, canonicalEventID)
	}
	if conflictType != "resolved-lineage-conflict" {
		t.Fatalf("conflict_type = %q, want %q", conflictType, "resolved-lineage-conflict")
	}
	if conflictReason != "resolved lineage conflict reason" {
		t.Fatalf("conflict_reason = %q, want %q", conflictReason, "resolved lineage conflict reason")
	}
}

func stageOverlapAttachesToOpenCluster(t *testing.T, kind seedstore.EventReviewIdentityKeyKind, sourceKeys, exactKeys []string) {
	t.Helper()

	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	runOne := mustCreateImportRun(t, st, "overlap-one")
	runTwo := mustCreateImportRun(t, st, "overlap-two")
	keys := sourceKeys
	if kind == seedstore.EventReviewIdentityKeyKindExact {
		keys = exactKeys
	}

	first, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runOne},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/overlap",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "overlap-first-" + string(kind),
		Payload:             `{"payload":"first"}`,
		SourceIdentityKeys:  sourceKeys,
		ExactIdentityKeys:   exactKeys,
		StagingKey:          eventReviewTestStagingKey("overlap-first-" + string(kind)),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage first overlap evidence: %v", err)
	}
	second, err := st.StageEventReviewEvidence(ctx, seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runTwo},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/overlap",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "overlap-second-" + string(kind),
		Payload:             `{"payload":"second"}`,
		SourceIdentityKeys:  sourceKeys,
		ExactIdentityKeys:   exactKeys,
		StagingKey:          eventReviewTestStagingKey("overlap-second-" + string(kind)),
		StagingKeyVersion:   1,
	})
	if err != nil {
		t.Fatalf("stage second overlap evidence: %v", err)
	}
	if second.ClusterID != first.ClusterID {
		t.Fatalf("cluster id = %d, want %d", second.ClusterID, first.ClusterID)
	}
	var clusterEvidenceCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_evidence
		WHERE cluster_id = ?
			AND active = 1
	`, first.ClusterID).Scan(&clusterEvidenceCount); err != nil {
		t.Fatalf("count cluster evidence: %v", err)
	}
	if clusterEvidenceCount != 2 {
		t.Fatalf("active cluster evidence count = %d, want 2", clusterEvidenceCount)
	}
	_ = keys
}

type separationBlockKind int

const (
	separationBlockEvidence separationBlockKind = iota
	separationBlockIdentity
	separationBlockEvent
)

func stageSeparationBlocksAttach(t *testing.T, blockKind separationBlockKind) {
	t.Helper()

	ctx := context.Background()
	st, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	runOne := mustCreateImportRun(t, st, "separation-one")
	runTwo := mustCreateImportRun(t, st, "separation-two")

	existingEventID := (*int64)(nil)
	var newEventID *int64
	var firstSourceKeys []string
	var firstExactKeys []string
	var secondSourceKeys []string
	var secondExactKeys []string
	switch blockKind {
	case separationBlockEvidence:
		firstSourceKeys = []string{"separator"}
		secondSourceKeys = []string{"separator"}
	case separationBlockIdentity:
		firstSourceKeys = []string{"separator"}
		secondExactKeys = []string{"separator-exact"}
	case separationBlockEvent:
		venueID := lookupStoreVenueID(t, db, "leadmill")
		insertLegacyEvent(t, db, "separation-existing", venueID, sourceID, domain.OriginLive)
		insertLegacyEvent(t, db, "separation-new", venueID, sourceID, domain.OriginLive)
		existing := lookupEventIDBySlug(t, db, "separation-existing")
		newID := lookupEventIDBySlug(t, db, "separation-new")
		existingEventID = &existing
		newEventID = &newID
		firstSourceKeys = []string{"separator"}
		secondSourceKeys = []string{"separator"}
	}

	firstInput := seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runOne},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/separation",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "separation-first",
		Payload:             `{"payload":"first"}`,
		SourceIdentityKeys:  firstSourceKeys,
		ExactIdentityKeys:   firstExactKeys,
		StagingKey:          eventReviewTestStagingKey("separation-first"),
		StagingKeyVersion:   1,
	}
	if existingEventID != nil {
		firstInput.EventID = existingEventID
	}
	first, err := st.StageEventReviewEvidence(ctx, firstInput)
	if err != nil {
		t.Fatalf("stage first separation evidence: %v", err)
	}

	secondInput := seedstore.StageEventReviewEvidenceInput{
		RunRef:              seedstore.EventReviewRunRef{Kind: seedstore.EventReviewRunKindImport, ID: runTwo},
		SourceID:            sourceID,
		SourceName:          "Fixture source",
		SourceURL:           "https://example.test/separation",
		SourceAuthority:     seedstore.SourceAuthoritySupporting,
		EvidenceFingerprint: "separation-second",
		Payload:             `{"payload":"second"}`,
		SourceIdentityKeys:  secondSourceKeys,
		ExactIdentityKeys:   secondExactKeys,
		StagingKey:          eventReviewTestStagingKey("separation-second"),
		StagingKeyVersion:   1,
	}
	if newEventID != nil {
		secondInput.EventID = newEventID
	}

	switch blockKind {
	case separationBlockEvidence:
		secondEvidenceID := insertEventReviewEvidenceOK(t, db, sourceID, newEventID, "separation-second", `{"payload":"second"}`)
		endpointA, endpointB := separationEvidenceEndpoints(t, db, first)
		endpointB.EvidenceID = int64Ptr(secondEvidenceID)
		if _, err := insertEventReviewSeparation(t, db, endpointA, endpointB, true, "block evidence", time.Date(2026, time.May, 15, 12, 0, 0, 0, time.UTC), time.Date(2026, time.May, 15, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert evidence separation: %v", err)
		}
	case separationBlockIdentity:
		identityHashA := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindSource, eventReviewIdentityKeyVersion, "separator")
		identityHashB := buildEventReviewIdentityKeyHash(seedstore.EventReviewIdentityKeyKindExact, eventReviewIdentityKeyVersion, "separator-exact")
		if _, err := insertEventReviewIdentityKey(t, db, identityHashB, seedstore.EventReviewIdentityKeyKindExact, "separator-exact"); err != nil {
			t.Fatalf("insert identity B: %v", err)
		}
		var identityAID, identityBID int64
		if err := db.QueryRow(`SELECT id FROM event_review_identity_keys WHERE identity_key_hash = ?`, identityHashA).Scan(&identityAID); err != nil {
			t.Fatalf("lookup identity A: %v", err)
		}
		if err := db.QueryRow(`SELECT id FROM event_review_identity_keys WHERE identity_key_hash = ?`, identityHashB).Scan(&identityBID); err != nil {
			t.Fatalf("lookup identity B: %v", err)
		}
		endpointA := seedstore.EventReviewSeparationEndpoint{
			Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
			Key:           EventReviewSeparationEndpointKeyIdentity(identityHashA),
			IdentityKeyID: int64Ptr(identityAID),
		}
		endpointB := seedstore.EventReviewSeparationEndpoint{
			Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
			Key:           EventReviewSeparationEndpointKeyIdentity(identityHashB),
			IdentityKeyID: int64Ptr(identityBID),
		}
		if _, err := insertEventReviewSeparation(t, db, endpointA, endpointB, true, "block identity", time.Date(2026, time.May, 15, 12, 5, 0, 0, time.UTC), time.Date(2026, time.May, 15, 12, 5, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert identity separation: %v", err)
		}
	case separationBlockEvent:
		firstEvent := *existingEventID
		secondEvent := *newEventID
		endpointA := seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(firstEvent),
			EventID: int64Ptr(firstEvent),
		}
		endpointB := seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(secondEvent),
			EventID: int64Ptr(secondEvent),
		}
		if _, err := insertEventReviewSeparation(t, db, endpointA, endpointB, true, "block event", time.Date(2026, time.May, 15, 12, 10, 0, 0, time.UTC), time.Date(2026, time.May, 15, 12, 10, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert event separation: %v", err)
		}
	}

	second, err := st.StageEventReviewEvidence(ctx, secondInput)
	if err != nil {
		t.Fatalf("stage separated evidence: %v", err)
	}
	if second.ClusterID == first.ClusterID {
		t.Fatalf("separated evidence reused blocked cluster %d", second.ClusterID)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 2 {
		t.Fatalf("event_review_clusters rows = %d, want 2", got)
	}
}

func separationEvidenceEndpoints(t *testing.T, db *sql.DB, stageResult seedstore.StageEventReviewEvidenceResult) (seedstore.EventReviewSeparationEndpoint, seedstore.EventReviewSeparationEndpoint) {
	t.Helper()

	var fingerprint string
	if err := db.QueryRow(`SELECT evidence_fingerprint FROM event_review_evidence WHERE id = ?`, stageResult.EvidenceID).Scan(&fingerprint); err != nil {
		t.Fatalf("load evidence fingerprint: %v", err)
	}
	firstID := lookupEventReviewClusterEvidenceIDByFingerprint(t, db, fingerprint)
	secondFingerprint := "separation-second"
	endpointA := seedstore.EventReviewSeparationEndpoint{
		Kind:       seedstore.EventReviewSeparationEndpointKindEvidence,
		Key:        eventReviewSeparationEndpointKeyEvidence(fingerprint),
		EvidenceID: int64Ptr(firstID),
	}
	endpointB := seedstore.EventReviewSeparationEndpoint{
		Kind: seedstore.EventReviewSeparationEndpointKindEvidence,
		Key:  eventReviewSeparationEndpointKeyEvidence(secondFingerprint),
	}
	return endpointA, endpointB
}

func lookupEventReviewClusterEvidenceIDByFingerprint(t *testing.T, db *sql.DB, fingerprint string) int64 {
	t.Helper()

	var evidenceID int64
	if err := db.QueryRow(`
		SELECT id
		FROM event_review_evidence
		WHERE evidence_fingerprint = ?
	`, fingerprint).Scan(&evidenceID); err != nil {
		t.Fatalf("lookup evidence id %q: %v", fingerprint, err)
	}
	return evidenceID
}

type eventReviewClusterObservationRow struct {
	FieldName            string
	TargetKind           string
	SourceAuthority      string
	Outcome              string
	IncomingRaw          string
	IncomingNormalized   string
	EventID              sql.NullInt64
	ReviewGroupID        sql.NullInt64
	EventReviewClusterID sql.NullInt64
}

func mustLoadEventReviewClusterObservations(t *testing.T, db *sql.DB, clusterID int64) map[string]eventReviewClusterObservationRow {
	t.Helper()

	rows, err := db.Query(`
		SELECT
			field_name,
			target_kind,
			source_authority,
			outcome,
			incoming_raw,
			incoming_normalized,
			event_id,
			review_group_id,
			event_review_cluster_id
		FROM event_source_attribute_observations
		WHERE event_review_cluster_id = ?
		ORDER BY field_name
	`, clusterID)
	if err != nil {
		t.Fatalf("query cluster observations: %v", err)
	}
	defer rows.Close()

	out := make(map[string]eventReviewClusterObservationRow)
	for rows.Next() {
		var row eventReviewClusterObservationRow
		if err := rows.Scan(&row.FieldName, &row.TargetKind, &row.SourceAuthority, &row.Outcome, &row.IncomingRaw, &row.IncomingNormalized, &row.EventID, &row.ReviewGroupID, &row.EventReviewClusterID); err != nil {
			t.Fatalf("scan cluster observation: %v", err)
		}
		out[row.FieldName] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate cluster observations: %v", err)
	}
	return out
}

func insertRepairRunFixture(t *testing.T, db *sql.DB, id int64) {
	t.Helper()

	now := time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`
		INSERT INTO repair_runs (id, started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?, ?)
	`, id, formatRFC3339UTC(now), formatRFC3339UTC(now.Add(5*time.Minute)), "succeeded", "repair staging fixture"); err != nil {
		t.Fatalf("insert repair run: %v", err)
	}
}

func insertEventReviewClusterAtID(t *testing.T, db *sql.DB, id int64, status string, supersededBy, previous, canonical *int64) {
	t.Helper()

	if _, err := db.Exec(`
		INSERT INTO event_review_clusters (
			id,
			status,
			version,
			superseded_by_cluster_id,
			previous_cluster_id,
			canonical_event_id,
			conflict_type,
			conflict_reason,
			created_at,
			updated_at
		) VALUES (?, ?, 1, ?, ?, ?, '', '', ?, ?)
	`, id, status, supersededBy, previous, canonical, "2026-05-15T10:00:00Z", "2026-05-15T10:00:00Z"); err != nil {
		t.Fatalf("insert cluster %d: %v", id, err)
	}
}

func mustCreateImportRun(t *testing.T, st *Store, notes string) int64 {
	t.Helper()

	runID, _, err := st.CreateImportRun(context.Background(), "succeeded", notes)
	if err != nil {
		t.Fatalf("create import run: %v", err)
	}
	return runID
}
