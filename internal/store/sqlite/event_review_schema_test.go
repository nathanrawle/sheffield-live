package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	seedstore "sheffield-live/internal/store"
)

func TestOpenMigratesEventReviewSchemaFoundationPreservesObservations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV26)
	insertMigrationRowsThrough(t, db, schemaVersionV26, time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC))

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "leadmill", "Leadmill", domain.OriginLive)
	if _, err := db.Exec(`
		INSERT INTO import_runs (
			id,
			started_at,
			finished_at,
			status,
			notes
		) VALUES (?, ?, ?, ?, ?)
	`, 1, "2026-05-15T09:00:00Z", "2026-05-15T09:05:00Z", "succeeded", "fixture import run"); err != nil {
		t.Fatalf("insert import run: %v", err)
	}
	insertLegacyEvent(t, db, "event-review-observation", venueID, sourceID, domain.OriginLive)
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "event-review-observation").Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_attribute_observations (
			run_scope,
			source_id,
			source_identity_key,
			source_authority,
			target_kind,
			event_id,
			review_group_id,
			field_name,
			incoming_raw,
			incoming_normalized,
			canonical_before_raw,
			canonical_before_normalized,
			outcome,
			is_conflict,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "import:1", sourceID, "fixture-uid", string(seedstore.SourceAuthoritySupporting), string(seedstore.ObservationTargetKindEvent), eventID, "title", "Raw title", "raw title", "Canonical title", "canonical title", "accepted", 0, "2026-05-15T09:10:00Z", "2026-05-15T09:10:00Z"); err != nil {
		t.Fatalf("insert legacy observation: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db = mustRawDB(t, path)
	defer db.Close()

	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionCurrent {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionCurrent)
	}
	for _, table := range []string{
		"event_review_clusters",
		"event_review_evidence",
		"event_review_cluster_evidence",
		"event_review_identity_keys",
		"event_review_evidence_identity_keys",
		"event_review_cluster_identity_keys",
		"event_review_canonical_choices",
		"event_review_draft_choices",
		"event_review_live_actions",
		"event_review_source_identity_choices",
		"event_review_separations",
		"event_review_resolutions",
		"import_run_event_review_clusters",
		"repair_run_event_review_clusters",
		"import_run_event_review_evidence",
		"repair_run_event_review_evidence",
	} {
		ok, err := tableExists(context.Background(), db, table)
		if err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !ok {
			t.Fatalf("table %s does not exist", table)
		}
	}

	for _, column := range []string{
		"staging_key",
		"staging_key_version",
	} {
		ok, err := columnExists(context.Background(), db, "event_review_clusters", column)
		if err != nil {
			t.Fatalf("check event_review_clusters.%s: %v", column, err)
		}
		if !ok {
			t.Fatalf("column event_review_clusters.%s does not exist", column)
		}
	}

	ok, err := columnExists(context.Background(), db, "event_source_attribute_observations", "event_review_cluster_id")
	if err != nil {
		t.Fatalf("check observation column: %v", err)
	}
	if !ok {
		t.Fatal("column event_source_attribute_observations.event_review_cluster_id does not exist")
	}

	if got := mustCount(t, db, "event_source_attribute_observations"); got != 1 {
		t.Fatalf("event_source_attribute_observations rows = %d, want 1", got)
	}
	var clusterID sql.NullInt64
	if err := db.QueryRow(`
		SELECT event_review_cluster_id
		FROM event_source_attribute_observations
		WHERE id = 1
	`).Scan(&clusterID); err != nil {
		t.Fatalf("scan observation cluster id: %v", err)
	}
	if clusterID.Valid {
		t.Fatalf("event_review_cluster_id = %d, want NULL", clusterID.Int64)
	}
}

func TestEventReviewRunEvidenceProvenanceTablesUseCompositeKeys(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "fingerprint-provenance", `{"payload":"provenance"}`)

	if _, err := db.Exec(`
		INSERT INTO import_runs (
			id,
			started_at,
			finished_at,
			status,
			notes
		) VALUES (?, ?, ?, ?, ?)
	`, 41, "2026-05-15T11:20:00Z", "2026-05-15T11:21:00Z", "succeeded", "import provenance run"); err != nil {
		t.Fatalf("insert import run: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO repair_runs (
			id,
			started_at,
			finished_at,
			status,
			notes
		) VALUES (?, ?, ?, ?, ?)
	`, 42, "2026-05-15T11:22:00Z", "2026-05-15T11:23:00Z", "succeeded", "repair provenance run"); err != nil {
		t.Fatalf("insert repair run: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO import_run_event_review_evidence (
			import_run_id,
			cluster_id,
			evidence_id,
			linked_at,
			link_reason
		) VALUES (?, ?, ?, ?, ?)
	`, 41, clusterID, evidenceID, "2026-05-15T11:24:00Z", "import provenance"); err != nil {
		t.Fatalf("insert import provenance link: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO import_run_event_review_evidence (
			import_run_id,
			cluster_id,
			evidence_id,
			linked_at,
			link_reason
		) VALUES (?, ?, ?, ?, ?)
	`, 41, clusterID, evidenceID, "2026-05-15T11:25:00Z", "duplicate import provenance"); err == nil {
		t.Fatal("expected duplicate import provenance link error")
	}
	if _, err := db.Exec(`
		INSERT INTO repair_run_event_review_evidence (
			repair_run_id,
			cluster_id,
			evidence_id,
			linked_at,
			link_reason
		) VALUES (?, ?, ?, ?, ?)
	`, 42, clusterID, evidenceID, "2026-05-15T11:26:00Z", "repair provenance"); err != nil {
		t.Fatalf("insert repair provenance link: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO import_run_event_review_evidence (
			import_run_id,
			cluster_id,
			evidence_id,
			linked_at,
			link_reason
		) VALUES (?, ?, ?, ?, ?)
	`, 41, clusterID, evidenceID+1, "2026-05-15T11:27:00Z", "missing evidence"); err == nil {
		t.Fatal("expected import provenance foreign key error")
	}

	if got := mustCount(t, db, "import_run_event_review_evidence"); got != 1 {
		t.Fatalf("import_run_event_review_evidence rows = %d, want 1", got)
	}
	if got := mustCount(t, db, "repair_run_event_review_evidence"); got != 1 {
		t.Fatalf("repair_run_event_review_evidence rows = %d, want 1", got)
	}
	mustHaveIndexNamed(t, db, "import_run_event_review_evidence", "idx_import_run_event_review_evidence_cluster_evidence")
	mustHaveIndexNamed(t, db, "repair_run_event_review_evidence", "idx_repair_run_event_review_evidence_cluster_evidence")
}

func TestEventReviewClusterResolutionTriggersAreImmutable(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, nil, nil)
	resolutionID := insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")

	if _, err := db.Exec(`UPDATE event_review_resolutions SET snapshot = ? WHERE id = ?`, `{"cluster":"updated"}`, resolutionID); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("update resolution error = %v, want immutable trigger", err)
	}
	if _, err := db.Exec(`DELETE FROM event_review_resolutions WHERE id = ?`, resolutionID); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("delete resolution error = %v, want immutable trigger", err)
	}
}

func TestEventReviewResolutionDiscardReasonRequired(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusDiscarded), nil, nil, nil)
	if _, err := insertEventReviewResolution(t, db, clusterID, seedstore.EventReviewResolutionStatusDiscarded, `{"cluster":"discarded"}`, ""); err == nil {
		t.Fatal("expected discard reason constraint error")
	}
}

func TestEventReviewEvidenceAndMembershipHistory(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "fingerprint-1", `{"payload":"one"}`)
	if _, err := insertEventReviewEvidence(t, db, sourceID, nil, "fingerprint-1", `{"payload":"dup"}`); err == nil {
		t.Fatal("expected duplicate evidence fingerprint error")
	}

	clusterA := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	clusterB := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	linkedAt := time.Date(2026, time.May, 15, 10, 0, 0, 0, time.UTC)
	unlinkedAt := linkedAt.Add(10 * time.Minute)

	linkID := insertEventReviewClusterEvidenceOK(t, db, clusterA, evidenceID, true, linkedAt, nil, "initial attach")
	if linkID <= 0 {
		t.Fatal("missing cluster evidence link id")
	}
	if _, err := insertEventReviewClusterEvidence(t, db, clusterB, evidenceID, true, linkedAt, nil, "duplicate active"); err == nil {
		t.Fatal("expected second active evidence attachment error")
	}

	if _, err := db.Exec(`
		UPDATE event_review_cluster_evidence
		SET active = 0,
			unlinked_at = ?
		WHERE id = ?
	`, formatRFC3339UTC(unlinkedAt), linkID); err != nil {
		t.Fatalf("deactivate cluster evidence link: %v", err)
	}

	if _, err := insertEventReviewClusterEvidence(t, db, clusterA, evidenceID, false, linkedAt.Add(20*time.Minute), &unlinkedAt, "historical relink"); err != nil {
		t.Fatalf("insert historical inactive relink: %v", err)
	}
	if _, err := insertEventReviewClusterEvidence(t, db, clusterB, evidenceID, true, linkedAt.Add(30*time.Minute), nil, "survivor attach"); err != nil {
		t.Fatalf("insert survivor cluster link: %v", err)
	}
}

func TestEventReviewIdentityKeySharingAndLookupIndexes(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	evidenceA := insertEventReviewEvidenceOK(t, db, sourceID, nil, "fingerprint-a", "")
	evidenceB := insertEventReviewEvidenceOK(t, db, sourceID, nil, "fingerprint-b", "")
	identityKeyID := insertEventReviewIdentityKeyOK(t, db, "identity-hash", seedstore.EventReviewIdentityKeyKindExact, "normalized-identity")

	if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceA, identityKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("link evidence A identity key: %v", err)
	}
	if _, err := insertEventReviewEvidenceIdentityKey(t, db, evidenceB, identityKeyID, nil, seedstore.EventReviewEvidenceIdentityKeyRoleObserved); err != nil {
		t.Fatalf("link evidence B identity key: %v", err)
	}

	clusterA := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	clusterB := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	linkedAt := time.Date(2026, time.May, 15, 10, 15, 0, 0, time.UTC)
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterA, identityKeyID, true, linkedAt, nil); err != nil {
		t.Fatalf("link cluster A identity key: %v", err)
	}
	if _, err := insertEventReviewClusterIdentityKey(t, db, clusterB, identityKeyID, true, linkedAt.Add(5*time.Minute), nil); err != nil {
		t.Fatalf("link cluster B identity key: %v", err)
	}

	var evidenceCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_evidence_identity_keys
		WHERE identity_key_id = ?
	`, identityKeyID).Scan(&evidenceCount); err != nil {
		t.Fatalf("count evidence identity key links: %v", err)
	}
	if evidenceCount != 2 {
		t.Fatalf("evidence identity key links = %d, want 2", evidenceCount)
	}

	var clusterCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_review_cluster_identity_keys
		WHERE identity_key_id = ?
			AND active = 1
	`, identityKeyID).Scan(&clusterCount); err != nil {
		t.Fatalf("count cluster identity key links: %v", err)
	}
	if clusterCount != 2 {
		t.Fatalf("active cluster identity key links = %d, want 2", clusterCount)
	}
}

func TestEventReviewSupersedeAndSelfReferenceValidation(t *testing.T) {
	t.Run("superseded_requires_superseded_by", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		setIgnoreCheckConstraints(t, db, true)
		clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusSuperseded), nil, nil, nil)
		insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatusSuperseded, `{"cluster":"superseded"}`, "")
		st := mustStoreFromDB(t, db)
		if err := st.Validate(context.Background()); err == nil || !strings.Contains(err.Error(), "superseded without superseded_by_cluster_id") {
			t.Fatalf("validate error = %v, want missing superseded_by_cluster_id", err)
		}
	})

	t.Run("non_superseded_rejects_superseded_by", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		setIgnoreCheckConstraints(t, db, true)
		superseder := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
		supersededBy := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
		if _, err := db.Exec(`UPDATE event_review_clusters SET superseded_by_cluster_id = ? WHERE id = ?`, supersededBy, superseder); err != nil {
			t.Fatalf("set superseded_by_cluster_id: %v", err)
		}
		st := mustStoreFromDB(t, db)
		if err := st.Validate(context.Background()); err == nil || !strings.Contains(err.Error(), "must not carry superseded_by_cluster_id") {
			t.Fatalf("validate error = %v, want forbidden superseded_by_cluster_id", err)
		}
	})

	t.Run("self_reference_validation", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		setIgnoreCheckConstraints(t, db, true)
		clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
		if _, err := db.Exec(`UPDATE event_review_clusters SET previous_cluster_id = ? WHERE id = ?`, clusterID, clusterID); err != nil {
			t.Fatalf("set previous_cluster_id self reference: %v", err)
		}
		st := mustStoreFromDB(t, db)
		if err := st.Validate(context.Background()); err == nil || !strings.Contains(err.Error(), "self reference") {
			t.Fatalf("validate error = %v, want self reference", err)
		}
	})

	t.Run("superseded_active_identity_links", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		clusterB := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
		clusterA := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusSuperseded), &clusterB, nil, nil)
		insertEventReviewResolutionOK(t, db, clusterA, seedstore.EventReviewResolutionStatusSuperseded, `{"cluster":"superseded"}`, "")
		identityKeyID := insertEventReviewIdentityKeyOK(t, db, "superseded-hash", seedstore.EventReviewIdentityKeyKindSource, "normalized-superseded")
		if _, err := insertEventReviewClusterIdentityKey(t, db, clusterA, identityKeyID, true, time.Date(2026, time.May, 15, 10, 20, 0, 0, time.UTC), nil); err != nil {
			t.Fatalf("insert active identity link: %v", err)
		}
		st := mustStoreFromDB(t, db)
		if err := st.Validate(context.Background()); err == nil || !strings.Contains(err.Error(), "retains 1 active identity-key links") {
			t.Fatalf("validate error = %v, want active identity-key link rejection", err)
		}
	})
}

func TestEventReviewCanonicalEventValidationAndChoices(t *testing.T) {
	t.Run("withheld_canonical_event_rejected", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		sourceID := insertStoreTestSource(t, db)
		venueID := lookupStoreVenueID(t, db, "leadmill")
		insertLegacyEvent(t, db, "canonical-withheld", venueID, sourceID, domain.OriginLive)
		canonicalEventID := lookupEventIDBySlug(t, db, "canonical-withheld")
		if _, err := db.Exec(`UPDATE events SET publication_state = ?, withheld_reason = ? WHERE id = ?`, string(domain.PublicationStateWithheld), "redacted", canonicalEventID); err != nil {
			t.Fatalf("update canonical event to withheld: %v", err)
		}
		clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, nil, &canonicalEventID)
		insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")
		st := mustStoreFromDB(t, db)
		if err := st.Validate(context.Background()); err == nil || !strings.Contains(err.Error(), "withheld canonical event") {
			t.Fatalf("validate error = %v, want withheld canonical event", err)
		}
	})

	t.Run("live_non_withheld_event_validates_and_canonical_choice_is_separate", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		sourceID := insertStoreTestSource(t, db)
		venueID := lookupStoreVenueID(t, db, "leadmill")
		insertLegacyEvent(t, db, "canonical-live", venueID, sourceID, domain.OriginLive)
		insertLegacyEvent(t, db, "choice-live", venueID, sourceID, domain.OriginLive)
		canonicalEventID := lookupEventIDBySlug(t, db, "canonical-live")
		choiceEventID := lookupEventIDBySlug(t, db, "choice-live")
		clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusResolved), nil, nil, &canonicalEventID)
		insertEventReviewResolutionOK(t, db, clusterID, seedstore.EventReviewResolutionStatusResolved, `{"cluster":"resolved"}`, "")
		if _, err := insertEventReviewCanonicalChoice(t, db, clusterID, "canonical_event_id", seedstore.EventReviewChoiceKindEvent, &choiceEventID, nil, "choice-live", time.Date(2026, time.May, 15, 10, 30, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert canonical choice: %v", err)
		}
		st := mustStoreFromDB(t, db)
		if err := st.Validate(context.Background()); err != nil {
			t.Fatalf("validate store: %v", err)
		}
	})
}

func TestEventReviewSeparationNormalizationAndLookup(t *testing.T) {
	t.Run("queryable_by_evidence_fingerprint_and_identity_hash", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		sourceID := insertStoreTestSource(t, db)
		evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "fingerprint-separation", "")
		identityKeyID := insertEventReviewIdentityKeyOK(t, db, "identity-hash", seedstore.EventReviewIdentityKeyKindExact, "normalized-identity")
		endpointAKey, err := seedstore.NormalizeEventReviewSeparationEndpointKey(seedstore.EventReviewSeparationEndpointKindEvidence, "fingerprint-separation")
		if err != nil {
			t.Fatalf("normalize evidence endpoint: %v", err)
		}
		endpointBKey, err := seedstore.NormalizeEventReviewSeparationEndpointKey(seedstore.EventReviewSeparationEndpointKindIdentityKey, "identity-hash")
		if err != nil {
			t.Fatalf("normalize identity endpoint: %v", err)
		}
		if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
			Kind:       seedstore.EventReviewSeparationEndpointKindEvidence,
			Key:        endpointAKey,
			EvidenceID: int64Ptr(evidenceID),
		}, seedstore.EventReviewSeparationEndpoint{
			Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
			Key:           endpointBKey,
			IdentityKeyID: int64Ptr(identityKeyID),
		}, true, "duplicate source", time.Date(2026, time.May, 15, 10, 35, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 35, 0, 0, time.UTC)); err != nil {
			t.Fatalf("insert separation: %v", err)
		}

		var separationID int64
		if err := db.QueryRow(`
			SELECT id
			FROM event_review_separations
			WHERE active = 1
				AND (
					(endpoint_a_key = ? AND endpoint_b_key = ?)
					OR (endpoint_a_key = ? AND endpoint_b_key = ?)
				)
		`, endpointAKey, endpointBKey, endpointBKey, endpointAKey).Scan(&separationID); err != nil {
			t.Fatalf("query separation by endpoint keys: %v", err)
		}
		if separationID <= 0 {
			t.Fatal("missing separation by endpoint keys")
		}
	})

	t.Run("rejects_untrimmed_endpoint_keys", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		sourceID := insertStoreTestSource(t, db)
		eventA := lookupOrInsertTestEvent(t, db, "separation-event-a")
		eventB := lookupOrInsertTestEvent(t, db, "separation-event-b")
		eventAKey := seedstore.EventReviewSeparationEventEndpointKey(eventA)
		eventBKey := seedstore.EventReviewSeparationEventEndpointKey(eventB)
		if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     eventAKey + " ",
			EventID: int64Ptr(eventA),
		}, seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     eventBKey,
			EventID: int64Ptr(eventB),
		}, true, "trimmed rejection", time.Date(2026, time.May, 15, 10, 40, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 40, 0, 0, time.UTC)); err == nil {
			t.Fatal("expected untrimmed endpoint key error")
		}

		identityKeyID := insertEventReviewIdentityKeyOK(t, db, "identity-trim", seedstore.EventReviewIdentityKeyKindSource, "normalized-trim")
		if _, err := insertEventReviewEvidence(t, db, sourceID, nil, "fingerprint-trim", ""); err != nil {
			t.Fatalf("insert evidence for trimming test: %v", err)
		}
		if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
			Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
			Key:           "identity:identity-trim ",
			IdentityKeyID: int64Ptr(identityKeyID),
		}, seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     eventBKey,
			EventID: int64Ptr(eventB),
		}, true, "trimmed rejection", time.Date(2026, time.May, 15, 10, 45, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 45, 0, 0, time.UTC)); err == nil {
			t.Fatal("expected untrimmed endpoint key error for identity endpoint")
		}
	})

	t.Run("rejects_mismatched_evidence_or_identity_keys", func(t *testing.T) {
		_, db := openEventReviewSchemaStore(t)
		defer db.Close()

		sourceID := insertStoreTestSource(t, db)
		evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "fingerprint-separation-trigger", "")
		identityKeyID := insertEventReviewIdentityKeyOK(t, db, "identity-separation-trigger", seedstore.EventReviewIdentityKeyKindExact, "normalized-separation-trigger")
		eventID := lookupOrInsertTestEvent(t, db, "separation-trigger-event")
		if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
			Kind:       seedstore.EventReviewSeparationEndpointKindEvidence,
			Key:        "evidence:not-the-fingerprint",
			EvidenceID: int64Ptr(evidenceID),
		}, seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(eventID),
			EventID: int64Ptr(eventID),
		}, true, "mismatched evidence key", time.Date(2026, time.May, 15, 10, 46, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 46, 0, 0, time.UTC)); err == nil {
			t.Fatal("expected mismatched evidence key error")
		}
		if _, err := insertEventReviewSeparation(t, db, seedstore.EventReviewSeparationEndpoint{
			Kind:          seedstore.EventReviewSeparationEndpointKindIdentityKey,
			Key:           "identity:not-the-hash",
			IdentityKeyID: int64Ptr(identityKeyID),
		}, seedstore.EventReviewSeparationEndpoint{
			Kind:    seedstore.EventReviewSeparationEndpointKindEvent,
			Key:     seedstore.EventReviewSeparationEventEndpointKey(eventID),
			EventID: int64Ptr(eventID),
		}, true, "mismatched identity key", time.Date(2026, time.May, 15, 10, 47, 0, 0, time.UTC), time.Date(2026, time.May, 15, 10, 47, 0, 0, time.UTC)); err == nil {
			t.Fatal("expected mismatched identity key error")
		}
	})
}

func TestEventReviewDraftAndSourceIdentityChoices(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	evidenceID := insertEventReviewEvidenceOK(t, db, sourceID, nil, "draft-evidence", "")
	eventID := lookupOrInsertTestEvent(t, db, "draft-event")

	if _, err := insertEventReviewSourceIdentityChoice(t, db, clusterID, sourceID, "source-identity-key", true, "preferred source identity", time.Date(2026, time.May, 15, 10, 50, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert source identity choice: %v", err)
	}
	var selected int
	var selectionReason string
	if err := db.QueryRow(`
		SELECT selected, selection_reason
		FROM event_review_source_identity_choices
		WHERE cluster_id = ? AND source_id = ? AND source_identity_key = ?
	`, clusterID, sourceID, "source-identity-key").Scan(&selected, &selectionReason); err != nil {
		t.Fatalf("scan source identity choice: %v", err)
	}
	if selected != 1 || selectionReason != "preferred source identity" {
		t.Fatalf("source identity choice = (%d, %q), want (1, %q)", selected, selectionReason, "preferred source identity")
	}

	if _, err := insertEventReviewDraftChoice(t, db, clusterID, "manual_field", seedstore.EventReviewChoiceKindManual, nil, nil, "manual-value", time.Date(2026, time.May, 15, 10, 55, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert manual draft choice: %v", err)
	}
	if _, err := insertEventReviewDraftChoice(t, db, clusterID, "event_field", seedstore.EventReviewChoiceKindEvent, &eventID, nil, "event-value", time.Date(2026, time.May, 15, 10, 56, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert event draft choice: %v", err)
	}
	if _, err := insertEventReviewDraftChoice(t, db, clusterID, "evidence_field", seedstore.EventReviewChoiceKindEvidence, nil, &evidenceID, "evidence-value", time.Date(2026, time.May, 15, 10, 57, 0, 0, time.UTC)); err != nil {
		t.Fatalf("insert evidence draft choice: %v", err)
	}
	if got := mustCount(t, db, "event_review_draft_choices"); got != 3 {
		t.Fatalf("draft choices rows = %d, want 3", got)
	}
	if _, err := insertEventReviewDraftChoice(t, db, clusterID, "invalid_field", seedstore.EventReviewChoiceKindManual, &eventID, nil, "invalid", time.Date(2026, time.May, 15, 10, 58, 0, 0, time.UTC)); err == nil {
		t.Fatal("expected invalid manual draft choice shape error")
	}
}

func TestEventReviewLiveActionEnumAndReason(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	eventID := lookupOrInsertTestEvent(t, db, "live-action-target")
	actionID := insertEventReviewLiveActionOK(t, db, clusterID, eventID, seedstore.EventReviewLiveActionKindWithholdDuplicate, "duplicate source")
	if actionID <= 0 {
		t.Fatal("missing live action id")
	}
	var reason string
	if err := db.QueryRow(`SELECT reason FROM event_review_live_actions WHERE id = ?`, actionID).Scan(&reason); err != nil {
		t.Fatalf("scan live action reason: %v", err)
	}
	if reason != "duplicate source" {
		t.Fatalf("live action reason = %q, want %q", reason, "duplicate source")
	}
	if _, err := insertEventReviewLiveAction(t, db, clusterID, eventID, "auto_resolved", "bad value", time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("expected invalid live action value error")
	}
}

func TestEventReviewRunClusterLinksUseClusterID(t *testing.T) {
	_, db := openEventReviewSchemaStore(t)
	defer db.Close()

	clusterID := insertEventReviewClusterOK(t, db, string(seedstore.EventReviewClusterStatusOpen), nil, nil, nil)
	if _, err := db.Exec(`
		INSERT INTO import_runs (
			started_at,
			finished_at,
			status,
			notes
		) VALUES (?, ?, ?, ?)
	`, "2026-05-15T11:05:00Z", "2026-05-15T11:06:00Z", "succeeded", "import run for event review link test"); err != nil {
		t.Fatalf("insert import run: %v", err)
	}
	var importRunID int64
	if err := db.QueryRow(`SELECT id FROM import_runs ORDER BY id DESC LIMIT 1`).Scan(&importRunID); err != nil {
		t.Fatalf("lookup import run id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO repair_runs (
			started_at,
			finished_at,
			status,
			notes
		) VALUES (?, ?, ?, ?)
	`, "2026-05-15T11:10:00Z", "2026-05-15T11:11:00Z", "succeeded", "repair run for event review link test"); err != nil {
		t.Fatalf("insert repair run: %v", err)
	}
	var repairRunID int64
	if err := db.QueryRow(`SELECT id FROM repair_runs ORDER BY id DESC LIMIT 1`).Scan(&repairRunID); err != nil {
		t.Fatalf("lookup repair run id: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO import_run_event_review_clusters (import_run_id, cluster_id, linked_at)
		VALUES (?, ?, ?)
	`, importRunID, clusterID, "2026-05-15T11:12:00Z"); err != nil {
		t.Fatalf("insert import run cluster link: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO repair_run_event_review_clusters (repair_run_id, cluster_id, linked_at)
		VALUES (?, ?, ?)
	`, repairRunID, clusterID, "2026-05-15T11:13:00Z"); err != nil {
		t.Fatalf("insert repair run cluster link: %v", err)
	}

	var linkedClusterID int64
	if err := db.QueryRow(`
		SELECT cluster_id
		FROM import_run_event_review_clusters
		WHERE import_run_id = ?
	`, importRunID).Scan(&linkedClusterID); err != nil {
		t.Fatalf("scan import run cluster link: %v", err)
	}
	if linkedClusterID != clusterID {
		t.Fatalf("import run cluster id = %d, want %d", linkedClusterID, clusterID)
	}
	if err := db.QueryRow(`
		SELECT cluster_id
		FROM repair_run_event_review_clusters
		WHERE repair_run_id = ?
	`, repairRunID).Scan(&linkedClusterID); err != nil {
		t.Fatalf("scan repair run cluster link: %v", err)
	}
	if linkedClusterID != clusterID {
		t.Fatalf("repair run cluster id = %d, want %d", linkedClusterID, clusterID)
	}
}

func openEventReviewSchemaStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	st.db.SetMaxOpenConns(1)
	st.db.SetMaxIdleConns(1)
	return st, st.db
}

func mustHaveIndexNamed(t *testing.T, db *sql.DB, table, want string) {
	t.Helper()

	rows, err := db.Query(`PRAGMA index_list(` + table + `)`)
	if err != nil {
		t.Fatalf("index list for %s: %v", table, err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var seq, unique, partial int
		var name, origin string
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan index row for %s: %v", table, err)
		}
		if name == want {
			found = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index list for %s: %v", table, err)
	}
	if !found {
		t.Fatalf("missing index %s on %s", want, table)
	}
}

func mustStoreFromDB(t *testing.T, db *sql.DB) *Store {
	t.Helper()

	return &Store{db: db}
}

func setIgnoreCheckConstraints(t *testing.T, db *sql.DB, enabled bool) {
	t.Helper()

	value := "OFF"
	if enabled {
		value = "ON"
	}
	if _, err := db.Exec(`PRAGMA ignore_check_constraints = ` + value); err != nil {
		t.Fatalf("set ignore_check_constraints=%s: %v", value, err)
	}
}

func insertEventReviewCluster(t *testing.T, db *sql.DB, status string, supersededBy, previous, canonical *int64) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_clusters (
			status,
			version,
			superseded_by_cluster_id,
			previous_cluster_id,
			canonical_event_id,
			conflict_type,
			conflict_reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, status, 1, supersededBy, previous, canonical, "", "", "2026-05-15T10:00:00Z", "2026-05-15T10:00:00Z")
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewClusterOK(t *testing.T, db *sql.DB, status string, supersededBy, previous, canonical *int64) int64 {
	t.Helper()

	clusterID, err := insertEventReviewCluster(t, db, status, supersededBy, previous, canonical)
	if err != nil {
		t.Fatalf("insert cluster %q: %v", status, err)
	}
	return clusterID
}

func insertEventReviewEvidence(t *testing.T, db *sql.DB, sourceID int64, eventID *int64, fingerprint, payload string) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_evidence (
			source_id,
			event_id,
			evidence_fingerprint,
			fingerprint_version,
			payload,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, sourceID, eventID, fingerprint, 1, payload, "2026-05-15T10:00:00Z", "2026-05-15T10:00:00Z")
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewEvidenceOK(t *testing.T, db *sql.DB, sourceID int64, eventID *int64, fingerprint, payload string) int64 {
	t.Helper()

	evidenceID, err := insertEventReviewEvidence(t, db, sourceID, eventID, fingerprint, payload)
	if err != nil {
		t.Fatalf("insert evidence %q: %v", fingerprint, err)
	}
	return evidenceID
}

func insertEventReviewIdentityKey(t *testing.T, db *sql.DB, identityKeyHash string, kind seedstore.EventReviewIdentityKeyKind, normalizedKey string) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_identity_keys (
			identity_key_hash,
			key_kind,
			key_version,
			normalized_key,
			created_at
		) VALUES (?, ?, ?, ?, ?)
	`, identityKeyHash, string(kind), 1, normalizedKey, "2026-05-15T10:00:00Z")
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewIdentityKeyOK(t *testing.T, db *sql.DB, identityKeyHash string, kind seedstore.EventReviewIdentityKeyKind, normalizedKey string) int64 {
	t.Helper()

	identityKeyID, err := insertEventReviewIdentityKey(t, db, identityKeyHash, kind, normalizedKey)
	if err != nil {
		t.Fatalf("insert identity key %q: %v", identityKeyHash, err)
	}
	return identityKeyID
}

func insertEventReviewEvidenceIdentityKey(t *testing.T, db *sql.DB, evidenceID, identityKeyID int64, sourceID *int64, role seedstore.EventReviewEvidenceIdentityKeyRole) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_evidence_identity_keys (
			evidence_id,
			identity_key_id,
			source_id,
			role
		) VALUES (?, ?, ?, ?)
	`, evidenceID, identityKeyID, sourceID, string(role))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewClusterEvidence(t *testing.T, db *sql.DB, clusterID, evidenceID int64, active bool, linkedAt time.Time, unlinkedAt *time.Time, reason string) (int64, error) {
	t.Helper()

	var unlinkedAtText any
	if unlinkedAt != nil {
		unlinkedAtText = formatRFC3339UTC(*unlinkedAt)
	}
	res, err := db.Exec(`
		INSERT INTO event_review_cluster_evidence (
			cluster_id,
			evidence_id,
			active,
			linked_at,
			unlinked_at,
			link_reason
		) VALUES (?, ?, ?, ?, ?, ?)
	`, clusterID, evidenceID, boolInt(active), formatRFC3339UTC(linkedAt), unlinkedAtText, reason)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewClusterEvidenceOK(t *testing.T, db *sql.DB, clusterID, evidenceID int64, active bool, linkedAt time.Time, unlinkedAt *time.Time, reason string) int64 {
	t.Helper()

	linkID, err := insertEventReviewClusterEvidence(t, db, clusterID, evidenceID, active, linkedAt, unlinkedAt, reason)
	if err != nil {
		t.Fatalf("insert cluster evidence link: %v", err)
	}
	return linkID
}

func insertEventReviewClusterIdentityKey(t *testing.T, db *sql.DB, clusterID, identityKeyID int64, active bool, linkedAt time.Time, unlinkedAt *time.Time) (int64, error) {
	t.Helper()

	var unlinkedAtText any
	if unlinkedAt != nil {
		unlinkedAtText = formatRFC3339UTC(*unlinkedAt)
	}
	res, err := db.Exec(`
		INSERT INTO event_review_cluster_identity_keys (
			cluster_id,
			identity_key_id,
			active,
			linked_at,
			unlinked_at
		) VALUES (?, ?, ?, ?, ?)
	`, clusterID, identityKeyID, boolInt(active), formatRFC3339UTC(linkedAt), unlinkedAtText)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewResolution(t *testing.T, db *sql.DB, clusterID int64, status seedstore.EventReviewResolutionStatus, snapshot, discardReason string) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_resolutions (
			cluster_id,
			status,
			snapshot,
			discard_reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, clusterID, string(status), snapshot, discardReason, "2026-05-15T10:00:00Z", "2026-05-15T10:00:00Z")
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewResolutionOK(t *testing.T, db *sql.DB, clusterID int64, status seedstore.EventReviewResolutionStatus, snapshot, discardReason string) int64 {
	t.Helper()

	id, err := insertEventReviewResolution(t, db, clusterID, status, snapshot, discardReason)
	if err != nil {
		t.Fatalf("insert resolution: %v", err)
	}
	return id
}

func insertEventReviewCanonicalChoice(t *testing.T, db *sql.DB, clusterID int64, fieldName string, choiceKind seedstore.EventReviewChoiceKind, eventID, evidenceID *int64, value string, updatedAt time.Time) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_canonical_choices (
			cluster_id,
			field_name,
			choice_kind,
			event_id,
			evidence_id,
			value,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, clusterID, fieldName, string(choiceKind), eventID, evidenceID, value, formatRFC3339UTC(updatedAt))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewDraftChoice(t *testing.T, db *sql.DB, clusterID int64, fieldName string, choiceKind seedstore.EventReviewChoiceKind, eventID, evidenceID *int64, value string, updatedAt time.Time) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_draft_choices (
			cluster_id,
			field_name,
			choice_kind,
			event_id,
			evidence_id,
			value,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, clusterID, fieldName, string(choiceKind), eventID, evidenceID, value, formatRFC3339UTC(updatedAt))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewSourceIdentityChoice(t *testing.T, db *sql.DB, clusterID, sourceID int64, sourceIdentityKey string, selected bool, selectionReason string, updatedAt time.Time) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_source_identity_choices (
			cluster_id,
			source_id,
			source_identity_key,
			selected,
			selection_reason,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, clusterID, sourceID, sourceIdentityKey, boolInt(selected), selectionReason, formatRFC3339UTC(updatedAt))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewLiveAction(t *testing.T, db *sql.DB, clusterID, eventID int64, action string, reason string, createdAt time.Time) (int64, error) {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO event_review_live_actions (
			cluster_id,
			event_id,
			action,
			reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, clusterID, eventID, action, reason, formatRFC3339UTC(createdAt), formatRFC3339UTC(createdAt))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEventReviewLiveActionOK(t *testing.T, db *sql.DB, clusterID, eventID int64, action seedstore.EventReviewLiveActionKind, reason string) int64 {
	t.Helper()

	id, err := insertEventReviewLiveAction(t, db, clusterID, eventID, string(action), reason, time.Date(2026, time.May, 15, 11, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("insert live action: %v", err)
	}
	return id
}

func insertEventReviewSeparation(t *testing.T, db *sql.DB, endpointA, endpointB seedstore.EventReviewSeparationEndpoint, active bool, reason string, createdAt, updatedAt time.Time) (int64, error) {
	t.Helper()

	var endpointAEventID, endpointAEvidenceID, endpointAIdentityKeyID, endpointACanonicalEventID any
	var endpointBEventID, endpointBEvidenceID, endpointBIdentityKeyID, endpointBCanonicalEventID any
	if endpointA.EventID != nil {
		endpointAEventID = *endpointA.EventID
	}
	if endpointA.EvidenceID != nil {
		endpointAEvidenceID = *endpointA.EvidenceID
	}
	if endpointA.IdentityKeyID != nil {
		endpointAIdentityKeyID = *endpointA.IdentityKeyID
	}
	if endpointA.CanonicalEventID != nil {
		endpointACanonicalEventID = *endpointA.CanonicalEventID
	}
	if endpointB.EventID != nil {
		endpointBEventID = *endpointB.EventID
	}
	if endpointB.EvidenceID != nil {
		endpointBEvidenceID = *endpointB.EvidenceID
	}
	if endpointB.IdentityKeyID != nil {
		endpointBIdentityKeyID = *endpointB.IdentityKeyID
	}
	if endpointB.CanonicalEventID != nil {
		endpointBCanonicalEventID = *endpointB.CanonicalEventID
	}

	res, err := db.Exec(`
		INSERT INTO event_review_separations (
			endpoint_a_kind,
			endpoint_a_key,
			endpoint_a_event_id,
			endpoint_a_evidence_id,
			endpoint_a_identity_key_id,
			endpoint_a_canonical_event_id,
			endpoint_b_kind,
			endpoint_b_key,
			endpoint_b_event_id,
			endpoint_b_evidence_id,
			endpoint_b_identity_key_id,
			endpoint_b_canonical_event_id,
			active,
			reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, string(endpointA.Kind), endpointA.Key, endpointAEventID, endpointAEvidenceID, endpointAIdentityKeyID, endpointACanonicalEventID, string(endpointB.Kind), endpointB.Key, endpointBEventID, endpointBEvidenceID, endpointBIdentityKeyID, endpointBCanonicalEventID, boolInt(active), reason, formatRFC3339UTC(createdAt), formatRFC3339UTC(updatedAt))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func lookupEventIDBySlug(t *testing.T, db *sql.DB, slug string) int64 {
	t.Helper()

	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, slug).Scan(&eventID); err != nil {
		t.Fatalf("lookup event id %q: %v", slug, err)
	}
	return eventID
}

func lookupOrInsertTestEvent(t *testing.T, db *sql.DB, slug string) int64 {
	t.Helper()

	venueID := lookupStoreVenueID(t, db, "leadmill")
	sourceName := "Store test source " + slug
	sourceURL := "https://example.test/store-test/" + slug
	res, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, sourceName, sourceURL)
	if err != nil {
		t.Fatalf("insert source for %q: %v", slug, err)
	}
	sourceID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("source id for %q: %v", slug, err)
	}
	insertLegacyEvent(t, db, slug, venueID, sourceID, domain.OriginLive)
	return lookupEventIDBySlug(t, db, slug)
}
