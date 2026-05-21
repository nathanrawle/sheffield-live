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

func TestOpenMigratesVersion19DatabaseAddsUnit2SchemaFoundation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV19)
	insertMigrationRowsThrough(t, db, schemaVersionV19, time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC))
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.Validate(context.Background()); err != nil {
		t.Fatalf("validate store: %v", err)
	}

	db = mustRawDB(t, path)
	defer db.Close()

	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionCurrent {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionCurrent)
	}
	for _, table := range []string{
		"repair_runs",
		"slug_aliases",
		"event_exact_identities",
		"event_source_attribute_observations",
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
		"withheld_reason",
		"canonical_event_id",
		"withheld_repair_run_id",
	} {
		ok, err := columnExists(context.Background(), db, "events", column)
		if err != nil {
			t.Fatalf("check events.%s: %v", column, err)
		}
		if !ok {
			t.Fatalf("column events.%s does not exist", column)
		}
	}

	mustHaveUniqueIndex(t, db, "slug_aliases", false)
	mustHaveUniqueIndex(t, db, "event_exact_identities", true)
	mustHaveUniqueIndex(t, db, "event_source_attribute_observations", true)
}

func TestValidateDanglingObservationRowsWorksBeforeEventReviewClusterTargetExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV26)
	insertMigrationRowsThrough(t, db, schemaVersionV26, time.Date(2026, time.May, 12, 9, 0, 0, 0, time.UTC))

	sourceID := insertStoreTestSource(t, db)
	venueID := insertLegacyVenue(t, db, "leadmill", "Leadmill", domain.OriginLive)
	insertLegacyEvent(t, db, "observation-regression", venueID, sourceID, domain.OriginLive)
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "observation-regression").Scan(&eventID); err != nil {
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
	`, "import:1", sourceID, "observation-source", string(seedstore.SourceAuthoritySupporting), string(seedstore.ObservationTargetKindEvent), eventID, "title", "Raw title", "raw title", "Canonical title", "canonical title", "accepted", 0, "2026-05-12T09:10:00Z", "2026-05-12T09:10:00Z"); err != nil {
		t.Fatalf("insert observation: %v", err)
	}

	if err := validateDanglingObservationRows(context.Background(), db); err != nil {
		t.Fatalf("validate dangling observations: %v", err)
	}
}

func TestLoadEventPreservesWithheldPublicationState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := mustRawDB(t, path)
	insertStoreTestEvent(t, db, "withheld-event", "leadmill")
	if _, err := db.Exec(`
		UPDATE events
		SET publication_state = ?, withheld_reason = ?
		WHERE slug = ?
	`, string(domain.PublicationStateWithheld), "duplicate canonical listing", "withheld-event"); err != nil {
		t.Fatalf("update event publication state: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	event, ok, err := st.LoadEventBySlug(context.Background(), "withheld-event")
	if err != nil {
		t.Fatalf("load event: %v", err)
	}
	if !ok {
		t.Fatal("withheld event not found")
	}
	if event.PublicationState != domain.PublicationStateWithheld {
		t.Fatalf("publication state = %q, want %q", event.PublicationState, domain.PublicationStateWithheld)
	}
}

func TestObservationRunScopeConstructors(t *testing.T) {
	scope, err := seedstore.NewObservationRunScopeImport(12)
	if err != nil {
		t.Fatalf("import scope: %v", err)
	}
	if got, want := scope.String(), "import:12"; got != want {
		t.Fatalf("import scope = %q, want %q", got, want)
	}

	scope, err = seedstore.NewObservationRunScopeRepair(34)
	if err != nil {
		t.Fatalf("repair scope: %v", err)
	}
	if got, want := scope.String(), "repair:34"; got != want {
		t.Fatalf("repair scope = %q, want %q", got, want)
	}

	if _, err := seedstore.NewObservationRunScopeImport(0); err == nil {
		t.Fatal("expected import scope zero ID error")
	}
	if _, err := seedstore.NewObservationRunScopeRepair(0); err == nil {
		t.Fatal("expected repair scope zero ID error")
	}
}

func TestNormalizedPublicationStatePreservesWithheld(t *testing.T) {
	if got := normalizedPublicationState(domain.PublicationStateWithheld); got != domain.PublicationStateWithheld {
		t.Fatalf("normalized withheld state = %q, want %q", got, domain.PublicationStateWithheld)
	}
	if got := normalizedPublicationState(domain.PublicationState("unexpected")); got != domain.PublicationStateReviewed {
		t.Fatalf("normalized unexpected state = %q, want %q", got, domain.PublicationStateReviewed)
	}
}

func TestUnit2SchemaFoundationConstraints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := st.db
	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "slug-alias-target-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "slug-alias-target-event-2", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "exact-identity-target-event", venueID, sourceID, domain.OriginLive)
	insertLegacyEvent(t, db, "exact-identity-target-event-2", venueID, sourceID, domain.OriginLive)

	var aliasEventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "slug-alias-target-event").Scan(&aliasEventID); err != nil {
		t.Fatalf("lookup alias target event id: %v", err)
	}
	var aliasEventID2 int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "slug-alias-target-event-2").Scan(&aliasEventID2); err != nil {
		t.Fatalf("lookup alias target event id 2: %v", err)
	}
	var identityEventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "exact-identity-target-event").Scan(&identityEventID); err != nil {
		t.Fatalf("lookup identity target event id: %v", err)
	}
	var identityEventID2 int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "exact-identity-target-event-2").Scan(&identityEventID2); err != nil {
		t.Fatalf("lookup identity target event id 2: %v", err)
	}

	repairRunID := mustInsertRepairRun(t, db)

	if _, err := db.Exec(`
		INSERT INTO slug_aliases (
			alias_slug,
			target_kind,
			target_event_id,
			target_venue_id,
			repair_run_id,
			reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "alt-slug", string(seedstore.SlugAliasTargetKindEvent), aliasEventID, nil, repairRunID, "typo correction", "2026-05-12T09:00:00Z", "2026-05-12T09:00:00Z"); err != nil {
		t.Fatalf("insert slug alias: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO slug_aliases (
			alias_slug,
			target_kind,
			target_event_id,
			target_venue_id,
			repair_run_id,
			reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "alt-slug", string(seedstore.SlugAliasTargetKindEvent), aliasEventID2, nil, repairRunID, "duplicate alias", "2026-05-12T09:01:00Z", "2026-05-12T09:01:00Z"); err == nil {
		t.Fatal("expected unique slug alias constraint error")
	}
	if _, err := db.Exec(`
		INSERT INTO slug_aliases (
			alias_slug,
			target_kind,
			target_event_id,
			target_venue_id,
			repair_run_id,
			reason,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "bad-target", string(seedstore.SlugAliasTargetKindEvent), nil, venueID, repairRunID, "shape mismatch", "2026-05-12T09:02:00Z", "2026-05-12T09:02:00Z"); err == nil {
		t.Fatal("expected slug alias target shape check error")
	}

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
	`, identityEventID, "identity-key", 1, "leadmill", "2026-05-12T19:00:00Z", "Exact Identity Target Event", 1, "2026-05-12T09:10:00Z", "2026-05-12T09:10:00Z", nil, "", repairRunID); err != nil {
		t.Fatalf("insert active exact identity: %v", err)
	}
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
	`, identityEventID, "identity-key", 1, "leadmill", "2026-05-12T19:00:00Z", "Exact Identity Target Event", 0, "2026-05-12T09:11:00Z", "2026-05-12T09:11:00Z", "2026-05-12T10:00:00Z", "superseded", repairRunID); err != nil {
		t.Fatalf("insert inactive historical exact identity: %v", err)
	}
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
	`, identityEventID2, "identity-key", 1, "leadmill", "2026-05-12T19:00:00Z", "Exact Identity Target Event 2", 1, "2026-05-12T09:12:00Z", "2026-05-12T09:12:00Z", nil, "", repairRunID); err == nil {
		t.Fatal("expected active exact identity uniqueness error")
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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "import:1", sourceID, "fixture-uid-1", string(seedstore.SourceAuthorityAuthoritative), string(seedstore.ObservationTargetKindEvent), identityEventID, nil, "title", "Raw title", "raw title", "Canonical title", "canonical title", "accepted", 0, "2026-05-12T09:20:00Z", "2026-05-12T09:20:00Z"); err != nil {
		t.Fatalf("insert observation: %v", err)
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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "import:1", sourceID, "fixture-uid-1", string(seedstore.SourceAuthorityAuthoritative), string(seedstore.ObservationTargetKindEvent), identityEventID, nil, "title", "Raw title 2", "raw title 2", "Canonical title", "canonical title", "accepted", 0, "2026-05-12T09:21:00Z", "2026-05-12T09:21:00Z"); err == nil {
		t.Fatal("expected observation unique constraint error")
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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "repair:1", sourceID, "fixture-uid-2", string(seedstore.SourceAuthoritySupporting), string(seedstore.ObservationTargetKindReviewGroup), identityEventID, nil, "title", "Raw title", "raw title", "Canonical title", "canonical title", "accepted", 0, "2026-05-12T09:22:00Z", "2026-05-12T09:22:00Z"); err == nil {
		t.Fatal("expected observation target shape check error")
	}
}

func TestValidateRejectsInvalidObservationRunScopes(t *testing.T) {
	cases := []struct {
		name     string
		runScope string
		want     string
	}{
		{name: "manual", runScope: "manual", want: "invalid run_scope"},
		{name: "import-zero", runScope: "import:0", want: "invalid run_scope"},
		{name: "repair-missing", runScope: "repair:999999", want: "missing repair run"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := bootstrapUnit2DirtyStore(t)
			db := mustRawDB(t, path)
			sourceID, eventID := insertUnit2ReferenceEvent(t, db, "observation-"+tc.name)
			recreateDirtyObservationTable(t, db)
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
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, tc.runScope, sourceID, "fixture-uid", string(seedstore.SourceAuthorityAuthoritative), string(seedstore.ObservationTargetKindEvent), eventID, nil, "title", "Raw title", "raw title", "Canonical title", "canonical title", "accepted", 0, "2026-05-12T09:20:00Z", "2026-05-12T09:20:00Z"); err != nil {
				t.Fatalf("insert observation: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close raw db: %v", err)
			}

			if _, err := Open(path); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestValidateRejectsEventCanonicalAndWithheldRepairReferences(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T, db *sql.DB, eventID int64)
		want   string
	}{
		{
			name: "missing-canonical",
			mutate: func(t *testing.T, db *sql.DB, eventID int64) {
				t.Helper()
				if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
					t.Fatalf("disable foreign keys: %v", err)
				}
				if _, err := db.Exec(`UPDATE events SET canonical_event_id = ? WHERE id = ?`, 999999, eventID); err != nil {
					t.Fatalf("update canonical_event_id: %v", err)
				}
			},
			want: "missing canonical event",
		},
		{
			name: "self-canonical",
			mutate: func(t *testing.T, db *sql.DB, eventID int64) {
				t.Helper()
				if _, err := db.Exec(`UPDATE events SET canonical_event_id = id WHERE id = ?`, eventID); err != nil {
					t.Fatalf("update self canonical_event_id: %v", err)
				}
			},
			want: "references itself as canonical event",
		},
		{
			name: "missing-withheld-repair",
			mutate: func(t *testing.T, db *sql.DB, eventID int64) {
				t.Helper()
				if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
					t.Fatalf("disable foreign keys: %v", err)
				}
				if _, err := db.Exec(`UPDATE events SET withheld_repair_run_id = ? WHERE id = ?`, 999999, eventID); err != nil {
					t.Fatalf("update withheld_repair_run_id: %v", err)
				}
			},
			want: "missing withheld repair run",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := bootstrapUnit2DirtyStore(t)
			db := mustRawDB(t, path)
			_, eventID := insertUnit2ReferenceEvent(t, db, "event-"+tc.name)
			tc.mutate(t, db, eventID)
			if err := db.Close(); err != nil {
				t.Fatalf("close raw db: %v", err)
			}

			if _, err := Open(path); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestValidateRejectsInvalidExactIdentityLifecycleMetadata(t *testing.T) {
	cases := []struct {
		name              string
		active            int
		deactivatedAt     any
		deactivatedReason string
		want              string
	}{
		{
			name:              "active-with-deactivation-metadata",
			active:            1,
			deactivatedAt:     "2026-05-12T10:00:00Z",
			deactivatedReason: "should be empty",
			want:              "active but has deactivation metadata",
		},
		{
			name:              "inactive-without-deactivation-metadata",
			active:            0,
			deactivatedAt:     nil,
			deactivatedReason: "",
			want:              "inactive but lacks deactivation metadata",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := bootstrapUnit2DirtyStore(t)
			db := mustRawDB(t, path)
			_, eventID := insertUnit2ReferenceEvent(t, db, "exact-"+tc.name)
			recreateDirtyExactIdentityTable(t, db)
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
			`, eventID, "identity-key", 1, "leadmill", "2026-05-12T19:00:00Z", "Exact Identity Target Event", tc.active, "2026-05-12T09:10:00Z", "2026-05-12T09:10:00Z", tc.deactivatedAt, tc.deactivatedReason, nil); err != nil {
				t.Fatalf("insert exact identity: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close raw db: %v", err)
			}

			if _, err := Open(path); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestValidateRejectsActiveExactIdentitiesForWithheldOrNonLiveEvents(t *testing.T) {
	cases := []struct {
		name             string
		origin           domain.Origin
		publicationState domain.PublicationState
		withheldReason   string
		want             string
	}{
		{
			name:             "withheld-event",
			origin:           domain.OriginLive,
			publicationState: domain.PublicationStateWithheld,
			withheldReason:   "duplicate listing",
			want:             "active for withheld event",
		},
		{
			name:             "non-live-event",
			origin:           domain.OriginTest,
			publicationState: domain.PublicationStateReviewed,
			want:             "active for event",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := bootstrapUnit2DirtyStore(t)
			db := mustRawDB(t, path)
			_, eventID := insertUnit2ReferenceEvent(t, db, "exact-"+tc.name)
			recreateDirtyExactIdentityTable(t, db)
			if _, err := db.Exec(`
				UPDATE events
				SET origin = ?, publication_state = ?, withheld_reason = ?
				WHERE id = ?
			`, string(tc.origin), string(tc.publicationState), tc.withheldReason, eventID); err != nil {
				t.Fatalf("update event state: %v", err)
			}
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
			`, eventID, "identity-key", 1, "leadmill", "2026-05-12T19:00:00Z", "Exact Identity Target Event", 1, "2026-05-12T09:10:00Z", "2026-05-12T09:10:00Z", nil, "", nil); err != nil {
				t.Fatalf("insert active exact identity: %v", err)
			}
			if err := validateUnit2Schema(context.Background(), db); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate error = %v, want substring %q", err, tc.want)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close raw db: %v", err)
			}
		})
	}
}

func TestOpenBackfillsActiveExactIdentitiesForWithheldOrNonLiveEvents(t *testing.T) {
	cases := []struct {
		name             string
		origin           domain.Origin
		publicationState domain.PublicationState
		withheldReason   string
		wantReason       string
	}{
		{
			name:             "withheld-event",
			origin:           domain.OriginLive,
			publicationState: domain.PublicationStateWithheld,
			withheldReason:   "duplicate listing",
			wantReason:       "event is withheld",
		},
		{
			name:             "non-live-event",
			origin:           domain.OriginTest,
			publicationState: domain.PublicationStateReviewed,
			wantReason:       "event is not live",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := bootstrapUnit2DirtyStore(t)
			db := mustRawDB(t, path)
			_, eventID := insertUnit2ReferenceEvent(t, db, "open-"+tc.name)
			recreateDirtyExactIdentityTable(t, db)
			if _, err := db.Exec(`
				UPDATE events
				SET origin = ?, publication_state = ?, withheld_reason = ?
				WHERE id = ?
			`, string(tc.origin), string(tc.publicationState), tc.withheldReason, eventID); err != nil {
				t.Fatalf("update event state: %v", err)
			}
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
			`, eventID, "identity-key", 1, "leadmill", "2026-05-12T19:00:00Z", "Exact Identity Target Event", 1, "2026-05-12T09:10:00Z", "2026-05-12T09:10:00Z", nil, "", nil); err != nil {
				t.Fatalf("insert active exact identity: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close raw db: %v", err)
			}

			st, err := Open(path)
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			rows := mustExactIdentityRowsByEvent(t, st.db, eventID)
			if got, want := len(rows), 1; got != want {
				t.Fatalf("exact identity rows after open = %d, want %d", got, want)
			}
			if rows[0].Active != 0 {
				t.Fatalf("exact identity active after open = %d, want 0", rows[0].Active)
			}
			if rows[0].DeactivatedReason != tc.wantReason {
				t.Fatalf("deactivated reason = %q, want %q", rows[0].DeactivatedReason, tc.wantReason)
			}
			if rows[0].DeactivatedAt == "" {
				t.Fatal("deactivated exact identity missing deactivated_at")
			}
		})
	}
}

func bootstrapUnit2DirtyStore(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("bootstrap store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close bootstrap store: %v", err)
	}
	return path
}

func insertUnit2ReferenceEvent(t *testing.T, db *sql.DB, slug string) (int64, int64) {
	t.Helper()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, slug, venueID, sourceID, domain.OriginLive)

	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, slug).Scan(&eventID); err != nil {
		t.Fatalf("lookup event id %q: %v", slug, err)
	}
	return sourceID, eventID
}

func recreateDirtyObservationTable(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(`DROP TABLE IF EXISTS event_source_attribute_observations`); err != nil {
		t.Fatalf("drop observation table: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE event_source_attribute_observations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_scope TEXT NOT NULL,
			source_id INTEGER NOT NULL,
			source_identity_key TEXT NOT NULL,
			source_authority TEXT NOT NULL,
			target_kind TEXT NOT NULL,
			event_id INTEGER,
			review_group_id INTEGER,
			field_name TEXT NOT NULL,
			incoming_raw TEXT NOT NULL DEFAULT '',
			incoming_normalized TEXT NOT NULL DEFAULT '',
			canonical_before_raw TEXT NOT NULL DEFAULT '',
			canonical_before_normalized TEXT NOT NULL DEFAULT '',
			outcome TEXT NOT NULL DEFAULT '',
			is_conflict INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create dirty observation table: %v", err)
	}
}

func recreateDirtyExactIdentityTable(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(`DROP TABLE IF EXISTS event_exact_identities`); err != nil {
		t.Fatalf("drop exact identity table: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE event_exact_identities (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id INTEGER NOT NULL,
			identity_key TEXT NOT NULL,
			key_version INTEGER NOT NULL DEFAULT 1,
			venue_slug TEXT NOT NULL,
			utc_start_at TEXT NOT NULL,
			clean_title TEXT NOT NULL,
			active INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deactivated_at TEXT,
			deactivated_reason TEXT NOT NULL DEFAULT '',
			repair_run_id INTEGER
		)
	`); err != nil {
		t.Fatalf("create dirty exact identity table: %v", err)
	}
}

func mustHaveUniqueIndex(t *testing.T, db *sql.DB, table string, wantPartial bool) {
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
		if unique != 1 {
			continue
		}
		if wantPartial && partial != 1 {
			continue
		}
		if !wantPartial && partial != 0 {
			continue
		}
		found = true
		break
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index list for %s: %v", table, err)
	}
	if !found {
		t.Fatalf("missing unique index on %s", table)
	}
}

func mustInsertRepairRun(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO repair_runs (started_at, status, notes)
		VALUES (?, ?, ?)
	`, "2026-05-12T09:00:00Z", "running", "fixture repair run")
	if err != nil {
		t.Fatalf("insert repair run: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("repair run id: %v", err)
	}
	return id
}
