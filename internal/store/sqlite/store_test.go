package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

func TestOpenBootstrapsFreshDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	seed := seedstore.NewSeedStore()
	if got, want := st.Venues(), seed.Venues(); !reflect.DeepEqual(got, want) {
		t.Fatalf("venues = %#v, want %#v", got, want)
	}
	if got, want := st.Events(), seed.Events(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
	if err := st.Validate(context.Background()); err != nil {
		t.Fatalf("validate store: %v", err)
	}

	db := mustRawDB(t, path)

	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV11 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV11)
	}
	if got := mustCount(t, db, "venues"); got != 7 {
		t.Fatalf("venues rows = %d, want 7", got)
	}
	if got := mustCount(t, db, "events"); got != 4 {
		t.Fatalf("events rows = %d, want 4", got)
	}
	if got := mustCount(t, db, "sources"); got != 3 {
		t.Fatalf("sources rows = %d, want 3", got)
	}
	if got := mustCount(t, db, "import_runs"); got != 0 {
		t.Fatalf("import_runs rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "snapshots"); got != 0 {
		t.Fatalf("snapshots rows = %d, want 0", got)
	}

	var version int
	var appliedAt string
	if err := db.QueryRow(`SELECT version, applied_at FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&version, &appliedAt); err != nil {
		t.Fatalf("scan migration row: %v", err)
	}
	if version != schemaVersionV11 {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionV11)
	}
	if _, err := time.Parse(time.RFC3339, appliedAt); err != nil {
		t.Fatalf("applied_at %q is not RFC3339: %v", appliedAt, err)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "review_candidates"); got != 0 {
		t.Fatalf("review_candidates rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "review_draft_choices"); got != 0 {
		t.Fatalf("review_draft_choices rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_secondary_source_info"); got != 0 {
		t.Fatalf("event_secondary_source_info rows = %d, want 0", got)
	}
}

func TestOpenReopensPersistentData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`INSERT INTO venues (slug, name, address, neighbourhood, description, website, origin) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"persisted-venue", "Persisted Venue", "1 Persisted Street, Sheffield", "Centre", "Persisted venue", "https://example.test/venue", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert venue: %v", err)
	}
	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, "persisted-venue").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Persisted source", "https://example.test/source"); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Persisted source", "https://example.test/source").Scan(&sourceID); err != nil {
		t.Fatalf("lookup source id: %v", err)
	}
	start := time.Date(2026, time.May, 20, 18, 30, 0, 0, time.UTC)
	end := time.Date(2026, time.May, 20, 21, 0, 0, 0, time.UTC)
	checked := time.Date(2026, time.May, 20, 9, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`
		INSERT INTO events (
			slug, venue_id, source_id, name, start_at, end_at, genre, status, description, last_checked_at, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "persisted-event", venueID, sourceID, "Persisted Event", formatRFC3339UTC(start), formatRFC3339UTC(end), "Indie", "Listed", "Persisted event", formatRFC3339UTC(checked), string(domain.OriginLive)); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	venue, ok := st.VenueBySlug("persisted-venue")
	if !ok {
		t.Fatal("missing persisted venue")
	}
	if venue.Name != "Persisted Venue" {
		t.Fatalf("venue name = %q, want %q", venue.Name, "Persisted Venue")
	}

	event, ok := st.EventBySlug("persisted-event")
	if !ok {
		t.Fatal("missing persisted event")
	}
	if event.VenueSlug != "persisted-venue" {
		t.Fatalf("event venue slug = %q, want %q", event.VenueSlug, "persisted-venue")
	}
	if got := st.EventsForVenue("persisted-venue"); len(got) != 1 || got[0].Slug != "persisted-event" {
		t.Fatalf("events for venue = %#v, want one persisted event", got)
	}
}

func TestOpenMigratesVersion1Database(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	initSQL, err := readMigration("migrations/0001_init.sql")
	if err != nil {
		t.Fatalf("read v1 migration: %v", err)
	}
	if _, err := db.Exec(initSQL); err != nil {
		t.Fatalf("apply v1 migration: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO schema_migrations (version, applied_at)
		VALUES (?, ?)
	`, schemaVersionV1, formatRFC3339UTC(time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC))); err != nil {
		t.Fatalf("insert v1 migration row: %v", err)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV11 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV11)
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("scan max schema version: %v", err)
	}
	if version != schemaVersionV11 {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionV11)
	}
	if got := mustCount(t, db, "review_groups"); got != 0 {
		t.Fatalf("review_groups rows = %d, want 0", got)
	}
}

func TestOpenMigratesVersion2DatabasePreservesReviewDataAndAddsStagingKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	initSQL, err := readMigration("migrations/0001_init.sql")
	if err != nil {
		t.Fatalf("read v1 migration: %v", err)
	}
	if _, err := db.Exec(initSQL); err != nil {
		t.Fatalf("apply v1 migration: %v", err)
	}
	reviewSQL, err := readMigration("migrations/0002_review.sql")
	if err != nil {
		t.Fatalf("read v2 migration: %v", err)
	}
	if _, err := db.Exec(reviewSQL); err != nil {
		t.Fatalf("apply v2 migration: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO schema_migrations (version, applied_at)
		VALUES (?, ?)
	`, schemaVersionV1, formatRFC3339UTC(time.Date(2026, time.April, 19, 9, 0, 0, 0, time.UTC))); err != nil {
		t.Fatalf("insert v1 migration row: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO schema_migrations (version, applied_at)
		VALUES (?, ?)
	`, schemaVersionV2, formatRFC3339UTC(time.Date(2026, time.April, 19, 10, 0, 0, 0, time.UTC))); err != nil {
		t.Fatalf("insert v2 migration row: %v", err)
	}
	groupRes, err := db.Exec(`
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "Migrated review", "Fixture ICS", "file:migrated.ics", review.StatusOpen, "Preserved notes", formatRFC3339UTC(time.Date(2026, time.April, 19, 11, 0, 0, 0, time.UTC)), formatRFC3339UTC(time.Date(2026, time.April, 19, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("insert review group: %v", err)
	}
	groupID, err := groupRes.LastInsertId()
	if err != nil {
		t.Fatalf("review group id: %v", err)
	}
	candidateRes, err := db.Exec(`
		INSERT INTO review_candidates (
			group_id,
			position,
			external_id,
			name,
			venue_slug,
			start_at,
			end_at,
			genre,
			status,
			description,
			source_name,
			source_url,
			provenance
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, groupID, 1, "candidate-a", "Candidate A", "leadmill", "2026-05-01T19:00:00Z", "2026-05-01T22:00:00Z", "Indie", "Listed", "Description", "Fixture ICS", "file:candidate-a.ics", "fixture UID candidate-a")
	if err != nil {
		t.Fatalf("insert review candidate: %v", err)
	}
	candidateID, err := candidateRes.LastInsertId()
	if err != nil {
		t.Fatalf("review candidate id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO review_draft_choices (
			group_id,
			field,
			candidate_id,
			value,
			updated_at
		) VALUES (?, ?, ?, ?, ?)
	`, groupID, string(review.FieldName), candidateID, "Candidate A", formatRFC3339UTC(time.Date(2026, time.April, 19, 13, 0, 0, 0, time.UTC))); err != nil {
		t.Fatalf("insert review draft choice: %v", err)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV11 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV11)
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("scan max schema version: %v", err)
	}
	if version != schemaVersionV11 {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionV11)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}

	group, ok, err := st.LoadReviewGroup(context.Background(), groupID)
	if err != nil {
		t.Fatalf("load review group: %v", err)
	}
	if !ok {
		t.Fatal("review group not found")
	}
	if group.Status != review.StatusOpen {
		t.Fatalf("status = %q, want %q", group.Status, review.StatusOpen)
	}
	if group.Notes != "Preserved notes" {
		t.Fatalf("notes = %q, want %q", group.Notes, "Preserved notes")
	}
	if len(group.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(group.Candidates))
	}
	if got := len(group.DraftChoices); got != 1 {
		t.Fatalf("draft choice count = %d, want 1", got)
	}
	if _, ok := group.DraftChoices[review.FieldName]; !ok {
		t.Fatal("missing draft choice after migration")
	}

	var stagingKey sql.NullString
	if err := db.QueryRow(`SELECT staging_key FROM review_groups WHERE id = ?`, groupID).Scan(&stagingKey); err != nil {
		t.Fatalf("scan staging key: %v", err)
	}
	if stagingKey.Valid {
		t.Fatalf("staging key valid = true, want false")
	}

	rows, err := db.Query(`PRAGMA table_info(review_groups)`)
	if err != nil {
		t.Fatalf("table info: %v", err)
	}
	defer rows.Close()
	foundColumn := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan table info row: %v", err)
		}
		if name == "staging_key" {
			foundColumn = true
			if notnull != 0 {
				t.Fatalf("staging_key notnull = %d, want 0", notnull)
			}
			if dflt.Valid {
				t.Fatalf("staging_key default = %q, want NULL", dflt.String)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table info: %v", err)
	}
	if !foundColumn {
		t.Fatal("missing staging_key column")
	}

	indexRows, err := db.Query(`PRAGMA index_list(review_groups)`)
	if err != nil {
		t.Fatalf("index list: %v", err)
	}
	defer indexRows.Close()
	foundIndex := false
	for indexRows.Next() {
		var seq, unique, partial int
		var name, origin string
		if err := indexRows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan index row: %v", err)
		}
		if name == "idx_review_groups_staging_key" {
			foundIndex = true
			if unique != 1 {
				t.Fatalf("staging key index unique = %d, want 1", unique)
			}
		}
	}
	if err := indexRows.Err(); err != nil {
		t.Fatalf("iterate index list: %v", err)
	}
	if !foundIndex {
		t.Fatal("missing staging key index")
	}
}

func TestOpenMigratesVersion3DatabaseAddsEventSourceLinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	initSQL, err := readMigration("migrations/0001_init.sql")
	if err != nil {
		t.Fatalf("read v1 migration: %v", err)
	}
	if _, err := db.Exec(initSQL); err != nil {
		t.Fatalf("apply v1 migration: %v", err)
	}
	reviewSQL, err := readMigration("migrations/0002_review.sql")
	if err != nil {
		t.Fatalf("read v2 migration: %v", err)
	}
	if _, err := db.Exec(reviewSQL); err != nil {
		t.Fatalf("apply v2 migration: %v", err)
	}
	stagingSQL, err := readMigration("migrations/0003_review_staging_idempotency.sql")
	if err != nil {
		t.Fatalf("read v3 migration: %v", err)
	}
	if _, err := db.Exec(stagingSQL); err != nil {
		t.Fatalf("apply v3 migration: %v", err)
	}
	for version := schemaVersionV1; version <= schemaVersionV3; version++ {
		if _, err := db.Exec(`
			INSERT INTO schema_migrations (version, applied_at)
			VALUES (?, ?)
		`, version, formatRFC3339UTC(time.Date(2026, time.April, 19, 8+version, 0, 0, 0, time.UTC))); err != nil {
			t.Fatalf("insert v%d migration row: %v", version, err)
		}
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV11 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV11)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_secondary_source_info"); got != 0 {
		t.Fatalf("event_secondary_source_info rows = %d, want 0", got)
	}

	indexRows, err := db.Query(`PRAGMA index_list(event_source_links)`)
	if err != nil {
		t.Fatalf("index list: %v", err)
	}
	defer indexRows.Close()
	foundUnique := false
	for indexRows.Next() {
		var seq, unique, partial int
		var name, origin string
		if err := indexRows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan index row: %v", err)
		}
		if unique == 1 {
			foundUnique = true
		}
	}
	if err := indexRows.Err(); err != nil {
		t.Fatalf("iterate index list: %v", err)
	}
	if !foundUnique {
		t.Fatal("missing unique event source link index")
	}
}

func TestOpenMigratesVersion4DatabaseAddsReviewGroupAuthoritativeLinkColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	initSQL, err := readMigration("migrations/0001_init.sql")
	if err != nil {
		t.Fatalf("read v1 migration: %v", err)
	}
	if _, err := db.Exec(initSQL); err != nil {
		t.Fatalf("apply v1 migration: %v", err)
	}
	for _, path := range []string{
		"migrations/0002_review.sql",
		"migrations/0003_review_staging_idempotency.sql",
		"migrations/0004_event_source_links.sql",
	} {
		migrationSQL, err := readMigration(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		if _, err := db.Exec(migrationSQL); err != nil {
			t.Fatalf("apply migration %s: %v", path, err)
		}
	}
	for version := schemaVersionV1; version <= schemaVersionV4; version++ {
		if _, err := db.Exec(`
			INSERT INTO schema_migrations (version, applied_at)
			VALUES (?, ?)
		`, version, formatRFC3339UTC(time.Date(2026, time.April, 20, 8+version, 0, 0, 0, time.UTC))); err != nil {
			t.Fatalf("insert v%d migration row: %v", version, err)
		}
	}
	openGroupID := mustInsertReviewGroupRow(t, db, `
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "Migrated authoritative review", "Sidney & Matilda manual ingest", "https://calendar.example.test/live.ics", "group-staging-key", review.StatusOpen, "Preserved notes", formatRFC3339UTC(time.Date(2026, time.April, 20, 13, 0, 0, 0, time.UTC)), formatRFC3339UTC(time.Date(2026, time.April, 20, 14, 0, 0, 0, time.UTC)))
	if _, err := db.Exec(`
		INSERT INTO review_candidates (
			group_id,
			position,
			external_id,
			name,
			venue_slug,
			start_at,
			end_at,
			genre,
			status,
			description,
			source_name,
			source_url,
			provenance
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, openGroupID, 1, "shared-uid", "Migrated authoritative review", "sidney-and-matilda", "2026-05-01T19:00:00Z", "2026-05-01T22:00:00Z", "Indie", "Listed", "Description", "", "", "fixture UID shared-uid"); err != nil {
		t.Fatalf("insert open review candidate: %v", err)
	}
	closedGroupID := mustInsertReviewGroupRow(t, db, `
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "Migrated resolved review", "Sidney & Matilda manual ingest", "https://calendar.example.test/live.ics", "resolved-group-staging-key", review.StatusResolved, "Resolved notes", formatRFC3339UTC(time.Date(2026, time.April, 20, 15, 0, 0, 0, time.UTC)), formatRFC3339UTC(time.Date(2026, time.April, 20, 16, 0, 0, 0, time.UTC)))
	if _, err := db.Exec(`
		INSERT INTO review_candidates (
			group_id,
			position,
			external_id,
			name,
			venue_slug,
			start_at,
			end_at,
			genre,
			status,
			description,
			source_name,
			source_url,
			provenance
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, closedGroupID, 1, "resolved-shared-uid", "Migrated resolved review", "sidney-and-matilda", "2026-05-02T19:00:00Z", "2026-05-02T22:00:00Z", "Indie", "Listed", "Description", "", "", "fixture UID resolved-shared-uid"); err != nil {
		t.Fatalf("insert closed review candidate: %v", err)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV11 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV11)
	}

	group, ok, err := st.LoadReviewGroup(context.Background(), openGroupID)
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

	closed, ok, err := st.LoadReviewGroup(context.Background(), closedGroupID)
	if err != nil {
		t.Fatalf("load closed review group: %v", err)
	}
	if !ok {
		t.Fatal("closed review group not found")
	}
	if closed.AuthoritativeSourceName != "" || closed.AuthoritativeSourceURL != "" || closed.AuthoritativeSourceEventKey != "" {
		t.Fatalf("closed authoritative fields = %#v, want empty", closed)
	}

	rows, err := db.Query(`PRAGMA table_info(review_groups)`)
	if err != nil {
		t.Fatalf("table info: %v", err)
	}
	defer rows.Close()
	foundColumns := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan table info row: %v", err)
		}
		switch name {
		case "authoritative_source_name", "authoritative_source_url", "authoritative_source_event_key":
			foundColumns[name] = true
			if notnull != 0 {
				t.Fatalf("%s notnull = %d, want 0", name, notnull)
			}
			if dflt.Valid {
				t.Fatalf("%s default = %q, want NULL", name, dflt.String)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table info: %v", err)
	}
	for _, name := range []string{"authoritative_source_name", "authoritative_source_url", "authoritative_source_event_key"} {
		if !foundColumns[name] {
			t.Fatalf("missing %s column", name)
		}
	}
}

func TestOpenMigratesVersion5DatabaseAddsEventSecondarySourceInfoTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	for _, migrationPath := range []string{
		"migrations/0001_init.sql",
		"migrations/0002_review.sql",
		"migrations/0003_review_staging_idempotency.sql",
		"migrations/0004_event_source_links.sql",
		"migrations/0005_review_group_authoritative_link.sql",
	} {
		migrationSQL, err := readMigration(migrationPath)
		if err != nil {
			t.Fatalf("read migration %s: %v", migrationPath, err)
		}
		if _, err := db.Exec(migrationSQL); err != nil {
			t.Fatalf("apply migration %s: %v", migrationPath, err)
		}
	}
	for version := schemaVersionV1; version <= schemaVersionV5; version++ {
		if _, err := db.Exec(`
			INSERT INTO schema_migrations (version, applied_at)
			VALUES (?, ?)
		`, version, formatRFC3339UTC(time.Date(2026, time.April, 21, 8+version, 0, 0, 0, time.UTC))); err != nil {
			t.Fatalf("insert v%d migration row: %v", version, err)
		}
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV11 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV11)
	}
	if got := mustCount(t, db, "event_secondary_source_info"); got != 0 {
		t.Fatalf("event_secondary_source_info rows = %d, want 0", got)
	}

	indexRows, err := db.Query(`PRAGMA index_list(event_secondary_source_info)`)
	if err != nil {
		t.Fatalf("index list: %v", err)
	}
	defer indexRows.Close()
	foundUnique := false
	foundEventIndex := false
	for indexRows.Next() {
		var seq, unique, partial int
		var name, origin string
		if err := indexRows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan index row: %v", err)
		}
		if unique == 1 {
			foundUnique = true
		}
		if name == "idx_event_secondary_source_info_event_id" {
			foundEventIndex = true
		}
	}
	if err := indexRows.Err(); err != nil {
		t.Fatalf("iterate index list: %v", err)
	}
	if !foundUnique {
		t.Fatal("missing unique secondary source info index")
	}
	if !foundEventIndex {
		t.Fatal("missing event_secondary_source_info event_id index")
	}
}

func TestOpenRoundTripsUTCTimes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	start := time.Date(2026, time.May, 8, 19, 30, 0, 0, time.FixedZone("BST", 60*60))
	end := time.Date(2026, time.May, 8, 23, 0, 0, 0, time.FixedZone("BST", 60*60))
	checked := time.Date(2026, time.April, 19, 10, 0, 0, 0, time.FixedZone("BST", 60*60))
	if _, err := db.Exec(`
		UPDATE events
		SET start_at = ?, end_at = ?, last_checked_at = ?
		WHERE slug = ?
	`, start.Format(time.RFC3339), end.Format(time.RFC3339), checked.Format(time.RFC3339), "matinee-noise-at-the-leadmill"); err != nil {
		t.Fatalf("update event: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	event, ok := st.EventBySlug("matinee-noise-at-the-leadmill")
	if !ok {
		t.Fatal("missing event")
	}
	if event.Start.Location() != time.UTC {
		t.Fatalf("start location = %v, want UTC", event.Start.Location())
	}
	if !event.Start.Equal(start.UTC()) {
		t.Fatalf("start = %v, want %v", event.Start, start.UTC())
	}
	if !event.End.Equal(end.UTC()) {
		t.Fatalf("end = %v, want %v", event.End, end.UTC())
	}
	if !event.LastChecked.Equal(checked.UTC()) {
		t.Fatalf("last checked = %v, want %v", event.LastChecked, checked.UTC())
	}
}

func TestOpenRoundTripsNullCanonicalEndTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		UPDATE events
		SET end_at = NULL
		WHERE slug = ?
	`, "matinee-noise-at-the-leadmill"); err != nil {
		t.Fatalf("clear event end: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()

	event, ok := st.EventBySlug("matinee-noise-at-the-leadmill")
	if !ok {
		t.Fatal("missing event")
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for unknown end", event.End)
	}
}

func TestOpenBackfillsOwnedVenueAuthoritativePlaceholderEndToNull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Cafe No. 9 manual ingest", "https://www.wegottickets.com/Cafe9"); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, "cafe-no-9").Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Cafe No. 9 manual ingest", "https://www.wegottickets.com/Cafe9").Scan(&sourceID); err != nil {
		t.Fatalf("lookup source id: %v", err)
	}
	start := "2026-05-10T18:30:00Z"
	if _, err := db.Exec(`
		INSERT INTO events (
			slug, venue_id, source_id, name, start_at, end_at, genre, status, description, last_checked_at, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "live-cafe-no-9-placeholder", venueID, sourceID, "Cafe No. 9 Placeholder", start, start, "", "Listed", "", "2026-05-10T12:00:00Z", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "live-cafe-no-9-placeholder").Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_source_links (
			source_id, event_id, source_event_key, is_authoritative, created_at, updated_at
		) VALUES (?, ?, ?, 1, ?, ?)
	`, sourceID, eventID, "cafe-no-9-placeholder", "2026-05-10T12:00:00Z", "2026-05-10T12:00:00Z"); err != nil {
		t.Fatalf("insert event source link: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()

	event, ok := st.EventBySlug("live-cafe-no-9-placeholder")
	if !ok {
		t.Fatal("missing event")
	}
	if !event.End.IsZero() {
		t.Fatalf("end = %v, want zero time for backfilled unknown end", event.End)
	}

	db = mustRawDB(t, path)
	defer db.Close()
	var endAt sql.NullString
	if err := db.QueryRow(`SELECT end_at FROM events WHERE slug = ?`, "live-cafe-no-9-placeholder").Scan(&endAt); err != nil {
		t.Fatalf("load end_at: %v", err)
	}
	if endAt.Valid {
		t.Fatalf("end_at = %q, want NULL", endAt.String)
	}
}

func TestOpenRejectsCanonicalEqualTimeEndOutsideOwnedVenueBackfill(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		UPDATE events
		SET end_at = start_at
		WHERE slug = ?
	`, "matinee-noise-at-the-leadmill"); err != nil {
		t.Fatalf("set equal-time end: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	if _, err := Open(path); err == nil {
		t.Fatal("expected open error")
	} else if !strings.Contains(err.Error(), "still uses placeholder end_at equal to start_at") {
		t.Fatalf("open error = %q, want placeholder audit error", err.Error())
	}
}

func TestOpenBackfillsImportRunReviewGroupLinksFromLegacyNotes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		INSERT INTO import_runs (id, started_at, finished_at, status, notes)
		VALUES (?, ?, ?, ?, ?)
	`, 12, "2026-04-20T10:00:00Z", "2026-04-20T10:05:00Z", "succeeded", "legacy fixture"); err != nil {
		t.Fatalf("insert import run: %v", err)
	}
	groupID := mustInsertReviewGroupRow(t, db, `
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "Legacy linked review", "Fixture ICS", "file:test.ics", review.StatusOpen, "Created from manual ingest run 12 review staging.", "2026-04-20T10:10:00Z", "2026-04-20T10:10:00Z")
	if _, err := db.Exec(`
		INSERT INTO review_candidates (
			group_id,
			position,
			external_id,
			name,
			venue_slug,
			start_at,
			end_at,
			genre,
			status,
			description,
			source_name,
			source_url,
			provenance
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, groupID, 1, "", "Legacy candidate", "leadmill", "2026-05-01T19:00:00Z", "2026-05-01T22:00:00Z", "", "Listed", "", "Fixture ICS", "file:test.ics", "fixture"); err != nil {
		t.Fatalf("insert review candidate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()

	groups, err := st.ListReviewGroupsForImportRun(context.Background(), 12)
	if err != nil {
		t.Fatalf("list review groups for import run: %v", err)
	}
	if got, want := len(groups), 1; got != want {
		t.Fatalf("review groups = %d, want %d", got, want)
	}
	if groups[0].ID != groupID {
		t.Fatalf("group id = %d, want %d", groups[0].ID, groupID)
	}

	db = mustRawDB(t, path)
	defer db.Close()
	if got := mustCount(t, db, "import_run_review_groups"); got != 1 {
		t.Fatalf("import_run_review_groups rows = %d, want 1", got)
	}
}

func TestOpenBootstrapsVenueCoverageData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	lescar, ok, err := st.LoadVenueBySlug(context.Background(), "lescar")
	if err != nil {
		t.Fatalf("load lescar: %v", err)
	}
	if !ok {
		t.Fatal("lescar venue not found")
	}
	if lescar.CoverageKind != domain.CoverageKindProgram {
		t.Fatalf("lescar coverage kind = %q, want %q", lescar.CoverageKind, domain.CoverageKindProgram)
	}
	if lescar.CoverageNote == "" {
		t.Fatal("lescar coverage note is empty")
	}
}

func TestOpenRejectsDanglingVenueReference(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := db.Exec(`UPDATE events SET venue_id = ? WHERE slug = ?`, 999999, "matinee-noise-at-the-leadmill"); err != nil {
		t.Fatalf("corrupt venue reference: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	if _, err := Open(path); err == nil {
		t.Fatal("expected open error")
	}
}

func TestOpenRejectsDanglingEventSecondarySourceInfoReference(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Secondary source", "https://example.test/secondary"); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Secondary source", "https://example.test/secondary").Scan(&sourceID); err != nil {
		t.Fatalf("lookup source id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_secondary_source_info (
			event_id,
			source_id,
			venue_slug,
			event_name,
			start_at,
			info_type,
			value,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, 999999, sourceID, "leadmill", "Broken secondary row", "2026-05-01T19:00:00Z", "description", "Broken", "2026-04-21T10:00:00Z", "2026-04-21T10:00:00Z"); err != nil {
		t.Fatalf("insert dangling secondary row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	if _, err := Open(path); err == nil {
		t.Fatal("expected open error")
	}
}

func mustInsertReviewGroupRow(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()

	res, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("insert review group row: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("review group row id: %v", err)
	}
	return id
}

func mustRawDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", dsnForPath(path))
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping raw db: %v", err)
	}
	return db
}

func mustCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()

	row := db.QueryRow("SELECT COUNT(*) FROM " + table)
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}
