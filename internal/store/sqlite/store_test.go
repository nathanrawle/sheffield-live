package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
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
	if err := st.Validate(); err != nil {
		t.Fatalf("validate store: %v", err)
	}

	db := mustRawDB(t, path)
	defer db.Close()

	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV3 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV3)
	}
	if got := mustCount(t, db, "venues"); got != 3 {
		t.Fatalf("venues rows = %d, want 3", got)
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
	if version != schemaVersionV3 {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionV3)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV3 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV3)
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("scan max schema version: %v", err)
	}
	if version != schemaVersionV3 {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionV3)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionV3 {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionV3)
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("scan max schema version: %v", err)
	}
	if version != schemaVersionV3 {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionV3)
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
