package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
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

	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionCurrent {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionCurrent)
	}
	if got := mustCount(t, db, "venues"); got != 7 {
		t.Fatalf("venues rows = %d, want 7", got)
	}
	if got := mustCount(t, db, "events"); got != 0 {
		t.Fatalf("events rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "sources"); got != 0 {
		t.Fatalf("sources rows = %d, want 0", got)
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
	if version != schemaVersionCurrent {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionCurrent)
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
		"import_run_snapshot_retention",
	} {
		ok, err := tableExists(context.Background(), db, table)
		if err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !ok {
			t.Fatalf("table %s does not exist", table)
		}
	}
	for _, check := range []struct {
		table  string
		column string
	}{
		{table: "event_review_clusters", column: "staging_key"},
		{table: "event_review_clusters", column: "staging_key_version"},
	} {
		ok, err := columnExists(context.Background(), db, check.table, check.column)
		if err != nil {
			t.Fatalf("check column %s.%s: %v", check.table, check.column, err)
		}
		if !ok {
			t.Fatalf("column %s.%s does not exist", check.table, check.column)
		}
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_secondary_source_info"); got != 0 {
		t.Fatalf("event_secondary_source_info rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_review_clusters"); got != 0 {
		t.Fatalf("event_review_clusters rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "event_review_evidence"); got != 0 {
		t.Fatalf("event_review_evidence rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "import_run_event_review_evidence"); got != 0 {
		t.Fatalf("import_run_event_review_evidence rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_run_event_review_evidence"); got != 0 {
		t.Fatalf("repair_run_event_review_evidence rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "repair_runs"); got != 0 {
		t.Fatalf("repair_runs rows = %d, want 0", got)
	}
	if got := mustCount(t, db, "import_run_snapshot_retention"); got != 0 {
		t.Fatalf("import_run_snapshot_retention rows = %d, want 0", got)
	}
}

func TestOpenRepairsPreRebaseEventImageMigrationNumbering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")
	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV15)
	insertMigrationRowsThrough(t, db, schemaVersionV15, time.Date(2026, time.May, 9, 10, 0, 0, 0, time.UTC))
	applyMigrationFile(t, db, "migrations/0017_event_images.sql")
	applyMigrationFile(t, db, "migrations/0018_image_focus.sql")
	if _, err := db.Exec(`
		INSERT INTO schema_migrations (version, applied_at)
		VALUES (?, ?), (?, ?)
	`, schemaVersionV16, "2026-05-09T10:06:36Z", schemaVersionV17, "2026-05-09T10:06:36Z"); err != nil {
		t.Fatalf("insert legacy branch migration rows: %v", err)
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
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("scan max schema version: %v", err)
	}
	if version != schemaVersionCurrent {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionCurrent)
	}
	for _, table := range []string{"genre_rules", "event_genres", "image_assets"} {
		ok, err := tableExists(context.Background(), db, table)
		if err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !ok {
			t.Fatalf("table %s does not exist", table)
		}
	}
	for _, check := range []struct {
		table  string
		column string
	}{
		{table: "events", column: "image_url"},
		{table: "events", column: "image_focus_x"},
		{table: "review_candidates", column: "image_url"},
		{table: "review_candidates", column: "image_focus_x"},
		{table: "image_assets", column: "focus_x"},
	} {
		ok, err := columnExists(context.Background(), db, check.table, check.column)
		if err != nil {
			t.Fatalf("check column %s.%s: %v", check.table, check.column, err)
		}
		if !ok {
			t.Fatalf("column %s.%s does not exist", check.table, check.column)
		}
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
	if _, err := db.Exec(`INSERT INTO venues (slug, name, address, neighbourhood, description, website, validation_state, origin) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"persisted-venue", "Persisted Venue", "1 Persisted Street, Sheffield", "Centre", "Persisted venue", "https://example.test/venue", "unknown", string(domain.OriginLive)); err != nil {
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
	if venue.ValidationState != domain.ValidationStateProvisional {
		t.Fatalf("venue validation state = %q, want provisional", venue.ValidationState)
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

func TestOpenBackfillsSecondarySourceCompatibilityObservations(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Source A", "https://example.test/event/a"); err != nil {
		t.Fatalf("insert source A: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Source B", "https://example.test/event/b"); err != nil {
		t.Fatalf("insert source B: %v", err)
	}
	var sourceAID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Source A", "https://example.test/event/a").Scan(&sourceAID); err != nil {
		t.Fatalf("lookup source A id: %v", err)
	}
	var sourceBID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Source B", "https://example.test/event/b").Scan(&sourceBID); err != nil {
		t.Fatalf("lookup source B id: %v", err)
	}
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "compatibility-backfill-event", venueID, sourceAID, domain.OriginLive)

	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "compatibility-backfill-event").Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	if err := insertStoreTestSecondarySourceInfoRows(t, db, eventID, sourceAID, "leadmill", "Legacy Event A", time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC), []secondarySourceInfoRow{
		{InfoType: "description", Value: "Line one"},
	}); err != nil {
		t.Fatalf("insert legacy secondary source info A: %v", err)
	}
	if err := insertStoreTestSecondarySourceInfoRows(t, db, eventID, sourceBID, "leadmill", "Legacy Event B", time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC), []secondarySourceInfoRow{
		{InfoType: "description", Value: "Line two"},
	}); err != nil {
		t.Fatalf("insert legacy secondary source info B: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	db = mustRawDB(t, path)

	legacy, err := st.EventSecondarySourceInfoByEventSlug(ctx, "compatibility-backfill-event")
	if err != nil {
		t.Fatalf("load legacy secondary source info: %v", err)
	}
	if got, want := len(legacy), 2; got != want {
		t.Fatalf("legacy secondary source groups = %d, want %d", got, want)
	}
	observations, err := loadEventSecondarySourceInfoBySlugFromObservations(ctx, db, "compatibility-backfill-event")
	if err != nil {
		t.Fatalf("load observation secondary source info: %v", err)
	}
	if !reflect.DeepEqual(observations, legacy) {
		t.Fatalf("observation secondary source info = %#v, want %#v", observations, legacy)
	}
	if got, want := mustCount(t, db, "event_source_attribute_observations"), 2; got != want {
		t.Fatalf("observation rows = %d, want %d", got, want)
	}
	canonicalA, ok := ingest.SourceIdentityKey("https://example.test/event/a")
	if !ok {
		t.Fatal("canonical identity for source A not derived")
	}
	canonicalB, ok := ingest.SourceIdentityKey("https://example.test/event/b")
	if !ok {
		t.Fatal("canonical identity for source B not derived")
	}
	var storedKeys []string
	rows, err := db.Query(`
		SELECT source_identity_key
		FROM event_source_attribute_observations
		WHERE event_id = ?
		ORDER BY source_id, id
	`, eventID)
	if err != nil {
		t.Fatalf("query observation identity keys: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatalf("scan observation identity key: %v", err)
		}
		storedKeys = append(storedKeys, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate observation identity keys: %v", err)
	}
	if got, want := len(storedKeys), 2; got != want {
		t.Fatalf("stored identity keys = %d, want %d", got, want)
	}
	if storedKeys[0] != canonicalA {
		t.Fatalf("stored identity key[0] = %q, want %q", storedKeys[0], canonicalA)
	}
	if storedKeys[1] != canonicalB {
		t.Fatalf("stored identity key[1] = %q, want %q", storedKeys[1], canonicalB)
	}
	if got, want := mustCount(t, db, "repair_runs"), 1; got != want {
		t.Fatalf("repair runs = %d, want %d", got, want)
	}

	if err := st.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close reopened raw db: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("open store again: %v", err)
	}
	defer st.Close()

	db = mustRawDB(t, path)
	defer db.Close()

	if got, want := mustCount(t, db, "event_source_attribute_observations"), 2; got != want {
		t.Fatalf("observation rows after reopen = %d, want %d", got, want)
	}
	if got, want := mustCount(t, db, "repair_runs"), 1; got != want {
		t.Fatalf("repair runs after reopen = %d, want %d", got, want)
	}
}

func TestOpenSkipsSecondarySourceCompatibilityBackfillWithoutStableIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db := mustRawDB(t, path)
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Leadmill listing feed", "https://leadmill.co.uk/listings/?ical=1"); err != nil {
		t.Fatalf("insert unstable source: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Leadmill event page", "https://leadmill.co.uk/event/feed-detail/"); err != nil {
		t.Fatalf("insert stable source: %v", err)
	}
	var unstableSourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Leadmill listing feed", "https://leadmill.co.uk/listings/?ical=1").Scan(&unstableSourceID); err != nil {
		t.Fatalf("lookup unstable source id: %v", err)
	}
	var stableSourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Leadmill event page", "https://leadmill.co.uk/event/feed-detail/").Scan(&stableSourceID); err != nil {
		t.Fatalf("lookup stable source id: %v", err)
	}
	venueID := lookupStoreVenueID(t, db, "leadmill")
	insertLegacyEvent(t, db, "compatibility-backfill-event", venueID, unstableSourceID, domain.OriginLive)

	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "compatibility-backfill-event").Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	if err := insertStoreTestSecondarySourceInfoRows(t, db, eventID, unstableSourceID, "leadmill", "Legacy Event Unstable", time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC), []secondarySourceInfoRow{
		{InfoType: "description", Value: "Unstable line"},
	}); err != nil {
		t.Fatalf("insert unstable secondary source info: %v", err)
	}
	if err := insertStoreTestSecondarySourceInfoRows(t, db, eventID, stableSourceID, "leadmill", "Legacy Event Stable", time.Date(2026, time.May, 10, 19, 0, 0, 0, time.UTC), []secondarySourceInfoRow{
		{InfoType: "description", Value: "Stable line"},
	}); err != nil {
		t.Fatalf("insert stable secondary source info: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()

	db = mustRawDB(t, path)
	defer db.Close()

	legacy, err := st.EventSecondarySourceInfoByEventSlug(ctx, "compatibility-backfill-event")
	if err != nil {
		t.Fatalf("load legacy secondary source info: %v", err)
	}
	if got, want := len(legacy), 2; got != want {
		t.Fatalf("legacy secondary source groups = %d, want %d", got, want)
	}
	legacyRows := loadSecondarySourceInfoRows(t, db)
	if got, want := len(legacyRows), 2; got != want {
		t.Fatalf("legacy secondary source rows = %d, want %d", got, want)
	}

	observations, err := loadEventSecondarySourceInfoBySlugFromObservations(ctx, db, "compatibility-backfill-event")
	if err != nil {
		t.Fatalf("load observation secondary source info: %v", err)
	}
	if got, want := len(observations), 1; got != want {
		t.Fatalf("observation secondary source info groups = %d, want %d", got, want)
	}
	stableKey := ingest.SourceIdentities(ingest.SourceIdentityInput{SourceURL: "https://leadmill.co.uk/event/feed-detail/"}).PrimaryKey()
	if stableKey == "" {
		t.Fatal("stable identity for leadmill event page not derived")
	}
	var storedStableKey string
	if err := db.QueryRow(`
		SELECT source_identity_key
		FROM event_source_attribute_observations
		WHERE event_id = ? AND source_id = ?
	`, eventID, stableSourceID).Scan(&storedStableKey); err != nil {
		t.Fatalf("lookup stable observation identity key: %v", err)
	}
	if storedStableKey != stableKey {
		t.Fatalf("stable observation identity key = %q, want %q", storedStableKey, stableKey)
	}
	var unstableCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM event_source_attribute_observations
		WHERE event_id = ? AND source_id = ?
	`, eventID, unstableSourceID).Scan(&unstableCount); err != nil {
		t.Fatalf("count unstable observations: %v", err)
	}
	if unstableCount != 0 {
		t.Fatalf("unstable observation rows = %d, want 0", unstableCount)
	}
	if got, want := mustCount(t, db, "event_source_attribute_observations"), 1; got != want {
		t.Fatalf("observation rows = %d, want %d", got, want)
	}
	if got, want := mustCount(t, db, "repair_runs"), 1; got != want {
		t.Fatalf("repair runs = %d, want %d", got, want)
	}
}

func TestLoadEventBySlugDecoratesCalendarSourceWithListingsFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var venueID int64
	if err := st.db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, "leadmill").Scan(&venueID); err != nil {
		t.Fatalf("lookup leadmill venue id: %v", err)
	}
	result, err := st.db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "The Leadmill manual ingest", "https://leadmill.co.uk/listings/?ical=1")
	if err != nil {
		t.Fatalf("insert calendar source: %v", err)
	}
	sourceID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("calendar source id: %v", err)
	}
	start := time.Date(2026, time.May, 20, 18, 30, 0, 0, time.UTC)
	end := time.Date(2026, time.May, 20, 21, 0, 0, 0, time.UTC)
	checked := time.Date(2026, time.May, 20, 9, 0, 0, 0, time.UTC)
	if _, err := st.db.Exec(`
		INSERT INTO events (
			slug, venue_id, source_id, name, start_at, end_at, genre, status, description, last_checked_at, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "leadmill-calendar-source", venueID, sourceID, "Leadmill Calendar Source", formatRFC3339UTC(start), formatRFC3339UTC(end), "Indie", "Listed", "Calendar event", formatRFC3339UTC(checked), string(domain.OriginLive)); err != nil {
		t.Fatalf("insert calendar event: %v", err)
	}

	event, ok, err := st.LoadEventBySlug(context.Background(), "leadmill-calendar-source")
	if err != nil {
		t.Fatalf("load calendar event: %v", err)
	}
	if !ok {
		t.Fatal("calendar event not found")
	}
	if got, want := event.OfficialListingURL, "https://leadmill.co.uk/live/#:~:text=Leadmill%20Calendar%20Source"; got != want {
		t.Fatalf("official listing url = %q, want %q", got, want)
	}
	if got, want := event.CalendarURL, "https://leadmill.co.uk/listings/?ical=1"; got != want {
		t.Fatalf("calendar url = %q, want %q", got, want)
	}
}

func TestOpenMigratesVersion11DatabaseAddsVenueValidationState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV11)
	if _, err := db.Exec(`
		INSERT INTO venues (
			slug, name, address, neighbourhood, description, website, coverage_kind, coverage_note, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "pre-v12-venue", "Pre-v12 Venue", "1 Old Street, Sheffield", "Centre", "Legacy venue", "https://example.test/pre-v12", "venue", "", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert legacy venue: %v", err)
	}
	insertMigrationRowsThrough(t, db, schemaVersionV11, time.Date(2026, time.April, 22, 8, 0, 0, 0, time.UTC))
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	venue, ok, err := st.LoadVenueBySlug(context.Background(), "pre-v12-venue")
	if err != nil {
		t.Fatalf("load venue: %v", err)
	}
	if !ok {
		t.Fatal("legacy venue not found")
	}
	if venue.ValidationState != domain.ValidationStateValidated {
		t.Fatalf("venue validation state = %q, want validated", venue.ValidationState)
	}

	db = mustRawDB(t, path)
	defer db.Close()
	var validationState string
	if err := db.QueryRow(`SELECT validation_state FROM venues WHERE slug = ?`, "pre-v12-venue").Scan(&validationState); err != nil {
		t.Fatalf("scan validation state: %v", err)
	}
	if validationState != string(domain.ValidationStateValidated) {
		t.Fatalf("validation_state = %q, want %q", validationState, domain.ValidationStateValidated)
	}
}

func TestOpenMigratesVersion12DatabaseAddsReviewCandidateVenueEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV12)
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
	`, "Venue evidence migration", "Fixture ICS", "file:migration.ics", review.StatusOpen, "Preserved notes", formatRFC3339UTC(time.Date(2026, time.April, 23, 9, 0, 0, 0, time.UTC)), formatRFC3339UTC(time.Date(2026, time.April, 23, 10, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("insert review group: %v", err)
	}
	groupID, err := groupRes.LastInsertId()
	if err != nil {
		t.Fatalf("review group id: %v", err)
	}
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
	`, groupID, 1, "candidate-a", "Candidate A", "leadmill", "2026-05-01T19:00:00Z", "2026-05-01T22:00:00Z", "Indie", "Listed", "Description", "Fixture ICS", "file:candidate-a.ics", "fixture UID candidate-a"); err != nil {
		t.Fatalf("insert review candidate: %v", err)
	}
	insertMigrationRowsThrough(t, db, schemaVersionV12, time.Date(2026, time.April, 23, 11, 0, 0, 0, time.UTC))
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
	var title, sourceName, sourceURL, status, notes string
	if err := db.QueryRow(`
		SELECT title, source_name, source_url, status, notes
		FROM review_groups
		WHERE id = ?
	`, groupID).Scan(&title, &sourceName, &sourceURL, &status, &notes); err != nil {
		t.Fatalf("scan review group: %v", err)
	}
	if title != "Venue evidence migration" || sourceName != "Fixture ICS" || sourceURL != "file:migration.ics" {
		t.Fatalf("review group = %q/%q/%q, want migrated values", title, sourceName, sourceURL)
	}
	if status != string(review.StatusOpen) || notes != "Preserved notes" {
		t.Fatalf("review group status/notes = %q/%q, want open/preserved", status, notes)
	}
	var venueText, venueLocationRaw string
	if err := db.QueryRow(`SELECT venue_text, venue_location_raw FROM review_candidates WHERE group_id = ?`, groupID).Scan(&venueText, &venueLocationRaw); err != nil {
		t.Fatalf("scan review candidate venue evidence: %v", err)
	}
	if venueText != "" {
		t.Fatalf("stored venue text = %q, want empty", venueText)
	}
	if venueLocationRaw != "" {
		t.Fatalf("stored venue location raw = %q, want empty", venueLocationRaw)
	}
}

func TestOpenMigratesVersion13DatabaseMarksBootstrapRecordsLive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV13)

	if _, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Legacy source", "https://example.test/legacy"); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	var sourceID int64
	if err := db.QueryRow(`SELECT id FROM sources WHERE name = ? AND url = ?`, "Legacy source", "https://example.test/legacy").Scan(&sourceID); err != nil {
		t.Fatalf("lookup source id: %v", err)
	}

	bootstrapVenueID := insertLegacyVenue(t, db, "leadmill", "The Leadmill", domain.OriginSeed)
	unrelatedVenueID := insertLegacyVenue(t, db, "community-room", "Community Room", domain.OriginSeed)
	insertLegacyEvent(t, db, "matinee-noise-at-the-leadmill", bootstrapVenueID, sourceID, domain.OriginSeed)
	insertLegacyEvent(t, db, "community-room-show", unrelatedVenueID, sourceID, domain.OriginSeed)
	insertMigrationRowsThrough(t, db, schemaVersionV13, time.Date(2026, time.April, 24, 8, 0, 0, 0, time.UTC))
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
	assertStoredVenueOrigin(t, db, "leadmill", domain.OriginLive)
	assertStoredVenueOrigin(t, db, "community-room", domain.OriginSeed)
	assertStoredEventMissing(t, db, "matinee-noise-at-the-leadmill")
	assertStoredEventOrigin(t, db, "community-room-show", domain.OriginSeed)
}

func TestValidateVenueMarksProvisionalVenueValidated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		INSERT INTO venues (
			slug, name, address, neighbourhood, description, website, validation_state, coverage_kind, coverage_note, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "provisional-room", "Provisional Room", "1 Test Street, Sheffield", "Centre", "Fixture provisional venue", "https://example.test/provisional-room", string(domain.ValidationStateProvisional), string(domain.CoverageKindVenue), "", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert provisional venue: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	if err := st.ValidateVenue(context.Background(), "provisional-room"); err != nil {
		t.Fatalf("validate venue: %v", err)
	}

	venue, ok, err := st.LoadVenueBySlug(context.Background(), "provisional-room")
	if err != nil {
		t.Fatalf("load venue: %v", err)
	}
	if !ok {
		t.Fatal("validated venue not found")
	}
	if venue.ValidationState != domain.ValidationStateValidated {
		t.Fatalf("venue validation state = %q, want %q", venue.ValidationState, domain.ValidationStateValidated)
	}
}

func TestValidateVenueRejectsMissingAndNonProvisionalVenues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	if err := st.ValidateVenue(context.Background(), "missing-room"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("validate missing venue error = %v, want not found", err)
	}
	if err := st.ValidateVenue(context.Background(), "leadmill"); err == nil || !strings.Contains(err.Error(), "not provisional") {
		t.Fatalf("validate validated venue error = %v, want not provisional", err)
	}
}

func TestUpdateProvisionalVenuePersistsEditedFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	if _, err := db.Exec(`
		INSERT INTO venues (
			slug, name, address, neighbourhood, description, website, validation_state, coverage_kind, coverage_note, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "provisional-room", "Provisional Room", "1 Test Street, Sheffield", "Centre", "Fixture provisional venue", "https://example.test/provisional-room", string(domain.ValidationStateProvisional), string(domain.CoverageKindVenue), "", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert provisional venue: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	err = st.UpdateProvisionalVenue(context.Background(), seedstore.VenueUpdateInput{
		Slug:          "provisional-room",
		Name:          "Updated Room",
		Address:       "99 Updated Street, Sheffield",
		Neighbourhood: "Kelham",
		Description:   "Updated fixture description.",
		Website:       "https://example.test/updated-room",
		CoverageKind:  domain.CoverageKindProgram,
		CoverageNote:  "Programme-only for now.",
	})
	if err != nil {
		t.Fatalf("update provisional venue: %v", err)
	}

	venue, ok, err := st.LoadVenueBySlug(context.Background(), "provisional-room")
	if err != nil {
		t.Fatalf("load venue: %v", err)
	}
	if !ok {
		t.Fatal("updated venue not found")
	}
	if venue.Name != "Updated Room" {
		t.Fatalf("venue name = %q, want %q", venue.Name, "Updated Room")
	}
	if venue.Address != "99 Updated Street, Sheffield" {
		t.Fatalf("venue address = %q, want %q", venue.Address, "99 Updated Street, Sheffield")
	}
	if venue.Neighbourhood != "Kelham" {
		t.Fatalf("venue neighbourhood = %q, want %q", venue.Neighbourhood, "Kelham")
	}
	if venue.Description != "Updated fixture description." {
		t.Fatalf("venue description = %q, want %q", venue.Description, "Updated fixture description.")
	}
	if venue.Website != "https://example.test/updated-room" {
		t.Fatalf("venue website = %q, want %q", venue.Website, "https://example.test/updated-room")
	}
	if venue.CoverageKind != domain.CoverageKindProgram {
		t.Fatalf("venue coverage kind = %q, want %q", venue.CoverageKind, domain.CoverageKindProgram)
	}
	if venue.CoverageNote != "Programme-only for now." {
		t.Fatalf("venue coverage note = %q, want %q", venue.CoverageNote, "Programme-only for now.")
	}
	if venue.ValidationState != domain.ValidationStateProvisional {
		t.Fatalf("venue validation state = %q, want %q", venue.ValidationState, domain.ValidationStateProvisional)
	}
}

func TestUpdateProvisionalVenueRejectsMissingNonProvisionalAndInvalidCoverageKind(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	if err := st.UpdateProvisionalVenue(context.Background(), seedstore.VenueUpdateInput{
		Slug:         "missing-room",
		CoverageKind: domain.CoverageKindVenue,
	}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("update missing venue error = %v, want not found", err)
	}
	if err := st.UpdateProvisionalVenue(context.Background(), seedstore.VenueUpdateInput{
		Slug:         "leadmill",
		CoverageKind: domain.CoverageKindVenue,
	}); err == nil || !strings.Contains(err.Error(), "not provisional") {
		t.Fatalf("update validated venue error = %v, want not provisional", err)
	}
	if err := st.UpdateProvisionalVenue(context.Background(), seedstore.VenueUpdateInput{
		Slug:         "leadmill",
		CoverageKind: domain.CoverageKind("sideways"),
	}); err == nil || !strings.Contains(err.Error(), "invalid coverage kind") {
		t.Fatalf("update invalid coverage kind error = %v, want invalid coverage kind", err)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionCurrent {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionCurrent)
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("scan max schema version: %v", err)
	}
	if version != schemaVersionCurrent {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionCurrent)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionCurrent {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionCurrent)
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("scan max schema version: %v", err)
	}
	if version != schemaVersionCurrent {
		t.Fatalf("schema version = %d, want %d", version, schemaVersionCurrent)
	}
	if got := mustCount(t, db, "event_source_links"); got != 0 {
		t.Fatalf("event_source_links rows = %d, want 0", got)
	}

	var status, notes string
	var stagingKey sql.NullString
	if err := db.QueryRow(`SELECT status, notes, staging_key FROM review_groups WHERE id = ?`, groupID).Scan(&status, &notes, &stagingKey); err != nil {
		t.Fatalf("scan review group: %v", err)
	}
	if status != string(review.StatusOpen) {
		t.Fatalf("status = %q, want %q", status, review.StatusOpen)
	}
	if notes != "Preserved notes" {
		t.Fatalf("notes = %q, want %q", notes, "Preserved notes")
	}
	if stagingKey.Valid {
		t.Fatalf("staging key valid = true, want false")
	}

	var candidateCount, draftChoiceCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM review_candidates WHERE group_id = ?`, groupID).Scan(&candidateCount); err != nil {
		t.Fatalf("scan review candidate count: %v", err)
	}
	if candidateCount != 1 {
		t.Fatalf("candidate count = %d, want 1", candidateCount)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM review_draft_choices WHERE group_id = ?`, groupID).Scan(&draftChoiceCount); err != nil {
		t.Fatalf("scan review draft choice count: %v", err)
	}
	if draftChoiceCount != 1 {
		t.Fatalf("draft choice count = %d, want 1", draftChoiceCount)
	}
	var draftFieldCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM review_draft_choices WHERE group_id = ? AND field = ?`, groupID, string(review.FieldName)).Scan(&draftFieldCount); err != nil {
		t.Fatalf("scan review draft choice field count: %v", err)
	}
	if draftFieldCount != 1 {
		t.Fatal("missing draft choice after migration")
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionCurrent {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionCurrent)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionCurrent {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionCurrent)
	}

	var openName, openURL, openEventKey sql.NullString
	if err := db.QueryRow(`
		SELECT authoritative_source_name, authoritative_source_url, authoritative_source_event_key
		FROM review_groups
		WHERE id = ?
	`, openGroupID).Scan(&openName, &openURL, &openEventKey); err != nil {
		t.Fatalf("scan open review group: %v", err)
	}
	if openName.Valid || openURL.Valid || openEventKey.Valid {
		t.Fatalf("open authoritative fields = %#v %#v %#v, want empty", openName, openURL, openEventKey)
	}

	var closedName, closedURL, closedEventKey sql.NullString
	if err := db.QueryRow(`
		SELECT authoritative_source_name, authoritative_source_url, authoritative_source_event_key
		FROM review_groups
		WHERE id = ?
	`, closedGroupID).Scan(&closedName, &closedURL, &closedEventKey); err != nil {
		t.Fatalf("scan closed review group: %v", err)
	}
	if closedName.Valid || closedURL.Valid || closedEventKey.Valid {
		t.Fatalf("closed authoritative fields = %#v %#v %#v, want empty", closedName, closedURL, closedEventKey)
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
	if got := mustCount(t, db, "schema_migrations"); got != schemaVersionCurrent {
		t.Fatalf("schema_migrations rows = %d, want %d", got, schemaVersionCurrent)
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
	insertStoreTestEvent(t, db, "round-trip-time-event", "leadmill")
	start := time.Date(2026, time.May, 8, 19, 30, 0, 0, time.FixedZone("BST", 60*60))
	end := time.Date(2026, time.May, 8, 23, 0, 0, 0, time.FixedZone("BST", 60*60))
	checked := time.Date(2026, time.April, 19, 10, 0, 0, 0, time.FixedZone("BST", 60*60))
	if _, err := db.Exec(`
		UPDATE events
		SET start_at = ?, end_at = ?, last_checked_at = ?
		WHERE slug = ?
	`, start.Format(time.RFC3339), end.Format(time.RFC3339), checked.Format(time.RFC3339), "round-trip-time-event"); err != nil {
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

	event, ok := st.EventBySlug("round-trip-time-event")
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

func TestRecomputeEventGenresUsesCanonicalAndSecondaryDescriptions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	db := mustRawDB(t, path)
	defer db.Close()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, "leadmill")
	if _, err := db.Exec(`
		INSERT INTO events (
			slug, venue_id, source_id, name, start_at, end_at, genre, status, description, last_checked_at, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "genre-backfill-event", venueID, sourceID, "Genre Backfill Event", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "", "Listed", "A rock opener.", "2026-05-09T10:00:00Z", string(domain.OriginLive)); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	var eventID int64
	if err := db.QueryRow(`SELECT id FROM events WHERE slug = ?`, "genre-backfill-event").Scan(&eventID); err != nil {
		t.Fatalf("lookup event id: %v", err)
	}
	res, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, "Secondary genre source", "https://example.test/secondary-genre")
	if err != nil {
		t.Fatalf("insert secondary source: %v", err)
	}
	secondaryID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("secondary source id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_secondary_source_info (
			event_id, source_id, venue_slug, event_name, start_at, info_type, value, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, eventID, secondaryID, "leadmill", "Genre Backfill Event", "2026-05-10T19:00:00Z", "description", "Late jazz, more jazz.", "2026-05-09T11:00:00Z", "2026-05-09T11:00:00Z"); err != nil {
		t.Fatalf("insert secondary description: %v", err)
	}

	if err := st.RecomputeEventGenres(ctx); err != nil {
		t.Fatalf("recompute event genres: %v", err)
	}
	event, ok, err := st.LoadEventBySlug(ctx, "genre-backfill-event")
	if err != nil {
		t.Fatalf("load event: %v", err)
	}
	if !ok {
		t.Fatal("event not found")
	}
	if got, want := event.Genre, "Rock, Jazz"; got != want {
		t.Fatalf("event genre = %q, want %q", got, want)
	}
	matches, err := st.EventGenresByEventSlug(ctx, "genre-backfill-event")
	if err != nil {
		t.Fatalf("load event genres: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("event genres = %d, want 2", len(matches))
	}
	if matches[0].Name != "Rock" || matches[0].MentionCount != 1 {
		t.Fatalf("first genre = %#v, want Rock with 1 mention", matches[0])
	}
	if matches[1].Name != "Jazz" || matches[1].MentionCount != 2 {
		t.Fatalf("second genre = %#v, want Jazz with 2 mentions", matches[1])
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
	insertStoreTestEvent(t, db, "null-end-event", "leadmill")
	if _, err := db.Exec(`
		UPDATE events
		SET end_at = NULL
		WHERE slug = ?
	`, "null-end-event"); err != nil {
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

	event, ok := st.EventBySlug("null-end-event")
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
	insertStoreTestEvent(t, db, "equal-end-event", "leadmill")
	if _, err := db.Exec(`
		UPDATE events
		SET end_at = start_at
		WHERE slug = ?
	`, "equal-end-event"); err != nil {
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

func TestOpenBackfillsCanonicalEqualTimeEndForUniversityMultiVenueAuthoritativeSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	db := mustRawDB(t, path)

	const (
		sourceName = "University of Sheffield Performance Venues manual ingest"
		sourceURL  = "https://performancevenues.group.shef.ac.uk/event/man-in-the-mirror/"
		startAt    = "2026-08-07T19:30:00Z"
	)
	slug := mustLiveEventSlug(t, "Man in the Mirror", "octagon-centre", startAt)
	insertLegacyVenue(t, db, "octagon-centre", "Octagon Centre", domain.OriginLive)
	sourceID := mustEnsureSourceID(t, st, sourceName, sourceURL)
	mustInsertRepairLegacyEvent(t, db, sourceID, slug, "octagon-centre", "Man in the Mirror", startAt, "University venue test event")
	if _, err := db.Exec(`
		UPDATE events
		SET end_at = start_at
		WHERE slug = ?
	`, slug); err != nil {
		t.Fatalf("set equal-time end: %v", err)
	}
	mustInsertAuthoritativeSourceLink(t, db, slug, sourceName, sourceURL, "pv-1")
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	}()

	db = mustRawDB(t, path)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close reopened raw db: %v", err)
		}
	}()

	var endAt sql.NullString
	if err := db.QueryRow(`SELECT end_at FROM events WHERE slug = ?`, slug).Scan(&endAt); err != nil {
		t.Fatalf("load end_at: %v", err)
	}
	if endAt.Valid {
		t.Fatalf("end_at = %q, want NULL", endAt.String)
	}
}

func TestOpenRejectsAbandonedHistoricalDuplicateBranchSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV5)
	insertMigrationRowsThrough(t, db, schemaVersionV5, time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC))
	if _, err := db.Exec(`ALTER TABLE review_groups ADD COLUMN kind TEXT NOT NULL DEFAULT 'standard'`); err != nil {
		t.Fatalf("add review_groups.kind: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE review_historical_duplicate_actions (
			group_id INTEGER NOT NULL,
			existing_event_id INTEGER NOT NULL,
			is_canonical INTEGER NOT NULL DEFAULT 0,
			action TEXT,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(group_id, existing_event_id)
		)
	`); err != nil {
		t.Fatalf("create historical duplicate actions table: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO schema_migrations (version, applied_at)
		VALUES (?, ?)
	`, schemaVersionV26, "2026-04-20T10:30:00Z"); err != nil {
		t.Fatalf("insert v26 migration row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "reset or recreate the local DB") {
		t.Fatalf("open error = %v, want abandoned-branch reset guidance", err)
	}
}

func TestOpenRejectsAbandonedEventReviewBranchWithoutStagingKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV26)
	insertMigrationRowsThrough(t, db, schemaVersionV26, time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC))
	if _, err := db.Exec(`
		INSERT INTO schema_migrations (version, applied_at)
		VALUES (?, ?)
	`, schemaVersionV27, "2026-05-15T09:30:00Z"); err != nil {
		t.Fatalf("insert v27 migration row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "reset or recreate the local DB") {
		t.Fatalf("open error = %v, want abandoned-branch reset guidance", err)
	}
}

func TestOpenRejectsNewerSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sheffield-live.db")

	db := mustRawDB(t, path)
	applyMigrationsThrough(t, db, schemaVersionV27)
	insertMigrationRowsThrough(t, db, schemaVersionV27, time.Date(2026, time.May, 15, 9, 0, 0, 0, time.UTC))
	if _, err := db.Exec(`
		INSERT INTO schema_migrations (version, applied_at)
		VALUES (?, ?)
	`, schemaVersionCurrent+1, "2026-05-15T09:45:00Z"); err != nil {
		t.Fatalf("insert newer migration row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "reset or recreate the local DB") {
		t.Fatalf("open error = %v, want newer-schema reset guidance", err)
	}
}

func TestOpenDoesNotBackfillImportRunReviewGroupLinksFromLegacyNotes(t *testing.T) {
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

	db = mustRawDB(t, path)
	defer db.Close()
	if got := mustCount(t, db, "import_run_review_groups"); got != 0 {
		t.Fatalf("import_run_review_groups rows = %d, want 0", got)
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
	insertStoreTestEvent(t, db, "dangling-venue-event", "leadmill")
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := db.Exec(`UPDATE events SET venue_id = ? WHERE slug = ?`, 999999, "dangling-venue-event"); err != nil {
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

func insertLegacyVenue(t *testing.T, db *sql.DB, slug, name string, origin domain.Origin) int64 {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO venues (
			slug, name, address, neighbourhood, description, website, validation_state, coverage_kind, coverage_note, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, name, "1 Legacy Street, Sheffield", "Centre", "Legacy venue", "https://example.test/"+slug, string(domain.ValidationStateValidated), string(domain.CoverageKindVenue), "", string(origin))
	if err != nil {
		t.Fatalf("insert legacy venue %q: %v", slug, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("legacy venue %q id: %v", slug, err)
	}
	return id
}

func insertLegacyEvent(t *testing.T, db *sql.DB, slug string, venueID, sourceID int64, origin domain.Origin) {
	t.Helper()

	if _, err := db.Exec(`
		INSERT INTO events (
			slug, venue_id, source_id, name, start_at, end_at, genre, status, description, last_checked_at, origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, slug, venueID, sourceID, "Legacy Event", "2026-05-10T19:00:00Z", "2026-05-10T22:00:00Z", "Indie", "Listed", "Legacy event", "2026-05-09T10:00:00Z", string(origin)); err != nil {
		t.Fatalf("insert legacy event %q: %v", slug, err)
	}
}

func insertStoreTestEvent(t *testing.T, db *sql.DB, slug, venueSlug string) {
	t.Helper()

	sourceID := insertStoreTestSource(t, db)
	venueID := lookupStoreVenueID(t, db, venueSlug)
	insertLegacyEvent(t, db, slug, venueID, sourceID, domain.OriginLive)
}

func insertStoreTestSecondarySourceInfoRows(t *testing.T, db *sql.DB, eventID, sourceID int64, venueSlug, eventName string, startAt time.Time, rows []secondarySourceInfoRow) error {
	t.Helper()

	for _, row := range rows {
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
		`, eventID, sourceID, venueSlug, eventName, formatRFC3339UTC(startAt), row.InfoType, row.Value, "2026-05-10T19:00:00Z", "2026-05-10T19:05:00Z"); err != nil {
			return err
		}
	}
	return nil
}

func insertStoreTestSource(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	return insertStoreNamedSource(t, db, "Store test source", "https://example.test/store-test")
}

func insertStoreNamedSource(t *testing.T, db *sql.DB, name, url string) int64 {
	t.Helper()

	res, err := db.Exec(`INSERT INTO sources (name, url) VALUES (?, ?)`, name, url)
	if err != nil {
		t.Fatalf("insert store test source: %v", err)
	}
	sourceID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("store test source id: %v", err)
	}
	return sourceID
}

func lookupStoreVenueID(t *testing.T, db *sql.DB, slug string) int64 {
	t.Helper()

	var venueID int64
	if err := db.QueryRow(`SELECT id FROM venues WHERE slug = ?`, slug).Scan(&venueID); err != nil {
		t.Fatalf("lookup venue id %q: %v", slug, err)
	}
	return venueID
}

func assertStoredVenueOrigin(t *testing.T, db *sql.DB, slug string, want domain.Origin) {
	t.Helper()

	var origin string
	if err := db.QueryRow(`SELECT origin FROM venues WHERE slug = ?`, slug).Scan(&origin); err != nil {
		t.Fatalf("scan venue origin %q: %v", slug, err)
	}
	if domain.Origin(origin) != want {
		t.Fatalf("venue %q origin = %q, want %q", slug, origin, want)
	}
}

func assertStoredEventOrigin(t *testing.T, db *sql.DB, slug string, want domain.Origin) {
	t.Helper()

	var origin string
	if err := db.QueryRow(`SELECT origin FROM events WHERE slug = ?`, slug).Scan(&origin); err != nil {
		t.Fatalf("scan event origin %q: %v", slug, err)
	}
	if domain.Origin(origin) != want {
		t.Fatalf("event %q origin = %q, want %q", slug, origin, want)
	}
}

func assertStoredEventMissing(t *testing.T, db *sql.DB, slug string) {
	t.Helper()

	var found int
	if err := db.QueryRow(`SELECT 1 FROM events WHERE slug = ?`, slug).Scan(&found); err == nil {
		t.Fatalf("event %q still exists", slug)
	} else if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("lookup event %q: %v", slug, err)
	}
}

func applyMigrationsThrough(t *testing.T, db *sql.DB, maxVersion int) {
	t.Helper()

	for _, migration := range migrations {
		if migration.version > maxVersion {
			continue
		}
		applyMigrationFile(t, db, migration.path)
	}
}

func applyMigrationFile(t *testing.T, db *sql.DB, path string) {
	t.Helper()

	migrationSQL, err := readMigration(path)
	if err != nil {
		t.Fatalf("read migration %s: %v", path, err)
	}
	if _, err := db.Exec(migrationSQL); err != nil {
		t.Fatalf("apply migration %s: %v", path, err)
	}
}

func insertMigrationRowsThrough(t *testing.T, db *sql.DB, maxVersion int, base time.Time) {
	t.Helper()

	for _, migration := range migrations {
		if migration.version > maxVersion {
			continue
		}
		if _, err := db.Exec(`
			INSERT INTO schema_migrations (version, applied_at)
			VALUES (?, ?)
		`, migration.version, formatRFC3339UTC(base.Add(time.Duration(migration.version)*time.Hour))); err != nil {
			t.Fatalf("insert v%d migration row: %v", migration.version, err)
		}
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
