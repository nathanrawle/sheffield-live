package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	seedstore "sheffield-live/internal/store"
)

const (
	defaultPath       = "./data/sheffield-live.db"
	schemaVersionV1   = 1
	schemaVersionV2   = 2
	schemaVersionV3   = 3
	rfc3339Timestamp  = time.RFC3339
	foreignKeysPragma = "PRAGMA foreign_keys = ON"
)

var migrations = []struct {
	version int
	path    string
}{
	{version: schemaVersionV1, path: "migrations/0001_init.sql"},
	{version: schemaVersionV2, path: "migrations/0002_review.sql"},
	{version: schemaVersionV3, path: "migrations/0003_review_staging_idempotency.sql"},
}

//go:embed migrations/*.sql
var migrationFS embed.FS

type Store struct {
	db *sql.DB
}

var _ seedstore.ReadOnlyStore = (*Store)(nil)
var _ ingest.ImportRunStore = (*Store)(nil)

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func Open(path string) (st *Store, err error) {
	if path == "" {
		path = defaultPath
	}

	if info, statErr := os.Stat(path); statErr == nil {
		if info.IsDir() {
			return nil, fmt.Errorf("open sqlite store %q: path is a directory", path)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("open sqlite store %q: stat path: %w", path, statErr)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: create parent directories: %w", path, err)
	}

	db, err := sql.Open("sqlite", dsnForPath(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite store %q: open database: %w", path, err)
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: ping database: %w", path, err)
	}
	if _, err := db.ExecContext(ctx, foreignKeysPragma); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: enable foreign keys: %w", path, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store %q: begin transaction: %w", path, err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := migrate(ctx, tx); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: migrate: %w", path, err)
	}
	if err := bootstrapIfEmpty(ctx, tx); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: bootstrap seed data: %w", path, err)
	}
	if err := validate(ctx, tx); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: validate store: %w", path, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: commit transaction: %w", path, err)
	}

	st = &Store{db: db}
	return st, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Venues() []domain.Venue {
	venues, err := loadVenues(context.Background(), s.db)
	if err != nil {
		return nil
	}
	return venues
}

func (s *Store) Events() []domain.Event {
	events, err := loadEvents(context.Background(), s.db, `
		SELECT
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			s.name,
			s.url,
			e.last_checked_at,
			e.origin
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		ORDER BY e.start_at, e.slug
	`)
	if err != nil {
		return nil
	}
	return events
}

func (s *Store) VenueBySlug(slug string) (domain.Venue, bool) {
	venue, ok, err := loadVenueBySlug(context.Background(), s.db, slug)
	if err != nil || !ok {
		return domain.Venue{}, false
	}
	return venue, true
}

func (s *Store) EventBySlug(slug string) (domain.Event, bool) {
	event, ok, err := loadEventBySlug(context.Background(), s.db, slug)
	if err != nil || !ok {
		return domain.Event{}, false
	}
	return event, true
}

func (s *Store) EventsForVenue(venueSlug string) []domain.Event {
	events, err := loadEvents(context.Background(), s.db, `
		SELECT
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			s.name,
			s.url,
			e.last_checked_at,
			e.origin
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE v.slug = ?
		ORDER BY e.start_at, e.slug
	`, venueSlug)
	if err != nil {
		return nil
	}
	return events
}

func (s *Store) Validate() error {
	return validate(context.Background(), s.db)
}

func migrate(ctx context.Context, tx *sql.Tx) error {
	version, err := schemaVersion(ctx, tx)
	if err != nil {
		return err
	}
	if version > schemaVersionV3 {
		return fmt.Errorf("database schema version %d is newer than supported version %d", version, schemaVersionV3)
	}

	for _, migration := range migrations {
		if migration.version <= version {
			continue
		}
		migrationSQL, err := readMigration(migration.path)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, migrationSQL); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_migrations (version, applied_at)
			VALUES (?, ?)
		`, migration.version, time.Now().UTC().Format(rfc3339Timestamp)); err != nil {
			return err
		}
	}
	return nil
}

func bootstrapIfEmpty(ctx context.Context, tx *sql.Tx) error {
	venueCount, err := countRows(ctx, tx, "venues")
	if err != nil {
		return err
	}
	eventCount, err := countRows(ctx, tx, "events")
	if err != nil {
		return err
	}
	if venueCount != 0 || eventCount != 0 {
		return nil
	}
	return bootstrapSeedData(ctx, tx)
}

func bootstrapSeedData(ctx context.Context, tx *sql.Tx) error {
	seed := seedstore.NewSeedStore()
	venueIDs := make(map[string]int64, len(seed.Venues()))
	for _, venue := range seed.Venues() {
		id, err := insertVenue(ctx, tx, venue)
		if err != nil {
			return err
		}
		venueIDs[venue.Slug] = id
	}

	type sourceKey struct {
		name string
		url  string
	}
	sourceIDs := make(map[sourceKey]int64)
	for _, event := range seed.Events() {
		key := sourceKey{name: event.SourceName, url: event.SourceURL}
		if _, ok := sourceIDs[key]; ok {
			continue
		}
		id, err := insertSource(ctx, tx, event.SourceName, event.SourceURL)
		if err != nil {
			return err
		}
		sourceIDs[key] = id
	}

	for _, event := range seed.Events() {
		venueID, ok := venueIDs[event.VenueSlug]
		if !ok {
			return fmt.Errorf("bootstrap seed data: missing venue %q for event %q", event.VenueSlug, event.Slug)
		}
		key := sourceKey{name: event.SourceName, url: event.SourceURL}
		sourceID, ok := sourceIDs[key]
		if !ok {
			return fmt.Errorf("bootstrap seed data: missing source %q for event %q", event.SourceName, event.Slug)
		}
		if err := insertEvent(ctx, tx, event, venueID, sourceID); err != nil {
			return err
		}
	}
	return nil
}

func insertVenue(ctx context.Context, tx execer, venue domain.Venue) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO venues (slug, name, address, neighbourhood, description, website, origin)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, venue.Slug, venue.Name, venue.Address, venue.Neighbourhood, venue.Description, venue.Website, string(venue.Origin))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertSource(ctx context.Context, tx execer, name, url string) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO sources (name, url)
		VALUES (?, ?)
	`, name, url)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEvent(ctx context.Context, tx execer, event domain.Event, venueID, sourceID int64) error {
	_, err := tx.ExecContext(ctx, `
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
			origin
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event.Slug, venueID, sourceID, event.Name,
		formatRFC3339UTC(event.Start),
		formatRFC3339UTC(event.End),
		event.Genre, event.Status, event.Description,
		formatRFC3339UTC(event.LastChecked),
		string(event.Origin))
	return err
}

func validate(ctx context.Context, q queryer) error {
	if _, err := loadVenues(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingVenueRefs(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingSourceRefs(ctx, q); err != nil {
		return err
	}
	if _, err := loadEvents(ctx, q, `
		SELECT
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			s.name,
			s.url,
			e.last_checked_at,
			e.origin
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		ORDER BY e.start_at, e.slug
	`); err != nil {
		return err
	}
	return nil
}

func validateDanglingVenueRefs(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT e.slug, v.slug
		FROM events e
		LEFT JOIN venues v ON v.id = e.venue_id
		WHERE v.id IS NULL
		ORDER BY e.id
		LIMIT 1
	`)
	var eventSlug string
	var venueSlug string
	switch err := row.Scan(&eventSlug, &venueSlug); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if venueSlug == "" {
		venueSlug = "<missing>"
	}
	return fmt.Errorf("event %q references missing venue %q", eventSlug, venueSlug)
}

func validateDanglingSourceRefs(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT e.slug
		FROM events e
		LEFT JOIN sources s ON s.id = e.source_id
		WHERE s.id IS NULL
		ORDER BY e.id
		LIMIT 1
	`)
	var eventSlug string
	switch err := row.Scan(&eventSlug); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event %q references missing source", eventSlug)
}

func countRows(ctx context.Context, q queryer, table string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	row := q.QueryRowContext(ctx, query)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func schemaVersion(ctx context.Context, q queryer) (int, error) {
	exists, err := tableExists(ctx, q, "schema_migrations")
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	row := q.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
	var version int
	if err := row.Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func tableExists(ctx context.Context, q queryer, table string) (bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT 1
		FROM sqlite_master
		WHERE type = 'table' AND name = ?
	`, table)
	var exists int
	switch err := row.Scan(&exists); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func loadVenues(ctx context.Context, q queryer) ([]domain.Venue, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT slug, name, address, neighbourhood, description, website, origin
		FROM venues
		ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var venues []domain.Venue
	for rows.Next() {
		var venue domain.Venue
		var origin string
		if err := rows.Scan(&venue.Slug, &venue.Name, &venue.Address, &venue.Neighbourhood, &venue.Description, &venue.Website, &origin); err != nil {
			return nil, err
		}
		venue.Origin = domain.Origin(origin)
		venues = append(venues, venue)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return venues, nil
}

func loadVenueBySlug(ctx context.Context, q queryer, slug string) (domain.Venue, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT slug, name, address, neighbourhood, description, website, origin
		FROM venues
		WHERE slug = ?
		LIMIT 1
	`, slug)
	var venue domain.Venue
	var origin string
	switch err := row.Scan(&venue.Slug, &venue.Name, &venue.Address, &venue.Neighbourhood, &venue.Description, &venue.Website, &origin); {
	case errors.Is(err, sql.ErrNoRows):
		return domain.Venue{}, false, nil
	case err != nil:
		return domain.Venue{}, false, err
	}
	venue.Origin = domain.Origin(origin)
	return venue, true, nil
}

func loadEvents(ctx context.Context, q queryer, query string, args ...any) ([]domain.Event, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func loadEventBySlug(ctx context.Context, q queryer, slug string) (domain.Event, bool, error) {
	events, err := loadEvents(ctx, q, `
		SELECT
			e.slug,
			e.name,
			v.slug,
			e.start_at,
			e.end_at,
			e.genre,
			e.status,
			e.description,
			s.name,
			s.url,
			e.last_checked_at,
			e.origin
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.slug = ?
		LIMIT 1
	`, slug)
	if err != nil {
		return domain.Event{}, false, err
	}
	if len(events) == 0 {
		return domain.Event{}, false, nil
	}
	return events[0], true, nil
}

func scanEvent(rows *sql.Rows) (domain.Event, error) {
	var event domain.Event
	var origin string
	var startText string
	var endText string
	var lastCheckedText string
	if err := rows.Scan(
		&event.Slug,
		&event.Name,
		&event.VenueSlug,
		&startText,
		&endText,
		&event.Genre,
		&event.Status,
		&event.Description,
		&event.SourceName,
		&event.SourceURL,
		&lastCheckedText,
		&origin,
	); err != nil {
		return domain.Event{}, err
	}

	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return domain.Event{}, fmt.Errorf("parse event %q start time: %w", event.Slug, err)
	}
	end, err := parseRFC3339UTC(endText)
	if err != nil {
		return domain.Event{}, fmt.Errorf("parse event %q end time: %w", event.Slug, err)
	}
	lastChecked, err := parseRFC3339UTC(lastCheckedText)
	if err != nil {
		return domain.Event{}, fmt.Errorf("parse event %q last checked time: %w", event.Slug, err)
	}

	event.Start = start
	event.End = end
	event.LastChecked = lastChecked
	event.Origin = domain.Origin(origin)
	return event, nil
}

func formatRFC3339UTC(t time.Time) string {
	return t.UTC().Format(rfc3339Timestamp)
}

func parseRFC3339UTC(value string) (time.Time, error) {
	parsed, err := time.Parse(rfc3339Timestamp, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func readMigration(path string) (string, error) {
	raw, err := migrationFS.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func dsnForPath(path string) string {
	slashed := filepath.ToSlash(path)
	if filepath.IsAbs(path) {
		return "file://" + slashed + "?_pragma=foreign_keys(1)"
	}
	return "file:" + slashed + "?_pragma=foreign_keys(1)"
}
