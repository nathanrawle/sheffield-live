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
	"sheffield-live/internal/review"
	seedstore "sheffield-live/internal/store"
)

const (
	defaultPath       = "./data/sheffield-live.db"
	schemaVersionV1   = 1
	schemaVersionV2   = 2
	schemaVersionV3   = 3
	schemaVersionV4   = 4
	schemaVersionV5   = 5
	schemaVersionV6   = 6
	schemaVersionV7   = 7
	schemaVersionV8   = 8
	schemaVersionV9   = 9
	schemaVersionV10  = 10
	schemaVersionV11  = 11
	schemaVersionV12  = 12
	schemaVersionV13  = 13
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
	{version: schemaVersionV4, path: "migrations/0004_event_source_links.sql"},
	{version: schemaVersionV5, path: "migrations/0005_review_group_authoritative_link.sql"},
	{version: schemaVersionV6, path: "migrations/0006_event_secondary_source_info.sql"},
	{version: schemaVersionV7, path: "migrations/0007_import_run_review_groups.sql"},
	{version: schemaVersionV8, path: "migrations/0008_venue_coverage.sql"},
	{version: schemaVersionV9, path: "migrations/0009_events_nullable_end_at.sql"},
	{version: schemaVersionV10, path: "migrations/0010_review_canonical_defaults.sql"},
	{version: schemaVersionV11, path: "migrations/0011_events_publication_state.sql"},
	{version: schemaVersionV12, path: "migrations/0012_venue_validation_state.sql"},
	{version: schemaVersionV13, path: "migrations/0013_review_candidate_venue_evidence.sql"},
}

//go:embed migrations/*.sql
var migrationFS embed.FS

type Store struct {
	db             *sql.DB
	sourceMetadata ingest.SourceMetadataLookup
}

var _ seedstore.CatalogStore = (*Store)(nil)
var _ ingest.ImportRunStore = (*Store)(nil)

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func Open(path string, sourceMetadata ...ingest.SourceMetadataLookup) (st *Store, err error) {
	if path == "" {
		path = defaultPath
	}
	var metadata ingest.SourceMetadataLookup
	if len(sourceMetadata) > 0 {
		metadata = sourceMetadata[0]
	}
	if metadata == nil {
		metadata, err = ingest.DefaultCatalog()
		if err != nil {
			return nil, fmt.Errorf("open sqlite store %q: load source metadata: %w", path, err)
		}
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
	if err := backfillReviewGroupImportRunLinks(ctx, tx); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: backfill review group import-run links: %w", path, err)
	}
	if err := backfillOpenReviewGroupsAuthoritativeLinks(ctx, tx, metadata); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: backfill review group authoritative links: %w", path, err)
	}
	if err := backfillReviewFieldDefaults(ctx, tx); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: backfill review field defaults: %w", path, err)
	}
	if err := backfillCanonicalUnknownEnds(ctx, tx, metadata); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: backfill canonical unknown ends: %w", path, err)
	}
	if err := auditCanonicalEqualTimeEnds(ctx, tx); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: audit canonical equal-time ends: %w", path, err)
	}
	if err := validate(ctx, tx); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: validate store: %w", path, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("open sqlite store %q: commit transaction: %w", path, err)
	}

	st = &Store{db: db, sourceMetadata: metadata}
	return st, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Venues() []domain.Venue {
	venues, _ := s.ListVenues(context.Background())
	return venues
}

func (s *Store) ListVenues(ctx context.Context) ([]domain.Venue, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	return loadVenues(ctx, s.db)
}

func (s *Store) Events() []domain.Event {
	events, _ := s.ListEvents(context.Background())
	return events
}

func (s *Store) ListEvents(ctx context.Context) ([]domain.Event, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	return loadEvents(ctx, s.db, `
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
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		ORDER BY e.start_at, e.slug
	`)
}

func (s *Store) VenueBySlug(slug string) (domain.Venue, bool) {
	venue, ok, _ := s.LoadVenueBySlug(context.Background(), slug)
	return venue, ok
}

func (s *Store) LoadVenueBySlug(ctx context.Context, slug string) (domain.Venue, bool, error) {
	if s == nil || s.db == nil {
		return domain.Venue{}, false, errors.New("sqlite store is not open")
	}
	return loadVenueBySlug(ctx, s.db, slug)
}

func (s *Store) ValidateVenue(ctx context.Context, slug string) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return errors.New("venue slug is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	venue, ok, err := loadVenueBySlug(ctx, tx, slug)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("venue %q not found", slug)
	}
	if venue.ValidationState != domain.ValidationStateProvisional {
		return fmt.Errorf("venue %q is not provisional", slug)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE venues
		SET validation_state = ?
		WHERE slug = ?
	`, string(domain.ValidationStateValidated), slug); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) EventBySlug(slug string) (domain.Event, bool) {
	event, ok, _ := s.LoadEventBySlug(context.Background(), slug)
	return event, ok
}

func (s *Store) LoadEventBySlug(ctx context.Context, slug string) (domain.Event, bool, error) {
	if s == nil || s.db == nil {
		return domain.Event{}, false, errors.New("sqlite store is not open")
	}
	return loadEventBySlug(ctx, s.db, slug)
}

func (s *Store) EventSecondarySourceInfoByEventSlug(ctx context.Context, slug string) ([]seedstore.EventSecondarySourceInfo, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	return loadEventSecondarySourceInfoBySlug(ctx, s.db, slug)
}

func (s *Store) EventsForVenue(venueSlug string) []domain.Event {
	events, _ := s.ListEventsForVenue(context.Background(), venueSlug)
	return events
}

func (s *Store) ListEventsForVenue(ctx context.Context, venueSlug string) ([]domain.Event, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	return loadEvents(ctx, s.db, `
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
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE v.slug = ?
		ORDER BY e.start_at, e.slug
	`, venueSlug)
}

func (s *Store) Validate(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	return validate(ctx, s.db)
}

func (s *Store) Ready(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	var ready int
	if err := s.db.QueryRowContext(ctx, `SELECT 1`).Scan(&ready); err != nil {
		return err
	}
	return nil
}

func migrate(ctx context.Context, tx *sql.Tx) error {
	version, err := schemaVersion(ctx, tx)
	if err != nil {
		return err
	}
	if version > schemaVersionV13 {
		return fmt.Errorf("database schema version %d is newer than supported version %d", version, schemaVersionV13)
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
		INSERT INTO venues (slug, name, address, neighbourhood, description, website, validation_state, coverage_kind, coverage_note, origin)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, venue.Slug, venue.Name, venue.Address, venue.Neighbourhood, venue.Description, venue.Website, string(normalizedValidationState(venue.ValidationState)), normalizedCoverageKind(venue), strings.TrimSpace(venue.CoverageNote), string(venue.Origin))
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
			origin,
			publication_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event.Slug, venueID, sourceID, event.Name,
		formatRFC3339UTC(event.Start),
		nullableRFC3339UTC(event.End),
		event.Genre, event.Status, event.Description,
		formatRFC3339UTC(event.LastChecked),
		string(event.Origin),
		string(normalizedPublicationState(event.PublicationState)))
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
	if err := validateDanglingEventSourceLinkRefs(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingEventSecondarySourceInfoRefs(ctx, q); err != nil {
		return err
	}
	if err := validateDanglingImportRunReviewGroupRefs(ctx, q); err != nil {
		return err
	}
	if err := auditCanonicalEqualTimeEnds(ctx, q); err != nil {
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
			e.origin,
			e.publication_state
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

func validateDanglingEventSourceLinkRefs(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT l.id
		FROM event_source_links l
		LEFT JOIN sources s ON s.id = l.source_id
		LEFT JOIN events e ON e.id = l.event_id
		WHERE s.id IS NULL OR e.id IS NULL
		ORDER BY l.id
		LIMIT 1
	`)
	var linkID int64
	switch err := row.Scan(&linkID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event source link %d references missing source or event", linkID)
}

func validateDanglingEventSecondarySourceInfoRefs(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT i.id
		FROM event_secondary_source_info i
		LEFT JOIN sources s ON s.id = i.source_id
		LEFT JOIN events e ON e.id = i.event_id
		WHERE s.id IS NULL OR e.id IS NULL
		ORDER BY i.id
		LIMIT 1
	`)
	var infoID int64
	switch err := row.Scan(&infoID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event secondary source info %d references missing source or event", infoID)
}

func validateDanglingImportRunReviewGroupRefs(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT l.import_run_id, l.review_group_id
		FROM import_run_review_groups l
		LEFT JOIN import_runs ir ON ir.id = l.import_run_id
		LEFT JOIN review_groups rg ON rg.id = l.review_group_id
		WHERE ir.id IS NULL OR rg.id IS NULL
		ORDER BY l.review_group_id, l.import_run_id
		LIMIT 1
	`)
	var importRunID int64
	var reviewGroupID int64
	switch err := row.Scan(&importRunID, &reviewGroupID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("import-run review-group link (%d, %d) references missing rows", importRunID, reviewGroupID)
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
		SELECT slug, name, address, neighbourhood, description, website, validation_state, coverage_kind, coverage_note, origin
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
		var validationState string
		var coverageKind string
		var origin string
		if err := rows.Scan(&venue.Slug, &venue.Name, &venue.Address, &venue.Neighbourhood, &venue.Description, &venue.Website, &validationState, &coverageKind, &venue.CoverageNote, &origin); err != nil {
			return nil, err
		}
		venue.ValidationState = normalizedValidationState(domain.ValidationState(validationState))
		venue.CoverageKind = domain.CoverageKind(normalizedCoverageKindValue(coverageKind))
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
		SELECT slug, name, address, neighbourhood, description, website, validation_state, coverage_kind, coverage_note, origin
		FROM venues
		WHERE slug = ?
		LIMIT 1
	`, slug)
	var venue domain.Venue
	var validationState string
	var coverageKind string
	var origin string
	switch err := row.Scan(&venue.Slug, &venue.Name, &venue.Address, &venue.Neighbourhood, &venue.Description, &venue.Website, &validationState, &coverageKind, &venue.CoverageNote, &origin); {
	case errors.Is(err, sql.ErrNoRows):
		return domain.Venue{}, false, nil
	case err != nil:
		return domain.Venue{}, false, err
	}
	venue.ValidationState = normalizedValidationState(domain.ValidationState(validationState))
	venue.CoverageKind = domain.CoverageKind(normalizedCoverageKindValue(coverageKind))
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
			e.origin,
			e.publication_state
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

func loadEventSecondarySourceInfoBySlug(ctx context.Context, q queryer, slug string) ([]seedstore.EventSecondarySourceInfo, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			s.name,
			s.url,
			i.info_type,
			i.value
		FROM event_secondary_source_info i
		JOIN events e ON e.id = i.event_id
		JOIN sources s ON s.id = i.source_id
		WHERE e.slug = ?
		ORDER BY s.name, s.url, i.info_type, i.value
	`, slug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []seedstore.EventSecondarySourceInfo
	groupIndex := make(map[string]int)
	for rows.Next() {
		var sourceName string
		var sourceURL string
		var infoType string
		var value string
		if err := rows.Scan(&sourceName, &sourceURL, &infoType, &value); err != nil {
			return nil, err
		}
		key := sourceName + "\x00" + sourceURL
		idx, ok := groupIndex[key]
		if !ok {
			idx = len(groups)
			groupIndex[key] = idx
			groups = append(groups, seedstore.EventSecondarySourceInfo{
				SourceName: sourceName,
				SourceURL:  sourceURL,
			})
		}
		switch infoType {
		case "genre":
			groups[idx].Genres = appendUniqueString(groups[idx].Genres, value)
		case "description":
			groups[idx].Descriptions = appendUniqueString(groups[idx].Descriptions, value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groups, nil
}

func appendUniqueString(values []string, candidate string) []string {
	for _, value := range values {
		if value == candidate {
			return values
		}
	}
	return append(values, candidate)
}

func scanEvent(rows *sql.Rows) (domain.Event, error) {
	var event domain.Event
	var origin string
	var publicationState string
	var startText string
	var endText sql.NullString
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
		&publicationState,
	); err != nil {
		return domain.Event{}, err
	}

	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return domain.Event{}, fmt.Errorf("parse event %q start time: %w", event.Slug, err)
	}
	end, err := parseNullableRFC3339UTC(endText)
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
	event.PublicationState = normalizedPublicationState(domain.PublicationState(publicationState))
	if err := event.ValidateCanonical(); err != nil {
		return domain.Event{}, fmt.Errorf("event %q %w", event.Slug, err)
	}
	return event, nil
}

func normalizedPublicationState(state domain.PublicationState) domain.PublicationState {
	switch state {
	case domain.PublicationStateProvisional:
		return domain.PublicationStateProvisional
	case domain.PublicationStateReviewed, "":
		return domain.PublicationStateReviewed
	default:
		return domain.PublicationStateReviewed
	}
}

func normalizedValidationState(state domain.ValidationState) domain.ValidationState {
	switch state {
	case domain.ValidationStateProvisional:
		return domain.ValidationStateProvisional
	case domain.ValidationStateValidated, "":
		return domain.ValidationStateValidated
	default:
		return domain.ValidationStateProvisional
	}
}

func formatRFC3339UTC(t time.Time) string {
	return t.UTC().Format(rfc3339Timestamp)
}

func nullableRFC3339UTC(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return formatRFC3339UTC(t)
}

func parseRFC3339UTC(value string) (time.Time, error) {
	parsed, err := time.Parse(rfc3339Timestamp, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func parseNullableRFC3339UTC(value sql.NullString) (time.Time, error) {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return time.Time{}, nil
	}
	return parseRFC3339UTC(value.String)
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

func backfillCanonicalUnknownEnds(ctx context.Context, tx interface {
	execer
	queryer
}, sourceMetadata ingest.SourceMetadataLookup) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			e.id,
			e.slug,
			v.slug,
			s.name,
			EXISTS(
				SELECT 1
				FROM event_source_links l
				WHERE l.event_id = e.id
					AND l.source_id = e.source_id
					AND l.is_authoritative = 1
			)
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.origin = ?
			AND e.end_at IS NOT NULL
			AND e.start_at = e.end_at
		ORDER BY e.id
	`, string(domain.OriginLive))
	if err != nil {
		return err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var eventID int64
		var eventSlug string
		var venueSlug string
		var sourceName string
		var authoritative int
		if err := rows.Scan(&eventID, &eventSlug, &venueSlug, &sourceName, &authoritative); err != nil {
			return err
		}
		if authoritative != 1 {
			continue
		}
		if strings.TrimSpace(sourceMetadata.OwnedVenueSlugForReviewStageSourceName(sourceName)) != strings.TrimSpace(venueSlug) {
			continue
		}
		ids = append(ids, eventID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, eventID := range ids {
		if _, err := tx.ExecContext(ctx, `
			UPDATE events
			SET end_at = NULL
			WHERE id = ?
		`, eventID); err != nil {
			return err
		}
	}
	return nil
}

func auditCanonicalEqualTimeEnds(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT slug
		FROM events
		WHERE end_at IS NOT NULL
			AND start_at = end_at
		ORDER BY id
		LIMIT 1
	`)
	var slug string
	switch err := row.Scan(&slug); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event %q still uses placeholder end_at equal to start_at; set a real end or clear end_at to NULL", slug)
}

func backfillReviewGroupImportRunLinks(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, notes, created_at
		FROM review_groups
		ORDER BY id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var groupID int64
		var notes string
		var createdAt string
		if err := rows.Scan(&groupID, &notes, &createdAt); err != nil {
			return err
		}
		importRunID, ok := review.ParseOriginImportRunID(notes)
		if !ok {
			continue
		}
		linkedAt, err := parseRFC3339UTC(createdAt)
		if err != nil {
			return fmt.Errorf("parse review group %d created_at for import-run backfill: %w", groupID, err)
		}
		if err := linkReviewGroupToImportRunTx(ctx, tx, importRunID, groupID, linkedAt); err != nil {
			return err
		}
	}
	return rows.Err()
}

func linkReviewGroupToImportRunTx(ctx context.Context, tx interface {
	execer
	queryer
}, importRunID, reviewGroupID int64, linkedAt time.Time) error {
	if importRunID <= 0 || reviewGroupID <= 0 {
		return nil
	}
	var exists int
	switch err := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM import_runs
		WHERE id = ?
		LIMIT 1
	`, importRunID).Scan(&exists); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO import_run_review_groups (import_run_id, review_group_id, linked_at)
		VALUES (?, ?, ?)
	`, importRunID, reviewGroupID, formatRFC3339UTC(linkedAt))
	return err
}

func normalizedCoverageKind(venue domain.Venue) string {
	return normalizedCoverageKindValue(string(venue.CoverageKind))
}

func normalizedCoverageKindValue(value string) string {
	if strings.TrimSpace(value) == string(domain.CoverageKindProgram) {
		return string(domain.CoverageKindProgram)
	}
	return string(domain.CoverageKindVenue)
}
