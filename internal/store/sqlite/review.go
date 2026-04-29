package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/ingest"
	"sheffield-live/internal/review"
)

func (s *Store) CreateReviewGroup(ctx context.Context, input review.GroupInput) (int64, error) {
	return s.createReviewGroup(ctx, input, "")
}

func (s *Store) StageReviewGroup(ctx context.Context, input review.GroupInput) (int64, bool, error) {
	if s == nil || s.db == nil {
		return 0, false, errors.New("sqlite store is not open")
	}
	stagingKey := strings.TrimSpace(input.StagingKey)
	if stagingKey == "" {
		groupID, err := s.createReviewGroup(ctx, input, "")
		if err != nil {
			return 0, false, err
		}
		return groupID, true, nil
	}
	input.Title = strings.TrimSpace(input.Title)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	if input.Title == "" {
		input.Title = "Review group"
	}
	if input.SourceName == "" {
		return 0, false, errors.New("review source name is required")
	}
	if input.SourceURL == "" {
		return 0, false, errors.New("review source URL is required")
	}
	if len(input.Candidates) == 0 {
		return 0, false, errors.New("at least one review candidate is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO review_groups (
			title,
			source_name,
			source_url,
			authoritative_source_name,
			authoritative_source_url,
			authoritative_source_event_key,
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, input.Title, input.SourceName, input.SourceURL,
		nullableReviewText(input.AuthoritativeSourceName),
		nullableReviewText(input.AuthoritativeSourceURL),
		nullableReviewText(input.AuthoritativeSourceEventKey),
		stagingKeyValue(stagingKey), review.StatusOpen, input.Notes, formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return 0, false, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}

	if rowsAffected == 1 {
		groupID, err := res.LastInsertId()
		if err != nil {
			return 0, false, err
		}
		for i, candidate := range input.Candidates {
			if err := insertReviewCandidate(ctx, tx, groupID, i+1, candidate, input.SourceName, input.SourceURL); err != nil {
				return 0, false, err
			}
		}
		if err := linkReviewGroupInputToImportRunTx(ctx, tx, input, groupID, now); err != nil {
			return 0, false, err
		}
		if err := tx.Commit(); err != nil {
			return 0, false, err
		}
		return groupID, true, nil
	}

	group, ok, err := loadReviewGroupByStagingKey(ctx, tx, stagingKey)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, errors.New("staged review group not found after ignore")
	}
	if group.Status == review.StatusOpen {
		if err := refreshReviewGroupAuthoritativeLinkTx(ctx, tx, group.ID, reviewGroupAuthoritativeLinkInput{
			SourceName:     input.AuthoritativeSourceName,
			SourceURL:      input.AuthoritativeSourceURL,
			SourceEventKey: input.AuthoritativeSourceEventKey,
		}, now); err != nil {
			return 0, false, err
		}
	}
	if err := linkReviewGroupInputToImportRunTx(ctx, tx, input, group.ID, now); err != nil {
		return 0, false, err
	}

	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return group.ID, false, nil
}

func (s *Store) PromoteSingletonReviewGroupIfMissing(ctx context.Context, input review.GroupInput) (string, bool, error) {
	if s == nil || s.db == nil {
		return "", false, errors.New("sqlite store is not open")
	}
	now := time.Now().UTC()

	eventSlug, applied, err := s.promoteAuthoritativeSingletonReviewGroupIfMissing(ctx, input, now)
	if err != nil || applied {
		return eventSlug, applied, err
	}
	return s.promoteNonAuthoritativeSingletonReviewGroupIfMissing(ctx, input, now)
}

func (s *Store) promoteAuthoritativeSingletonReviewGroupIfMissing(ctx context.Context, input review.GroupInput, now time.Time) (string, bool, error) {
	sourceEventKey := authoritativeSingletonSourceEventKey(input)
	if sourceEventKey == "" {
		return "", false, nil
	}

	event, err := singletonResolvedEventFromGroupInput(input, now)
	if err != nil {
		return "", false, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	appliedEvent, applied, err := applyAuthoritativeEventTx(ctx, tx, event, sourceEventKey, now)
	if err != nil {
		return "", false, err
	}
	if applied {
		if err := resolveMatchingOpenReviewGroupsTx(ctx, tx, input, now); err != nil {
			return "", false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	if !applied {
		return "", false, nil
	}
	return appliedEvent.Event.Slug, true, nil
}

func (s *Store) promoteNonAuthoritativeSingletonReviewGroupIfMissing(ctx context.Context, input review.GroupInput, now time.Time) (string, bool, error) {
	expectedVenueSlug := nonAuthoritativeSingletonVenueSlug(input)
	if expectedVenueSlug == "" {
		return "", false, nil
	}

	event, err := singletonResolvedEventFromGroupInput(input, now)
	if err != nil {
		return "", false, nil
	}
	if event.VenueSlug != expectedVenueSlug {
		return "", false, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, ok, err := loadEventRecordBySlugTx(ctx, tx, event.Slug); err != nil {
		return "", false, err
	} else if ok {
		return "", false, nil
	}

	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, expectedVenueSlug)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	sourceID, err := ensureSourceTx(ctx, tx, event.SourceName, event.SourceURL)
	if err != nil {
		return "", false, err
	}
	if _, err := insertEventTx(ctx, tx, event, venueID, sourceID); err != nil {
		return "", false, err
	}
	if err := resolveMatchingOpenNonAuthoritativeSingletonReviewGroupsTx(ctx, tx, input, now); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return event.Slug, true, nil
}

func (s *Store) createReviewGroup(ctx context.Context, input review.GroupInput, stagingKey string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("sqlite store is not open")
	}
	input.Title = strings.TrimSpace(input.Title)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	if input.Title == "" {
		input.Title = "Review group"
	}
	if input.SourceName == "" {
		return 0, errors.New("review source name is required")
	}
	if input.SourceURL == "" {
		return 0, errors.New("review source URL is required")
	}
	if len(input.Candidates) == 0 {
		return 0, errors.New("at least one review candidate is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO review_groups (
			title,
			source_name,
			source_url,
			authoritative_source_name,
			authoritative_source_url,
			authoritative_source_event_key,
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, input.Title, input.SourceName, input.SourceURL,
		nullableReviewText(input.AuthoritativeSourceName),
		nullableReviewText(input.AuthoritativeSourceURL),
		nullableReviewText(input.AuthoritativeSourceEventKey),
		stagingKeyValue(stagingKey), review.StatusOpen, input.Notes, formatRFC3339UTC(now), formatRFC3339UTC(now))
	if err != nil {
		return 0, err
	}
	groupID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	for i, candidate := range input.Candidates {
		if err := insertReviewCandidate(ctx, tx, groupID, i+1, candidate, input.SourceName, input.SourceURL); err != nil {
			return 0, err
		}
	}
	if err := linkReviewGroupInputToImportRunTx(ctx, tx, input, groupID, now); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return groupID, nil
}

func linkReviewGroupInputToImportRunTx(ctx context.Context, tx interface {
	execer
	queryer
}, input review.GroupInput, groupID int64, linkedAt time.Time) error {
	importRunID := input.ImportRunID
	if importRunID <= 0 {
		importRunID, _ = review.ParseOriginImportRunID(input.Notes)
	}
	return linkReviewGroupToImportRunTx(ctx, tx, importRunID, groupID, linkedAt)
}

func singletonResolvedEventFromGroupInput(input review.GroupInput, publishedAt time.Time) (domain.Event, error) {
	if len(input.Candidates) != 1 {
		return domain.Event{}, errors.New("singleton review group promotion requires exactly one candidate")
	}

	candidate := input.Candidates[0]
	group := review.Group{
		Title:      strings.TrimSpace(input.Title),
		SourceName: strings.TrimSpace(input.SourceName),
		SourceURL:  strings.TrimSpace(input.SourceURL),
	}
	selectedCandidate := review.Candidate{
		ID:          1,
		ExternalID:  strings.TrimSpace(candidate.ExternalID),
		Name:        strings.TrimSpace(candidate.Name),
		VenueSlug:   strings.TrimSpace(candidate.VenueSlug),
		StartAt:     strings.TrimSpace(candidate.StartAt),
		EndAt:       strings.TrimSpace(candidate.EndAt),
		Genre:       strings.TrimSpace(candidate.Genre),
		Status:      strings.TrimSpace(candidate.Status),
		Description: strings.TrimSpace(candidate.Description),
		SourceName:  strings.TrimSpace(candidate.SourceName),
		SourceURL:   strings.TrimSpace(candidate.SourceURL),
		Provenance:  strings.TrimSpace(candidate.Provenance),
	}
	selected := make(map[review.Field]review.Candidate, len(review.CanonicalFields))
	for _, field := range review.CanonicalFields {
		selected[field] = selectedCandidate
	}
	return buildResolvedEvent(group, selected, publishedAt)
}

func authoritativeSourceEventKey(input review.GroupInput) string {
	if len(input.Candidates) != 1 {
		return ""
	}
	candidate := input.Candidates[0]
	for _, value := range []string{candidate.ExternalID, candidate.SourceURL} {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func authoritativeSingletonSourceEventKey(input review.GroupInput) string {
	sourceEventKey := authoritativeSourceEventKey(input)
	if sourceEventKey == "" {
		return ""
	}

	sourceName := strings.TrimSpace(input.SourceName)
	if ingest.NonAuthoritativeSingletonVenueSlugForReviewStageSourceName(sourceName) != "" {
		return ""
	}

	ownedVenueSlug := strings.TrimSpace(ingest.OwnedVenueSlugForReviewStageSourceName(sourceName))
	if ownedVenueSlug == "" {
		return sourceEventKey
	}
	if strings.TrimSpace(input.Candidates[0].VenueSlug) != ownedVenueSlug {
		return ""
	}
	return sourceEventKey
}

func nonAuthoritativeSingletonVenueSlug(input review.GroupInput) string {
	if len(input.Candidates) != 1 {
		return ""
	}
	expectedVenueSlug := strings.TrimSpace(ingest.NonAuthoritativeSingletonVenueSlugForReviewStageSourceName(input.SourceName))
	if expectedVenueSlug == "" {
		return ""
	}
	if strings.TrimSpace(input.Candidates[0].VenueSlug) != expectedVenueSlug {
		return ""
	}
	return expectedVenueSlug
}

func resolveMatchingOpenReviewGroupsTx(ctx context.Context, tx execer, input review.GroupInput, now time.Time) error {
	stagingKey := strings.TrimSpace(input.StagingKey)
	if stagingKey == "" {
		return nil
	}

	args := []any{review.StatusResolved, formatRFC3339UTC(now), review.StatusOpen, stagingKey}
	query := `
		UPDATE review_groups
		SET status = ?, updated_at = ?
		WHERE status = ?
		  AND staging_key = ?
	`
	if sourceEventKey := strings.TrimSpace(input.AuthoritativeSourceEventKey); sourceEventKey != "" {
		query += ` AND authoritative_source_event_key = ?`
		args = append(args, sourceEventKey)
	}
	_, err := tx.ExecContext(ctx, query, args...)
	return err
}

func resolveMatchingOpenNonAuthoritativeSingletonReviewGroupsTx(ctx context.Context, tx interface {
	execer
	queryer
}, input review.GroupInput, now time.Time) error {
	stagingKey := strings.TrimSpace(input.StagingKey)
	if stagingKey == "" {
		return nil
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT g.id
		FROM review_groups g
		JOIN review_candidates c ON c.group_id = g.id
		WHERE g.status = ?
		  AND g.staging_key = ?
		GROUP BY g.id
		HAVING COUNT(c.id) = 1
	`, review.StatusOpen, stagingKey)
	if err != nil {
		return err
	}
	defer rows.Close()

	var groupIDs []int64
	for rows.Next() {
		var groupID int64
		if err := rows.Scan(&groupID); err != nil {
			return err
		}
		groupIDs = append(groupIDs, groupID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, groupID := range groupIDs {
		if err := linkReviewGroupInputToImportRunTx(ctx, tx, input, groupID, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE review_groups
			SET status = ?, updated_at = ?
			WHERE id = ?
		`, review.StatusResolved, formatRFC3339UTC(now), groupID); err != nil {
			return err
		}
	}
	return nil
}

type eventRecord struct {
	ID    int64
	Event domain.Event
}

func applyAuthoritativeEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, event domain.Event, sourceEventKey string, now time.Time) (eventRecord, bool, error) {
	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return eventRecord{}, false, err
	}
	if !ok {
		return eventRecord{}, false, nil
	}
	sourceID, err := ensureSourceTx(ctx, tx, event.SourceName, event.SourceURL)
	if err != nil {
		return eventRecord{}, false, err
	}

	if linked, ok, err := loadEventRecordBySourceLinkTx(ctx, tx, sourceID, sourceEventKey); err != nil {
		return eventRecord{}, false, err
	} else if ok {
		updated, err := updateEventAuthoritativelyTx(ctx, tx, linked, event, venueID, sourceID)
		if err != nil {
			return eventRecord{}, false, err
		}
		if err := ensureEventSourceLinkTx(ctx, tx, linked.ID, sourceID, sourceEventKey, now); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{ID: linked.ID, Event: updated}, true, nil
	}

	if legacy, ok, err := loadEventRecordBySlugTx(ctx, tx, event.Slug); err != nil {
		return eventRecord{}, false, err
	} else if ok {
		updated, err := updateEventAuthoritativelyTx(ctx, tx, legacy, event, venueID, sourceID)
		if err != nil {
			return eventRecord{}, false, err
		}
		if err := ensureEventSourceLinkTx(ctx, tx, legacy.ID, sourceID, sourceEventKey, now); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{ID: legacy.ID, Event: updated}, true, nil
	}

	eventID, err := insertEventTx(ctx, tx, event, venueID, sourceID)
	if err != nil {
		return eventRecord{}, false, err
	}
	if err := ensureEventSourceLinkTx(ctx, tx, eventID, sourceID, sourceEventKey, now); err != nil {
		return eventRecord{}, false, err
	}
	return eventRecord{ID: eventID, Event: event}, true, nil
}

func insertEventTx(ctx context.Context, tx execer, event domain.Event, venueID, sourceID int64) (int64, error) {
	res, err := tx.ExecContext(ctx, `
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
		nullableRFC3339UTC(event.End),
		event.Genre, event.Status, event.Description,
		formatRFC3339UTC(event.LastChecked),
		string(event.Origin))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func updateEventAuthoritativelyTx(ctx context.Context, tx execer, existing eventRecord, authoritative domain.Event, venueID, sourceID int64) (domain.Event, error) {
	updated := existing.Event
	updated.Name = authoritative.Name
	updated.VenueSlug = authoritative.VenueSlug
	updated.Start = authoritative.Start
	updated.End = authoritative.End
	if updated.Genre == "" {
		updated.Genre = authoritative.Genre
	}
	if authoritative.Status != "" {
		updated.Status = authoritative.Status
	}
	if updated.Description == "" {
		updated.Description = authoritative.Description
	}
	updated.SourceName = authoritative.SourceName
	updated.SourceURL = authoritative.SourceURL
	updated.LastChecked = authoritative.LastChecked.UTC()
	updated.Origin = authoritative.Origin

	if _, err := tx.ExecContext(ctx, `
		UPDATE events
		SET venue_id = ?,
			source_id = ?,
			name = ?,
			start_at = ?,
			end_at = ?,
			genre = ?,
			status = ?,
			description = ?,
			last_checked_at = ?,
			origin = ?
		WHERE id = ?
	`, venueID, sourceID, updated.Name, formatRFC3339UTC(updated.Start), nullableRFC3339UTC(updated.End), updated.Genre, updated.Status, updated.Description, formatRFC3339UTC(updated.LastChecked), string(updated.Origin), existing.ID); err != nil {
		return domain.Event{}, err
	}
	return updated, nil
}

func ensureEventSourceLinkTx(ctx context.Context, tx execer, eventID, sourceID int64, sourceEventKey string, now time.Time) error {
	if eventID <= 0 {
		return errors.New("event source link event ID is required")
	}
	if sourceID <= 0 {
		return errors.New("event source link source ID is required")
	}
	sourceEventKey = strings.TrimSpace(sourceEventKey)
	if sourceEventKey == "" {
		return errors.New("event source link key is required")
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO event_source_links (
			event_id,
			source_id,
			source_event_key,
			is_authoritative,
			created_at,
			updated_at
		) VALUES (?, ?, ?, 1, ?, ?)
		ON CONFLICT(source_id, source_event_key) DO UPDATE SET
			event_id = excluded.event_id,
			is_authoritative = excluded.is_authoritative,
			updated_at = excluded.updated_at
	`, eventID, sourceID, sourceEventKey, formatRFC3339UTC(now), formatRFC3339UTC(now))
	return err
}

func loadEventRecordBySourceLinkTx(ctx context.Context, q queryer, sourceID int64, sourceEventKey string) (eventRecord, bool, error) {
	return loadEventRecord(ctx, q, `
		SELECT
			e.id,
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
		FROM event_source_links l
		JOIN events e ON e.id = l.event_id
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE l.source_id = ? AND l.source_event_key = ?
		LIMIT 1
	`, sourceID, sourceEventKey)
}

func loadEventRecordBySlugTx(ctx context.Context, q queryer, slug string) (eventRecord, bool, error) {
	return loadEventRecord(ctx, q, `
		SELECT
			e.id,
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
}

func loadEventRecord(ctx context.Context, q queryer, query string, args ...any) (eventRecord, bool, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return eventRecord{}, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return eventRecord{}, false, err
		}
		return eventRecord{}, false, nil
	}

	var record eventRecord
	var origin string
	var startText string
	var endText sql.NullString
	var lastCheckedText string
	if err := rows.Scan(
		&record.ID,
		&record.Event.Slug,
		&record.Event.Name,
		&record.Event.VenueSlug,
		&startText,
		&endText,
		&record.Event.Genre,
		&record.Event.Status,
		&record.Event.Description,
		&record.Event.SourceName,
		&record.Event.SourceURL,
		&lastCheckedText,
		&origin,
	); err != nil {
		return eventRecord{}, false, err
	}
	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return eventRecord{}, false, fmt.Errorf("parse event %q start time: %w", record.Event.Slug, err)
	}
	end, err := parseNullableRFC3339UTC(endText)
	if err != nil {
		return eventRecord{}, false, fmt.Errorf("parse event %q end time: %w", record.Event.Slug, err)
	}
	lastChecked, err := parseRFC3339UTC(lastCheckedText)
	if err != nil {
		return eventRecord{}, false, fmt.Errorf("parse event %q last checked time: %w", record.Event.Slug, err)
	}
	record.Event.Start = start
	record.Event.End = end
	record.Event.LastChecked = lastChecked
	record.Event.Origin = domain.Origin(origin)
	if err := record.Event.ValidateCanonical(); err != nil {
		return eventRecord{}, false, fmt.Errorf("event %q %w", record.Event.Slug, err)
	}
	if err := rows.Err(); err != nil {
		return eventRecord{}, false, err
	}
	return record, true, nil
}

func stagingKeyValue(value string) any {
	return nullableReviewText(value)
}

func nullableReviewText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func (s *Store) ListOpenReviewGroups(ctx context.Context) ([]review.GroupSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COUNT(DISTINCT c.id),
			COUNT(DISTINCT d.field),
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			CASE
				WHEN COALESCE(g.authoritative_source_name, '') <> ''
					AND COALESCE(g.authoritative_source_url, '') <> ''
					AND COALESCE(g.authoritative_source_event_key, '') <> ''
				THEN 1
				ELSE 0
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.start_at), '')) = 1
				THEN MIN(NULLIF(TRIM(c.start_at), ''))
				ELSE NULL
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(NULLIF(TRIM(c.venue_slug), ''))
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(COALESCE(v.name, ''))
				ELSE ''
			END
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.status = ?
		GROUP BY g.id
		ORDER BY g.updated_at DESC, g.id DESC
	`, review.StatusOpen)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []review.GroupSummary
	for rows.Next() {
		group, err := scanReviewGroupSummaryRow(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groups, nil
}

func (s *Store) ListClosedReviewGroups(ctx context.Context, limit int) ([]review.GroupSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	if limit <= 0 {
		return nil, errors.New("review group limit must be positive")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COUNT(DISTINCT c.id),
			COUNT(DISTINCT d.field),
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			CASE
				WHEN COALESCE(g.authoritative_source_name, '') <> ''
					AND COALESCE(g.authoritative_source_url, '') <> ''
					AND COALESCE(g.authoritative_source_event_key, '') <> ''
				THEN 1
				ELSE 0
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.start_at), '')) = 1
				THEN MIN(NULLIF(TRIM(c.start_at), ''))
				ELSE NULL
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(NULLIF(TRIM(c.venue_slug), ''))
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(COALESCE(v.name, ''))
				ELSE ''
			END
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.status IN (?, ?)
		GROUP BY g.id
		ORDER BY g.updated_at DESC, g.id DESC
		LIMIT ?
	`, review.StatusResolved, review.StatusRejected, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []review.GroupSummary
	for rows.Next() {
		group, err := scanReviewGroupSummaryRow(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groups, nil
}

func (s *Store) ListReviewGroupsForImportRun(ctx context.Context, importRunID int64) ([]review.GroupSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	if importRunID <= 0 {
		return nil, errors.New("import run ID is required")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COUNT(DISTINCT c.id),
			COUNT(DISTINCT d.field),
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			CASE
				WHEN COALESCE(g.authoritative_source_name, '') <> ''
					AND COALESCE(g.authoritative_source_url, '') <> ''
					AND COALESCE(g.authoritative_source_event_key, '') <> ''
				THEN 1
				ELSE 0
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.start_at), '')) = 1
				THEN MIN(NULLIF(TRIM(c.start_at), ''))
				ELSE NULL
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(NULLIF(TRIM(c.venue_slug), ''))
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(COALESCE(v.name, ''))
				ELSE ''
			END
		FROM review_groups g
		JOIN import_run_review_groups l
			ON l.review_group_id = g.id
			AND l.import_run_id = ?
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.status IN (?, ?, ?)
		GROUP BY g.id
		ORDER BY g.updated_at DESC, g.id DESC
	`, importRunID, review.StatusOpen, review.StatusResolved, review.StatusRejected)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []review.GroupSummary
	for rows.Next() {
		group, err := scanReviewGroupSummaryRow(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groups, nil
}

func (s *Store) LoadReviewGroup(ctx context.Context, id int64) (review.Group, bool, error) {
	if s == nil || s.db == nil {
		return review.Group{}, false, errors.New("sqlite store is not open")
	}
	if id <= 0 {
		return review.Group{}, false, nil
	}

	group, ok, err := loadReviewGroup(ctx, s.db, id)
	if err != nil || !ok {
		return review.Group{}, ok, err
	}
	candidates, err := loadReviewCandidates(ctx, s.db, id)
	if err != nil {
		return review.Group{}, false, err
	}
	choices, err := loadReviewDraftChoices(ctx, s.db, id)
	if err != nil {
		return review.Group{}, false, err
	}
	group.Candidates = candidates
	group.DraftChoices = choices
	return group, true, nil
}

func (s *Store) SaveReviewDraftChoices(ctx context.Context, groupID int64, choices []review.DraftChoiceInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if groupID <= 0 {
		return errors.New("review group ID is required")
	}
	if len(choices) == 0 {
		return errors.New("at least one review choice is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	group, ok, err := loadReviewGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("review group %d not found", groupID)
	}
	if group.Status != review.StatusOpen {
		return fmt.Errorf("review group %d is not open", groupID)
	}

	now := time.Now().UTC()
	for _, choice := range choices {
		if !choice.Field.Valid() {
			return fmt.Errorf("invalid review field %q", choice.Field)
		}
		if choice.CandidateID <= 0 {
			return fmt.Errorf("candidate ID is required for %s", choice.Field.Label())
		}
		candidate, ok, err := loadReviewCandidate(ctx, tx, groupID, choice.CandidateID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("review candidate %d not found in group %d", choice.CandidateID, groupID)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO review_draft_choices (
				group_id,
				field,
				candidate_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(group_id, field) DO UPDATE SET
				candidate_id = excluded.candidate_id,
				value = excluded.value,
				updated_at = excluded.updated_at
		`, groupID, string(choice.Field), choice.CandidateID, review.CandidateValue(candidate, choice.Field), formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET updated_at = ?
		WHERE id = ?
	`, formatRFC3339UTC(now), groupID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *Store) ResolveReviewGroup(ctx context.Context, groupID int64, choices []review.DraftChoiceInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if groupID <= 0 {
		return errors.New("review group ID is required")
	}
	if len(choices) != len(review.CanonicalFields) {
		return fmt.Errorf("all review fields must be selected before resolving")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	group, ok, err := loadReviewGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("review group %d not found", groupID)
	}
	if group.Status != review.StatusOpen {
		return fmt.Errorf("review group %d is not open", groupID)
	}
	candidates, err := loadReviewCandidates(ctx, tx, groupID)
	if err != nil {
		return err
	}

	seen := make(map[review.Field]struct{}, len(choices))
	selectedCandidates := make(map[review.Field]review.Candidate, len(choices))
	now := time.Now().UTC()
	for _, choice := range choices {
		if !choice.Field.Valid() {
			return fmt.Errorf("invalid review field %q", choice.Field)
		}
		if _, exists := seen[choice.Field]; exists {
			return fmt.Errorf("duplicate review field %q", choice.Field)
		}
		seen[choice.Field] = struct{}{}
		if choice.CandidateID <= 0 {
			return fmt.Errorf("candidate ID is required for %s", choice.Field.Label())
		}
		candidate, ok, err := loadReviewCandidate(ctx, tx, groupID, choice.CandidateID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("review candidate %d not found in group %d", choice.CandidateID, groupID)
		}
		selectedCandidates[choice.Field] = candidate
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO review_draft_choices (
				group_id,
				field,
				candidate_id,
				value,
				updated_at
			) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(group_id, field) DO UPDATE SET
				candidate_id = excluded.candidate_id,
				value = excluded.value,
				updated_at = excluded.updated_at
		`, groupID, string(choice.Field), choice.CandidateID, review.CandidateValue(candidate, choice.Field), formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	event, err := buildResolvedEvent(group, selectedCandidates, now)
	if err != nil {
		return err
	}
	if authoritative, ok := reviewGroupAuthoritativeSource(group); ok {
		event.SourceName = authoritative.SourceName
		event.SourceURL = authoritative.SourceURL
		canonicalRecord, applied, err := applyAuthoritativeEventTx(ctx, tx, event, authoritative.SourceEventKey, now)
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("venue %q not found", event.VenueSlug)
		}
		if err := upsertEventSecondarySourceInfoTx(ctx, tx, canonicalRecord.ID, authoritative, candidates, now); err != nil {
			return err
		}
	} else {
		if err := upsertEventTx(ctx, tx, event); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET status = ?, updated_at = ?
		WHERE id = ?
	`, review.StatusResolved, formatRFC3339UTC(now), groupID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *Store) UpdateReviewGroupStatus(ctx context.Context, groupID int64, status string) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if groupID <= 0 {
		return errors.New("review group ID is required")
	}
	if status != review.StatusRejected {
		return fmt.Errorf("invalid review status %q", status)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	group, ok, err := loadReviewGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("review group %d not found", groupID)
	}
	if group.Status != review.StatusOpen {
		return fmt.Errorf("review group %d is not open", groupID)
	}

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET status = ?, updated_at = ?
		WHERE id = ?
	`, status, formatRFC3339UTC(now), groupID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func insertReviewCandidate(ctx context.Context, tx execer, groupID int64, position int, input review.CandidateInput, defaultSourceName, defaultSourceURL string) error {
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	if input.SourceName == "" {
		input.SourceName = defaultSourceName
	}
	if input.SourceURL == "" {
		input.SourceURL = defaultSourceURL
	}
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" {
		return fmt.Errorf("review candidate %d name is required", position)
	}

	_, err := tx.ExecContext(ctx, `
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
	`, groupID, position, strings.TrimSpace(input.ExternalID), input.Name,
		strings.TrimSpace(input.VenueSlug),
		strings.TrimSpace(input.StartAt),
		strings.TrimSpace(input.EndAt),
		strings.TrimSpace(input.Genre),
		strings.TrimSpace(input.Status),
		strings.TrimSpace(input.Description),
		input.SourceName,
		input.SourceURL,
		strings.TrimSpace(input.Provenance))
	return err
}

func loadReviewGroup(ctx context.Context, q queryer, id int64) (review.Group, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.authoritative_source_name,
			g.authoritative_source_url,
			g.authoritative_source_event_key,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(NULLIF(TRIM(c.venue_slug), ''))
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(COALESCE(v.name, ''))
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.start_at), '')) = 1
				THEN MIN(NULLIF(TRIM(c.start_at), ''))
				ELSE NULL
			END
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.id = ?
		GROUP BY g.id
		LIMIT 1
	`, id)
	return scanReviewGroupRow(row, id)
}

func loadReviewGroupByStagingKey(ctx context.Context, q queryer, stagingKey string) (review.Group, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT
			g.id,
			g.title,
			g.source_name,
			g.source_url,
			g.authoritative_source_name,
			g.authoritative_source_url,
			g.authoritative_source_event_key,
			g.status,
			g.notes,
			g.created_at,
			g.updated_at,
			COALESCE((
				SELECT l.import_run_id
				FROM import_run_review_groups l
				WHERE l.review_group_id = g.id
				ORDER BY l.linked_at DESC, l.import_run_id DESC
				LIMIT 1
			), 0),
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(NULLIF(TRIM(c.venue_slug), ''))
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.venue_slug), '')) = 1
				THEN MIN(COALESCE(v.name, ''))
				ELSE ''
			END,
			CASE
				WHEN COUNT(DISTINCT NULLIF(TRIM(c.start_at), '')) = 1
				THEN MIN(NULLIF(TRIM(c.start_at), ''))
				ELSE NULL
			END
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN venues v ON v.slug = c.venue_slug
		WHERE g.staging_key = ?
		GROUP BY g.id
		LIMIT 1
	`, stagingKey)
	return scanReviewGroupRow(row, 0)
}

func scanReviewGroupRow(scanner interface {
	Scan(...any) error
}, fallbackID int64) (review.Group, bool, error) {
	var group review.Group
	var authoritativeSourceName sql.NullString
	var authoritativeSourceURL sql.NullString
	var authoritativeSourceEventKey sql.NullString
	var createdAt string
	var updatedAt string
	var sharedVenueSlug string
	var sharedVenueName string
	var sharedStartAt sql.NullString
	switch err := scanner.Scan(
		&group.ID,
		&group.Title,
		&group.SourceName,
		&group.SourceURL,
		&authoritativeSourceName,
		&authoritativeSourceURL,
		&authoritativeSourceEventKey,
		&group.Status,
		&group.Notes,
		&createdAt,
		&updatedAt,
		&group.LatestImportRunID,
		&sharedVenueSlug,
		&sharedVenueName,
		&sharedStartAt,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return review.Group{}, false, nil
	case err != nil:
		return review.Group{}, false, err
	}
	group.AuthoritativeSourceName = strings.TrimSpace(authoritativeSourceName.String)
	group.AuthoritativeSourceURL = strings.TrimSpace(authoritativeSourceURL.String)
	group.AuthoritativeSourceEventKey = strings.TrimSpace(authoritativeSourceEventKey.String)
	group.SharedVenueSlug = strings.TrimSpace(sharedVenueSlug)
	group.SharedVenueName = strings.TrimSpace(sharedVenueName)
	if sharedStartAt.Valid && strings.TrimSpace(sharedStartAt.String) != "" {
		parsedSharedStartAt, err := parseRFC3339UTC(sharedStartAt.String)
		if err != nil {
			if fallbackID == 0 {
				fallbackID = group.ID
			}
			return review.Group{}, false, fmt.Errorf("parse review group %d shared_start_at: %w", fallbackID, err)
		}
		group.SharedStartAt = &parsedSharedStartAt
	}
	parsedCreatedAt, err := parseRFC3339UTC(createdAt)
	if err != nil {
		if fallbackID == 0 {
			fallbackID = group.ID
		}
		return review.Group{}, false, fmt.Errorf("parse review group %d created_at: %w", fallbackID, err)
	}
	parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		if fallbackID == 0 {
			fallbackID = group.ID
		}
		return review.Group{}, false, fmt.Errorf("parse review group %d updated_at: %w", fallbackID, err)
	}
	group.CreatedAt = parsedCreatedAt
	group.UpdatedAt = parsedUpdatedAt
	return group, true, nil
}

func scanReviewGroupSummaryRow(scanner interface {
	Scan(...any) error
}) (review.GroupSummary, error) {
	var group review.GroupSummary
	var createdAt string
	var updatedAt string
	var authoritative int
	var sharedStartAt sql.NullString
	if err := scanner.Scan(
		&group.ID,
		&group.Title,
		&group.SourceName,
		&group.SourceURL,
		&group.Status,
		&group.Notes,
		&createdAt,
		&updatedAt,
		&group.CandidateCount,
		&group.DraftCount,
		&group.LatestImportRunID,
		&authoritative,
		&sharedStartAt,
		&group.SharedVenueSlug,
		&group.SharedVenueName,
	); err != nil {
		return review.GroupSummary{}, err
	}
	parsedCreatedAt, err := parseRFC3339UTC(createdAt)
	if err != nil {
		return review.GroupSummary{}, fmt.Errorf("parse review group %d created_at: %w", group.ID, err)
	}
	parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		return review.GroupSummary{}, fmt.Errorf("parse review group %d updated_at: %w", group.ID, err)
	}
	group.CreatedAt = parsedCreatedAt
	group.UpdatedAt = parsedUpdatedAt
	group.Authoritative = authoritative == 1
	group.SharedVenueSlug = strings.TrimSpace(group.SharedVenueSlug)
	group.SharedVenueName = strings.TrimSpace(group.SharedVenueName)
	if sharedStartAt.Valid && strings.TrimSpace(sharedStartAt.String) != "" {
		parsedSharedStartAt, err := parseRFC3339UTC(sharedStartAt.String)
		if err != nil {
			return review.GroupSummary{}, fmt.Errorf("parse review group %d shared_start_at: %w", group.ID, err)
		}
		group.SharedStartAt = &parsedSharedStartAt
	}
	return group, nil
}

type reviewGroupAuthoritativeLink struct {
	SourceName     string
	SourceURL      string
	SourceEventKey string
}

type reviewGroupAuthoritativeLinkInput struct {
	SourceName     string
	SourceURL      string
	SourceEventKey string
}

func reviewGroupAuthoritativeSource(group review.Group) (reviewGroupAuthoritativeLink, bool) {
	sourceName := strings.TrimSpace(group.AuthoritativeSourceName)
	sourceURL := strings.TrimSpace(group.AuthoritativeSourceURL)
	sourceEventKey := strings.TrimSpace(group.AuthoritativeSourceEventKey)
	if sourceName == "" || sourceURL == "" || sourceEventKey == "" {
		return reviewGroupAuthoritativeLink{}, false
	}
	return reviewGroupAuthoritativeLink{
		SourceName:     sourceName,
		SourceURL:      sourceURL,
		SourceEventKey: sourceEventKey,
	}, true
}

func backfillOpenReviewGroupsAuthoritativeLinks(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM review_groups
		WHERE status = ?
			AND (
				authoritative_source_name IS NULL
				OR authoritative_source_name = ''
				OR authoritative_source_url IS NULL
				OR authoritative_source_url = ''
				OR authoritative_source_event_key IS NULL
				OR authoritative_source_event_key = ''
			)
	`, review.StatusOpen)
	if err != nil {
		return err
	}
	defer rows.Close()

	var groupIDs []int64
	for rows.Next() {
		var groupID int64
		if err := rows.Scan(&groupID); err != nil {
			return err
		}
		groupIDs = append(groupIDs, groupID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	now := time.Now().UTC()
	for _, groupID := range groupIDs {
		group, ok, err := loadReviewGroup(ctx, tx, groupID)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		candidates, err := loadReviewCandidates(ctx, tx, groupID)
		if err != nil {
			return err
		}
		link, ok := deriveReviewGroupAuthoritativeLink(group, candidates)
		if !ok {
			continue
		}
		if err := refreshReviewGroupAuthoritativeLinkTx(ctx, tx, groupID, reviewGroupAuthoritativeLinkInput(link), now); err != nil {
			return err
		}
	}
	return nil
}

func refreshReviewGroupAuthoritativeLinkTx(ctx context.Context, tx execer, groupID int64, input reviewGroupAuthoritativeLinkInput, now time.Time) error {
	link, ok := normalizeReviewGroupAuthoritativeLinkInput(input)
	if !ok {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE review_groups
		SET authoritative_source_name = ?,
			authoritative_source_url = ?,
			authoritative_source_event_key = ?,
			updated_at = CASE
				WHEN COALESCE(authoritative_source_name, '') <> ?
					OR COALESCE(authoritative_source_url, '') <> ?
					OR COALESCE(authoritative_source_event_key, '') <> ?
				THEN ?
				ELSE updated_at
			END
		WHERE id = ?
	`, link.SourceName, link.SourceURL, link.SourceEventKey,
		link.SourceName, link.SourceURL, link.SourceEventKey,
		formatRFC3339UTC(now), groupID)
	return err
}

func normalizeReviewGroupAuthoritativeLinkInput(input reviewGroupAuthoritativeLinkInput) (reviewGroupAuthoritativeLink, bool) {
	link := reviewGroupAuthoritativeLink{
		SourceName:     strings.TrimSpace(input.SourceName),
		SourceURL:      strings.TrimSpace(input.SourceURL),
		SourceEventKey: strings.TrimSpace(input.SourceEventKey),
	}
	if link.SourceName == "" || link.SourceURL == "" || link.SourceEventKey == "" {
		return reviewGroupAuthoritativeLink{}, false
	}
	return link, true
}

func deriveReviewGroupAuthoritativeLink(group review.Group, candidates []review.Candidate) (reviewGroupAuthoritativeLink, bool) {
	if len(candidates) == 0 {
		return reviewGroupAuthoritativeLink{}, false
	}

	var venueSlug string
	var link reviewGroupAuthoritativeLink
	for _, candidate := range candidates {
		candidateVenueSlug := strings.TrimSpace(candidate.VenueSlug)
		if candidateVenueSlug == "" {
			return reviewGroupAuthoritativeLink{}, false
		}
		if venueSlug == "" {
			venueSlug = candidateVenueSlug
		} else if candidateVenueSlug != venueSlug {
			return reviewGroupAuthoritativeLink{}, false
		}

		candidateLink, ok := authoritativeLinkFromStoredReviewCandidate(group, candidate)
		if !ok {
			return reviewGroupAuthoritativeLink{}, false
		}
		if link.SourceEventKey == "" {
			link = candidateLink
			continue
		}
		if candidateLink != link {
			return reviewGroupAuthoritativeLink{}, false
		}
	}
	if ingest.OwnedVenueSlugForReviewStageSourceName(link.SourceName) != venueSlug {
		return reviewGroupAuthoritativeLink{}, false
	}
	return link, true
}

func authoritativeLinkFromStoredReviewCandidate(group review.Group, candidate review.Candidate) (reviewGroupAuthoritativeLink, bool) {
	sourceName := strings.TrimSpace(candidate.SourceName)
	if sourceName == "" {
		sourceName = strings.TrimSpace(group.SourceName)
	}
	sourceURL := strings.TrimSpace(candidate.SourceURL)
	if sourceURL == "" {
		sourceURL = strings.TrimSpace(group.SourceURL)
	}
	sourceEventKey := strings.TrimSpace(candidate.ExternalID)
	if sourceEventKey == "" {
		sourceEventKey = sourceURL
	}
	if sourceName == "" || sourceURL == "" || sourceEventKey == "" {
		return reviewGroupAuthoritativeLink{}, false
	}
	return reviewGroupAuthoritativeLink{
		SourceName:     sourceName,
		SourceURL:      sourceURL,
		SourceEventKey: sourceEventKey,
	}, true
}

func upsertEventSecondarySourceInfoTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, authoritative reviewGroupAuthoritativeLink, candidates []review.Candidate, now time.Time) error {
	if eventID <= 0 {
		return errors.New("secondary source info event ID is required")
	}
	rows := make([]eventSecondarySourceInfoRow, 0, len(candidates)*2)
	for _, candidate := range candidates {
		sourceName := strings.TrimSpace(candidate.SourceName)
		sourceURL := strings.TrimSpace(candidate.SourceURL)
		if sourceName == "" || sourceURL == "" {
			continue
		}
		if sourceName == authoritative.SourceName && sourceURL == authoritative.SourceURL {
			continue
		}

		sourceID, err := ensureSourceTx(ctx, tx, sourceName, sourceURL)
		if err != nil {
			return err
		}
		for _, item := range []struct {
			infoType string
			value    string
		}{
			{infoType: "genre", value: strings.TrimSpace(candidate.Genre)},
			{infoType: "description", value: strings.TrimSpace(candidate.Description)},
		} {
			if item.value == "" {
				continue
			}
			rows = append(rows, eventSecondarySourceInfoRow{
				EventID:    eventID,
				SourceID:   sourceID,
				VenueSlug:  strings.TrimSpace(candidate.VenueSlug),
				EventName:  strings.TrimSpace(candidate.Name),
				StartAt:    strings.TrimSpace(candidate.StartAt),
				InfoType:   item.infoType,
				Value:      item.value,
				RecordedAt: now,
			})
		}
	}
	if err := deleteEventSecondarySourceInfoForEventTx(ctx, tx, eventID); err != nil {
		return err
	}
	for _, row := range rows {
		if err := upsertEventSecondarySourceInfoRowTx(ctx, tx, row); err != nil {
			return err
		}
	}
	return nil
}

type eventSecondarySourceInfoRow struct {
	EventID    int64
	SourceID   int64
	VenueSlug  string
	EventName  string
	StartAt    string
	InfoType   string
	Value      string
	RecordedAt time.Time
}

func upsertEventSecondarySourceInfoRowTx(ctx context.Context, tx execer, row eventSecondarySourceInfoRow) error {
	if row.EventID <= 0 {
		return errors.New("secondary source info event ID is required")
	}
	if row.SourceID <= 0 {
		return errors.New("secondary source info source ID is required")
	}
	if row.VenueSlug == "" || row.EventName == "" || row.StartAt == "" || row.InfoType == "" || row.Value == "" {
		return errors.New("secondary source info row is incomplete")
	}
	recordedAt := formatRFC3339UTC(row.RecordedAt)
	_, err := tx.ExecContext(ctx, `
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
		ON CONFLICT(source_id, venue_slug, event_name, start_at, info_type) DO UPDATE SET
			event_id = excluded.event_id,
			value = excluded.value,
			updated_at = excluded.updated_at
	`, row.EventID, row.SourceID, row.VenueSlug, row.EventName, row.StartAt, row.InfoType, row.Value, recordedAt, recordedAt)
	return err
}

func deleteEventSecondarySourceInfoForEventTx(ctx context.Context, tx execer, eventID int64) error {
	if eventID <= 0 {
		return errors.New("secondary source info event ID is required")
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM event_secondary_source_info
		WHERE event_id = ?
	`, eventID)
	return err
}

func buildResolvedEvent(group review.Group, selected map[review.Field]review.Candidate, publishedAt time.Time) (domain.Event, error) {
	name := strings.TrimSpace(review.CandidateValue(selected[review.FieldName], review.FieldName))
	venueSlug := strings.TrimSpace(review.CandidateValue(selected[review.FieldVenueSlug], review.FieldVenueSlug))
	startText := strings.TrimSpace(review.CandidateValue(selected[review.FieldStartAt], review.FieldStartAt))
	endText := strings.TrimSpace(review.CandidateValue(selected[review.FieldEndAt], review.FieldEndAt))
	genre := strings.TrimSpace(review.CandidateValue(selected[review.FieldGenre], review.FieldGenre))
	status := strings.TrimSpace(review.CandidateValue(selected[review.FieldStatus], review.FieldStatus))
	description := strings.TrimSpace(review.CandidateValue(selected[review.FieldDescription], review.FieldDescription))
	sourceName := strings.TrimSpace(review.CandidateValue(selected[review.FieldSourceName], review.FieldSourceName))
	if sourceName == "" {
		sourceName = strings.TrimSpace(group.SourceName)
	}
	sourceURL := strings.TrimSpace(review.CandidateValue(selected[review.FieldSourceURL], review.FieldSourceURL))
	if sourceURL == "" {
		sourceURL = strings.TrimSpace(group.SourceURL)
	}

	if name == "" {
		return domain.Event{}, errors.New("review event name is required")
	}
	if venueSlug == "" {
		return domain.Event{}, errors.New("review event venue slug is required")
	}
	if startText == "" {
		return domain.Event{}, errors.New("review event start time is required")
	}
	if sourceName == "" {
		return domain.Event{}, errors.New("review event source name is required")
	}
	if sourceURL == "" {
		return domain.Event{}, errors.New("review event source URL is required")
	}

	start, err := parseRFC3339UTC(startText)
	if err != nil {
		return domain.Event{}, fmt.Errorf("parse review event start time: %w", err)
	}
	end := time.Time{}
	if endText != "" {
		end, err = parseRFC3339UTC(endText)
		if err != nil {
			return domain.Event{}, fmt.Errorf("parse review event end time: %w", err)
		}
	}
	slug, err := buildLiveEventSlug(name, venueSlug, start)
	if err != nil {
		return domain.Event{}, err
	}

	event := domain.Event{
		Slug:        slug,
		Name:        name,
		VenueSlug:   venueSlug,
		Start:       start,
		End:         end,
		Genre:       genre,
		Status:      status,
		Description: description,
		SourceName:  sourceName,
		SourceURL:   sourceURL,
		LastChecked: publishedAt.UTC(),
		Origin:      domain.OriginLive,
	}
	if err := event.ValidateCanonical(); err != nil {
		return domain.Event{}, fmt.Errorf("review event %w", err)
	}
	return event, nil
}

func buildLiveEventSlug(name, venueSlug string, start time.Time) (string, error) {
	nameSlug := slugFromText(name)
	venueSlugPart := slugFromText(venueSlug)
	if nameSlug == "" {
		return "", errors.New("review event name cannot produce a slug")
	}
	if venueSlugPart == "" {
		return "", errors.New("review event venue slug cannot produce a slug")
	}
	return fmt.Sprintf("live-%s-%s-%s", nameSlug, venueSlugPart, start.UTC().Format("20060102150405")), nil
}

func slugFromText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	wroteDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
			wroteDash = false
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
			wroteDash = false
		default:
			if builder.Len() > 0 && !wroteDash {
				builder.WriteByte('-')
				wroteDash = true
			}
		}
	}
	return strings.Trim(builder.String(), "-")
}

func upsertEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, event domain.Event) error {
	venueID, ok, err := loadVenueIDBySlugTx(ctx, tx, event.VenueSlug)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("venue %q not found", event.VenueSlug)
	}
	sourceID, err := ensureSourceTx(ctx, tx, event.SourceName, event.SourceURL)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
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
		ON CONFLICT(slug) DO UPDATE SET
			venue_id = excluded.venue_id,
			source_id = excluded.source_id,
			name = excluded.name,
			start_at = excluded.start_at,
			end_at = excluded.end_at,
			genre = excluded.genre,
			status = excluded.status,
			description = excluded.description,
			last_checked_at = excluded.last_checked_at,
			origin = excluded.origin
	`, event.Slug, venueID, sourceID, event.Name,
		formatRFC3339UTC(event.Start),
		nullableRFC3339UTC(event.End),
		event.Genre, event.Status, event.Description,
		formatRFC3339UTC(event.LastChecked),
		string(event.Origin))
	return err
}

func loadVenueIDBySlugTx(ctx context.Context, q queryer, slug string) (int64, bool, error) {
	row := q.QueryRowContext(ctx, `
		SELECT id
		FROM venues
		WHERE slug = ?
		LIMIT 1
	`, slug)
	var id int64
	switch err := row.Scan(&id); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	return id, true, nil
}

func reviewGroupExists(ctx context.Context, q queryer, id int64) (bool, error) {
	_, ok, err := loadReviewGroup(ctx, q, id)
	return ok, err
}

func loadReviewCandidates(ctx context.Context, q queryer, groupID int64) ([]review.Candidate, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			id,
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
		FROM review_candidates
		WHERE group_id = ?
		ORDER BY position, id
	`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []review.Candidate
	for rows.Next() {
		candidate, err := scanReviewCandidate(rows)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func loadReviewCandidate(ctx context.Context, q queryer, groupID, candidateID int64) (review.Candidate, bool, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			id,
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
		FROM review_candidates
		WHERE group_id = ? AND id = ?
		LIMIT 1
	`, groupID, candidateID)
	if err != nil {
		return review.Candidate{}, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return review.Candidate{}, false, err
		}
		return review.Candidate{}, false, nil
	}
	candidate, err := scanReviewCandidate(rows)
	if err != nil {
		return review.Candidate{}, false, err
	}
	if err := rows.Err(); err != nil {
		return review.Candidate{}, false, err
	}
	return candidate, true, nil
}

func scanReviewCandidate(rows *sql.Rows) (review.Candidate, error) {
	var candidate review.Candidate
	if err := rows.Scan(
		&candidate.ID,
		&candidate.GroupID,
		&candidate.Position,
		&candidate.ExternalID,
		&candidate.Name,
		&candidate.VenueSlug,
		&candidate.StartAt,
		&candidate.EndAt,
		&candidate.Genre,
		&candidate.Status,
		&candidate.Description,
		&candidate.SourceName,
		&candidate.SourceURL,
		&candidate.Provenance,
	); err != nil {
		return review.Candidate{}, err
	}
	return candidate, nil
}

func loadReviewDraftChoices(ctx context.Context, q queryer, groupID int64) (map[review.Field]review.DraftChoice, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT field, candidate_id, value, updated_at
		FROM review_draft_choices
		WHERE group_id = ?
		ORDER BY field
	`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	choices := make(map[review.Field]review.DraftChoice)
	for rows.Next() {
		var choice review.DraftChoice
		var field string
		var updatedAt string
		if err := rows.Scan(&field, &choice.CandidateID, &choice.Value, &updatedAt); err != nil {
			return nil, err
		}
		parsedField, ok := review.ParseField(field)
		if !ok {
			return nil, fmt.Errorf("invalid stored review field %q", field)
		}
		parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse review choice %q updated_at: %w", field, err)
		}
		choice.Field = parsedField
		choice.UpdatedAt = parsedUpdatedAt
		choices[parsedField] = choice
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return choices, nil
}
