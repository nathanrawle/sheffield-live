package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/domain"
	"sheffield-live/internal/eventidentity"
	"sheffield-live/internal/ingest"
)

const exactIdentityKeyVersion = eventidentity.ExactKeyVersion

type exactIdentityMaterial struct {
	venueSlug  string
	start      time.Time
	cleanTitle string
	material   string
}

type exactIdentityRow struct {
	id          int64
	identityKey string
}

func exactIdentityMaterialForEvent(event domain.Event) (exactIdentityMaterial, bool, error) {
	if event.Origin != domain.OriginLive {
		return exactIdentityMaterial{}, false, nil
	}
	if normalizedPublicationState(event.PublicationState) == domain.PublicationStateWithheld {
		return exactIdentityMaterial{}, false, nil
	}

	venueSlug := strings.TrimSpace(event.VenueSlug)
	if venueSlug == "" {
		return exactIdentityMaterial{}, false, fmt.Errorf("live event %q has empty venue slug", event.Slug)
	}
	if event.Start.IsZero() {
		return exactIdentityMaterial{}, false, fmt.Errorf("live event %q has empty start time", event.Slug)
	}

	cleanTitle := normalizeExactIdentityCleanTitle(ingest.CleanEventTitleForVenue(event.Name, venueSlug))
	if cleanTitle == "" {
		return exactIdentityMaterial{}, false, nil
	}

	material := exactIdentityMaterialText(exactIdentityKeyVersion, venueSlug, event.Start.UTC(), cleanTitle)
	return exactIdentityMaterial{
		venueSlug:  venueSlug,
		start:      event.Start.UTC(),
		cleanTitle: cleanTitle,
		material:   material,
	}, true, nil
}

func normalizeExactIdentityCleanTitle(title string) string {
	return eventidentity.NormalizeCleanTitle(title)
}

func exactIdentityMaterialText(version int, venueSlug string, start time.Time, cleanTitle string) string {
	return eventidentity.MaterialText(version, venueSlug, start, cleanTitle)
}

func buildExactIdentityKey(version int, venueSlug string, start time.Time, cleanTitle string) string {
	return eventidentity.BuildKey(version, venueSlug, start, cleanTitle)
}

func ensureActiveExactIdentityTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, event domain.Event, repairRunID int64, now time.Time) error {
	return syncActiveExactIdentityTx(ctx, tx, eventID, event, "exact identity refreshed", repairRunID, now)
}

func replaceActiveExactIdentityForLiveEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, event domain.Event, reason string, repairRunID int64, now time.Time) error {
	return syncActiveExactIdentityTx(ctx, tx, eventID, event, reason, repairRunID, now)
}

func exactIdentityDeactivationReasonForEvent(event domain.Event) string {
	if normalizedPublicationState(event.PublicationState) == domain.PublicationStateWithheld {
		return "event is withheld"
	}
	if event.Origin != domain.OriginLive {
		return "event is not live"
	}
	return ""
}

func deactivateActiveExactIdentitiesForEventTx(ctx context.Context, tx interface {
	execer
}, eventID int64, reason string, repairRunID int64, now time.Time) error {
	if eventID <= 0 {
		return errors.New("event ID is required")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return errors.New("deactivation reason is required")
	}

	nowText := formatRFC3339UTC(now.UTC())
	_, err := tx.ExecContext(ctx, `
		UPDATE event_exact_identities
		SET active = 0,
			updated_at = ?,
			deactivated_at = ?,
			deactivated_reason = ?,
			repair_run_id = CASE WHEN ? > 0 THEN ? ELSE repair_run_id END
		WHERE event_id = ?
			AND active = 1
	`, nowText, nowText, reason, repairRunID, repairRunID, eventID)
	return err
}

type activeExactIdentityRow struct {
	id      int64
	eventID int64
}

func loadActiveExactIdentityRowByKeyTx(ctx context.Context, q queryer, identityKey string) (activeExactIdentityRow, bool, error) {
	if strings.TrimSpace(identityKey) == "" {
		return activeExactIdentityRow{}, false, errors.New("exact identity key is required")
	}
	row := activeExactIdentityRow{}
	err := q.QueryRowContext(ctx, `
		SELECT id, event_id
		FROM event_exact_identities
		WHERE identity_key = ?
			AND active = 1
		LIMIT 1
	`, identityKey).Scan(&row.id, &row.eventID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return activeExactIdentityRow{}, false, nil
	case err != nil:
		return activeExactIdentityRow{}, false, err
	default:
		return row, true, nil
	}
}

func deactivateExactIdentityRowTx(ctx context.Context, tx execer, rowID int64, reason string, repairRunID int64, now time.Time) error {
	if rowID <= 0 {
		return errors.New("exact identity row ID is required")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return errors.New("deactivation reason is required")
	}
	nowText := formatRFC3339UTC(now.UTC())
	_, err := tx.ExecContext(ctx, `
		UPDATE event_exact_identities
		SET active = 0,
			updated_at = ?,
			deactivated_at = ?,
			deactivated_reason = ?,
			repair_run_id = CASE WHEN ? > 0 THEN ? ELSE repair_run_id END
		WHERE id = ?
	`, nowText, nowText, reason, repairRunID, repairRunID, rowID)
	return err
}

func ensureActiveExactIdentityWithConflictTransferTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, event domain.Event, repairRunID int64, now time.Time) error {
	material, ok, err := exactIdentityMaterialForEvent(event)
	if err != nil {
		return err
	}
	if !ok {
		return ensureActiveExactIdentityTx(ctx, tx, eventID, event, repairRunID, now)
	}

	key := buildExactIdentityKey(exactIdentityKeyVersion, material.venueSlug, material.start, material.cleanTitle)
	conflict, found, err := loadActiveExactIdentityRowByKeyTx(ctx, tx, key)
	if err != nil {
		return err
	}
	if found && conflict.eventID != eventID {
		if err := deactivateExactIdentityRowTx(ctx, tx, conflict.id, fmt.Sprintf("transferred to event %d", eventID), repairRunID, now); err != nil {
			return err
		}
	}
	return ensureActiveExactIdentityTx(ctx, tx, eventID, event, repairRunID, now)
}

func loadLiveEventRecordByExactIdentityKeyTx(ctx context.Context, q queryer, identityKey string) (eventRecord, bool, error) {
	if strings.TrimSpace(identityKey) == "" {
		return eventRecord{}, false, errors.New("exact identity key is required")
	}
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
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM event_exact_identities i
		JOIN events e ON e.id = i.event_id
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE i.identity_key = ?
			AND i.active = 1
			AND e.origin = ?
			AND TRIM(COALESCE(e.publication_state, '')) <> ?
		LIMIT 1
	`, identityKey, string(domain.OriginLive), string(domain.PublicationStateWithheld))
}

func matchLiveEventsByExactIdentityTx(ctx context.Context, q queryer, event domain.Event) ([]eventRecord, bool, error) {
	material, ok, err := exactIdentityMaterialForEvent(event)
	if err != nil || !ok {
		return nil, false, err
	}

	key := buildExactIdentityKey(exactIdentityKeyVersion, material.venueSlug, material.start, material.cleanTitle)
	record, found, err := loadLiveEventRecordByExactIdentityKeyTx(ctx, q, key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, true, nil
	}
	return []eventRecord{record}, true, nil
}

func matchLiveEventsByGuardedNearIdentityTx(ctx context.Context, q queryer, event domain.Event, window time.Duration) ([]eventRecord, bool, error) {
	material, ok, err := exactIdentityMaterialForEvent(event)
	if err != nil || !ok {
		return nil, false, err
	}
	if window <= 0 {
		window = 75 * time.Minute
	}

	records, err := loadLiveEventRecordsByVenueAndStartWindowTx(ctx, q, material.venueSlug, material.start.Add(-window), material.start.Add(window))
	if err != nil {
		return nil, false, err
	}

	matched := make([]eventRecord, 0, len(records))
	for _, record := range records {
		if normalizeExactIdentityCleanTitle(ingest.CleanEventTitleForVenue(record.Event.Name, material.venueSlug)) != material.cleanTitle {
			continue
		}
		matched = append(matched, record)
	}
	if len(matched) == 0 {
		return nil, true, nil
	}
	return matched, true, nil
}

func loadEventRecordByIDTx(ctx context.Context, q queryer, eventID int64) (eventRecord, bool, error) {
	if eventID <= 0 {
		return eventRecord{}, false, errors.New("event ID is required")
	}
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
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE e.id = ?
		LIMIT 1
	`, eventID)
}

func loadLiveEventRecordsByVenueAndStartWindowTx(ctx context.Context, q queryer, venueSlug string, startMin, startMax time.Time) ([]eventRecord, error) {
	return loadEventRecords(ctx, q, `
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
			e.image_url,
			e.image_source_url,
			e.image_alt,
			e.image_width,
			e.image_height,
			e.image_focus_x,
			e.image_focus_y,
			s.name,
			s.url,
			COALESCE(e.official_listing_url, ''),
			COALESCE(e.calendar_url, ''),
			e.last_checked_at,
			e.origin,
			e.publication_state
		FROM events e
		JOIN venues v ON v.id = e.venue_id
		JOIN sources s ON s.id = e.source_id
		WHERE v.slug = ?
		  AND e.start_at >= ?
		  AND e.start_at <= ?
		  AND e.origin = ?
		  AND TRIM(COALESCE(e.publication_state, '')) <> ?
		ORDER BY e.start_at, e.slug
	`, strings.TrimSpace(venueSlug), formatRFC3339UTC(startMin), formatRFC3339UTC(startMax), string(domain.OriginLive), string(domain.PublicationStateWithheld))
}

func loadActiveExactIdentityRowsByEventTx(ctx context.Context, q queryer, eventID int64) ([]exactIdentityRow, error) {
	if eventID <= 0 {
		return nil, errors.New("event ID is required")
	}

	rows, err := q.QueryContext(ctx, `
		SELECT id, identity_key
		FROM event_exact_identities
		WHERE event_id = ?
			AND active = 1
		ORDER BY id
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []exactIdentityRow
	for rows.Next() {
		var row exactIdentityRow
		if err := rows.Scan(&row.id, &row.identityKey); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func syncActiveExactIdentityTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, event domain.Event, reason string, repairRunID int64, now time.Time) error {
	if eventID <= 0 {
		return errors.New("event ID is required")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "exact identity refreshed"
	}

	material, ok, err := exactIdentityMaterialForEvent(event)
	if err != nil {
		return err
	}
	if !ok {
		if stateReason := exactIdentityDeactivationReasonForEvent(event); stateReason != "" {
			reason = stateReason
		}
		return deactivateActiveExactIdentitiesForEventTx(ctx, tx, eventID, reason, repairRunID, now)
	}

	key := buildExactIdentityKey(exactIdentityKeyVersion, material.venueSlug, material.start, material.cleanTitle)
	activeRows, err := loadActiveExactIdentityRowsByEventTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if len(activeRows) == 1 && activeRows[0].identityKey == key {
		return nil
	}

	if err := deactivateActiveExactIdentitiesForEventTx(ctx, tx, eventID, reason, repairRunID, now); err != nil {
		return err
	}
	return insertExactIdentityTx(ctx, tx, eventID, material, repairRunID, now)
}

func insertExactIdentityTx(ctx context.Context, tx execer, eventID int64, material exactIdentityMaterial, repairRunID int64, now time.Time) error {
	if eventID <= 0 {
		return errors.New("event ID is required")
	}

	nowText := formatRFC3339UTC(now.UTC())
	_, err := tx.ExecContext(ctx, `
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
		) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, NULL, '', ?)
	`, eventID, buildExactIdentityKey(exactIdentityKeyVersion, material.venueSlug, material.start, material.cleanTitle), exactIdentityKeyVersion, material.venueSlug, formatRFC3339UTC(material.start), material.cleanTitle, nowText, nowText, nullableRepairRunID(repairRunID))
	return err
}

func nullableRepairRunID(repairRunID int64) any {
	if repairRunID > 0 {
		return repairRunID
	}
	return nil
}
