package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/ingest"
	seedstore "sheffield-live/internal/store"
)

const (
	secondarySourceObservationCompatibilityNotes   = "unit3: secondary source observation compatibility backfill"
	secondarySourceObservationCompatibilityOutcome = "secondary_source_info_compat"
)

func backfillSecondarySourceObservationsCompatibilityTx(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			i.event_id,
			i.source_id,
			s.url,
			i.info_type,
			i.value,
			i.created_at,
			i.updated_at
		FROM event_secondary_source_info i
		JOIN events e ON e.id = i.event_id
		JOIN sources s ON s.id = i.source_id
		ORDER BY i.id, i.event_id, i.source_id, i.info_type, i.value
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return rows.Err()
	}

	scope, err := compatibilitySecondarySourceObservationRunScopeTx(ctx, tx)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	for {
		var eventID int64
		var sourceID int64
		var sourceURL string
		var infoType string
		var value string
		var createdAtText sql.NullString
		var updatedAtText sql.NullString
		if err := rows.Scan(&eventID, &sourceID, &sourceURL, &infoType, &value, &createdAtText, &updatedAtText); err != nil {
			return err
		}

		fieldName := strings.TrimSpace(infoType)
		switch fieldName {
		case "genre", "description":
		default:
			return fmt.Errorf("secondary source info row has unsupported type %q", infoType)
		}

		sourceIdentityKey := ingest.SourceIdentities(ingest.SourceIdentityInput{SourceURL: sourceURL}).PrimaryKey()
		if sourceIdentityKey == "" {
			if !rows.Next() {
				break
			}
			continue
		}
		createdAt, updatedAt := observationCompatibilityTimestamps(createdAtText, updatedAtText, now)
		if err := upsertSourceAttributeObservationRowTx(ctx, tx, seedstore.SourceAttributeObservationInput{
			RunScope:           scope,
			SourceID:           sourceID,
			SourceIdentityKey:  sourceIdentityKey,
			SourceAuthority:    seedstore.SourceAuthoritySupporting,
			TargetKind:         seedstore.ObservationTargetKindEvent,
			EventID:            int64Ptr(eventID),
			FieldName:          fieldName,
			IncomingRaw:        strings.TrimSpace(value),
			IncomingNormalized: strings.TrimSpace(value),
			Outcome:            secondarySourceObservationCompatibilityOutcome,
			IsConflict:         false,
		}, createdAt, updatedAt); err != nil {
			return err
		}
		if !rows.Next() {
			break
		}
	}
	return rows.Err()
}

func compatibilitySecondarySourceObservationRunScopeTx(ctx context.Context, tx interface {
	execer
	queryer
}) (seedstore.ObservationRunScope, error) {
	repairRunID, err := ensureRepairRunByNotesTx(ctx, tx, secondarySourceObservationCompatibilityNotes, time.Now().UTC())
	if err != nil {
		return "", err
	}
	scope, err := seedstore.NewObservationRunScopeRepair(repairRunID)
	if err != nil {
		return "", err
	}
	return scope, nil
}

func ensureRepairRunByNotesTx(ctx context.Context, tx interface {
	execer
	queryer
}, notes string, now time.Time) (int64, error) {
	notes = strings.TrimSpace(notes)
	if notes == "" {
		return 0, errors.New("repair run notes are required")
	}

	var repairRunID int64
	if err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM repair_runs
		WHERE notes = ?
		ORDER BY id DESC
		LIMIT 1
	`, notes).Scan(&repairRunID); err == nil {
		return repairRunID, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO repair_runs (
			started_at,
			finished_at,
			status,
			notes
		) VALUES (?, ?, ?, ?)
	`, formatRFC3339UTC(now), formatRFC3339UTC(now), "succeeded", notes)
	if err != nil {
		return 0, err
	}
	repairRunID, err = res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return repairRunID, nil
}

func upsertSourceAttributeObservationTx(ctx context.Context, tx interface {
	execer
	queryer
}, input seedstore.SourceAttributeObservationInput) error {
	return upsertSourceAttributeObservationRowTx(ctx, tx, input, time.Now().UTC(), time.Now().UTC())
}

func upsertSourceAttributeObservationRowTx(ctx context.Context, tx execer, input seedstore.SourceAttributeObservationInput, createdAt, updatedAt time.Time) error {
	cleaned, err := normalizeSourceAttributeObservationInput(input)
	if err != nil {
		return err
	}

	createdAtText := formatRFC3339UTC(createdAt)
	updatedAtText := formatRFC3339UTC(updatedAt)
	switch cleaned.TargetKind {
	case seedstore.ObservationTargetKindEvent:
		_, err = tx.ExecContext(ctx, `
			INSERT INTO event_source_attribute_observations (
				run_scope,
				source_id,
				source_identity_key,
				source_authority,
				target_kind,
				event_id,
				review_group_id,
				event_review_cluster_id,
				field_name,
				incoming_raw,
				incoming_normalized,
				canonical_before_raw,
				canonical_before_normalized,
				outcome,
				is_conflict,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(run_scope, event_id, source_id, source_identity_key, field_name) WHERE target_kind = 'event' DO UPDATE SET
				source_id = excluded.source_id,
				source_authority = excluded.source_authority,
				incoming_raw = excluded.incoming_raw,
				incoming_normalized = excluded.incoming_normalized,
				canonical_before_raw = excluded.canonical_before_raw,
				canonical_before_normalized = excluded.canonical_before_normalized,
				outcome = excluded.outcome,
				is_conflict = excluded.is_conflict,
				updated_at = excluded.updated_at
		`, cleaned.RunScope, cleaned.SourceID, cleaned.SourceIdentityKey, string(cleaned.SourceAuthority), string(cleaned.TargetKind), cleaned.EventID, cleaned.FieldName, cleaned.IncomingRaw, cleaned.IncomingNormalized, cleaned.CanonicalBeforeRaw, cleaned.CanonicalBeforeNormalized, cleaned.Outcome, boolInt(cleaned.IsConflict), createdAtText, updatedAtText)
	case seedstore.ObservationTargetKindReviewGroup:
		_, err = tx.ExecContext(ctx, `
			INSERT INTO event_source_attribute_observations (
				run_scope,
				source_id,
				source_identity_key,
				source_authority,
				target_kind,
				event_id,
				review_group_id,
				event_review_cluster_id,
				field_name,
				incoming_raw,
				incoming_normalized,
				canonical_before_raw,
				canonical_before_normalized,
				outcome,
				is_conflict,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(run_scope, review_group_id, source_id, source_identity_key, field_name) WHERE target_kind = 'review_group' DO UPDATE SET
				source_id = excluded.source_id,
				source_authority = excluded.source_authority,
				incoming_raw = excluded.incoming_raw,
				incoming_normalized = excluded.incoming_normalized,
				canonical_before_raw = excluded.canonical_before_raw,
				canonical_before_normalized = excluded.canonical_before_normalized,
				outcome = excluded.outcome,
				is_conflict = excluded.is_conflict,
				updated_at = excluded.updated_at
		`, cleaned.RunScope, cleaned.SourceID, cleaned.SourceIdentityKey, string(cleaned.SourceAuthority), string(cleaned.TargetKind), cleaned.ReviewGroupID, cleaned.FieldName, cleaned.IncomingRaw, cleaned.IncomingNormalized, cleaned.CanonicalBeforeRaw, cleaned.CanonicalBeforeNormalized, cleaned.Outcome, boolInt(cleaned.IsConflict), createdAtText, updatedAtText)
	case seedstore.ObservationTargetKindEventReviewCluster:
		_, err = tx.ExecContext(ctx, `
			INSERT INTO event_source_attribute_observations (
				run_scope,
				source_id,
				source_identity_key,
				source_authority,
				target_kind,
				event_id,
				review_group_id,
				event_review_cluster_id,
				field_name,
				incoming_raw,
				incoming_normalized,
				canonical_before_raw,
				canonical_before_normalized,
				outcome,
				is_conflict,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(run_scope, event_review_cluster_id, source_id, source_identity_key, field_name) WHERE target_kind = 'event_review_cluster' DO UPDATE SET
				source_id = excluded.source_id,
				source_authority = excluded.source_authority,
				incoming_raw = excluded.incoming_raw,
				incoming_normalized = excluded.incoming_normalized,
				canonical_before_raw = excluded.canonical_before_raw,
				canonical_before_normalized = excluded.canonical_before_normalized,
				outcome = excluded.outcome,
				is_conflict = excluded.is_conflict,
				updated_at = excluded.updated_at
		`, cleaned.RunScope, cleaned.SourceID, cleaned.SourceIdentityKey, string(cleaned.SourceAuthority), string(cleaned.TargetKind), cleaned.EventReviewClusterID, cleaned.FieldName, cleaned.IncomingRaw, cleaned.IncomingNormalized, cleaned.CanonicalBeforeRaw, cleaned.CanonicalBeforeNormalized, cleaned.Outcome, boolInt(cleaned.IsConflict), createdAtText, updatedAtText)
	default:
		return fmt.Errorf("unsupported observation target kind %q", cleaned.TargetKind)
	}
	return err
}

func normalizeSourceAttributeObservationInput(input seedstore.SourceAttributeObservationInput) (seedstore.SourceAttributeObservationInput, error) {
	cleaned := seedstore.SourceAttributeObservationInput{
		RunScope:                  seedstore.ObservationRunScope(strings.TrimSpace(input.RunScope.String())),
		SourceID:                  input.SourceID,
		SourceIdentityKey:         strings.TrimSpace(input.SourceIdentityKey),
		SourceAuthority:           seedstore.SourceAuthority(strings.TrimSpace(string(input.SourceAuthority))),
		TargetKind:                seedstore.ObservationTargetKind(strings.TrimSpace(string(input.TargetKind))),
		EventID:                   input.EventID,
		ReviewGroupID:             input.ReviewGroupID,
		EventReviewClusterID:      input.EventReviewClusterID,
		FieldName:                 strings.TrimSpace(input.FieldName),
		IncomingRaw:               strings.TrimSpace(input.IncomingRaw),
		IncomingNormalized:        strings.TrimSpace(input.IncomingNormalized),
		CanonicalBeforeRaw:        strings.TrimSpace(input.CanonicalBeforeRaw),
		CanonicalBeforeNormalized: strings.TrimSpace(input.CanonicalBeforeNormalized),
		Outcome:                   strings.TrimSpace(input.Outcome),
		IsConflict:                input.IsConflict,
	}
	if cleaned.RunScope == "" {
		return seedstore.SourceAttributeObservationInput{}, errors.New("observation run scope is required")
	}
	if _, _, err := seedstore.ParseObservationRunScope(cleaned.RunScope); err != nil {
		return seedstore.SourceAttributeObservationInput{}, err
	}
	if cleaned.SourceID <= 0 {
		return seedstore.SourceAttributeObservationInput{}, errors.New("observation source ID is required")
	}
	if cleaned.SourceIdentityKey == "" {
		return seedstore.SourceAttributeObservationInput{}, errors.New("observation source identity key is required")
	}
	switch cleaned.SourceAuthority {
	case seedstore.SourceAuthorityAuthoritative, seedstore.SourceAuthoritySupporting:
	default:
		return seedstore.SourceAttributeObservationInput{}, fmt.Errorf("unsupported observation source authority %q", cleaned.SourceAuthority)
	}
	switch cleaned.TargetKind {
	case seedstore.ObservationTargetKindEvent:
		if cleaned.EventID == nil || *cleaned.EventID <= 0 {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation event ID is required")
		}
		if cleaned.ReviewGroupID != nil {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation review group ID must be nil for event targets")
		}
		if cleaned.EventReviewClusterID != nil {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation event review cluster ID must be nil for event targets")
		}
	case seedstore.ObservationTargetKindReviewGroup:
		if cleaned.ReviewGroupID == nil || *cleaned.ReviewGroupID <= 0 {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation review group ID is required")
		}
		if cleaned.EventID != nil {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation event ID must be nil for review group targets")
		}
		if cleaned.EventReviewClusterID != nil {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation event review cluster ID must be nil for review group targets")
		}
	case seedstore.ObservationTargetKindEventReviewCluster:
		if cleaned.EventReviewClusterID == nil || *cleaned.EventReviewClusterID <= 0 {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation event review cluster ID is required")
		}
		if cleaned.EventID != nil {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation event ID must be nil for event review cluster targets")
		}
		if cleaned.ReviewGroupID != nil {
			return seedstore.SourceAttributeObservationInput{}, errors.New("observation review group ID must be nil for event review cluster targets")
		}
	default:
		return seedstore.SourceAttributeObservationInput{}, fmt.Errorf("unsupported observation target kind %q", cleaned.TargetKind)
	}
	if cleaned.FieldName == "" {
		return seedstore.SourceAttributeObservationInput{}, errors.New("observation field name is required")
	}
	if cleaned.Outcome == "" {
		return seedstore.SourceAttributeObservationInput{}, errors.New("observation outcome is required")
	}
	return cleaned, nil
}

func deleteCompatibilityEventSourceObservationsForEventTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64) error {
	if eventID <= 0 {
		return errors.New("observation event ID is required")
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM event_source_attribute_observations
		WHERE target_kind = ?
			AND outcome = ?
			AND event_id = ?
	`, string(seedstore.ObservationTargetKindEvent), secondarySourceObservationCompatibilityOutcome, eventID)
	return err
}

func upsertCompatibilitySecondarySourceObservationTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID, sourceID int64, sourceIdentityKey, fieldName, value string, now time.Time) error {
	sourceIdentityKey = strings.TrimSpace(sourceIdentityKey)
	if sourceIdentityKey == "" {
		return nil
	}
	scope, err := compatibilitySecondarySourceObservationRunScopeTx(ctx, tx)
	if err != nil {
		return err
	}
	return upsertSourceAttributeObservationRowTx(ctx, tx, seedstore.SourceAttributeObservationInput{
		RunScope:           scope,
		SourceID:           sourceID,
		SourceIdentityKey:  sourceIdentityKey,
		SourceAuthority:    seedstore.SourceAuthoritySupporting,
		TargetKind:         seedstore.ObservationTargetKindEvent,
		EventID:            int64Ptr(eventID),
		FieldName:          strings.TrimSpace(fieldName),
		IncomingRaw:        strings.TrimSpace(value),
		IncomingNormalized: strings.TrimSpace(value),
		Outcome:            secondarySourceObservationCompatibilityOutcome,
		IsConflict:         false,
	}, now, now)
}

func rehomeEventSourceAttributeObservationsForDuplicateEventTx(ctx context.Context, tx execer, duplicateEventID, targetEventID int64, now time.Time) error {
	if duplicateEventID <= 0 {
		return errors.New("duplicate event ID is required")
	}
	if targetEventID <= 0 {
		return errors.New("target event ID is required")
	}
	if duplicateEventID == targetEventID {
		return nil
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM event_source_attribute_observations AS o
		WHERE o.event_id = ?
			AND target_kind = 'event'
			AND EXISTS (
				SELECT 1
				FROM event_source_attribute_observations AS target
				WHERE target.event_id = ?
					AND target.target_kind = 'event'
					AND target.source_id = o.source_id
					AND target.run_scope = o.run_scope
					AND target.source_identity_key = o.source_identity_key
					AND target.field_name = o.field_name
			)
	`, duplicateEventID, targetEventID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE event_source_attribute_observations
		SET event_id = ?,
			updated_at = ?
		WHERE event_id = ?
			AND target_kind = 'event'
	`, targetEventID, formatRFC3339UTC(now), duplicateEventID)
	return err
}

func loadEventSecondarySourceInfoBySlugFromObservations(ctx context.Context, q queryer, slug string) ([]seedstore.EventSecondarySourceInfo, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT
			s.name,
			s.url,
			o.field_name,
			o.incoming_raw,
			o.id
		FROM event_source_attribute_observations o
		JOIN events e ON e.id = o.event_id
		JOIN sources s ON s.id = o.source_id
		WHERE e.slug = ?
			AND o.target_kind = ?
			AND o.source_authority = ?
			AND o.outcome = ?
		ORDER BY s.name, s.url, o.field_name, o.incoming_raw, o.id
	`, slug, string(seedstore.ObservationTargetKindEvent), string(seedstore.SourceAuthoritySupporting), secondarySourceObservationCompatibilityOutcome)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []seedstore.EventSecondarySourceInfo
	groupIndex := make(map[string]int)
	for rows.Next() {
		var sourceName string
		var sourceURL string
		var fieldName string
		var value string
		var observationID int64
		if err := rows.Scan(&sourceName, &sourceURL, &fieldName, &value, &observationID); err != nil {
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
		switch fieldName {
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

func observationCompatibilityTimestamps(createdAtText, updatedAtText sql.NullString, fallback time.Time) (time.Time, time.Time) {
	createdAt, err := parseNullableRFC3339UTC(createdAtText)
	if err != nil {
		createdAt = fallback
	}
	updatedAt, err := parseNullableRFC3339UTC(updatedAtText)
	if err != nil {
		updatedAt = fallback
	}
	if createdAt.IsZero() && !updatedAt.IsZero() {
		createdAt = updatedAt
	}
	if updatedAt.IsZero() && !createdAt.IsZero() {
		updatedAt = createdAt
	}
	if createdAt.IsZero() {
		createdAt = fallback
	}
	if updatedAt.IsZero() {
		updatedAt = fallback
	}
	return createdAt, updatedAt
}

func int64Ptr(v int64) *int64 {
	return &v
}
