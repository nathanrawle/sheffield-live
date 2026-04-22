package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/domain"
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
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, input.Title, input.SourceName, input.SourceURL, stagingKeyValue(stagingKey), review.StatusOpen, input.Notes, formatRFC3339UTC(now), formatRFC3339UTC(now))
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
		if err := tx.Commit(); err != nil {
			return 0, false, err
		}
		return groupID, true, nil
	}

	var groupID int64
	row := tx.QueryRowContext(ctx, `
		SELECT id
		FROM review_groups
		WHERE staging_key = ?
		LIMIT 1
	`, stagingKey)
	switch err := row.Scan(&groupID); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, errors.New("staged review group not found after ignore")
	case err != nil:
		return 0, false, err
	}

	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return groupID, false, nil
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
			staging_key,
			status,
			notes,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, input.Title, input.SourceName, input.SourceURL, stagingKeyValue(stagingKey), review.StatusOpen, input.Notes, formatRFC3339UTC(now), formatRFC3339UTC(now))
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
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return groupID, nil
}

func stagingKeyValue(value string) any {
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
			g.created_at,
			g.updated_at,
			COUNT(DISTINCT c.id),
			COUNT(DISTINCT d.field)
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
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
		var group review.GroupSummary
		var createdAt string
		var updatedAt string
		if err := rows.Scan(
			&group.ID,
			&group.Title,
			&group.SourceName,
			&group.SourceURL,
			&group.Status,
			&createdAt,
			&updatedAt,
			&group.CandidateCount,
			&group.DraftCount,
		); err != nil {
			return nil, err
		}
		parsedCreatedAt, err := parseRFC3339UTC(createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse review group %d created_at: %w", group.ID, err)
		}
		parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse review group %d updated_at: %w", group.ID, err)
		}
		group.CreatedAt = parsedCreatedAt
		group.UpdatedAt = parsedUpdatedAt
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
			g.created_at,
			g.updated_at,
			COUNT(DISTINCT c.id),
			COUNT(DISTINCT d.field)
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
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
		var group review.GroupSummary
		var createdAt string
		var updatedAt string
		if err := rows.Scan(
			&group.ID,
			&group.Title,
			&group.SourceName,
			&group.SourceURL,
			&group.Status,
			&createdAt,
			&updatedAt,
			&group.CandidateCount,
			&group.DraftCount,
		); err != nil {
			return nil, err
		}
		parsedCreatedAt, err := parseRFC3339UTC(createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse review group %d created_at: %w", group.ID, err)
		}
		parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse review group %d updated_at: %w", group.ID, err)
		}
		group.CreatedAt = parsedCreatedAt
		group.UpdatedAt = parsedUpdatedAt
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

	idText := fmt.Sprintf("%d", importRunID)
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
			COUNT(DISTINCT d.field)
		FROM review_groups g
		LEFT JOIN review_candidates c ON c.group_id = g.id
		LEFT JOIN review_draft_choices d ON d.group_id = g.id
		WHERE g.status IN (?, ?, ?)
			AND (g.notes LIKE ? OR g.notes LIKE ?)
		GROUP BY g.id
		ORDER BY g.updated_at DESC, g.id DESC
	`, review.StatusOpen, review.StatusResolved, review.StatusRejected, "%manual ingest run "+idText+"%", "%import run "+idText+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []review.GroupSummary
	for rows.Next() {
		var group review.GroupSummary
		var notes string
		var createdAt string
		var updatedAt string
		if err := rows.Scan(
			&group.ID,
			&group.Title,
			&group.SourceName,
			&group.SourceURL,
			&group.Status,
			&notes,
			&createdAt,
			&updatedAt,
			&group.CandidateCount,
			&group.DraftCount,
		); err != nil {
			return nil, err
		}
		originID, ok := review.ParseOriginImportRunID(notes)
		if !ok || originID != importRunID {
			continue
		}
		parsedCreatedAt, err := parseRFC3339UTC(createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse review group %d created_at: %w", group.ID, err)
		}
		parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse review group %d updated_at: %w", group.ID, err)
		}
		group.CreatedAt = parsedCreatedAt
		group.UpdatedAt = parsedUpdatedAt
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
	if err := upsertEventTx(ctx, tx, event); err != nil {
		return err
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
		SELECT id, title, source_name, source_url, status, notes, created_at, updated_at
		FROM review_groups
		WHERE id = ?
		LIMIT 1
	`, id)
	var group review.Group
	var createdAt string
	var updatedAt string
	switch err := row.Scan(&group.ID, &group.Title, &group.SourceName, &group.SourceURL, &group.Status, &group.Notes, &createdAt, &updatedAt); {
	case errors.Is(err, sql.ErrNoRows):
		return review.Group{}, false, nil
	case err != nil:
		return review.Group{}, false, err
	}
	parsedCreatedAt, err := parseRFC3339UTC(createdAt)
	if err != nil {
		return review.Group{}, false, fmt.Errorf("parse review group %d created_at: %w", id, err)
	}
	parsedUpdatedAt, err := parseRFC3339UTC(updatedAt)
	if err != nil {
		return review.Group{}, false, fmt.Errorf("parse review group %d updated_at: %w", id, err)
	}
	group.CreatedAt = parsedCreatedAt
	group.UpdatedAt = parsedUpdatedAt
	return group, true, nil
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
	if endText == "" {
		return domain.Event{}, errors.New("review event end time is required")
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
	end, err := parseRFC3339UTC(endText)
	if err != nil {
		return domain.Event{}, fmt.Errorf("parse review event end time: %w", err)
	}
	slug, err := buildLiveEventSlug(name, venueSlug, start)
	if err != nil {
		return domain.Event{}, err
	}

	return domain.Event{
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
	}, nil
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
		formatRFC3339UTC(event.End),
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
