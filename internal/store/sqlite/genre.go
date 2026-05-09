package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/genre"
)

func (s *Store) ListGenreRules(ctx context.Context) ([]genre.Rule, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	return loadGenreRulesTx(ctx, s.db, false)
}

func (s *Store) SaveGenreRule(ctx context.Context, input genre.RuleInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	rule := genre.RuleFromInput(input)
	if rule.MatchType == "" {
		rule.MatchType = genre.MatchTypePlain
	}
	if err := genre.ValidateRule(rule); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	if rule.ID > 0 {
		res, err := tx.ExecContext(ctx, `
			UPDATE genre_rules
			SET rule_key = ?,
				name = ?,
				match_type = ?,
				pattern = ?,
				enabled = ?,
				sort_order = ?,
				admin_modified = 1,
				deleted = 0,
				updated_at = ?
			WHERE id = ? AND deleted = 0
		`, rule.Key, rule.Name, rule.MatchType, rule.Pattern, boolInt(rule.Enabled), rule.SortOrder, formatRFC3339UTC(now), rule.ID)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("genre rule %d not found", rule.ID)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO genre_rules (
				rule_key,
				name,
				match_type,
				pattern,
				enabled,
				sort_order,
				admin_modified,
				deleted,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?, ?)
			ON CONFLICT(rule_key) DO UPDATE SET
				name = excluded.name,
				match_type = excluded.match_type,
				pattern = excluded.pattern,
				enabled = excluded.enabled,
				sort_order = excluded.sort_order,
				admin_modified = 1,
				deleted = 0,
				updated_at = excluded.updated_at
		`, rule.Key, rule.Name, rule.MatchType, rule.Pattern, boolInt(rule.Enabled), rule.SortOrder, formatRFC3339UTC(now), formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	if err := recomputeAllEventGenresTx(ctx, tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeleteGenreRule(ctx context.Context, id int64) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if id <= 0 {
		return errors.New("genre rule ID is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		UPDATE genre_rules
		SET enabled = 0,
			admin_modified = 1,
			deleted = 1,
			updated_at = ?
		WHERE id = ? AND deleted = 0
	`, formatRFC3339UTC(now), id)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("genre rule %d not found", id)
	}
	if err := recomputeAllEventGenresTx(ctx, tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RecomputeEventGenres(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := recomputeAllEventGenresTx(ctx, tx, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) EventGenresByEventSlug(ctx context.Context, slug string) ([]genre.Match, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.name, g.rank, g.score, g.mention_count, g.earliest_position
		FROM event_genres g
		JOIN events e ON e.id = g.event_id
		WHERE e.slug = ?
		ORDER BY g.rank, g.name
	`, strings.TrimSpace(slug))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []genre.Match
	for rows.Next() {
		var match genre.Match
		if err := rows.Scan(&match.Name, &match.Rank, &match.Score, &match.MentionCount, &match.EarliestPosition); err != nil {
			return nil, err
		}
		out = append(out, match)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func syncGenreDefaultsTx(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	defaults, err := genre.LoadRepoDefaults()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, rule := range defaults {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO genre_rules (
				rule_key,
				name,
				match_type,
				pattern,
				enabled,
				sort_order,
				admin_modified,
				deleted,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
			ON CONFLICT(rule_key) DO UPDATE SET
				name = excluded.name,
				match_type = excluded.match_type,
				pattern = excluded.pattern,
				enabled = excluded.enabled,
				sort_order = excluded.sort_order,
				updated_at = excluded.updated_at
			WHERE genre_rules.admin_modified = 0 AND genre_rules.deleted = 0
		`, rule.Key, rule.Name, rule.MatchType, rule.Pattern, boolInt(rule.Enabled), rule.SortOrder, formatRFC3339UTC(now), formatRFC3339UTC(now)); err != nil {
			return err
		}
	}
	return nil
}

func backfillEventGenresTx(ctx context.Context, tx interface {
	execer
	queryer
}) error {
	return recomputeAllEventGenresTx(ctx, tx, time.Now().UTC())
}

func recomputeAllEventGenresTx(ctx context.Context, tx interface {
	execer
	queryer
}, now time.Time) error {
	rules, err := loadEnabledGenreRulesTx(ctx, tx)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, description
		FROM events
		ORDER BY id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type eventDescription struct {
		id          int64
		description string
	}
	var events []eventDescription
	for rows.Next() {
		var event eventDescription
		if err := rows.Scan(&event.id, &event.description); err != nil {
			return err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, event := range events {
		if err := refreshEventGenresWithRulesTx(ctx, tx, event.id, event.description, nil, rules, now); err != nil {
			return err
		}
	}
	return nil
}

func refreshEventGenresTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, canonicalDescription string, extraDescriptions []string, now time.Time) error {
	rules, err := loadEnabledGenreRulesTx(ctx, tx)
	if err != nil {
		return err
	}
	return refreshEventGenresWithRulesTx(ctx, tx, eventID, canonicalDescription, extraDescriptions, rules, now)
}

func refreshEventGenresWithRulesTx(ctx context.Context, tx interface {
	execer
	queryer
}, eventID int64, canonicalDescription string, extraDescriptions []string, rules []genre.Rule, now time.Time) error {
	if eventID <= 0 {
		return errors.New("event genre event ID is required")
	}
	descriptions := []string{canonicalDescription}
	descriptions = append(descriptions, extraDescriptions...)
	secondary, err := loadSecondaryDescriptionsForEventIDTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	descriptions = append(descriptions, secondary...)

	matches, err := genre.Infer(descriptions, rules)
	if err != nil {
		return err
	}
	if err := replaceEventGenresTx(ctx, tx, eventID, matches, now); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE events
		SET genre = ?
		WHERE id = ?
	`, genre.Summary(matches, 2), eventID)
	return err
}

func replaceEventGenresTx(ctx context.Context, tx execer, eventID int64, matches []genre.Match, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM event_genres
		WHERE event_id = ?
	`, eventID); err != nil {
		return err
	}
	recordedAt := formatRFC3339UTC(now)
	for _, match := range matches {
		if strings.TrimSpace(match.Name) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_genres (
				event_id,
				name,
				rank,
				score,
				mention_count,
				earliest_position,
				created_at,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, eventID, match.Name, match.Rank, match.Score, match.MentionCount, match.EarliestPosition, recordedAt, recordedAt); err != nil {
			return err
		}
	}
	return nil
}

func loadGenreRulesTx(ctx context.Context, q queryer, enabledOnly bool) ([]genre.Rule, error) {
	query := `
		SELECT
			id,
			rule_key,
			name,
			match_type,
			pattern,
			enabled,
			sort_order,
			admin_modified,
			created_at,
			updated_at
		FROM genre_rules
		WHERE deleted = 0
	`
	if enabledOnly {
		query += ` AND enabled = 1`
	}
	query += ` ORDER BY sort_order, name, id`
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []genre.Rule
	for rows.Next() {
		var rule genre.Rule
		var enabled int
		var adminModified int
		var createdAt string
		var updatedAt string
		if err := rows.Scan(&rule.ID, &rule.Key, &rule.Name, &rule.MatchType, &rule.Pattern, &enabled, &rule.SortOrder, &adminModified, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		rule.Enabled = enabled == 1
		rule.AdminModified = adminModified == 1
		var err error
		rule.CreatedAt, err = parseRFC3339UTC(createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse genre rule %q created_at: %w", rule.Key, err)
		}
		rule.UpdatedAt, err = parseRFC3339UTC(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse genre rule %q updated_at: %w", rule.Key, err)
		}
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return rules, nil
}

func loadEnabledGenreRulesTx(ctx context.Context, q queryer) ([]genre.Rule, error) {
	return loadGenreRulesTx(ctx, q, true)
}

func loadSecondaryDescriptionsForEventIDTx(ctx context.Context, q queryer, eventID int64) ([]string, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT value
		FROM event_secondary_source_info
		WHERE event_id = ? AND info_type = 'description'
		ORDER BY source_id, value
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var descriptions []string
	for rows.Next() {
		var description string
		if err := rows.Scan(&description); err != nil {
			return nil, err
		}
		if strings.TrimSpace(description) != "" {
			descriptions = append(descriptions, description)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return descriptions, nil
}

func validateDanglingEventGenreRefs(ctx context.Context, q queryer) error {
	row := q.QueryRowContext(ctx, `
		SELECT g.id
		FROM event_genres g
		LEFT JOIN events e ON e.id = g.event_id
		WHERE e.id IS NULL
		ORDER BY g.id
		LIMIT 1
	`)
	var genreID int64
	switch err := row.Scan(&genreID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("event genre %d references missing event", genreID)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
