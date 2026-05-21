package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	seedstore "sheffield-live/internal/store"
)

func (s *Store) ResolveEventSlugAlias(ctx context.Context, aliasSlug string) (string, bool, error) {
	if s == nil || s.db == nil {
		return "", false, errors.New("sqlite store is not open")
	}

	aliasSlug = strings.TrimSpace(aliasSlug)
	if aliasSlug == "" {
		return "", false, nil
	}

	row := s.db.QueryRowContext(ctx, `
		SELECT e.slug
		FROM slug_aliases a
		JOIN events e ON e.id = a.target_event_id
		WHERE a.alias_slug = ?
			AND a.target_kind = ?
		LIMIT 1
	`, aliasSlug, string(seedstore.SlugAliasTargetKindEvent))

	var targetSlug string
	switch err := row.Scan(&targetSlug); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, err
	}

	targetSlug = strings.TrimSpace(targetSlug)
	if targetSlug == "" {
		return "", false, nil
	}
	return targetSlug, true, nil
}
