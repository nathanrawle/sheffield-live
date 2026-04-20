package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (s *Store) EnsureSource(ctx context.Context, name, sourceURL string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("sqlite store is not open")
	}
	name = strings.TrimSpace(name)
	sourceURL = strings.TrimSpace(sourceURL)
	if name == "" {
		return 0, errors.New("source name is required")
	}
	if sourceURL == "" {
		return 0, errors.New("source URL is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	id, err := ensureSourceTx(ctx, tx, name, sourceURL)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

func ensureSourceTx(ctx context.Context, tx interface {
	execer
	queryer
}, name, sourceURL string) (int64, error) {
	if strings.TrimSpace(name) == "" {
		return 0, errors.New("source name is required")
	}
	if strings.TrimSpace(sourceURL) == "" {
		return 0, errors.New("source URL is required")
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO sources (name, url)
		VALUES (?, ?)
	`, name, sourceURL); err != nil {
		return 0, err
	}

	var id int64
	if err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM sources
		WHERE name = ? AND url = ?
		LIMIT 1
	`, name, sourceURL).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) CreateImportRun(ctx context.Context, status, notes string) (int64, time.Time, error) {
	if s == nil || s.db == nil {
		return 0, time.Time{}, errors.New("sqlite store is not open")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return 0, time.Time{}, errors.New("import run status is required")
	}

	startedAt := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO import_runs (started_at, status, notes)
		VALUES (?, ?, ?)
	`, formatRFC3339UTC(startedAt), status, notes)
	if err != nil {
		return 0, time.Time{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, time.Time{}, err
	}
	return id, startedAt, nil
}

func (s *Store) CreateSnapshot(ctx context.Context, importRunID int64, sourceID *int64, capturedAt time.Time, payload string) (int64, time.Time, error) {
	if s == nil || s.db == nil {
		return 0, time.Time{}, errors.New("sqlite store is not open")
	}
	if importRunID <= 0 {
		return 0, time.Time{}, errors.New("import run ID is required")
	}
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	if payload == "" {
		return 0, time.Time{}, errors.New("snapshot payload is required")
	}

	var sourceValue any
	if sourceID != nil {
		sourceValue = *sourceID
	}

	res, err := s.db.ExecContext(ctx, `
		INSERT INTO snapshots (import_run_id, source_id, captured_at, payload)
		VALUES (?, ?, ?, ?)
	`, importRunID, sourceValue, formatRFC3339UTC(capturedAt), payload)
	if err != nil {
		return 0, time.Time{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, time.Time{}, err
	}
	return id, capturedAt.UTC(), nil
}

func (s *Store) FinishImportRun(ctx context.Context, id int64, status, notes string) (time.Time, error) {
	if s == nil || s.db == nil {
		return time.Time{}, errors.New("sqlite store is not open")
	}
	if id <= 0 {
		return time.Time{}, errors.New("import run ID is required")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return time.Time{}, errors.New("import run status is required")
	}

	finishedAt := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE import_runs
		SET finished_at = ?, status = ?, notes = ?
		WHERE id = ?
	`, formatRFC3339UTC(finishedAt), status, notes, id)
	if err != nil {
		return time.Time{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return time.Time{}, err
	}
	if affected != 1 {
		return time.Time{}, fmt.Errorf("import run %d not found", id)
	}
	return finishedAt, nil
}
