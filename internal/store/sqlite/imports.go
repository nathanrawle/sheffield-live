package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sheffield-live/internal/ingest"
)

var _ ingest.ReplayStore = (*Store)(nil)

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

func (s *Store) ListImportRuns(ctx context.Context, limit int) ([]ingest.ImportRunSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			ir.id,
			ir.started_at,
			ir.finished_at,
			ir.status,
			ir.notes,
			COUNT(sn.id) AS snapshot_count
		FROM import_runs ir
		LEFT JOIN snapshots sn ON sn.import_run_id = ir.id
		GROUP BY ir.id, ir.started_at, ir.finished_at, ir.status, ir.notes
		ORDER BY ir.started_at DESC, ir.id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	runs := make([]ingest.ImportRunSummary, 0, limit)
	for rows.Next() {
		summary, err := scanImportRunSummary(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runs, nil
}

func (s *Store) LatestSuccessfulImport(ctx context.Context) (*ingest.ImportRunSummary, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("sqlite store is not open")
	}

	row := s.db.QueryRowContext(ctx, `
		SELECT
			ir.id,
			ir.started_at,
			ir.finished_at,
			ir.status,
			ir.notes,
			COUNT(sn.id) AS snapshot_count
		FROM import_runs ir
		LEFT JOIN snapshots sn ON sn.import_run_id = ir.id
		WHERE LOWER(TRIM(ir.status)) = ? AND ir.finished_at IS NOT NULL AND TRIM(ir.finished_at) <> ''
		GROUP BY ir.id, ir.started_at, ir.finished_at, ir.status, ir.notes
		ORDER BY ir.finished_at DESC, ir.id DESC
		LIMIT 1
	`, ingestStatusSucceeded())

	summary, err := scanImportRunSummary(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &summary, nil
}

func scanImportRunSummary(row interface {
	Scan(dest ...any) error
}) (ingest.ImportRunSummary, error) {
	var summary ingest.ImportRunSummary
	var startedAtText string
	var finishedAtText sql.NullString
	if err := row.Scan(
		&summary.ID,
		&startedAtText,
		&finishedAtText,
		&summary.Status,
		&summary.Notes,
		&summary.SnapshotCount,
	); err != nil {
		return ingest.ImportRunSummary{}, err
	}

	startedAt, err := parseRFC3339UTC(startedAtText)
	if err != nil {
		return ingest.ImportRunSummary{}, fmt.Errorf("parse import run %d started_at: %w", summary.ID, err)
	}
	summary.StartedAt = startedAt
	if finishedAtText.Valid && strings.TrimSpace(finishedAtText.String) != "" {
		finishedAt, err := parseRFC3339UTC(finishedAtText.String)
		if err != nil {
			return ingest.ImportRunSummary{}, fmt.Errorf("parse import run %d finished_at: %w", summary.ID, err)
		}
		summary.FinishedAt = &finishedAt
	}
	return summary, nil
}

func ingestStatusSucceeded() string {
	return "succeeded"
}

func (s *Store) LoadImportRun(ctx context.Context, id int64) (ingest.ReplayRun, error) {
	if s == nil || s.db == nil {
		return ingest.ReplayRun{}, errors.New("sqlite store is not open")
	}
	if id <= 0 {
		return ingest.ReplayRun{}, errors.New("import run ID is required")
	}

	var run ingest.ReplayRun
	var startedAtText string
	var finishedAtText sql.NullString
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, started_at, finished_at, status, notes
		FROM import_runs
		WHERE id = ?
	`, id).Scan(&run.ID, &startedAtText, &finishedAtText, &run.Status, &run.Notes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ingest.ReplayRun{}, fmt.Errorf("import run %d not found", id)
		}
		return ingest.ReplayRun{}, err
	}

	startedAt, err := parseRFC3339UTC(startedAtText)
	if err != nil {
		return ingest.ReplayRun{}, fmt.Errorf("parse import run %d started_at: %w", id, err)
	}
	run.StartedAt = startedAt
	if finishedAtText.Valid && strings.TrimSpace(finishedAtText.String) != "" {
		finishedAt, err := parseRFC3339UTC(finishedAtText.String)
		if err != nil {
			return ingest.ReplayRun{}, fmt.Errorf("parse import run %d finished_at: %w", id, err)
		}
		run.FinishedAt = &finishedAt
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			sn.id,
			sn.source_id,
			sn.captured_at,
			sn.payload,
			COALESCE(src.name, ''),
			COALESCE(src.url, '')
		FROM snapshots sn
		LEFT JOIN sources src ON src.id = sn.source_id
		WHERE sn.import_run_id = ?
		ORDER BY sn.captured_at, sn.id
	`, id)
	if err != nil {
		return ingest.ReplayRun{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var snapshot ingest.ReplaySnapshot
		var sourceID sql.NullInt64
		var capturedAtText string
		if err := rows.Scan(
			&snapshot.ID,
			&sourceID,
			&capturedAtText,
			&snapshot.Payload,
			&snapshot.SourceName,
			&snapshot.SourceURL,
		); err != nil {
			return ingest.ReplayRun{}, err
		}

		capturedAt, err := parseRFC3339UTC(capturedAtText)
		if err != nil {
			return ingest.ReplayRun{}, fmt.Errorf("parse snapshot %d captured_at: %w", snapshot.ID, err)
		}
		snapshot.CapturedAt = capturedAt
		if sourceID.Valid {
			value := sourceID.Int64
			snapshot.SourceID = &value
		}
		run.Snapshots = append(run.Snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return ingest.ReplayRun{}, err
	}

	return run, nil
}
