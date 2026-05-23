package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	SnapshotPruneReasonBoundedStale            = "bounded_stale"
	SnapshotPruneReasonUnknownNoBounds         = "unknown_no_bounds"
	SnapshotPruneReasonUnknownNoParseableStart = "unknown_no_parseable_start"
	DefaultSnapshotUnknownRetentionGrace       = 7 * 24 * time.Hour
	snapshotRetentionLocationName              = "Europe/London"
)

type ImportRunSnapshotRetentionInput struct {
	ImportRunID         int64
	LatestStartAt       *time.Time
	CandidateCount      int
	ParseableStartCount int
	RecordedAt          time.Time
}

type SnapshotCleanupOptions struct {
	Now          time.Time
	Location     *time.Location
	UnknownGrace time.Duration
}

type SnapshotCleanupReport struct {
	ScannedRuns      int                  `json:"scanned_runs"`
	DeletedRuns      int                  `json:"deleted_runs"`
	DeletedSnapshots int64                `json:"deleted_snapshots"`
	Vacuumed         bool                 `json:"vacuumed"`
	VacuumError      string               `json:"vacuum_error,omitempty"`
	Runs             []SnapshotCleanupRun `json:"runs,omitempty"`
}

type SnapshotCleanupRun struct {
	ImportRunID   int64  `json:"import_run_id"`
	SnapshotCount int64  `json:"snapshot_count"`
	Reason        string `json:"reason"`
}

type snapshotCleanupCandidate struct {
	importRunID         int64
	finishedAt          time.Time
	snapshotCount       int64
	hasRetention        bool
	latestStartAt       *time.Time
	parseableStartCount int
	reason              string
}

func (s *Store) UpsertImportRunSnapshotRetention(ctx context.Context, input ImportRunSnapshotRetentionInput) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	if input.ImportRunID <= 0 {
		return errors.New("import run ID is required")
	}
	if input.CandidateCount < 0 {
		return errors.New("candidate count must be non-negative")
	}
	if input.ParseableStartCount < 0 {
		return errors.New("parseable start count must be non-negative")
	}
	if input.CandidateCount < input.ParseableStartCount {
		return errors.New("candidate count must be greater than or equal to parseable start count")
	}
	if input.ParseableStartCount == 0 && input.LatestStartAt != nil {
		return errors.New("latest start must be nil when parseable start count is zero")
	}
	if input.ParseableStartCount > 0 && input.LatestStartAt == nil {
		return errors.New("latest start is required when parseable start count is positive")
	}
	if input.RecordedAt.IsZero() {
		input.RecordedAt = time.Now().UTC()
	}

	var latestStart any
	if input.LatestStartAt != nil {
		latestStart = formatRFC3339UTC(*input.LatestStartAt)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO import_run_snapshot_retention (
			import_run_id,
			latest_start_at,
			candidate_count,
			parseable_start_count,
			recorded_at,
			snapshots_pruned_at,
			snapshots_pruned_count,
			prune_reason
		) VALUES (?, ?, ?, ?, ?, NULL, 0, '')
		ON CONFLICT(import_run_id) DO UPDATE SET
			latest_start_at = excluded.latest_start_at,
			candidate_count = excluded.candidate_count,
			parseable_start_count = excluded.parseable_start_count,
			recorded_at = excluded.recorded_at,
			snapshots_pruned_at = NULL,
			snapshots_pruned_count = 0,
			prune_reason = ''
	`, input.ImportRunID, latestStart, input.CandidateCount, input.ParseableStartCount, formatRFC3339UTC(input.RecordedAt))
	return err
}

func (s *Store) DeleteStaleImportRunSnapshots(ctx context.Context, opts SnapshotCleanupOptions) (SnapshotCleanupReport, error) {
	if s == nil || s.db == nil {
		return SnapshotCleanupReport{}, errors.New("sqlite store is not open")
	}
	opts, err := normalizeSnapshotCleanupOptions(opts)
	if err != nil {
		return SnapshotCleanupReport{}, err
	}
	candidates, err := s.snapshotCleanupCandidates(ctx, opts)
	if err != nil {
		return SnapshotCleanupReport{}, err
	}

	report := SnapshotCleanupReport{ScannedRuns: len(candidates)}
	toDelete := make([]snapshotCleanupCandidate, 0)
	cutoffUTC := snapshotRetentionTodayCutoffUTC(opts.Now, opts.Location)
	unknownCutoff := opts.Now.UTC().Add(-opts.UnknownGrace)
	for _, candidate := range candidates {
		switch {
		case candidate.latestStartAt != nil && candidate.parseableStartCount > 0:
			if candidate.latestStartAt.Before(cutoffUTC) {
				candidate.reason = SnapshotPruneReasonBoundedStale
				toDelete = append(toDelete, candidate)
			}
		case !candidate.hasRetention:
			if !candidate.finishedAt.After(unknownCutoff) {
				candidate.reason = SnapshotPruneReasonUnknownNoBounds
				toDelete = append(toDelete, candidate)
			}
		default:
			if !candidate.finishedAt.After(unknownCutoff) {
				candidate.reason = SnapshotPruneReasonUnknownNoParseableStart
				toDelete = append(toDelete, candidate)
			}
		}
	}
	if len(toDelete) == 0 {
		return report, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return report, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	prunedAt := opts.Now.UTC()
	for _, candidate := range toDelete {
		res, err := tx.ExecContext(ctx, `
			DELETE FROM snapshots
			WHERE import_run_id = ?
		`, candidate.importRunID)
		if err != nil {
			return report, err
		}
		deleted, err := res.RowsAffected()
		if err != nil {
			return report, err
		}
		if deleted == 0 {
			continue
		}
		if err := upsertSnapshotPruneMetadataTx(ctx, tx, candidate, prunedAt, deleted); err != nil {
			return report, err
		}
		report.DeletedRuns++
		report.DeletedSnapshots += deleted
		report.Runs = append(report.Runs, SnapshotCleanupRun{
			ImportRunID:   candidate.importRunID,
			SnapshotCount: deleted,
			Reason:        candidate.reason,
		})
	}
	if err := tx.Commit(); err != nil {
		return report, err
	}
	return report, nil
}

func (s *Store) Vacuum(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("sqlite store is not open")
	}
	_, err := s.db.ExecContext(ctx, `VACUUM`)
	return err
}

func normalizeSnapshotCleanupOptions(opts SnapshotCleanupOptions) (SnapshotCleanupOptions, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	opts.Now = opts.Now.UTC()
	if opts.UnknownGrace == 0 {
		opts.UnknownGrace = DefaultSnapshotUnknownRetentionGrace
	}
	if opts.UnknownGrace < 0 {
		return SnapshotCleanupOptions{}, errors.New("unknown snapshot retention grace must be non-negative")
	}
	if opts.Location == nil {
		loc, err := time.LoadLocation(snapshotRetentionLocationName)
		if err != nil {
			return SnapshotCleanupOptions{}, fmt.Errorf("load %s location: %w", snapshotRetentionLocationName, err)
		}
		opts.Location = loc
	}
	return opts, nil
}

func snapshotRetentionTodayCutoffUTC(now time.Time, loc *time.Location) time.Time {
	localNow := now.In(loc)
	localMidnight := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, loc)
	return localMidnight.UTC()
}

func (s *Store) snapshotCleanupCandidates(ctx context.Context, opts SnapshotCleanupOptions) ([]snapshotCleanupCandidate, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			ir.id,
			ir.finished_at,
			COUNT(sn.id) AS snapshot_count,
			r.import_run_id,
			r.latest_start_at,
			COALESCE(r.parseable_start_count, 0)
		FROM import_runs ir
		JOIN snapshots sn ON sn.import_run_id = ir.id
		LEFT JOIN import_run_snapshot_retention r ON r.import_run_id = ir.id
		WHERE ir.finished_at IS NOT NULL
		GROUP BY ir.id
		ORDER BY ir.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := make([]snapshotCleanupCandidate, 0)
	for rows.Next() {
		var candidate snapshotCleanupCandidate
		var finishedAtText string
		var retentionImportRunID sql.NullInt64
		var latestStartText sql.NullString
		if err := rows.Scan(
			&candidate.importRunID,
			&finishedAtText,
			&candidate.snapshotCount,
			&retentionImportRunID,
			&latestStartText,
			&candidate.parseableStartCount,
		); err != nil {
			return nil, err
		}
		finishedAt, err := parseRFC3339UTC(finishedAtText)
		if err != nil {
			return nil, fmt.Errorf("parse import run %d finished_at: %w", candidate.importRunID, err)
		}
		candidate.finishedAt = finishedAt
		candidate.hasRetention = retentionImportRunID.Valid
		if latestStartText.Valid && latestStartText.String != "" {
			latestStart, err := parseRFC3339UTC(latestStartText.String)
			if err != nil {
				return nil, fmt.Errorf("parse import run %d latest_start_at: %w", candidate.importRunID, err)
			}
			candidate.latestStartAt = &latestStart
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func upsertSnapshotPruneMetadataTx(ctx context.Context, tx interface {
	execer
}, candidate snapshotCleanupCandidate, prunedAt time.Time, prunedCount int64) error {
	if !candidate.hasRetention {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO import_run_snapshot_retention (
				import_run_id,
				latest_start_at,
				candidate_count,
				parseable_start_count,
				recorded_at,
				snapshots_pruned_at,
				snapshots_pruned_count,
				prune_reason
			) VALUES (?, NULL, 0, 0, ?, ?, ?, ?)
		`, candidate.importRunID, formatRFC3339UTC(prunedAt), formatRFC3339UTC(prunedAt), prunedCount, candidate.reason)
		return err
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE import_run_snapshot_retention
		SET snapshots_pruned_at = ?,
			snapshots_pruned_count = ?,
			prune_reason = ?
		WHERE import_run_id = ?
	`, formatRFC3339UTC(prunedAt), prunedCount, candidate.reason, candidate.importRunID)
	return err
}
