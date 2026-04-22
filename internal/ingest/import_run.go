package ingest

import (
	"context"
	"time"
)

type ImportRunSummary struct {
	ID            int64      `json:"id"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	Status        string     `json:"status"`
	Notes         string     `json:"notes"`
	SnapshotCount int        `json:"snapshot_count"`
}

type ImportRunStore interface {
	ListImportRuns(ctx context.Context, limit int) ([]ImportRunSummary, error)
	LatestSuccessfulImport(ctx context.Context) (*ImportRunSummary, error)
}
