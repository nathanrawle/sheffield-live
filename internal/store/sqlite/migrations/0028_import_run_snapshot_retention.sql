CREATE TABLE IF NOT EXISTS import_run_snapshot_retention (
  import_run_id INTEGER PRIMARY KEY,
  latest_start_at TEXT,
  candidate_count INTEGER NOT NULL DEFAULT 0,
  parseable_start_count INTEGER NOT NULL DEFAULT 0,
  recorded_at TEXT NOT NULL,
  snapshots_pruned_at TEXT,
  snapshots_pruned_count INTEGER NOT NULL DEFAULT 0,
  prune_reason TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CHECK(candidate_count >= 0),
  CHECK(parseable_start_count >= 0),
  CHECK(candidate_count >= parseable_start_count),
  CHECK(parseable_start_count <> 0 OR latest_start_at IS NULL),
  CHECK(snapshots_pruned_count >= 0),
  CHECK(prune_reason IN ('', 'bounded_stale', 'unknown_no_bounds', 'unknown_no_parseable_start'))
);

CREATE INDEX IF NOT EXISTS idx_import_run_snapshot_retention_latest_start
ON import_run_snapshot_retention(latest_start_at);
