# Import-Run Snapshot Retention Metadata

## Summary

Persist per-import-run event-date metadata at ingest time, then prune whole snapshot sets when the import run is stale.

- Compute latest parsed candidate `StartAt` from each live ingest report.
- Store retention metadata in a linked table keyed by `import_run_id`.
- Delete all snapshots for finished runs whose latest event start is before today in `Europe/London`.
- Delete finished runs with no stored latest start once their `finished_at` is at least 7 days old, including pre-migration runs with no bounds row.
- Manual cleanup runs `VACUUM` after deletions; automatic cleanup attempts the same best-effort.

## Key Changes

- Add migration `0028` and `schemaVersionV28` for `import_run_snapshot_retention` with:
  - `import_run_id`, nullable `latest_start_at`, `candidate_count`, `parseable_start_count`, `recorded_at`, prune fields, and an index on `latest_start_at`.
  - DB `CHECK` constraints for non-negative counts, `candidate_count >= parseable_start_count`, `latest_start_at IS NULL` when `parseable_start_count = 0`, and fixed prune reasons: `''`, `bounded_stale`, `unknown_no_bounds`, `unknown_no_parseable_start`.
- Record retention metadata for any live `runManualImport` result with an `ImportRunID`; never record it for replay-derived reports.
- Treat retention metadata upsert failure as a source-run failure; in `-all-sources`, continue to later sources and report the failed source normally.
- Automatic cleanup runs after normal live ingest, live `-stage-event-reviews`, and `-all-sources`; never after replay, replay staging, repairs, or image-focus backfill.
- Add standalone `cmd/ingest -cleanup-stale-snapshots`, mutually exclusive with explicit source selection, all-sources, replay, repair, staging, and backfill flags.
- Cleanup only considers `import_runs` with `finished_at IS NOT NULL`; running or unfinished runs are never eligible.
- Pruning a run with no retention row inserts a tombstone row with `latest_start_at = NULL`, counts `0`, `recorded_at = prune time`, and `prune_reason = unknown_no_bounds`.

## Retention Semantics

- "Latest starts at" means max parseable candidate `StartAt`, parsed with `time.RFC3339` and normalized to UTC before storage.
- Compute today's `Europe/London` midnight in Go, convert that instant to UTC, and treat bounded runs as stale when `latest_start_at < cutoffUTC`.
- Unknown runs are finished runs with snapshots and either no retention row or `parseable_start_count = 0`; delete them when `finished_at <= now - 7 days`.
- Use prune reason `bounded_stale`, `unknown_no_bounds`, or `unknown_no_parseable_start`.
- Automatic cleanup logs cleanup/vacuum errors without failing an otherwise successful ingest.
- Manual cleanup deletes snapshots in a transaction, then runs `VACUUM` outside that transaction; its JSON report distinguishes deletion success from vacuum success/failure.

## Test Plan

- Unit test report-bound extraction: max across calendars, invalid/blank starts ignored, candidate vs parseable counts, and no parseable starts.
- SQLite tests: migration/table/index/constraints, retention upsert, stale/today/future eligibility, unfinished run retention, unknown 7-day pruning, tombstone insertion, prune metadata, and delete-only-snapshots behavior.
- Command tests: cleanup JSON counts, flag conflicts, metadata upsert failure behavior, automatic cleanup after live/all-sources/live staging, and ingest-error preservation.
- Use an injected clock/location-aware cutoff helper for deterministic `Europe/London` midnight and 7-day grace tests.
- Validate with `go test ./internal/ingest ./internal/store/sqlite ./cmd/ingest`, then `go test ./...`.

## Assumptions

- Whole-run snapshot deletion only; partial deletion remains out of scope because replay expects complete snapshot sets.
- Pre-migration finished runs with no bounds row are intentionally treated as unknown and auto-pruned after 7 days.
- Pruned runs remain visible in import history, but replay is unavailable because snapshots are gone.
