# CHUNK-3: CLI And Report Contract Cleanup

## CURRENT_PLAN.md Coverage

- Keep only `-stage-event-reviews`.
- Remove `-stage-review-groups`, `-stage-review`, fixture review flags, and historical duplicate repair CLI flags.
- Replace duplicate auto-resolution report/log fields with `event_review_clusters_auto_resolved_count` and `event_review_clusters_auto_resolved[]`.
- Rename only `cmd/ingest` CLI config, command report structs, command tests, and CLI/logging docs from active review-group terminology to event-review cluster terminology.

## Affected Files

- `cmd/ingest/main.go`
- `cmd/ingest/main_test.go`
- `docs/commands.md`
- `docs/logging.md`
- `docs/qa/logging.md`
- `docs/common-tasks.md`

## Implementation Plan

- Simplify ingest flag parsing and validation around cluster staging.
- Remove fixture review mode and historical duplicate repair CLI entry points while leaving low-level store primitives for cluster resolution.
- Rename `cmd/ingest` command-local staging functions, types, report structs, and JSON fields to event-review cluster terminology.
- Update command tests for removed flags, current flag behavior, single-source review-stage JSON names, all-source nested review-stage JSON names, and finish/per-source log names.
- Update CLI/logging docs that still describe active review groups or old fields.

## Ownership Notes

- This chunk owns the field names and zero/empty output contract for `event_review_clusters_auto_resolved*`.
- CHUNK-7 owns producing real non-empty duplicate auto-resolution rows/counts and the live/store behavior behind them.
- CHUNK-5 owns cluster-native DTO replacement and removal of `review.GroupInput`.
- This chunk does not rename `internal/ingest` APIs or remove `review.GroupInput`.

## Completion Criteria

- Removed flags fail flag parsing/help expectations.
- Fixture review and historical duplicate repair runtime dispatch branches are removed from `cmd/ingest`.
- Reports/logs emit only cluster auto-resolution field names.
- Active `cmd/ingest` CLI/report/log naming no longer exposes review-group terminology except frozen schema references.
- `duplicate_auto_resolved`, `-stage-review`, `-review-ics-fixture`, and `-repair-historical-duplicates` do not appear in active `cmd/ingest` output tags or CLI docs.

## Validation

- `GOCACHE=/tmp/sheffield-live-gocache go test ./cmd/ingest ./internal/ingest`
- `rg "duplicate_auto_resolved|-stage-review|-review-ics-fixture|-repair-historical-duplicates" cmd/ingest/main.go cmd/ingest/main_test.go docs/commands.md docs/logging.md docs/qa/logging.md docs/common-tasks.md` returns no active output-tag or CLI-doc hits
