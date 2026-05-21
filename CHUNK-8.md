# CHUNK-8: Title Repair, Docs, And Final Validation

## CURRENT_PLAN.md Coverage

- Keep `-repair-event-titles`.
- Authoritative title fixes may update directly.
- Supporting/conflict cases stage event-review clusters only.
- Title repair reports expose cluster fields only, with no `review_groups_created`, `review_groups_reused`, `review_group_id`, or legacy review-created/reused counters.
- Docs mention only event-review clusters and current flags.
- Run focused and broad validation.

## Affected Files

- `internal/store/sqlite/title_repair.go`
- `internal/store/sqlite/title_repair_test.go` or existing title repair test files
- `cmd/ingest/main.go`
- `cmd/ingest/main_test.go`
- `README.md`
- `docs/common-tasks.md`
- `docs/commands.md`
- `docs/logging.md`
- `docs/qa/logging.md`
- `docs/sources.md`
- `docs/architecture.md`
- `CURRENT_PLAN.md`
- `CURRENT_PLAN_CHUNKS.md`

## Implementation Plan

- Remove remaining title-repair report fields named `review_groups_created`, `review_groups_reused`, `review_group_id`, `review_created`, or `review_reused`; replace active staged-review output with cluster IDs/counts.
- Remove or retire the legacy title-repair helper that writes review groups; supporting/conflict paths must stage event-review clusters only.
- Confirm authoritative title fixes still update directly.
- Add report-contract tests that marshalled `title_repair` output omits legacy review-group fields and includes only cluster IDs/counts for staged cluster cases.
- Sweep docs/tests for stale active review-group wording and removed flags.
- Run focused validation, then full `go test ./...`.
- Mark completed `CURRENT_PLAN.md` sections and close chunk tracking only after focused and broad validation pass.

## Completion Criteria

- Title repair report output uses cluster fields only.
- Title repair production code has no helper path that writes legacy review groups.
- Public docs and operational docs no longer advertise removed active review-group features.
- `CURRENT_PLAN.md` and `CURRENT_PLAN_CHUNKS.md` accurately reflect completed implementation.
- Static docs/report check finds no active references to removed flags or old JSON/report fields: `stage-review-groups`, `stage-review`, `review-ics-fixture`, `review-fixture`, `review-title`, `duplicate_auto_resolved`, `review_groups_created`, `review_groups_reused`, `review_group_id`.
- Historical schema mentions are allowed only where explicitly describing frozen migrations or inert historical schema.

## Validation

- `GOCACHE=/tmp/sheffield-live-gocache go test ./internal/store/sqlite -run 'RepairEventTitlesFromReport|TitleRepair'`
- `GOCACHE=/tmp/sheffield-live-gocache go test ./cmd/ingest ./internal/store/sqlite ./internal/web`
- `GOCACHE=/tmp/sheffield-live-gocache go test ./...`
- `rg "stage-review-groups|stage-review|review-ics-fixture|review-fixture|review-title|duplicate_auto_resolved|review_groups_created|review_groups_reused|review_group_id" README.md docs cmd/ingest/main.go internal/store/sqlite/title_repair.go`
