# CHUNK-9: Active Legacy Review-Group Store Cleanup

## CURRENT_PLAN.md Coverage

- Move decisively to `event_review_clusters` as the only active review model.
- Active runtime code must not read or write `review_groups` or `import_run_review_groups`.
- Frozen historical `review_groups` migration/schema references may remain only as inert compatibility coverage.
- Final broad validation after CHUNK-8 depends on this cleanup.

## Affected Files

- `internal/store/sqlite/review.go`
- `internal/store/sqlite/review_test.go`
- `internal/store/sqlite/review_unit5_test.go`
- `cmd/ingest/main_test.go`
- `internal/web/server_test.go`
- `internal/store/sqlite/store.go`, if `linkReviewGroupToImportRunTx` becomes unused
- `internal/store/sqlite/venue_match.go`, if legacy review-group shared venue helpers become unused
- `CURRENT_PLAN.md`
- `CURRENT_PLAN_CHUNKS.md`

## Implementation Plan

- Remove exported sqlite legacy review-group APIs instead of keeping compatibility stubs.
- Preserve unexported shared helpers used by event-review clusters, title repair, event mutation, source links, observations, secondary source info, genre refresh, and slug/identity handling.
- Delete or rewrite obsolete legacy review-group API tests; do not skip them.
- Remove fixture paths that intentionally seed or mutate `review_groups`.
- Remove leftover web test stubs for legacy review-group interfaces.
- Remove now-unused legacy helper functions after compile feedback.
- Keep frozen migration/schema references and tests that assert active paths do not write legacy tables.

## Parallel Ownership

- Production cleanup implementer owns `internal/store/sqlite/review.go`, plus `store.go` and `venue_match.go` only if needed.
- Test cleanup implementer owns `internal/store/sqlite/review_test.go`, `internal/store/sqlite/review_unit5_test.go`, `cmd/ingest/main_test.go`, and `internal/web/server_test.go`.
- The orchestrator owns `CURRENT_PLAN.md`, `CURRENT_PLAN_CHUNKS.md`, and this file.

## Completion Criteria

- No exported sqlite store review-group methods remain.
- No active runtime package calls legacy review-group APIs.
- Active ingest/web/store paths do not read or write `review_groups` or `import_run_review_groups`.
- Static search has no active legacy review-group API call sites outside frozen/inert schema coverage.
- Broad validation passes.

## Validation

- `rg "CreateReviewGroup|StageReviewGroup|ResolveReviewGroup|ListOpenReviewGroups|ListClosedReviewGroups|PromoteSingletonReviewGroupIfMissing|LoadReviewGroup" --glob '*.go'`
- `GOCACHE=/tmp/sheffield-live-gocache go test -count=1 ./internal/store/sqlite`
- `GOCACHE=/tmp/sheffield-live-gocache go test -count=1 ./cmd/ingest ./internal/web`
- `GOCACHE=/tmp/sheffield-live-gocache go test -count=1 ./...`

## Completion

<!-- completed-by: CHUNK-9 -->

- Reviewer approved the cleanup after targeted and full validation passed.
- The full suite requires unsandboxed local port binding for `httptest` in `internal/ingest`.
