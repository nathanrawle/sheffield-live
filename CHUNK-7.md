# CHUNK-7: Duplicate Auto-Resolution

## CURRENT_PLAN.md Coverage

- Port only `canonical_exact_match` and `unanimous_duplicate`.
- Preserve legacy live side effects.
- Run cluster persistence, live side effects, observations, and resolution snapshot insertion in one transaction.
- Make reruns of terminal auto-resolved clusters idempotent.
- Persist immutable resolution snapshots with cluster/live mutation context.

## Affected Files

- `internal/store/event_review.go`
- `internal/store/sqlite/event_review_staging.go`
- `internal/store/sqlite/event_review_resolution.go` or a new cluster-owned auto-resolution file
- `internal/store/sqlite/event_review_resolution_test.go`
- `internal/store/sqlite/event_review_staging_test.go`
- `cmd/ingest/main.go`
- `cmd/ingest/main_test.go`
- `internal/store/sqlite/review.go` as behavior source only, not long-term owner

## Implementation Plan

- Start only after cluster-native staging, terminal reuse/provenance, and CHUNK-3 report field names exist.
- Stage and merge all event-review evidence first, then evaluate auto-resolution on the final survivor cluster.
- Extract or preserve needed live-mutation helpers from legacy `review.go` before legacy review-group cleanup removes them.
- Define cluster-native eligibility:
  - detect `canonical_exact_match` from active `event_review_evidence`, canonical event identity, and exact source identity agreement;
  - detect `unanimous_duplicate` from active cluster evidence and candidate payloads without reading `review_groups` or legacy candidate IDs;
  - choose deterministic winners for unanimous duplicates;
  - reject ambiguous authoritative link, canonical mismatch, unresolved venue, and non-unanimous staged candidates.
- Store layer, not `cmd/ingest`, owns atomic auto-resolution orchestration.
- Add or reuse a store-level method that stages/merges the final survivor cluster and auto-resolves in one transaction.
- Move duplicate auto-resolution off legacy review-group persistence and onto event-review clusters.
- Preserve the narrow legacy live mutations for the two allowed result values.
- Ensure cluster status, live actions, observations, and resolution snapshots commit atomically.
- Populate auto-resolved cluster rows/counts using the JSON/log field names established in CHUNK-3.
- Add focused tests for side effects, transactional rollback, idempotent rerun, and immutable snapshots.

## Completion Criteria

- No active duplicate auto-resolution writes `review_groups`.
- Auto-resolution reports and stores only the two planned result values.
- Reruns link to existing terminal clusters without repeating non-idempotent mutations.
- Tests cover both allowed results, authoritative/supporting branches, no legacy table writes, source links, observations, reviewed state, secondary source info, genre refresh, rollback, terminal rerun linking, and no duplicate resolution snapshots.
- CLI/report tests assert new `event_review_clusters_auto_resolved*` fields and absence of old `duplicate_auto_resolved*` fields.

## Ownership Notes

- CHUNK-3 owns field names and zero/empty report/log shape.
- This chunk owns producing non-empty auto-resolution rows/counts and their transactional side effects.
- CHUNK-6 owns terminal reuse/provenance mechanics that this chunk relies on for terminal reruns.

## Validation

- `GOCACHE=/tmp/sheffield-live-gocache go test ./cmd/ingest ./internal/store/sqlite`
- `GOCACHE=/tmp/sheffield-live-gocache go test ./...`
