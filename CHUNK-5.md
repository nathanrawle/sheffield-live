# CHUNK-5: Cluster-Native Open Staging

## CURRENT_PLAN.md Coverage

- Introduce cluster-native staging DTOs/types and remove active reliance on `review.GroupInput`.
- Fresh ingest evidence wins over stale open cluster state.
- Compatible open clusters merge into the lowest-id survivor and report `superseded_cluster_ids`.
- Open restaging prunes stale source identity, event-backed, evidence-backed, and manual literal choices.
- Open import and repair cluster creation paths persist deterministic immutable staging keys and reuse by staging key first.

## Affected Files

- `internal/store/event_review.go`
- `internal/ingest/review_stage.go`
- `cmd/ingest/main.go`
- `cmd/ingest/main_test.go`
- `internal/store/sqlite/event_review_staging.go`
- `internal/store/sqlite/event_review_repair_staging.go`
- `internal/store/sqlite/event_review_source_identity_choices.go`
- `internal/store/sqlite/event_review_staging_test.go`
- `internal/store/sqlite/event_review_repair_staging_test.go`
- `internal/store/sqlite/event_review_source_identity_choices_test.go`

## Implementation Plan

- Introduce a cluster-native import staging DTO or add `StagingKey` and `StagingKeyVersion` to store staging input so import staging does not depend on `review.GroupInput`.
- Carry the deterministic key currently computed by report grouping into event-review cluster staging.
- Ensure open import and repair cluster creation set deterministic immutable creation keys.
- Defer terminal successor creation keys to CHUNK-6.
- Include title-repair cluster creation-key requirements only if the shared repair staging path covers them; otherwise leave title-repair report cleanup to CHUNK-8 and record any gap in `IN_FLIGHT_REVISIONS.md`.
- Ensure open restaging/merge keeps staging keys unchanged and reuses exact keys before evidence/identity fallback.
- Tighten merge/supersede behavior and returned report fields for open clusters.
- Define choice pruning across `event_review_source_identity_choices`, `event_review_canonical_choices`, and `event_review_draft_choices` during restage/merge: preserve choices only when their source identity, event, evidence, or manual field remains valid after refresh.
- Add or update store tests for no `review_groups` writes, deterministic keys, merge reporting, and choice pruning.
- Add command/report tests proving merged open clusters surface `superseded_cluster_ids`.

## Completion Criteria

- Active staging code no longer imports or accepts `review.GroupInput`.
- Open cluster reruns and merges are idempotent under the creation-key rules.
- Stale choices are pruned on open restaging.
- `rg "review\\.GroupInput" cmd/ingest internal/ingest internal/store/sqlite/event_review* internal/store/event_review.go` has no active open-staging hits.
- Open merge results include `superseded_cluster_ids` in command reports.

## Ownership Notes

- This chunk owns open-cluster creation keys and open staging behavior.
- CHUNK-6 owns terminal successor creation keys and terminal restaging behavior.
- CHUNK-3 owns CLI/report naming churn unless a report field is needed to expose open-merge behavior.

## Validation

- `GOCACHE=/tmp/sheffield-live-gocache go test ./internal/ingest ./cmd/ingest ./internal/store/sqlite -run 'ReviewStage|StageEventReview|StageRepairEventReview'`
- `GOCACHE=/tmp/sheffield-live-gocache go test ./cmd/ingest ./internal/store/sqlite`
