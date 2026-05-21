# CHUNK-4: Migrations And Legacy Runtime Isolation

## CURRENT_PLAN.md Coverage

- Delete the branch-only historical duplicate review-group migration.
- Renumber event-review migrations so the cluster schema is the next branch sequence.
- Add run-evidence provenance migration tables and indexes.
- Update migration constants, registration, and tests.
- Fail fast on incompatible abandoned branch DB schemas.
- Remove startup backfills and validators that read/write `review_groups` or `import_run_review_groups`.

## Affected Files

- `internal/store/sqlite/store.go`
- `internal/store/sqlite/unit2_schema.go`
- `internal/store/sqlite/event_review_schema.go`
- `internal/store/sqlite/event_review_schema_test.go`
- `internal/store/sqlite/store_test.go`
- `internal/store/sqlite/migrations/0026_event_review_schema_foundation.sql`
- `internal/store/sqlite/migrations/0027_event_review_cluster_staging_key.sql`

## Implementation Plan

- Replace abandoned branch migration numbering with the new event-review sequence: base current is v25, event-review foundation becomes new v26, staging-key plus provenance becomes new v27, and abandoned v28 fails fast.
- Add import/repair run-evidence provenance schema with required primary keys, foreign keys, and lookup indexes.
- Add pre-migration incompatible-schema detection immediately after reading `schemaVersion` and before any migration or reconciliation mutation.
- Detect abandoned schemas by markers, not only version numbers:
  - old v26: `review_historical_duplicate_actions`, `review_groups.kind`, or historical duplicate triggers from the abandoned migration;
  - old v27: recorded `schemaVersion == 27` and `event_review_clusters` exists but the staging-key marker is absent;
  - old v28: staging-key schema exists under the abandoned migration numbering;
  - any schema version newer than the new current version.
- Return an operator-facing error that says to reset or recreate the local DB.
- Remove legacy startup backfill and validation calls that operate on review groups, including `backfillReviewGroupImportRunLinks`, `backfillOpenReviewGroupsAuthoritativeLinks`, `backfillReviewFieldDefaults`, `validateDanglingImportRunReviewGroupRefs`, and `validateDanglingObservationRows` review-group joins.
- Update schema/migration tests for fresh migration and fail-fast cases.

## Ownership Notes

- This chunk removes startup and validation reads/writes of legacy review groups.
- This chunk does not delete broad legacy store APIs in `review.go`; CLI, staging, and auto-resolution chunks own later cleanup.
- CHUNK-6 owns runtime use of the run-evidence provenance tables; this chunk owns schema existence and constraints.

## Completion Criteria

- Fresh DB migration reaches the new current version.
- A DB migrated through new v26, with `event_review_clusters` present and no staging-key column yet, migrates normally to new current v27 and does not trigger abandoned-branch fail-fast.
- Reopening a valid new-current v27 DB with the staging-key marker present does not trigger abandoned-branch fail-fast.
- Incompatible v26/v27/v28 branch DB markers fail before normal migration.
- Runtime startup no longer runs preservation backfills or validators for active legacy review-group data.
- Run-evidence provenance tables exist with PK idempotency, FKs to runs/clusters/evidence, and `(cluster_id, evidence_id)` lookup indexes.
- `rg "review_groups|import_run_review_groups" internal/store/sqlite/store.go internal/store/sqlite/unit2_schema.go` shows no startup/open/validate path references except inert frozen-schema metadata.

## Validation

- `GOCACHE=/tmp/sheffield-live-gocache go test ./internal/store/sqlite`
