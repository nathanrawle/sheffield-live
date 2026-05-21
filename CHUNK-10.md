# CHUNK-10 Retrospective Blocker Fixes

Status: completed

Scope: implements `CURRENT_PLAN.md` `## Retrospective Blocker Fixes`.

- Enforce non-empty deterministic `StagingKey` and positive `StagingKeyVersion` at active `StageEventReviewEvidence` entry points.
- Guard open `createEventReviewClusterTx` calls against null staging keys.
- Prevent terminal-linked evidence rows from in-place evidence, event ID, or identity-link mutation.
- Replace active title and description repair use of legacy `review.GroupInput` with `ingest.ReviewStageClusterInput`.
- Delete the legacy review-stage adapter and `review.GroupInput` type after removing production callers.
- Update docs so retired `/admin/legacy-review*` routes are documented as authenticated 404s, not live audit routes.
- Remove stale `review_groups_created` and `review_groups_reused` report fields.

Validation:

- `GOCACHE=/tmp/sheffield-live-gocache go test -buildvcs=false -count=1 ./internal/store/sqlite -run 'TestStageEventReviewEvidence|TestStageRepairEventReviewCluster|TestRepairEventTitlesFromReport|TestRepairEventDescriptionsFromReport|TestRepairHistoricalDuplicateEvents'`
- `GOCACHE=/tmp/sheffield-live-gocache go test -buildvcs=false -count=1 ./internal/ingest ./internal/review ./internal/store/sqlite ./cmd/ingest ./internal/web`
