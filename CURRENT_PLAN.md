# Review Clusters Cleanup Plan

## Summary

Move the branch decisively to `event_review_clusters` as the only active review model. Remove legacy review-group UI, APIs, reports, CLI aliases, and active staging code. Keep old migration-created `review_groups` tables only as frozen historical schema; active runtime code must not read or write them.

Baseline before cleanup: branch is clean and `GOCACHE=/tmp/sheffield-live-gocache go test ./...` passes.

<!-- completed-by: CHUNK-1 scaffolding/tracking split -->

## Key Changes

- Admin review surface:
  - `/admin/review` remains the event-review cluster queue.
  - `/admin/event-review/{id}` and `/admin/event-review/history` remain the only detail/history routes.
  - `/admin/legacy-review*`, `/admin/review/history`, and `/admin/review/{id}` hard-return 404 after auth; non-GET `/admin/review` returns 405.
  <!-- completed-by: CHUNK-2 -->
- CLI/reporting surface:
  - Keep only `-stage-event-reviews`; remove `-stage-review-groups` and `-stage-review`.
  - Remove fixture mode flags: `-review-ics-fixture`, `-review-fixture`, `-review-title`.
  - Remove historical duplicate repair flags: `-repair-historical-duplicates`, `-apply-historical-duplicate-repairs`, `-historical-duplicate-window`.
  - Replace duplicate auto-resolution JSON/log fields with `event_review_clusters_auto_resolved_count` and `event_review_clusters_auto_resolved[]`.
  - Remove old `duplicate_auto_resolved*` fields everywhere.
  <!-- completed-by: CHUNK-3 for CLI/report naming and zero/empty output contract; non-empty auto-resolution rows remain CHUNK-7 -->
- Active code cleanup:
  - Introduce cluster-native staging DTOs/types and remove active reliance on `review.GroupInput`.
  - Rename active functions, config fields, report structs, tests, docs, and UI wording from review groups to event-review clusters.
- Delete active legacy review-group store APIs, web interfaces, handlers, templates, tests, and import-run readers.
  <!-- continued-by: IN_FLIGHT_REVISIONS.md#chunk-9-active-legacy-review-group-store-cleanup -->
  - Remove legacy admin links, import-run legacy review-group sections, and `HasReviewStorage` affordances.
  - Remove legacy startup backfills and validators that read/write `review_groups` or `import_run_review_groups`.
  <!-- completed-by: CHUNK-2 for web-level cleanup; completed-by: CHUNK-4 for startup backfills/validators -->
  <!-- completed-by: CHUNK-5 for active open-staging DTOs and `review.GroupInput` removal -->
  - Keep low-level repair-run/withhold primitives only where needed by event-review cluster resolution.

## Report and Log Contract

- Per-source `review_stage` JSON contains:
  - `enabled`
  - `event_review_clusters_created`
  - `event_review_clusters_reused`
  - `candidate_count`
  - `review_candidate_count`
  - `auto_promoted_count`
  - `event_review_clusters_auto_resolved_count`
  - `event_review_clusters`
  - `auto_promoted`
  - `event_review_clusters_auto_resolved`
  - `errors`
<!-- completed-by: CHUNK-3 for field naming and zero/empty output contract -->
- `event_review_clusters[]` rows contain `cluster_id`, `title`, `candidate_count`, `source_url`, `result`, and `superseded_cluster_ids` when staging merged/superseded other open clusters.
- `event_review_clusters_auto_resolved[]` rows contain `cluster_id`, `title`, `result`, `candidate_count`, and `canonical_event_slug` when known.
- Auto-resolved clusters still count in `event_review_clusters_created` or `event_review_clusters_reused` according to the staging action that found or created the cluster; auto-resolution count is additive.
- `result` values for duplicate auto-resolution are exactly `canonical_exact_match` and `unanimous_duplicate`.
- Finish logs and all-source JSON summaries use the same cluster field names and aggregate created, reused, auto-promoted, and auto-resolved counts across sources.

## Cluster Behavior

- Staging:
  - Fresh ingest evidence wins over stale cluster state.
  - Open clusters may be refreshed, merged, or superseded; active flows must not create or update `review_groups`.
  - When fresh evidence connects compatible open clusters, merge deterministically into the lowest-id survivor, supersede the others, preserve only still-valid evidence/choices, and report `superseded_cluster_ids`.
  <!-- completed-by: CHUNK-5 for open-cluster staging/merge behavior -->
- Evidence and staging keys:
  - Evidence linked only to open clusters may refresh in place when the evidence fingerprint is unchanged.
  - Evidence linked to any terminal cluster is frozen. Do not update its `payload`, `source_id`, `event_id`, `updated_at`, or evidence identity-key links in place.
  - If the same evidence fingerprint arrives with a changed payload after terminal linkage, persist it as a new evidence revision by encoding `original_fingerprint + payload_hash + evidence_revision_algorithm_version` into `evidence_fingerprint`; keep `fingerprint_version = 1` unless the entire fingerprint algorithm changes.
  - Add explicit run-evidence provenance tables for import and repair runs. They link `(run_id, cluster_id, evidence_id, linked_at, link_reason)` without changing cluster active evidence.
  - `import_run_event_review_evidence` uses primary key `(import_run_id, cluster_id, evidence_id)`; `repair_run_event_review_evidence` uses primary key `(repair_run_id, cluster_id, evidence_id)`.
  - Compatible fresh terminal evidence is persisted as the current run's evidence, linked through run-evidence provenance, and paired with the existing terminal run-cluster link; it is not linked as active evidence on the immutable terminal cluster.
  - Terminal reuse and exact replay matching must search both active terminal cluster evidence and run-evidence provenance evidence.
  - `staging_key` and `staging_key_version` are immutable creation keys, not mutable membership hashes.
  - Every active cluster creation path must write a deterministic creation key: import clusters use the initial sorted evidence identity-key set plus conflict type/reason; repair/title clusters use their repair staging material; successors use the fresh evidence identity set plus `previous_cluster_id` and conflict type/reason.
  - Open restaging/merge must not update staging keys. Reruns first reuse by exact staging key, then by active evidence/identity links so merged clusters remain idempotent after membership changes.
  - Bump `staging_key_version` only when the creation-key algorithm changes.
  <!-- completed-by: CHUNK-6 for terminal evidence immutability, run-evidence provenance usage, and terminal successor keys -->
- Choice pruning on open restaging:
  - Source identity choices remain only when their `(source_id, source_identity_key)` still appears in active source identity keys.
  - Event-backed choices remain only when the event is still the cluster canonical event or still appears through active linked evidence.
  - Evidence-backed choices remain only when the referenced evidence row remains active in the cluster.
  - Manual literal field choices remain only for fields still present in the refreshed cluster choice set; otherwise remove them rather than carrying stale manual values forward.
<!-- completed-by: CHUNK-5 -->
- Terminal clusters:
  - Resolved, discarded, and superseded clusters remain immutable.
  - Do not mutate `superseded_by_cluster_id` on terminal clusters during successor creation.

### Terminal Restaging Rules

| Existing terminal match | Fresh evidence state | Behavior |
| --- | --- | --- |
| Same evidence fingerprint and same payload | Exact replay | Link the run to the existing terminal cluster and report it as reused; do not create a successor. |
| Same evidence fingerprint with changed payload | Evidence changed under the same durable identity | Persist a new evidence revision and create a new open successor with `previous_cluster_id` pointing at the terminal cluster. |
| Same identity keys, new evidence fingerprint, compatible with terminal outcome | Fresh observation of the same resolved/discarded shape | Persist the fresh evidence for run provenance, link the run to the existing terminal cluster, and report it as reused. |
| Same identity keys, new evidence fingerprint, changed canonical candidate or changed selected live target | Fresh evidence conflicts with terminal outcome | Create a new open successor with `previous_cluster_id` pointing at the terminal cluster. |
| Evidence crosses an active separation/manual split recorded by identity keys | Incompatible separated evidence | Return a retryable conflict; do not mutate the terminal cluster. |
| Prior cluster is `superseded` | Fresh evidence points at an obsolete cluster | Follow lineage to the current terminal survivor when possible; otherwise create a successor from the superseded cluster with `previous_cluster_id` set. |

Successor idempotency key is the cluster staging key plus staging key version for the fresh evidence. Reruns of the same successor evidence reuse the open successor rather than creating additional clusters.
<!-- completed-by: CHUNK-6 -->

## Duplicate Auto-Resolution

- Port only `canonical_exact_match` and `unanimous_duplicate`.
- Preserve the legacy live side effects: event/source links, observations, reviewed state, secondary source info, and genre refresh where the legacy path did so.
- Run cluster persistence, live side effects, observations, and resolution snapshot insertion in one transaction.
  <!-- invalidated-by: IN_FLIGHT_REVISIONS.md#chunk-7-duplicate-auto-resolution-transaction-boundary -->
- Reruns of an already terminal auto-resolved cluster are idempotent: link the new run to the terminal cluster, report it as reused and auto-resolved, and do not repeat non-idempotent live mutations.
- Persist terminal `event_review_clusters` plus immutable `event_review_resolutions` snapshots including `cluster_id`, result, canonical event identity when known, applied live actions, and source/event mutation context.
<!-- completed-by: CHUNK-7 -->

## Title Repair

- Keep `-repair-event-titles`.
- Authoritative title fixes may update directly.
- Supporting/conflict cases stage event-review clusters only.
- Title repair reports expose cluster fields only; remove `review_groups_created` and `review_groups_reused`.
<!-- completed-by: CHUNK-8 -->
<!-- completed-by: CHUNK-9 for active legacy review-group store cleanup uncovered by final validation -->

## Migrations

- Delete the branch-only historical duplicate review-group migration.
- Renumber event-review migrations so the cluster schema is the next branch sequence.
- Add run-evidence provenance tables for import and repair event-review evidence links, with foreign keys to runs, clusters, and evidence plus indexes by `(cluster_id, evidence_id)` for history/reuse lookups.
- Update explicit migration constants, migration registration, and tests to match the new current version.
- Existing local DBs that applied the abandoned v26/v27/v28 branch schemas are unsupported.
- Add fail-fast detection before normal migration for:
  - old v26 historical duplicate review-group markers, including `review_historical_duplicate_actions` or historical-duplicate review-group triggers;
  - old v27 event-review cluster schema without the staging-key migration marker;
  - old v28 event-review staging-key branch schema with the abandoned migration numbering;
  - schema versions newer than the new current version.
- The fail-fast error must tell the operator to reset or recreate the local DB.
<!-- completed-by: CHUNK-4 -->

## Test Plan

- CLI tests:
  - `-stage-event-reviews` works.
  - Removed flags fail parse/help expectations.
  - Reports use `event_review_clusters_auto_resolved*` and no `duplicate_auto_resolved*`.
- Store tests:
  - Staging creates/merges/supersedes clusters without writing `review_groups`.
  - Open-cluster evidence may refresh in place, but terminal-linked evidence, event association, identity-key links, and timestamps never mutate in place.
  - Changed payload under a terminal-linked fingerprint creates a new evidence revision encoded in `evidence_fingerprint` while keeping `fingerprint_version = 1`.
  - All active cluster creation paths persist deterministic immutable staging keys and reuse clusters by staging key before falling back to active evidence/identity links.
  - Open restaging and merges do not mutate existing staging keys.
  - Fresh restaging prunes stale manual literal choices, event/evidence-backed choices, and source identity choices.
  - Terminal restaging follows the decision table, including exact replay reuse, successor creation, and retryable separation conflicts.
  - Compatible terminal fresh evidence is persisted through run-evidence provenance without active-link mutation on the terminal cluster.
  - Terminal exact replay/reuse finds provenance-only evidence through run-evidence provenance tables.
  - Run-evidence provenance tables enforce idempotent duplicate inserts and expected foreign-key behavior.
  - Auto-resolution preserves live side effects for `canonical_exact_match` and `unanimous_duplicate`.
  - Auto-resolution is transactionally idempotent across reruns.
  - Resolution snapshots are immutable and contain the expected cluster/live mutation context.
  - Title repair stages clusters only for supporting/conflict cases.
- Web tests:
  - Cluster queue/detail/history routes still work.
  - Removed legacy routes return 404 after auth.
  - `/admin/review` POST returns 405.
  - Admin and import-run pages no longer render legacy review-group links or sections.
- Migration/docs tests:
  - Fresh migration from pre-cluster schema reaches the new current version.
  - Incompatible branch DB markers fail fast before normal migration.
  - Docs mention only event-review clusters and current flags.
- Validation:
  - `GOCACHE=/tmp/sheffield-live-gocache go test ./cmd/ingest ./internal/store/sqlite ./internal/web`
  - `GOCACHE=/tmp/sheffield-live-gocache go test ./...`
<!-- completed-by: CHUNK-9 -->

## Assumptions

- This branch has not shipped, so rewriting branch-only migrations and breaking branch-local report JSON is acceptable.
- Frozen historical `review_groups` tables may remain in older/base migrations and schema tests, but active runtime code treats them as inert.
- No new dependencies are needed.

## Retrospective Blocker Fixes

<!-- completed-by: CHUNK-10 -->

Implement this as the final cleanup chunk after CHUNK-9, e.g. CHUNK-10. It closes Lovelace's retrospective blockers and the selected hardening items: no keyless active clusters, no active legacy `review.GroupInput` repair coupling, no stale docs for removed legacy admin routes, terminal evidence immutability guarded centrally, and no stale historical duplicate review-group report fields.

### Deterministic Staging Keys

- `StageEventReviewEvidence` must reject missing or half-populated `StagingKey` / `StagingKeyVersion` before evidence lookup, existing-evidence matching, terminal replay, compatible terminal evidence reuse, or cluster matching.
- Exact replay and compatible terminal evidence still require the caller's deterministic creation key, even though those paths do not mutate active cluster membership.
- Database nullable-key tolerance may remain only for frozen historical/schema compatibility; active store APIs must not create null-key clusters.
- Audit every production `createEventReviewClusterTx` call site and assert active call sites pass non-nil deterministic staging keys.
- Tests that currently stage without keys must either use deterministic keys produced by the same production helper/path as the caller being tested, or assert rejection for invalid direct API input. Do not use arbitrary fixture-only staging keys to satisfy the invariant.

### Repair DTO Cleanup

- Replace title and description repair use of `ReviewGroupsFromReportWithCatalog` with cluster-native or repair-specific DTO flow based on `ingest.ReviewStageClusterInput`.
- Production code must have zero references to `ReviewGroupsFromReport`, `ReviewGroupsFromReportWithCatalog`, or `review.GroupInput` after this chunk.
- Delete `internal/ingest/review_stage_legacy.go` if it has no remaining production caller; otherwise make it production-inert and limit any remaining references to tests proving removal or frozen historical comments.
- If no production caller remains, delete the `review.GroupInput` type itself; otherwise document the exact frozen/test-only reason it remains.
- `review.CandidateInput` remains allowed where already used by cluster-native inputs; the scope is removing legacy group DTO coupling, not rewriting candidate representation.
- Keep shared candidate/event helper logic only where it is DTO-neutral or cluster-native.

### Docs Contract Cleanup

- Update docs that still describe `/admin/legacy-review*` as read-only audit routes.
- State current behavior: legacy review routes return 404 after auth; `/admin/review` is the event-review cluster queue; history/detail live under `/admin/event-review/...`.
- Replace “legacy singleton audit records” wording with event-review cluster terminology.

### Terminal Evidence Hardening

- Add a central guard before evidence mutation helpers can mutate an existing evidence row.
- Treat evidence as terminal-linked if it appears via active `event_review_cluster_evidence` on a terminal cluster or via import/repair run-evidence provenance for a terminal cluster.
- Terminal-linked evidence must not update `payload`, `source_id`, `event_id`, `updated_at`, or evidence identity-key links in place.
- Exact terminal replay may return the existing row without touching immutable fields; changed terminal material must use the revision-fingerprint path.
- The immutability guard must apply to `upsertEventReviewEvidenceTx`, `fillEventReviewEvidenceEventIDTx`, `linkEventReviewEvidenceIdentityKeysTx`, or their successor helpers.

### Historical Duplicate Report Cleanup

- Remove `review_groups_created` and `review_groups_reused` from `HistoricalDuplicateRepairReport`; do not rename them unless a real replacement consumer is discovered during implementation.
- Do not reintroduce removed CLI exposure or old review-group report naming.

### Retrospective Fix Tests

- `StageEventReviewEvidence` rejects missing key/version pairs before evidence or terminal matching.
- Import review staging, terminal replay, compatible terminal evidence, title repair, and historical duplicate repair all pass deterministic production-derived staging keys.
- No active path can create an event-review cluster with a null staging key.
- Terminal evidence exact replay does not mutate immutable evidence columns or identity-key links.
- Changed terminal material creates revision evidence rather than updating terminal-linked evidence.
- Title and description repair no longer reference or depend on `review.GroupInput`; `review.CandidateInput` remains valid where used by cluster-native inputs.
- Historical duplicate repair reports omit stale `review_groups_created` and `review_groups_reused` fields.
- Static checks:
  - production code has no references to `ReviewGroupsFromReport`, `ReviewGroupsFromReportWithCatalog`, or `review.GroupInput`;
  - remaining mentions are limited to deleted files, tests proving removal, or frozen historical schema comments;
  - no docs advertise `/admin/legacy-review*` as a live/read-only feature;
  - no active report structs expose `review_groups_created` or `review_groups_reused` except frozen migration/schema fixtures if any.
- Validation:
  - `GOCACHE=/tmp/sheffield-live-gocache go test -buildvcs=false -count=1 ./cmd/ingest ./internal/store/sqlite ./internal/web`
  - `GOCACHE=/tmp/sheffield-live-gocache go test -buildvcs=false -count=1 ./...`
  - Full suite may require unsandboxed local port binding for `httptest`.
