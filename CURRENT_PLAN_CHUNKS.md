# Current Plan Chunks

Status values: `planned`, `in_review`, `implementing`, `completed`, `invalidated`.

Dependency notes:

- CHUNK-1 establishes tracking only.
- CHUNK-2 and CHUNK-3 can proceed in parallel after CHUNK-1 because they own disjoint web vs CLI/report files.
- CHUNK-4 should land before CHUNK-5/6 because staging and provenance behavior depends on the final migration shape.
- CHUNK-5 owns open-cluster staging keys, merge, pruning, and cluster-native DTO replacement.
- CHUNK-6 owns terminal restaging, terminal successor creation keys, evidence immutability, and run-evidence provenance wiring.
- CHUNK-3 owns CLI/report field names and zero/empty output contract; CHUNK-7 owns producing real duplicate auto-resolution rows/counts.
- CHUNK-8 sweeps title repair docs and report output.
- CHUNK-9 was added after broad validation exposed remaining active legacy review-group store APIs/tests not covered by earlier chunks.
- CHUNK-10 closes Lovelace's retrospective blockers after CHUNK-9 and owns the final deterministic-key, repair DTO, docs, immutability, and report-field cleanup.

Main-thread/shared-file rule:

- `CURRENT_PLAN.md`, `CURRENT_PLAN_CHUNKS.md`, `IN_FLIGHT_REVISIONS.md`, and chunk plan files are updated by the orchestrator.
- Shared implementation files are assigned to only one active implementer at a time.

| Chunk | Status | CURRENT_PLAN.md area | Very brief scope |
| --- | --- | --- | --- |
| [CHUNK-1](CHUNK-1.md) | completed | Development scaffolding | Create orchestration/tracking docs and agree chunk boundaries. |
| [CHUNK-2](CHUNK-2.md) | completed | Admin review surface; active code cleanup | Remove legacy admin review routes/templates/interfaces and import-run legacy review-group UI. |
| [CHUNK-3](CHUNK-3.md) | completed | CLI/reporting surface; report/log contract | Remove old flags and rename active ingest reports/logs to cluster terminology. |
| [CHUNK-4](CHUNK-4.md) | completed | Migrations; legacy runtime cleanup | Renumber branch migrations, add fail-fast checks, and remove legacy startup backfills/validators. |
| [CHUNK-5](CHUNK-5.md) | completed | Cluster behavior; staging keys; choice pruning | Make open staging cluster-native, deterministic, merge/prune correctly, and stop using `review.GroupInput`. |
| [CHUNK-6](CHUNK-6.md) | completed | Evidence policy; terminal restaging; run-evidence provenance | Add run-evidence provenance and enforce terminal evidence immutability/revision behavior. |
| [CHUNK-7](CHUNK-7.md) | completed | Duplicate auto-resolution | Produce real `canonical_exact_match`/`unanimous_duplicate` cluster auto-resolution rows and effects. |
| [CHUNK-8](CHUNK-8.md) | completed | Title repair; docs; final validation | Finish title repair cluster reporting, docs cleanup, broad tests, and plan completion marks. |
| [CHUNK-9](CHUNK-9.md) | completed | Active legacy review-group store cleanup | Retire remaining active legacy review-group store APIs/tests exposed by broad validation. |
| [CHUNK-10](CHUNK-10.md) | completed | Retrospective Blocker Fixes | Enforce keyed active staging, remove legacy repair DTOs, harden terminal evidence immutability, and clean stale docs/report fields. |
