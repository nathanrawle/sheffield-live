# CHUNK-1: Development Scaffolding

## CURRENT_PLAN.md Coverage

- Split the work into manageable chunks.
- Record chunk status in `CURRENT_PLAN_CHUNKS.md`.
- Record chunk plans in `CHUNK-*.md`.
- Define the completion marker convention for `CURRENT_PLAN.md` and keep `IN_FLIGHT_REVISIONS.md` revision notes current.

## Affected Files

- `CURRENT_PLAN_CHUNKS.md`
- `CHUNK-1.md` through `CHUNK-8.md`
- `IN_FLIGHT_REVISIONS.md`

## Implementation Plan

- Create `CURRENT_PLAN_CHUNKS.md` with dependency-ordered chunks and status values.
- Create `CHUNK-1.md` through `CHUNK-8.md` with concise scope, validation, and completion criteria.
- Add `IN_FLIGHT_REVISIONS.md` as an initially empty change log for plan changes discovered during implementation.
- Use markdown comments in `CURRENT_PLAN.md` for completion markers, e.g. `<!-- completed-by: CHUNK-3 -->`; use invalidation comments that point to `IN_FLIGHT_REVISIONS.md` when a plan section changes.
- Commit the scaffolding once the planner agrees the chunk split is workable.

## Completion Criteria

- Tracking files exist and are understandable without rereading the whole plan.
- Chunk boundaries preserve dependency order and avoid obvious file ownership conflicts.
- No production code changes are made in this chunk.

## Validation

- `git diff --check`
- After staging scaffold files: `git diff --cached --check`
- `git status --short` confirms only scaffold files changed
