# Ingest CLI Simplification

## Summary

- Refactor `cmd/ingest` into a standard-library `flag` dispatcher with root live ingest, `replay`, and `fix` command families.
- Bare `go run ./cmd/ingest` runs all catalog sources and stages event reviews by default.
- Old top-level mode flags are removed with no compatibility shims.

## CLI And Behavior

- Root live ingest flags: `-source`, `-limit`, `-timeout`, `-db`, `-contact`, `-user-agent`, `-dry-run`.
- Track whether `-source` was explicitly set:
  - no `-source`: run all catalog sources
  - `-source leadmill`: run one source
- Root ingest stages event reviews by default, including single-source runs. `-dry-run` skips event-review staging only; live ingest still writes import runs, snapshots, and normal media metadata.
- Remove `-all-sources`, `-stage-event-reviews`, `-import-run-id`, `-repair-*`, `-apply-*`, `-backfill-image-focus`, and `-http-user-agent`.
- Support global `-db` before subcommands as a convenience, e.g. `go run ./cmd/ingest -db path replay 42`. All other subcommand flags must follow the subcommand. If global `-db` and command-local `-db` are both set differently, return an error.

## Subcommands

- `replay [flags] [id]`
  - Flags: `-db`, `-limit`, `-dry-run`, `-titles`, `-descriptions`.
  - Command flags must appear before optional ID: `replay -db path -dry-run 42`; `replay 42 -dry-run` fails.
  - No ID selects the latest finished import run by highest ID.
  - Positive ID selects that import run; negative IDs fail.
  - Failed selected runs exit with a clear error; no fallback to latest successful.
  - Normal replay stages event reviews by default; `-dry-run` skips staging.
  - `-titles` and `-descriptions` are mutually exclusive repair modes, apply by default, respect `-dry-run`, and do not also run normal staging.
- `fix titles`
  - Live title repair; flags: `-source`, `-limit`, `-timeout`, `-db`, `-contact`, `-user-agent`, `-dry-run`.
  - No `-source` runs all catalog sources; per-source failures continue then exit non-zero.
  - Does not configure image ingestion or write media metadata.
- `fix descriptions`
  - Live description repair for Sidney & Matilda and Cafe No. 9 by default; `-source` narrows.
  - Unsupported sources, such as `-source leadmill`, fail early with a clear error.
  - Same live flags and batch failure behavior as `fix titles`.
  - Does not configure image ingestion or write media metadata.
- `fix historical-duplicates`
  - DB-only repair; flags: `-db`, `-dry-run`.
  - Uses the store default/max window; no window flag.
- `fix image-focus`
  - DB/media repair; flags: `-db`, `-media-root`, `-dry-run`.
  - `-media-root` defaults to `MEDIA_ROOT` or `./data/media`.
  - Non-zero exit only when the report's `errors` list is non-empty. Missing files, defaulted images, unsupported/no-signal cases, and decode failures remain report counters.

## Implementation Notes

- Add `LatestFinishedImportRun(ctx)` ordered by `id DESC` with non-empty `finished_at`; leave `LatestSuccessfulImport` unchanged.
- Keep `ReplayImportRunWithCatalog` snapshot-based and network-free; add preflight status handling so failed selected runs produce the intended clear CLI error.
- Add apply/dry-run support to description repair and image-focus repair.
- Description repair dry-runs create no `repair_runs`. Apply runs create/finalize a repair-run audit row only when at least one description is changed; report the `repair_run_id` when created.
- Use one shared batch executor only for source-iterating commands: root all-source ingest, `fix titles`, and `fix descriptions`.
- Update command docs/help to remove old flags and document strict subcommand flag placement.

## Output Contract

- Single-source root ingest and `replay` always emit a wrapped object with `report`.
- Batch source commands always emit `{ "results": [...] }`, including source-filtered fix commands for consistency.
- Staging output always appears as `review_stage` for ingest/replay outputs. When `-dry-run` skips staging, emit an explicit disabled object such as `{"enabled": false, "applied": false}`.
- Repair outputs use `title_repair`, `description_repair`, `historical_duplicate_repair`, or `image_focus`, each with `dry_run` and `applied` where applicable.

## Test Plan

- Parser tests for command dispatch, explicit `-source` tracking, removed flags, `-user-agent`, global `-db`, command-local `-db`, `-dry-run`, `replay`, and all `fix` subcommands.
- Malformed command tests for `fix`, `fix nope`, `replay 42 -db path`, negative replay IDs, extra replay args, and unsupported `fix descriptions -source leadmill`.
- Root run tests proving bare ingest runs all sources and stages by default; `-source` runs one source and also stages.
- Replay tests for latest-finished-by-ID selection, absolute ID selection, failed selected-run error, flags-before-ID parsing, and repair-mode mutual exclusion.
- Repair tests for apply-by-default and dry-run behavior across titles, descriptions, historical duplicates, and image focus.
- Batch tests proving source-iterating fixes continue after per-source failures, emit all results, and exit non-zero.
- Run focused tests first, then `GOCACHE=/tmp/sheffield-live-gocache go test -count=1 ./...` because store repair changes can affect admin/web behavior.

## Assumptions

- `-dry-run` means "skip event-review or repair writes," not "perform zero database writes."
- User-facing docs/help say "event reviews"; existing JSON names such as `review_stage` remain.
- No new CLI dependency is introduced.
