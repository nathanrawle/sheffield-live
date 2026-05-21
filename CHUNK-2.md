# CHUNK-2: Admin Review Surface Cleanup

## CURRENT_PLAN.md Coverage

- `/admin/review` remains the event-review cluster queue.
- `/admin/event-review/{id}` and `/admin/event-review/history` remain the only detail/history routes.
- `/admin/legacy-review*`, `/admin/review/history`, and `/admin/review/{id}` return 404 after auth.
- Non-GET `/admin/review` returns 405.
- Remove legacy admin links, import-run legacy review-group sections, `HasReviewStorage` affordances, legacy handlers/templates/tests.

## Affected Files

- `internal/web/server.go`
- `cmd/web/main.go`
- `internal/web/templates/admin.html`
- `internal/web/templates/admin_review.html`
- `internal/web/templates/admin_import_runs.html`
- `internal/web/templates/admin_import_run_detail.html`
- `internal/web/server_test.go`
- Legacy review templates under `internal/web/templates/`

## Implementation Plan

- Update web store interfaces and handlers so active admin routes use only event-review cluster APIs.
- Remove legacy review templates and legacy route behavior, keeping authenticated 404/405 semantics from the plan.
- Remove import-run detail legacy review-group rendering and admin navigation affordances.
- Rename visible UI wording that still says review group where it refers to active cluster work.
- Update focused web tests for live routes, removed routes, POST behavior, and import-run/admin pages.

## Ownership Notes

- This chunk owns web-level cleanup: `ReviewStore`, `ImportRunReviewGroupStore`, legacy handlers, legacy templates, legacy `PageData` fields, `cmd/web` wiring, and web tests.
- This chunk does not delete sqlite legacy APIs, legacy migrations, or store-level review-group functions; later chunks own store/migration cleanup.
- `review-group-*` CSS class names may stay if renaming them would create unrelated churn; visible UI wording should be cluster-native.

## Completion Criteria

- Active web runtime does not read or write legacy review-group store APIs.
- Legacy admin review detail/history routes are gone except for authenticated 404s.
- Web tests cover the intended route contract.
- Unauthenticated removed admin routes still require login; authenticated removed admin routes return 404.
- `/admin/legacy-review*`, `/admin/review/history`, `/admin/review/{id}`, query-string variants, malformed IDs, and trailing-slash variants return authenticated 404.
- Only non-GET `/admin/review` returns 405 with `Allow: GET`.
- Import-run list/detail pages render no legacy review-group column, section, or link even when legacy tables contain data.
- Removing `HasReviewStorage` does not hide unrelated venue/import/admin pages.
- `rg "legacy-review|HasReviewStorage|ImportRunReviewGroupStore|legacy review group" internal/web cmd/web` has no active hits.
- `rg "legacy-review|HasReviewStorage|ImportRunReviewGroupStore|ReviewStore|reviewStore|ListReviewGroups|LoadReviewGroup|legacy review group" internal/web cmd/web` has no active web-runtime hits.

## Validation

- `GOCACHE=/tmp/sheffield-live-gocache go test ./internal/web ./cmd/web`
