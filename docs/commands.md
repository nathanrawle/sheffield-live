# Command Reference

This repository has one Go monolith and two entrypoints:

- `./cmd/web` serves the site
- `./cmd/ingest` performs manual Sidney & Matilda ingestion and review staging

## `./cmd/web`

Run:

```bash
go run ./cmd/web
```

Environment:

- `ADDR` defaults to `:8080`
- `DB_PATH` defaults to `./data/sheffield-live.db`

Behavior:

- opens and bootstraps the SQLite database on startup
- serves server-rendered HTML
- uses `modernc.org/sqlite`
- requires writable storage for the database path

Routes:

- `GET /` home page
- `GET /events` event list
- `GET /events/{slug}` event detail
- `GET /venues` venue list
- `GET /venues/{slug}` venue detail
- `GET /admin/review` open review queue
- `GET /admin/review/{groupID}` review detail
- `POST /admin/review/{groupID}` review actions
- `GET /healthz` plain-text health check
- `GET /static/site.css` embedded stylesheet

`/events` query parameters:

- `window=all|today|week`
- `venue={venue-slug}`

`/admin/review` and `/admin/review/{groupID}` flash query parameters:

- `saved=1`
- `resolved=1`
- `accepted=1`
- `rejected=1`

Review behavior:

- duplicate groups use field-by-field draft choices and a canonical draft summary
- `action=save` stores draft choices for duplicate groups
- `action=resolved` confirms a duplicate and resolves it, publishing one canonical public event
- singleton groups use accept/reject actions
- `action=accept` resolves a singleton group and publishes one canonical public event
- `action=rejected` rejects a duplicate or singleton group without publishing
- closed groups are read-only and disappear from the open queue

## `./cmd/ingest`

Run:

```bash
go run ./cmd/ingest -user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Default behavior:

- `-source` defaults to `sidney-and-matilda`
- `-limit` defaults to `20`
- `-timeout` defaults to `10s`
- `-db` overrides `DB_PATH`
- `-user-agent` is required unless `-review-fixture` is set

Validation:

- `-limit` must be between `1` and `50`
- `-timeout` must be positive
- `-user-agent` must be non-empty in networked mode

Manual ingest behavior:

- fetches the Sidney & Matilda source page
- snapshots the source page and fetched ICS payloads
- parses candidates, skips, and errors
- writes `sources`, `import_runs`, and `snapshots`
- prints a JSON report to stdout

`-stage-review`:

- wraps the ingest report with `review_stage`
- creates duplicate review groups
- creates singleton review groups
- only runs after a successful ingest

`-review-fixture`:

- reads a local ICS file
- does not use the network
- parses candidates, skips, and errors
- creates one offline review group
- prints a JSON report with the fixture path, group ID, candidate count, skips, and errors

`-review-title`:

- sets the review-group title used with `-review-fixture`
