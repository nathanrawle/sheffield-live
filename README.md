# Sheffield Live

Sheffield Live is a small server-rendered Go project for browsing live music in Sheffield.

## What is in this slice

- a single Go web app
- SQLite-backed persistence with seed-data bootstrap on a fresh database
- seed data for a few venues and events, including source/freshness fields
- seed, test, and development records are visibly labeled on record surfaces; live records are not
- server-rendered pages for the home page, events, event details, venues, venue details, and health checks
- embedded templates and static CSS
- no HTMX yet

## Run it

```bash
go run ./cmd/web
```

The app listens on `:8080` by default. Set `ADDR` to override it.
Set `DB_PATH` to change the SQLite database path. It defaults to `./data/sheffield-live.db`, so `./data` must be writable when you use the default. The path you choose must point to writable storage because the app bootstraps and updates the database on disk.

## Manual ingestion

Phase 4A includes a manual snapshot and parse command for Sidney & Matilda:

```bash
go run ./cmd/ingest -user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

The command fetches the Sidney & Matilda source page, stores a JSON snapshot envelope in SQLite, extracts bounded "Google Calendar ICS" links, snapshots each fetched ICS body, parses event candidates/skips/errors, finishes the import run, and prints a JSON report to stdout.

It does not write public venue or event records. It only writes to `sources`, `import_runs`, and `snapshots`.

To also stage candidate review groups after a successful manual ingest:

```bash
go run ./cmd/ingest -user-agent "sheffield-live manual ingest (contact: you@example.com)" -stage-review
```

This creates review groups for likely duplicate clusters and singleton new listings. It still does not publish public event rows.

Flags:

- `-source` defaults to `sidney-and-matilda`
- `-limit` defaults to `20` and must be between `1` and `50`
- `-timeout` defaults to `10s`
- `-user-agent` is required
- `-db` overrides `DB_PATH`, which otherwise falls back to `./data/sheffield-live.db`
- `-stage-review` stages duplicate clusters and singleton new listings into `/admin/review` after a successful ingest

To create an offline review group from a local ICS fixture without network access:

```bash
go run ./cmd/ingest -review-fixture internal/ingest/testdata/sidney.ics
```

Review groups can be inspected at `/admin/review`. Duplicate reviews store field-level draft choices; new-listing reviews accept or reject the sole candidate. Neither flow publishes public event rows yet.

## Routes

- `/`
- `/events`
- `/events/{slug}`
- `/venues`
- `/venues/{slug}`
- `/admin/review`
- `/admin/review/{groupID}`
- `/healthz`

Static CSS is served from `/static/site.css`.

## Repo layout

- `cmd/web` - entrypoint
- `cmd/ingest` - manual ingestion entrypoint
- `internal/domain` - core data types
- `internal/ingest` - manual snapshot, ICS extraction, parsing, and reporting
- `internal/store` - in-memory seed store and store interface
- `internal/store/sqlite` - SQLite persistence adapter
- `internal/web` - HTTP server and templates
- `internal/web/static` - embedded static assets
- `docs` - phase 1 planning and editorial notes

## Next step

This increment now keeps the same route set while bootstrapping a SQLite database from the existing seed data. Later phases can add ingestion without changing the top-level route set.
