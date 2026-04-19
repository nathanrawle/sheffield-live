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

## Routes

- `/`
- `/events`
- `/events/{slug}`
- `/venues`
- `/venues/{slug}`
- `/healthz`

Static CSS is served from `/static/site.css`.

## Repo layout

- `cmd/web` - entrypoint
- `internal/domain` - core data types
- `internal/store` - in-memory seed store and store interface
- `internal/store/sqlite` - SQLite persistence adapter
- `internal/web` - HTTP server and templates
- `internal/web/static` - embedded static assets
- `docs` - phase 1 planning and editorial notes

## Next step

This increment now keeps the same route set while bootstrapping a SQLite database from the existing seed data. Later phases can add ingestion without changing the top-level route set.
