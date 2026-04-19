# Sheffield Live

Sheffield Live is a small server-rendered Go project for browsing live music in Sheffield.

## What is in this slice

- a single Go web app
- in-memory seed data for a few venues and events, including source/freshness fields
- seed, test, and development records are visibly labeled on record surfaces; live records are not
- server-rendered pages for the home page, events, event details, venues, venue details, and health checks
- embedded templates and static CSS
- no external dependencies
- no HTMX yet

## Run it

```bash
go run ./cmd/web
```

The app listens on `:8080` by default. Set `ADDR` to override it.

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
- `internal/store` - in-memory seed store
- `internal/web` - HTTP server and templates
- `internal/web/static` - embedded static assets
- `docs` - phase 1 planning and editorial notes

## Next step

This increment is intentionally seed-data only. Phase 2 can replace the store with persistence and ingestion without changing the top-level route set.
