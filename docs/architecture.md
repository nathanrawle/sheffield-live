# Architecture

## Overview

Sheffield Live is a single Go monolith. It serves server-rendered HTML from one SQLite-backed application and keeps the public browsing flow and the manual ingestion flow in the same repository.

`cmd/web` starts the site. `cmd/ingest` handles manual ingestion and review staging.

## Packages

- `cmd/web` starts the HTTP server
- `cmd/ingest` runs manual ingestion and optional review staging
- `internal/domain` defines shared venue, event, and origin types
- `internal/ingest` fetches Sidney & Matilda pages, extracts ICS links, parses ICS data, and stages review groups from ingest reports
- `internal/review` defines review group and draft-choice types
- `internal/store` provides the seed-store implementation and read-only store interface
- `internal/store/sqlite` opens SQLite, runs migrations, bootstraps seed data, and implements persistence
- `internal/web` routes requests and renders pages
- `internal/web/static` embeds `site.css`
- `internal/web/templates` embeds HTML templates

## Runtime

The app uses SQLite through `modernc.org/sqlite`.

`ADDR` defaults to `:8080`.
`DB_PATH` defaults to `./data/sheffield-live.db`.

The database path must point to writable storage because the application creates or updates the SQLite file on startup.

## Routes

- `/`
- `/events`
- `/events/{slug}`
- `/venues`
- `/venues/{slug}`
- `/admin/review`
- `/admin/review/{groupID}`
- `/admin/import-runs`
- `/healthz`
- `/static/site.css`

## Request Flow

1. `cmd/web` opens the SQLite store.
2. `internal/web` loads templates and embedded CSS.
3. The router matches the request path.
4. The page-specific template renders.
5. The shared layout wraps the page body.

## Data Model

Public records live in SQLite and are served from canonical `venues` and `events` rows.

- `Venue` stores slug, name, address, neighbourhood, description, website, and origin
- `Event` stores slug, name, venue slug, UTC start and end times, genre, status, description, source name, source URL, last checked time, and origin

Raw ingest snapshots, import runs, and review records are stored separately from canonical public events.

The admin UI exposes read-only import history when the backing store implements it.

## Data Lifecycle

Raw source snapshots feed review groups, and review resolution publishes canonical public events.

- raw snapshots capture fetched source pages and ICS payloads
- `review_groups.staging_key` has a unique index so staged reruns reuse the same group when the content key matches
- review groups hold duplicate clusters or singleton new listings
- resolving a duplicate or accepting a singleton publishes one canonical public event in the same transaction
- rejecting a review does not publish
- the venue must already exist
- the source row is ensured
- the published event uses live origin
- the live slug is deterministic and derived from name, venue, and UTC time
- slug conflicts are handled with upsert semantics

## Visibility

Seed, test, and development records are visible in the UI through their origin labels.
Live records are not tagged.
