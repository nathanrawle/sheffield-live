# Architecture

## Overview

Sheffield Live is a single Go monolith. It serves server-rendered HTML from one SQLite-backed application and keeps the public browsing flow and the manual ingestion flow in the same repository.

`cmd/web` starts the site. `cmd/ingest` handles manual ingestion and review staging.

## Packages

- `cmd/web` starts the HTTP server
- `cmd/ingest` runs manual ingestion and optional review staging
- `internal/domain` defines shared venue, event, and origin types
- `internal/ingest` fetches source pages, runs source-specific extraction and parsing, and stages review groups from ingest reports
- `internal/review` defines review group and draft-choice types
- `internal/store` provides the seed-store implementation and catalog interface
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
- `/admin/review/history`
- `/admin/review/{groupID}`
- `/admin/import-runs`
- `/admin/import-runs/{id}`
- `/healthz`
- `/readyz`
- `/static/site.css`

## Request Flow

1. `cmd/web` opens the SQLite store.
2. `cmd/web` validates the opened store and passes explicit `internal/web.ServerDeps`.
3. `internal/web` loads templates and embedded CSS.
4. The router matches the request path.
5. The page-specific template renders.
6. The shared layout wraps the page body.

## Data Model

Public records live in SQLite and are served from canonical `venues` and `events` rows.

- `Venue` stores slug, name, address, neighbourhood, description, website, coverage kind, coverage note, and origin
- `Event` stores slug, name, venue slug, a required UTC start time, an optional UTC end time, genre, status, description, source name, source URL, last checked time, and origin

Raw ingest snapshots, import runs, and review records are stored separately from canonical public events.
Authoritative review resolution can also persist secondary-source `genre` and `description` rows linked back to the canonical event without changing the canonical public schema.

The admin UI exposes read-only review history, import history, and per-run snapshot metadata when the backing store implements those read paths. The review history lists the 50 newest resolved and rejected review groups. The per-run view renders import run summary fields and decoded snapshot envelope metadata only; raw snapshot payload JSON and response bodies are not rendered.
When the backing store also exposes secondary-source event info, the public event detail page can render alternate `genre` and `description` values grouped by secondary source without altering the canonical event record.

## Data Lifecycle

Raw source snapshots feed review groups, and review resolution publishes canonical public events.

- raw snapshots capture fetched source pages and any source-specific secondary payloads such as ICS feeds
- `review_groups.staging_key` has a unique index so staged reruns reuse the same group when the content key matches
- `import_run_review_groups` records every persisted import-run to review-group link with link time
- review groups may also persist an authoritative source tuple when every staged candidate agrees on one owned-venue source event identity
- duplicate groups always stay in review; singleton new listings may either auto-promote or stay in review
- singleton auto-promotion can happen through authoritative owned-source identity or through configured non-authoritative slug-absent publish
- resolving a duplicate or accepting a singleton publishes one canonical public event in the same transaction
- authoritative review groups resolve through durable `event_source_links` identity before any slug-based fallback
- authoritative review groups may also persist secondary-source `genre` and `description` rows keyed by secondary source plus candidate venue, name, and start time
- non-authoritative singleton auto-promotion is insert-only, does not create authoritative `event_source_links`, and does not create `event_secondary_source_info` rows
- successful non-authoritative singleton auto-promotion resolves matching stale open singleton groups by `staging_key` and links the current import run to those groups
- rejecting a review does not publish
- the venue must already exist
- the source row is ensured
- the published event uses live origin
- canonical `events.end_at` may be `NULL` when the authoritative end time is unknown
- the live slug is deterministic and derived from name, venue, and UTC time
- slug conflicts are handled with upsert semantics
- venue coverage semantics are data-backed; most venues are full-venue coverage, while The Lescar is marked program-only with a UI note even when Jazz at The Lescar singletons auto-publish

## Visibility

Seed, test, and development records are visible in the UI through their origin labels.
Live records are not tagged.
