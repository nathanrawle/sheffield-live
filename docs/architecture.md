# Architecture

## Overview

This project is one repository with one deployable Go application. The current slice uses a SQLite store, and the package boundaries are intended to let Phase 2 add ingestion without changing the public route shape.

The codebase is split into a few small packages:

- `cmd/web` starts the HTTP server
- `internal/domain` holds the core venue and event types
- `internal/store` keeps the in-memory seed data and lookups
- `internal/store/sqlite` opens the SQLite database, bootstraps seed data on a fresh database, and keeps the read-only store interface
- `internal/web` renders HTML and handles routing
- `internal/web/static` holds embedded CSS

The app currently uses SQLite via `modernc.org/sqlite` for persistence.
`DB_PATH` must point to writable storage because the app bootstraps and updates the database on disk. If you keep the default path, `./data` must be writable.

## Request flow

1. `cmd/web` opens the SQLite-backed store.
2. `internal/web` loads embedded templates and CSS.
3. The server dispatches by path.
4. The page fragment renders first.
5. The shared layout wraps that fragment.

## Rendering

Rendering uses `html/template` from the standard library. The layout and the page fragment are separate templates so the shared shell stays consistent while the page body remains specific to each route.

## Data model

- `Venue` stores slug, name, address, neighbourhood, description, and website.
- `Event` stores slug, name, venue slug, UTC times, genre, status, description, source name, source URL, and last checked time.

Times are stored as UTC and rendered for `Europe/London`.

## Phase 2 notes

The current store is backed by SQLite but still exposes the same read-only lookup methods. The next step is to add ingestion behind that boundary while preserving raw source/provenance data before normalization.
