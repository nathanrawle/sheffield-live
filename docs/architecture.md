# Architecture

## Overview

This project is one repository with one deployable Go application. The current slice uses in-memory data, but the package boundaries are intended to let Phase 2 add persistence without changing the public route shape.

The codebase is split into a few small packages:

- `cmd/web` starts the HTTP server
- `internal/domain` holds the core venue and event types
- `internal/store` keeps the in-memory seed data and lookups
- `internal/web` renders HTML and handles routing
- `internal/web/static` holds embedded CSS

The app currently uses the Go standard library only.

## Request flow

1. `cmd/web` builds the seed store.
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

The current store is intentionally in-memory and fixed. Phase 2 should introduce a persistent store behind the same lookup methods before adding live ingestion. Source adapters should stay outside the web package and should preserve raw source/provenance data before normalization.
