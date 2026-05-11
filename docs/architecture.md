# Architecture

## Overview

Sheffield Live is a single Go monolith. It serves server-rendered HTML from one SQLite-backed application and keeps the public browsing flow and the manual ingestion flow in the same repository.

`cmd/web` starts the site. `cmd/ingest` handles manual ingestion and review staging. Both binaries load the source catalog from `config/sources` on startup. Genre inference defaults live in `config/genres.yaml` and are synced into SQLite for runtime admin edits.

## Packages

- `cmd/web` starts the HTTP server
- `cmd/ingest` runs manual ingestion and optional review staging
- `internal/domain` defines shared venue, event, and origin types
- `internal/ingest` loads the source catalog, fetches source pages, dispatches to bounded parser/extractor families, and stages review groups from ingest reports
- `internal/review` defines review group, candidate, default-choice, and draft-choice types
- `internal/store` provides the bootstrap-store implementation and catalog interface
- `internal/store/sqlite` opens SQLite, runs migrations, bootstraps curated baseline venues, and implements persistence
- `internal/web` routes requests and renders pages
- `internal/web/static` embeds `site.css`
- `internal/web/templates` embeds HTML templates

## Runtime

The app uses SQLite through `modernc.org/sqlite`.

`ADDR` defaults to `:8080`.
`DB_PATH` defaults to `./data/sheffield-live.db`.
`MEDIA_ROOT` defaults to `./data/media`.
`MEDIA_URL_PREFIX` defaults to `/media`.
`LOG_LEVEL` defaults to `info` and accepts `debug`, `info`, `warn`, or `error`.
`LOG_FORMAT` defaults to `text` and accepts `text` or `json`.

The database path must point to writable storage because the application creates or updates the SQLite file on startup.
The media root must point to writable storage when ingest copies event images. The current implementation stores files locally for development; the storage interface is intentionally small so a later cloud bucket implementation can replace it without changing parsers or review publishing.
Both entrypoints use standard-library structured logging to stderr. `cmd/web` logs startup, requests, readiness failures, and internal errors. `cmd/ingest` keeps stdout reserved for JSON reports and logs lifecycle summaries to stderr.

The source catalog path is fixed to the repository `config/sources` directory in v1. It is not a runtime flag yet.

## Routes

- `/`
- `/events`
- `/events/{slug}`
- `/venues`
- `/venues/{slug}`
- `/admin`
- `/admin/review`
- `/admin/review/history`
- `/admin/review/{groupID}`
- `/admin/import-runs`
- `/admin/import-runs/{id}`
- `/admin/venues`
- `/admin/venues/{slug}`
- `/admin/configuration`
- `/healthz`
- `/readyz`
- `/static/site.css`
- `/media/{path}` when `MEDIA_ROOT` is configured

## Request Flow

1. `cmd/web` loads the source catalog and opens the SQLite store with source-metadata lookup support.
2. `cmd/web` validates the opened store and passes explicit `internal/web.ServerDeps`.
3. `internal/web` loads templates and embedded CSS.
4. The router matches the request path.
5. The page-specific template renders.
6. The shared layout wraps the page body.

## Data Model

Public records live in SQLite and are served from canonical `venues` and `events` rows.

- `Venue` stores slug, name, address, neighbourhood, description, website, validation state, coverage kind, coverage note, and origin
- `Event` stores slug, name, venue slug, a required UTC start time, an optional UTC end time, top-two genre summary, status, description, copied image URL/source/alt/dimensions/focus, source name, source URL, last checked time, and origin

Raw ingest snapshots, import runs, and review records are stored separately from canonical public events.
Review persistence also stores canonical snapshot rows alongside staged candidates, persists source-derived venue evidence (`venue_text`, `venue_location_raw`), and keeps majority defaults separate from reviewer-edited draft choices. For ICS sources, `venue_location_raw` is parsing evidence rather than a display string: it keeps the unfolded raw `LOCATION` text so later venue derivation can decode ICS escapes before applying the normal comma/newline split.
Review resolution can also persist secondary-source `genre` and `description` rows linked back to the canonical event without changing the canonical public schema. Authoritative resolution reconciles the secondary rows supplied with that authoritative decision. Non-authoritative resolution upserts matching secondary candidates as cumulative evidence, so an omitted source in a later accepted review does not delete previously stored source information. A staged candidate matches the accepted event for secondary evidence when venue slug and start time match and the title matches after case and whitespace normalization.
Inferred genres are stored as ranked `event_genres` rows. Ranking is calculated across the canonical description plus persisted secondary-source descriptions, using a balanced score from mention frequency and earliest match position. Public summary cards still read `events.genre`, which is refreshed as the top two inferred genres.
Copied image assets are tracked separately by source URL and storage path, including a best-effort focus point for cropped display, so replay can reuse previously copied files without fetching the image again.

The admin UI exposes a landing page, read-only review history, import history, provisional venue queue/detail pages, genre configuration, and per-run snapshot metadata when the backing store implements those read paths. Public venue/event pages and admin provisional venue pages display normalized multiline addresses, including dropping an address first line that duplicates the venue name. The provisional venue queue lists only provisional venues. Detail pages remain read-only when venue admin writes are unavailable, and show save/validate controls only when the backing store exposes provisional venue write capability. The review history lists the 50 newest resolved and rejected review groups. The per-run view renders import run summary fields and decoded snapshot envelope metadata only; raw snapshot payload JSON and response bodies are not rendered.

When the backing store also exposes secondary-source event info, the public event detail page can render alternate `genre` and `description` values grouped by secondary source without altering the canonical event record. Public event cards render an available image on the right, and event detail renders landscape images as a top hero or tall images beside the description on wide screens.

## Data Lifecycle

Raw source snapshots feed review groups, and review resolution publishes canonical public events.

- raw snapshots capture fetched source pages and any source-specific secondary payloads such as ICS feeds
- live ingest extracts source image URLs where the parser can identify event artwork, copies supported remote images into local media storage, estimates a best-effort image focus point for cropped display, and stores image metadata on review candidates
- image-copy failures are non-fatal ingest warnings; candidates still stage or publish without an image
- replay uses existing image-asset metadata by source URL instead of fetching remote image bytes
- source metadata and ingest runtime selection come from repo-backed YAML catalog files
- repo-backed genre defaults are synced into SQLite, where admin changes take precedence and trigger event-genre recomputation
- replay and review identity depend on stable source identity fields: `key`, `name`, `url`, and `review_stage_source_name`
- `review_groups.staging_key` has a unique index so staged reruns reuse the same group when the content key matches
- `import_run_review_groups` records every persisted import-run to review-group link with link time
- review groups may also persist an authoritative source tuple when every staged candidate agrees on one owned-venue source event identity
- review group summaries derive shared venue context from deterministic matching over candidate venue slug, venue text, and raw location evidence
- duplicate groups may persist an attached live canonical snapshot row and separate majority defaults
- duplicate groups may stay in review or auto-resolve into closed history when they are exact canonical matches or unanimous staged duplicates
- singleton new listings may either auto-promote or stay in review
- any singleton may be attempted for auto-promotion when it is the first matching live record seen
- provisional venue creation derives address and neighbourhood from source-derived venue evidence, dropping duplicate venue-name address lines and recognizing Sheffield district names in the address
- exact staged `venue_slug` matches take precedence over conflicting venue-text or raw-location heuristics during canonical venue resolution
- ICS venue parsing preserves raw `LOCATION` evidence, decodes ICS escapes, and then derives venue identity using the normal comma/newline split
- new provisional venue slugs and names prefer that derived location-head venue name over the full generic ICS `LOCATION` string
- authoritative source identity controls overwrite rights and can upgrade a provisional event in place
- resolving a duplicate or accepting a singleton publishes one canonical public event in the same transaction
- manual review resolution first canonicalizes the selected venue to an existing venue when deterministic evidence matching yields one unique live venue
- newly created staged groups can create a `provisional` live venue row immediately when staged venue evidence is uniquely new
- reused open review groups can backfill a missing provisional venue row only when previously blank stored venue evidence is refreshed with usable raw venue evidence
- non-authoritative singleton auto-promotion can create a `provisional` live venue row immediately when singleton venue evidence is uniquely new
- when manual review resolution finds no unique existing venue match, it inserts a `provisional` live venue in the same transaction and publishes the event against that venue if an earlier flow has not already created it
- admins can later edit a provisional venue in place and flip it to `validated` from the provisional venue detail page without changing linked public events
- venue closure or retirement should use a future lifecycle state separate from `origin` and `validation_state`
- ambiguous venue evidence fails closed and rolls back the review resolution transaction
- authoritative review groups resolve through durable `event_source_links` identity before any slug-based fallback
- authoritative identity takes precedence over canonical slug attachment when they disagree
- review groups may also persist secondary-source `genre` and `description` rows keyed by secondary source plus candidate venue, name, and start time
- non-authoritative secondary-source rows are cumulative: repeated rows from the same source/event identity overwrite non-empty values, but an absent source in a later accepted review does not delete earlier evidence
- supporting singleton auto-promotion creates `provisional` live events, does not create authoritative `event_source_links`, and does not create `event_secondary_source_info` rows
- successful supporting singleton auto-promotion resolves matching stale open singleton groups by `staging_key` and links the current import run to those groups
- later supporting matches can fill blank live fields but conflicting populated fields stay in review
- rejecting a review does not publish
- the source row is ensured
- the published event uses live origin
- canonical `events.end_at` may be `NULL` when the authoritative end time is unknown
- canonical event images are copied asset URLs, not hotlinks to source sites
- the live slug is deterministic and derived from name, venue, and UTC time
- canonical-backed duplicate resolution can update a matched live event in place and rejects slug collisions with other event IDs
- venue coverage semantics are data-backed; most venues are full-venue coverage, while The Lescar is marked program-only with a UI note even when Jazz at The Lescar singletons auto-publish

## Visibility

Test and development records are visible in the UI through their origin labels.
Curated bootstrap venues and live records are not tagged.
