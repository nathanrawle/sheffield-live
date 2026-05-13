# Command Reference

This repository has one Go monolith and two entrypoints:

- `./cmd/web` serves the site
- `./cmd/ingest` performs manual venue-source ingestion and review staging

## `./cmd/web`

Run:

```bash
ADMIN_AUTH_DISABLED=1 go run ./cmd/web
```

Environment:

- `ADDR` defaults to `:8080`
- `DB_PATH` defaults to `./data/sheffield-live.db`
- `MEDIA_ROOT` defaults to `./data/media`
- `MEDIA_URL_PREFIX` defaults to `/media`
- `ADMIN_PASSWORD_HASH` is required unless `ADMIN_AUTH_DISABLED=1`
- `ADMIN_AUTH_DISABLED=1` disables admin login for disposable local development only
- `ADMIN_COOKIE_SECURE` defaults to `true`; set `false` only when testing auth over local HTTP
- `LOG_LEVEL` defaults to `info`; supported values are `debug`, `info`, `warn`, and `error`
- `LOG_FORMAT` defaults to `text`; supported values are `text` and `json`

See [Logging](logging.md) for log fields, examples, and stdout/stderr behavior.

Generate an admin password hash:

```bash
printf '%s' 'your admin passphrase' | go run ./cmd/admin-password-hash
```

Behavior:

- opens and bootstraps the SQLite database on startup
- validates the opened store once before serving
- serves server-rendered HTML
- protects `/admin` routes with a passphrase-backed session when auth is enabled
- serves copied local media files from `MEDIA_ROOT` under `MEDIA_URL_PREFIX`
- logs startup, request, readiness, and internal error events to stderr
- uses `modernc.org/sqlite`
- requires writable storage for the database path

Routes:

- `GET /` home page
- `GET /events` event list
- `GET /events/{slug}` event detail
- `GET /venues` venue list
- `GET /venues/{slug}` venue detail
- `GET /admin/login` admin login page
- `POST /admin/login` start an admin session
- `POST /admin/logout` end an admin session
- `GET /admin` admin landing page
- `GET /admin/review` open review queue
- `GET /admin/review/history` read-only resolved and rejected review history
- `GET /admin/review/{groupID}` review detail
- `GET /admin/import-runs` read-only import history
- `GET /admin/import-runs/{id}` read-only import run snapshot metadata
- `GET /admin/venues` provisional venue queue
- `GET /admin/venues/{slug}` provisional venue detail
- `GET /admin/configuration` genre inference configuration
- `POST /admin/configuration` save, delete, or recompute genre inference rules
- `POST /admin/venues/{slug}` save provisional venue field edits or validate the venue when venue writes are available
- `POST /admin/review/{groupID}` review actions
- `GET /healthz` plain-text health check
- `GET /readyz` plain-text readiness check backed by a cheap store probe
- `GET /static/site.css` embedded stylesheet
- `GET /media/{path}` copied event media when `MEDIA_ROOT` is configured

The provisional venue queue lists provisional venue rows created from newly detected venue evidence during new-group staging, singleton auto-promotion, or manual review resolution. Detail pages show upcoming linked events. When venue writes are available, they also show editable venue fields and support two POST actions on the same route: save field edits in place or validate the venue and return to the queue. If venue writes are unavailable, provisional venue detail remains read-only and hides those controls.

`/admin/configuration` exposes genre inference rules. Defaults are loaded from `config/genres.yaml`, copied into SQLite, and can be overridden through the admin page. Saving or deleting a rule recomputes stored event genres and refreshes the top-two `events.genre` summary cache.

`/events` query parameters:

- `window=all|today|tonight|week|weekend`
- `area={venue-neighbourhood}`
- `venue={venue-slug}`

`/admin/review` and `/admin/review/{groupID}` flash query parameters:

- `saved=1`
- `resolved=1`
- `accepted=1`
- `rejected=1`

Review behavior:

- duplicate groups use field-by-field draft choices, a canonical draft summary, and persisted majority defaults
- open duplicate reviews preselect those persisted defaults when no manual draft exists
- duplicate reviews may include a `Live canonical snapshot` matrix column sourced from an existing live event
- review summaries derive shared venue labels from deterministic matching over stored candidate venue slug, venue text, and raw location evidence
- the review queue shows a read-only link to the latest successful import when the store provides import history
- `action=save` stores draft choices for duplicate groups
- `action=resolved` confirms a duplicate and resolves it, publishing one canonical public event
- manual review resolution canonicalizes the selected venue to an existing venue when the evidence yields one unique match
- new-group staging and non-authoritative singleton auto-promotion can create provisional venue rows immediately when venue evidence is uniquely new
- open-group restaging can backfill a provisional venue row only when a previously evidence-less candidate is refreshed with usable raw venue evidence
- when no unique existing venue match exists at manual review resolution time, resolution creates a provisional venue row and publishes against it in the same transaction if one does not already exist
- ambiguous venue evidence fails closed and leaves the group open
- canonical-backed duplicate resolution can update the matched live event in place
- when authoritative source identity and canonical slug match point at different live events, authoritative identity wins
- resolved review groups can persist secondary-source `genre` and `description` evidence for matching non-selected candidates
- non-authoritative secondary-source evidence is cumulative; matching candidates use the same venue slug and start time plus a title match after case and whitespace normalization
- later accepted reviews overwrite matching source/event rows when they provide new non-empty values, but absence does not delete earlier rows
- singleton groups use accept/reject actions when they were staged instead of auto-promoted
- `action=accept` resolves a singleton group and publishes one canonical public event
- `action=rejected` rejects a duplicate or singleton group without publishing
- closed groups are read-only and disappear from the open queue
- review history lists the 50 newest resolved and rejected groups
- import history and import run detail pages are read-only and available only when the store implements them
- import run detail pages show summary fields and snapshot metadata only; stored snapshot payload bodies are not rendered

## `./cmd/ingest`

Run:

```bash
go run ./cmd/ingest -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Or let the command derive a default user agent from git config:

```bash
go run ./cmd/ingest
```

Defaults:

- `-source` defaults to `sidney-and-matilda`
- `-limit` defaults to `20`
- `-timeout` defaults to `10s`
- `-db` overrides `DB_PATH`
- logs are written to stderr; stdout remains the JSON report stream

Validation:

- `-limit` applies to live ingest and replay, and must be between `1` and `50`
- `-timeout` must be positive
- replay does not require a user agent

Live ingest:

- primary flag: `-http-user-agent`
- alias: `-user-agent`
- `-contact` overrides the contact detail used in the default user agent
- `-contact none|null|false` suppresses contact info in the default user agent even when git `user.email` is set
- when `-http-user-agent` is omitted, the command uses `sheffield-live ingest/1.0` and appends `(contact: <email>)` when it can derive an email from local or global git `user.email`
- supports `sidney-and-matilda`, `yellow-arch`, `cafe-no-9`, `jazz-at-the-lescar`, `the-greystones`, `leadmill`, and `corporation`
- `-all-sources` runs every registered source sequentially in registry order and emits one aggregated JSON report
- fetches the selected source page
- Sidney & Matilda snapshots the source page, fetched ICS payloads, and linked event detail pages. ICS remains authoritative for event identity/times; detail pages enrich blank descriptions from clean schema.org `Event` JSON-LD or bounded event content only.
- Cafe No. 9 snapshots the WeGotTickets organiser page, follows pagination, snapshots event detail pages, and enriches descriptions from the detail page `Event information` section.
- Jazz at The Lescar snapshots the source page and parses repeated listing blocks into review candidates without authoritative source event IDs
- The Greystones snapshots the events hub, discovers linked month pages, snapshots those pages, and parses repeated month-page listing rows into review candidates
- Leadmill snapshots the source page and fetched ICS payloads, then keeps only `Live` listings with Sheffield locations
- Yellow Arch snapshots the source page, parses embedded JSON-LD event data from that page, then snapshots candidate detail pages for description enrichment
- Corporation snapshots the source page, discovers linked event detail pages, snapshots those pages, and parses candidates from the detail-page HTML
- parses candidates, skips, and errors
- copies supported event images into local media storage when a source image URL is available; failures are reported as warnings and do not fail the ingest
- stores a best-effort image focus point for copied event images so card crops can prefer the most visually interesting area
- writes `sources`, `import_runs`, and `snapshots`
- prints a JSON report to stdout
- batch mode continues after per-source failures but returns non-zero if any source run fails
- `-all-sources` is mutually exclusive with `-source`, `-import-run-id`, and `-review-ics-fixture`

Replay:

- `-import-run-id <id> [-limit N] [-stage-review-groups]`
- network-free
- only replays finished succeeded runs
- validates the stored snapshot envelope version and body SHA-256
- refuses missing or ambiguous snapshot matches
- auto-detects the source from stored page snapshot metadata
- reconstructs source-specific extraction from stored source page snapshots
- reuses previously copied image assets by source URL and does not fetch remote image bytes
- Sidney & Matilda replays source-page extraction to ICS links and matching ICS snapshots by URL and final URL
- Leadmill replays source-page extraction to the linked official iCal feed and reapplies the same `Live` plus Sheffield filter from stored ICS snapshots
- Yellow Arch replays candidate parsing from the stored source page snapshot and replays stored detail-page snapshots for description enrichment without network access
- Sidney & Matilda and Cafe No. 9 replay stored detail-page snapshots for description enrichment without network access

Stage review groups:

- primary flag: `-stage-review-groups`
- alias: `-stage-review`
- wraps the ingest report with `review_stage`
- creates duplicate review groups
- creates singleton review groups only for singleton candidates that were not auto-promoted first
- persists review-candidate venue evidence as `venue_text` and `venue_location_raw`; for ICS sources `venue_text` stays cleaned for display while `venue_location_raw` preserves the unfolded raw `LOCATION` text for later decoded comma/newline venue parsing
- singleton candidates may auto-promote when they are the first matching live event seen; authoritative sources can also upgrade provisional events in place
- singleton auto-promotion can create a provisional venue row immediately for a uniquely new venue
- duplicate groups may also auto-resolve as `canonical_exact_match` or `unanimous_duplicate`
- reports `groups_created` and `groups_reused`
- reports `auto_promoted_count` and `auto_promoted`
- reports `duplicate_auto_resolved_count` and `duplicate_auto_resolved`
- each staged group includes `result: created|reused`
- each duplicate auto-resolved row includes `title`, `result`, `review_group_id`, `candidate_count`, and `canonical_event_slug` when applicable
- each staged or reused group persists a link to the current import run
- `-stage-review-groups` can create provisional venue rows immediately for newly created staged groups when venue evidence is uniquely new, even when no event is published yet
- successful supporting singleton auto-promotion creates provisional events, does not create authoritative source links, does not create secondary-source info rows, and resolves matching stale open singleton groups by `staging_key` while linking the current import run
- only runs after a successful ingest

Description repair:

- primary flag: `-repair-descriptions`
- live mode supports `-source sidney-and-matilda` and `-source cafe-no-9`
- replay mode supports `-import-run-id <id> -repair-descriptions`
- reuses the normal live or replay parser output, including stored detail-page snapshots during replay
- updates only eligible existing `events.description` values for owned authoritative sources
- does not stage review groups, auto-promote events, create new events, or mutate non-description event fields
- emits `description_repair` with `description_repaired`, `description_unchanged`, `description_skipped`, and repaired event slugs
- mutually exclusive with `-stage-review-groups`, `-all-sources`, and `-review-ics-fixture`

Image focus backfill:

- primary flag: `-backfill-image-focus`
- reads existing local copied image files from `MEDIA_ROOT`
- recomputes image focus metadata on `image_assets`, `review_candidates`, and `events`
- emits `updated`, `defaulted`, `missing_files`, `decode_failures`, and any per-asset errors
- mutually exclusive with live ingest, replay, review fixture creation, review staging, and description repair

Offline review fixture:

- primary flag: `-review-ics-fixture`
- alias: `-review-fixture`
- mutually exclusive with replay
- reads a local ICS file
- does not use the network
- parses candidates, skips, and errors
- creates one offline review group
- prints a JSON report with the fixture path, group ID, candidate count, skips, and errors

`-review-title` sets the review-group title used with `-review-ics-fixture`.
`-review-ics-fixture` remains non-idempotent.
