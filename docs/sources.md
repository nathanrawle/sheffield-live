# Sources

## Current Flow

The current manual source pipeline supports Sidney & Matilda, Yellow Arch, Cafe No. 9, Jazz at The Lescar, The Greystones, Leadmill, and Corporation.

Sources are registered in code with source metadata plus a page-processing mode. That mode decides whether ingest expands the page into linked ICS fetches or parses candidates directly from the stored page snapshot.
Owned-venue authority also lives in that registry. The registry is the only place that maps a source key or review-stage source name to an owned venue slug.

Every ingest run fetches the source page and stores a raw source-page snapshot.

After that, parsing depends on the source mode:

- Sidney & Matilda extracts ICS export links from the source page, fetches each ICS feed, stores raw ICS snapshots, and parses candidates, skips, and parse errors from ICS.
- Cafe No. 9 parses music listings directly from the WeGotTickets organiser page snapshot and filters out offsite and non-music rows.
- Jazz at The Lescar parses repeated source-page listing blocks into review candidates using the page-level default music time plus the footer year hint.
- The Greystones extracts linked month pages from the events hub, stores those month-page snapshots, and parses repeated listing rows from each month page into review candidates.
- Leadmill extracts the official iCal feed from the source page, fetches that ICS payload, stores the raw ICS snapshot, and keeps only `Live` listings with Sheffield locations.
- Yellow Arch parses candidates, skips, and parse errors directly from schema.org `Event` JSON-LD embedded in the source page. No secondary snapshots are fetched for that source.
- Corporation extracts linked official event detail pages from the source page, fetches and stores those detail-page snapshots, and parses candidates from the detail-page HTML.

Snapshots are kept as separate raw artifacts. They are not the same thing as canonical public event rows.

The ingest run writes to `sources`, `import_runs`, and `snapshots`, and it records the parsed report output rather than publishing public events directly.

Sidney & Matilda extraction accepts Squarespace `?format=ical` ICS links and legacy Google Calendar-style ICS labels.
Leadmill extraction accepts the source page `<link rel="alternate" type="text/calendar">` feed reference and applies a source-specific `Live` plus Sheffield filter during ICS parsing.
Yellow Arch parsing accepts embedded JSON-LD arrays or graphs that contain schema.org `Event` objects.
Cafe No. 9 parsing accepts WeGotTickets organiser-page listing blocks and uses the resolved event URL as the candidate identity.
Jazz at The Lescar parsing accepts repeated `art` / `ttl` / `dsc` blocks and emits review candidates without authoritative event identities.
The Greystones parsing accepts linked month pages and emits multiple review candidates from each stored month-page snapshot.

## Snapshot Payloads

Snapshot payloads are stored as JSON envelopes that contain the response body in base64, response metadata, a captured-body SHA-256, and a truncation flag.

## Review Staging

`cmd/ingest` can stage review groups from a successful ingest report.

Review staging always creates duplicate clusters, and it creates singleton review groups only when a singleton is not auto-promoted first. Duplicate review groups support field-level canonical choices plus a canonical draft summary. Singleton review groups support accept or reject.
Singletons may auto-promote in two paths:

- authoritative owned-source identity for registry-owned venue sources
- non-authoritative slug-absent publish for configured singleton sources such as The Greystones and Jazz at The Lescar

Review staging uses a durable key, so source metadata changes alone do not create a new group, closed groups are not reopened, and reruns link the group to the current import run through the persisted `import_run_review_groups` relation.
When every candidate in a staged group agrees on one owned-venue source identity from a registry-owned venue source, the group persists that authoritative source name, URL, and event key for later resolution. Non-authoritative singleton auto-promotion does not mint authoritative event identities, does not create `event_source_links`, and does not create `event_secondary_source_info` rows. Jazz at The Lescar remains non-authoritative and program-only even when its eligible singletons auto-publish.

Replay auto-detects the source from stored page snapshot metadata, reconstructs the same source-specific extraction path from stored snapshots, validates the snapshot envelope version and SHA-256, and refuses missing or ambiguous snapshot matches.

## Publish Rules

Resolving a duplicate group or accepting a singleton publishes exactly one canonical public event in the same SQLite transaction. Successful singleton auto-promotion also publishes exactly one canonical public event.

Rejecting either a duplicate or singleton review does not publish an event.

When a review group resolves:

- selected review fields map to `internal/domain.Event`
- authoritative groups pin source name and source URL from the persisted authoritative tuple
- non-authoritative groups let source name and source URL fall back to the review-group source only when the selected field is blank
- canonical end times may be omitted; unknown canonical ends publish as `events.end_at = NULL`
- the venue must already exist
- the source row is ensured transactionally
- authoritative groups resolve through `event_source_links` identity before any slug-based publish path
- authoritative groups can also store secondary-source `genre` and `description` rows for explicit non-authoritative candidate sources in the same transaction
- the published event origin is `live`
- the slug is `live-<slug(name)>-<slug(venue)>-<YYYYMMDDHHMMSS UTC>`
- slug conflicts are handled with upsert semantics

When a singleton auto-promotes without review:

- duplicate groups still require review
- authoritative auto-promotion can insert a new event or update an existing linked event through owned-source identity
- non-authoritative auto-promotion is insert-only and only succeeds when the derived live slug is absent
- non-authoritative auto-promotion resolves matching stale open singleton groups by `staging_key` and links those groups to the current import run

## Source Strategy

Prefer official venue listings first.
Use aggregators later for coverage and cross-checking.
Add APIs only where terms and value are clear.
