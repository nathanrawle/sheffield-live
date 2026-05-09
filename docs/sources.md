# Sources

## Source Catalog

The current manual source pipeline supports Sidney & Matilda, Yellow Arch, Cafe No. 9, Jazz at The Lescar, The Greystones, Leadmill, and Corporation.

Sources now live in repo-backed YAML files under `config/sources/`.
Each file defines:

- stable identity fields: `key`, `name`, `url`, `review_stage_source_name`
- ownership and singleton-review policy fields
- one page-processing `mode`
- one mode-specific runtime block

The catalog is loaded from that fixed repo path by `cmd/ingest` and `cmd/web` at startup.

The stable identity fields are replay-sensitive and review-sensitive in v1. Changing them can break historical replay or review/publish behavior, so treat them as immutable unless the replay/versioning design changes too.

Owned-venue authority also lives in the catalog. The catalog is the only place that maps a source key or review-stage source name to owned-venue and non-authoritative singleton policy.

## Current Flow

Every ingest run fetches the source page and stores a raw source-page snapshot.

After that, parsing depends on the source mode:

- Sidney & Matilda extracts ICS export links and event detail links from the source page, fetches each ICS feed and detail page, stores raw snapshots, and parses candidates, skips, and parse errors from ICS. ICS remains authoritative for event identity and times; detail pages can enrich blank descriptions from clean schema.org `Event` JSON-LD or bounded `.eventitem-column-content .sqs-html-content` content only.
- Cafe No. 9 parses music listings directly from the WeGotTickets organiser page snapshots, follows pagination, filters out offsite and non-music rows, and enriches descriptions from each event detail page's `Event information` section.
- Jazz at The Lescar parses repeated source-page listing blocks into review candidates using the page-level default music time plus the footer year hint.
- The Greystones extracts linked month pages from the events hub, stores those month-page snapshots, and parses repeated listing rows from each month page into review candidates.
- Leadmill extracts the official iCal feed from the source page, fetches that ICS payload, stores the raw ICS snapshot, and keeps only `Live` listings with Sheffield locations.
- Yellow Arch parses candidates, skips, and parse errors from schema.org `Event` JSON-LD embedded in the source page, then fetches candidate detail pages to enrich descriptions from bounded visible event content. Detail URLs are enrichment inputs and do not appear as report links.
- Corporation extracts linked official event detail pages from the source page, fetches and stores those detail-page snapshots, and parses candidates from the detail-page HTML.

Snapshots are kept as separate raw artifacts. They are not the same thing as canonical public event rows.

The ingest run writes to `sources`, `import_runs`, and `snapshots`, and it records the parsed report output rather than publishing public events directly.

## Runtime Families

The runtime no longer branches on source key directly. Each catalog entry selects a bounded family implementation for the chosen mode.

Template-fit families:

- Sidney & Matilda uses a configured ICS-link extractor, detail-link extractor, generic ICS parser, and conservative detail-page description parser
- Yellow Arch uses a configured JSON-LD source-page parser and a conservative detail-page description parser

Custom adapter families:

- Leadmill uses a custom ICS parser family
- Cafe No. 9 uses custom source-page parser, pagination, and detail-page description parser families
- Jazz at The Lescar uses a custom source-page parser family
- The Greystones uses custom month-link and month-page families
- Corporation uses custom detail-link and detail-page families

Adding a new source is YAML-only when it fits an existing family.
Add a new Go family only when the source needs new parsing or link-extraction behavior that cannot be expressed by the existing bounded family set.

## Snapshot Payloads

Snapshot payloads are stored as JSON envelopes that contain the response body in base64, response metadata, a captured-body SHA-256, and a truncation flag.

## Review Staging

`cmd/ingest` can stage review groups from a successful ingest report.

Review staging always creates duplicate clusters, and it creates singleton review groups only when a singleton is not auto-promoted first. Duplicate review groups support field-level canonical choices, a canonical draft summary, persisted majority defaults, and optional live canonical snapshot context. Singleton review groups support accept or reject.
Each persisted review candidate keeps the staged venue slug plus source-derived venue evidence used to interpret it: `venue_text` and `venue_location_raw`. For ICS sources, `venue_text` stays cleaned for display, while `venue_location_raw` preserves the unfolded raw `LOCATION` text so later venue parsing can decode ICS escapes before applying the normal comma/newline split; the fetched ICS payload remains raw in the snapshot.
Singletons may auto-promote from any source when they are the first matching live record the application has seen. Source authority controls later overwrite rights rather than initial publish eligibility.

Review staging uses a durable key, so source metadata changes alone do not create a new group, closed groups are not reopened, and reruns link the group to the current import run through the persisted `import_run_review_groups` relation.
When every candidate in a staged group agrees on one owned-venue source identity from a registry-owned venue source, the group persists that authoritative source name, URL, and event key for later resolution. Duplicate staging also derives the current live slug from `name + venue_slug + start_at`, derives shared-venue summary fields through deterministic venue matching over candidate venue evidence, can create a provisional venue row immediately for newly created staged groups when the venue evidence is uniquely new, and can attach one live canonical snapshot row when all staged slug matches point to the same `events.origin = 'live'` row. Exact staged `venue_slug` matches take precedence over conflicting `venue_text` or `venue_location_raw` heuristics, so a known canonical slug is not blocked by noisier venue evidence. ICS-derived venue evidence keeps the raw `LOCATION` text for later decoding, but venue identity is still derived from the decoded comma/newline split rather than treating escaped commas as venue-name structure. New provisional venues derive both slug and display name from that location-head venue name rather than the full generic ICS `LOCATION` string, derive their address from the remaining evidence, drop an address line that duplicates the venue name, normalize comma/newline-separated address parts for display, and set neighbourhood when a recognized Sheffield district appears in the source-derived address. Open-group restaging refreshes that snapshot, refreshes staged venue evidence in place for existing open candidates, can backfill a provisional venue row only when a previously evidence-less open candidate is restaged with usable raw venue evidence, and recomputes persisted defaults while preserving manual draft choices. Supporting singleton auto-promotion does not mint authoritative event identities, does not create `event_source_links` or `event_secondary_source_info` rows, and can also create a provisional venue row immediately for a uniquely new venue. Internally, first-seen supporting publishes are stored as `provisional` until a review or authoritative update confirms them.
Exact canonical duplicates and unanimous staged duplicates are stored as closed review history rows through duplicate auto-resolution rather than remaining in the open queue.

Replay auto-detects the source from stored page snapshot metadata, reconstructs the same catalog-selected extraction path from stored snapshots, validates the snapshot envelope version and SHA-256, and refuses missing or ambiguous snapshot matches.

## Publish Rules

Resolving a duplicate group or accepting a singleton publishes exactly one canonical public event in the same SQLite transaction. Successful singleton auto-promotion also publishes exactly one canonical public event.

Rejecting either a duplicate or singleton review does not publish an event.

When a review group resolves:

- selected review fields map to `internal/domain.Event`
- event genres are inferred from the selected canonical description plus persisted secondary-source descriptions; all matches are stored as ranked event genre rows and the public event row keeps the top two as its summary genre
- authoritative groups pin source name and source URL from the persisted authoritative tuple
- non-authoritative groups let source name and source URL fall back to the review-group source only when the selected field is blank
- canonical end times may be omitted; unknown canonical ends publish as `events.end_at = NULL`
- venue matching first trusts an exact staged `venue_slug` match when it names one existing venue, then falls back to deterministic matching over `venue_text` and `venue_location_raw`
- when that match is unique, the published event uses the existing venue slug
- when there is no existing match, resolution inserts a `provisional` live venue row in the same transaction and publishes the event against it if staging or singleton auto-promotion has not already created that venue
- when venue evidence is ambiguous, resolution fails closed and the transaction rolls back
- the source row is ensured transactionally
- authoritative groups resolve through `event_source_links` identity before any slug-based publish path
- if authoritative identity and canonical slug match point at different live events, authoritative identity wins
- authoritative groups reconcile secondary-source `genre` and `description` rows for explicit non-authoritative candidate sources in the same transaction
- non-authoritative groups upsert matching secondary-source `genre` and `description` rows as cumulative evidence; a missing source in a later accepted review does not delete an earlier stored row
- the published event origin is `live`
- the slug is `live-<slug(name)>-<slug(venue)>-<YYYYMMDDHHMMSS UTC>`
- canonical-backed non-authoritative duplicate resolution updates the matched live event row in place and recomputes the live slug
- canonical-backed in-place resolution fails if the recomputed slug already belongs to a different event
- non-canonical publish paths still use slug-based upsert semantics

When a singleton auto-promotes without review:

- duplicate groups still require review
- authoritative auto-promotion can insert a new event or update an existing linked event through owned-source identity and marks the event `reviewed`
- supporting auto-promotion creates a `provisional` live event when no existing live event matches by exact slug or exact `name + venue_slug + start_at`
- later supporting matches may fill blank canonical fields, but conflicting populated fields stay in review rather than silently rewriting the live event
- supporting auto-promotion resolves matching stale open singleton groups by `staging_key` and links those groups to the current import run
- authoritative later matches can upgrade a provisional live event in place and mark it `reviewed`

## Source Strategy

Prefer official venue listings first.
Use aggregators later for coverage and cross-checking.
Add APIs only where terms and value are clear.
