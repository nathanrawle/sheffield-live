# Sources

## Source Catalog

The current manual source pipeline supports Sidney & Matilda, Yellow Arch, Cafe No. 9, Jazz at The Lescar, The Greystones, Leadmill, Corporation, Hallamshire Hotel, The Washington, Network Sheffield, Alder, Crookes Club, Delicious Clam, Hagglers Corner, and University of Sheffield Performance Venues.

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
When parser output includes an event image URL, ingest copies the remote image into local media storage, records the copied asset metadata, estimates a best-effort focus point for cropped card display, and carries the copied public URL into review candidates. Image-copy failures are reported as warnings and do not block the ingest.

After that, parsing depends on the source mode:

- Sidney & Matilda extracts ICS export links and event detail links from the source page, fetches each ICS feed and detail page, stores raw snapshots, and parses candidates, skips, and parse errors from ICS. ICS remains authoritative for event identity and times; detail pages can enrich blank descriptions from clean schema.org `Event` JSON-LD or bounded `.eventitem-column-content .sqs-html-content` content only.
- Cafe No. 9 parses music listings directly from the WeGotTickets organiser page snapshots, follows pagination, filters out offsite and non-music rows, and enriches descriptions from each event detail page's `Event information` section.
- Jazz at The Lescar parses repeated source-page listing blocks into review candidates using the page-level default music time plus the footer year hint.
- The Greystones extracts linked month pages from the events hub, stores those month-page snapshots, and parses repeated listing rows from each month page into review candidates.
- Leadmill extracts the official iCal feed from the source page, fetches that ICS payload, stores the raw ICS snapshot, and keeps only `Live` listings with Sheffield locations.
- Yellow Arch parses candidates, skips, and parse errors from schema.org `Event` JSON-LD embedded in the source page, then fetches candidate detail pages to enrich descriptions from bounded visible event content. Detail URLs are enrichment inputs and do not appear as report links.
- Corporation extracts linked official event detail pages from the source page, fetches and stores those detail-page snapshots, and parses candidates from the detail-page HTML.
- Hallamshire Hotel extracts the hidden public Google Calendar ICS URL from the official homepage, fetches the ICS payload, stores the raw ICS snapshot, and parses it with the generic ICS parser. Hallamshire authority requires real `LOCATION` evidence from the feed; missing-location events are not defaulted to the venue.
- The Washington extracts the official embedded FullCalendar Google Calendar configuration from the official events page and calendar frame, then fetches the corresponding Google Calendar API JSON. The live official calendar currently omits per-event `location` fields, so this source has a narrow source-level venue-evidence exception for the known official Washington calendar ID only. Location-less events from any other Google Calendar ID are skipped.
- Network Sheffield extracts same-host official detail pages from the official events listing and parses detail-page structured data. It accepts Network, Network Sheffield, and Network 1/2/3 venue evidence, emits room evidence for Network rooms, skips adjacent/offsite venues, and uses the official Network detail URL as the authoritative source identity.
- Alder extracts venue-managed listing links and follows delegated Eventbrite, Fatsoma, and Ticketpass detail pages. It requires a music signal plus Alder-specific venue proof that includes an accepted Alder label and Percy Street or S3 8BT address evidence; end times are optional when a valid start time exists.
- Crookes Club parses the official homepage and lounge live-music page as source pages. The homepage produces concert-room candidates when date/time evidence is deterministic; the lounge page produces lounge candidates using the page-supported default time.
- Delicious Clam extracts current/future delegated Skiddle links from the official events page and parses delegated detail pages only when they were discovered from that official page. It rejects stale listing/detail dates, uses body/address-style Delicious Clam venue proof rather than title text alone, and freezes its source clock during replay.
- Hagglers Corner extracts same-host WordPress detail posts from the official events-and-gigs category and parses the detail pages. It requires positive music evidence, rejects non-music body/title signals, skips aggregate monthly posts, and skips explicit labelled venue/location/address values unless they resolve to Hagglers evidence.
- University of Sheffield Performance Venues extracts official `/our-events/.../` detail pages and maps known venue labels for Octagon Centre, Firth Hall, and Drama Studio. It requires a strict music signal and known venue label. The source is authoritative through `owned_venue_slugs`, but only when all candidates in a review cluster resolve to the same configured University venue slug; cross-venue clusters remain staged without an authoritative source tuple.

Snapshots are kept as separate raw artifacts. They are not the same thing as canonical public event rows.

The ingest run writes to `sources`, `import_runs`, and `snapshots`, and it records the parsed report output rather than publishing public events directly.

## Runtime Families

The runtime no longer branches on source key directly. Each catalog entry selects a bounded family implementation for the chosen mode.

Template-fit families:

- Sidney & Matilda uses a configured ICS-link extractor, detail-link extractor, generic ICS parser, and conservative detail-page description parser
- Hallamshire Hotel uses a configured hidden-calendar ICS-link extractor and the generic ICS parser
- Yellow Arch uses a configured JSON-LD source-page parser and a conservative detail-page description parser

Custom adapter families:

- Leadmill uses a custom ICS parser family
- Cafe No. 9 uses custom source-page parser, pagination, and detail-page description parser families
- Jazz at The Lescar uses a custom source-page parser family
- The Greystones uses custom month-link and month-page families
- Corporation uses custom detail-link and detail-page families
- The Washington uses custom official Google Calendar config discovery and API parser families
- Network Sheffield uses custom same-host detail-link and detail-page parser families with Network room mapping
- Alder uses custom venue-managed listing-link and delegated ticket-detail parser families
- Crookes Club uses a custom source-page parser plus secondary-page extraction for homepage and lounge coverage
- Delicious Clam uses custom official-page delegated-link and delegated detail parser families
- Hagglers Corner uses custom same-host detail-link and detail-page parser families with music/offsite filtering
- University of Sheffield Performance Venues uses custom same-host detail-link and detail-page parser families with source-local venue-label mapping

Shared source helpers cover same-host link extraction for WordPress-like/detail-page sources and hidden public calendar URL extraction for embedded calendar pages. Keep those helpers bounded: they should normalize, de-duplicate, and limit links, while source-specific predicates decide which URLs are acceptable.

Adding a new source is YAML-only when it fits an existing family.
Add a new Go family only when the source needs new parsing or link-extraction behavior that cannot be expressed by the existing bounded family set.

## Snapshot Payloads

Snapshot payloads are stored as JSON envelopes that contain the response body in base64, response metadata, a captured-body SHA-256, and a truncation flag.
Copied image files are not snapshot payloads. They are local media assets keyed by source image URL so replay can reuse the copied asset metadata, including focus point, without fetching remote image bytes.

## Source Identity Contract

Parser `EventCandidate.UID` values are source identity material. They must identify the concrete event occurrence whenever the source exposes occurrence identity. Do not put a series-level recurrence ID in `UID` when multiple dated occurrences share it; use the source's occurrence ID, or combine the series ID with a normalized recurrence instance value when that is the only stable occurrence discriminator.

Review staging defensively detects repeated non-empty UIDs that appear with more than one parsed UTC start time in the same ingest report. Such UIDs are kept in candidate payloads for provenance/debugging, but they are withheld from source identity keys and authoritative source event keys. If the candidate has a real per-event listing URL, that URL can still become the source identity; inherited source pages, shared feed URLs, and unsafe recurrence IDs must not.

Generic calendar parsers follow that contract: Google Calendar API candidates prefer `calendar id + event.id`, and generic ICS candidates combine `UID + RECURRENCE-ID` for recurrence overrides. New source parsers should preserve the same rule so current and future recurring feeds cannot collapse separate occurrences into one downstream event.

## Review Staging

`cmd/ingest` can stage event-review clusters from a successful ingest report.

Review staging always creates duplicate event-review clusters, and it creates singleton event-review clusters only when a singleton is not auto-promoted first. Duplicate event-review clusters support field-level canonical choices, a canonical draft summary, persisted majority defaults, and optional live canonical snapshot context. Singleton event-review clusters use the same resolve or discard flow.
Each persisted review candidate keeps the staged venue slug plus source-derived venue evidence used to interpret it: `venue_text` and `venue_location_raw`. For ICS sources, `venue_text` stays cleaned for display, while `venue_location_raw` preserves the unfolded raw `LOCATION` text so later venue parsing can decode ICS escapes before applying the normal comma/newline split; the fetched ICS payload remains raw in the snapshot. Candidates can also carry optional room evidence (`room_text` and room slug/name rows) when a source exposes room-level listings.
Singletons may auto-promote from any source when they are the first matching live record the application has seen. Source authority controls later overwrite rights rather than initial publish eligibility. Supporting-source singleton promotion also checks for a nearby same-venue live event with a matching exact clean title, aggressively normalized title, or normalized headliner before inserting a new provisional event.

Review staging uses a durable key, so source metadata changes alone do not create a new cluster, terminal clusters are not reopened, and reruns link the cluster to the current import run through the persisted `import_run_event_review_clusters` relation.
When every candidate in a staged cluster agrees on one owned-venue source identity from a registry-owned venue source, the cluster persists that authoritative source name, URL, and event key for later resolution. Sources configured with `owned_venue_slugs` are authoritative only when every candidate in the cluster resolves to the same configured owned venue slug; a cross-venue cluster from the same source remains staged without authoritative source fields. Duplicate staging also derives the current live slug from `name + venue_slug + start_at`, derives shared-venue summary fields through deterministic venue matching over candidate venue evidence, can create a provisional venue row immediately for newly created staged clusters when the venue evidence is uniquely new, can create provisional room rows under known venues when room evidence is new, and can attach one live canonical snapshot row when all staged slug matches point to the same `events.origin = 'live'` row. Exact staged `venue_slug` matches take precedence over conflicting `venue_text` or `venue_location_raw` heuristics, so a known canonical slug is not blocked by noisier venue evidence. ICS-derived venue evidence keeps the raw `LOCATION` text for later decoding, but venue identity is still derived from the decoded comma/newline split rather than treating escaped commas as venue-name structure. New provisional venues derive both slug and display name from that location-head venue name rather than the full generic ICS `LOCATION` string, derive their address from the remaining evidence, drop an address line that duplicates the venue name, normalize comma/newline-separated address parts for display, and set neighbourhood when a recognized Sheffield district appears in the source-derived address. Open-cluster restaging refreshes that snapshot, refreshes staged venue and room evidence in place for existing open candidates, can backfill a provisional venue row only when a previously evidence-less open candidate is restaged with usable raw venue evidence, and recomputes persisted defaults while preserving manual draft choices. Supporting singleton auto-promotion does not mint authoritative event identities, does not create `event_source_links` or `event_secondary_source_info` rows, and can also create a provisional venue row immediately for a uniquely new venue. If a supporting singleton is blocked by the near-title guard, no provisional event or source link is inserted; the staged evidence remains available for review and single-target conflicts record observations where possible. Internally, first-seen supporting publishes are stored as `provisional` until a review or authoritative update confirms them.
Exact canonical duplicates and unanimous staged duplicates are stored as closed review history rows through duplicate auto-resolution rather than remaining in the open queue.

Replay auto-detects the source from stored page snapshot metadata, reconstructs the same catalog-selected extraction path from stored snapshots, validates the snapshot envelope version and SHA-256, and refuses missing or ambiguous snapshot matches.

## Publish Rules

Resolving an event-review cluster publishes exactly one canonical public event in the same SQLite transaction. Successful singleton auto-promotion also publishes exactly one canonical public event.

Discarding an event-review cluster does not publish an event.

When an event-review cluster resolves:

- selected review fields map to `internal/domain.Event`
- event genres are inferred from the selected canonical description plus persisted secondary-source descriptions; all matches are stored as ranked event genre rows and the public event row keeps the top two as its summary genre
- selected image fields publish copied image URL, original source URL, alt text, and dimensions
- authoritative clusters pin source name and source URL from the persisted authoritative tuple
- non-authoritative clusters let source name and source URL fall back to the cluster source only when the selected field is blank
- canonical end times may be omitted; unknown canonical ends publish as `events.end_at = NULL`
- venue matching first trusts an exact staged `venue_slug` match when it names one existing venue, then falls back to deterministic matching over `venue_text` and `venue_location_raw`
- when that match is unique, the published event uses the existing venue slug
- selected room evidence publishes as venue-scoped event room links; absence of room evidence leaves the event venue-only
- when there is no existing match, resolution inserts a `provisional` live venue row in the same transaction and publishes the event against it if staging or singleton auto-promotion has not already created that venue
- when venue evidence is ambiguous, resolution fails closed and the transaction rolls back
- the source row is ensured transactionally
- authoritative clusters resolve through `event_source_links` identity before any slug-based publish path
- if authoritative identity and canonical slug match point at different live events, authoritative identity wins
- authoritative clusters reconcile secondary-source `genre` and `description` rows for explicit non-authoritative candidate sources in the same transaction
- non-authoritative clusters upsert matching secondary-source `genre` and `description` rows as cumulative evidence; matching requires the same venue slug and start time plus a title match after case and whitespace normalization
- a missing source in a later accepted non-authoritative review does not delete an earlier stored secondary-source row
- the published event origin is `live`
- published event images reference copied media assets rather than hotlinking the source site
- the slug is `live-<slug(name)>-<slug(venue)>-<YYYYMMDDHHMMSS UTC>`
- canonical-backed non-authoritative duplicate resolution updates the matched live event row in place and recomputes the live slug
- canonical-backed in-place resolution fails if the recomputed slug already belongs to a different event
- non-canonical publish paths still use slug-based upsert semantics
- supporting import evidence can resolve as a source for one existing live event rather than inserting a duplicate when all hard target signals agree
- near-title false positives create durable separations before inserting a separate reviewed event
- title-repair and historical-duplicate false positives record event-event separations so the same relation is not restaged as an unresolved conflict
- historical duplicate action overrides validate the submitted canonical and withhold choices before mutating live events
- unsupported conflict types are visible in admin detail but do not get terminal resolver forms until a producer and policy are added

When a singleton auto-promotes without review:

- duplicate clusters still require review
- authoritative auto-promotion can insert a new event or update an existing linked event through owned-source identity and marks the event `reviewed`
- supporting auto-promotion creates a `provisional` live event when no existing live event matches by exact event identity, exact slug, or exact `name + venue_slug + start_at`, and no nearby same-venue live event matches by exact clean title, aggressively normalized title, or normalized headliner
- later supporting matches may fill blank canonical fields, but conflicting populated fields stay in review rather than silently rewriting the live event
- supporting auto-promotion resolves the matching event-review cluster by deterministic `staging_key` and links that cluster to the current import run
- authoritative later matches can upgrade a provisional live event in place and mark it `reviewed`

## Source Strategy

Prefer official venue listings first.
Use aggregators later for coverage and cross-checking.
Add APIs only where terms and value are clear.

## Primary Source Research and Implementation Status

Research date: 2026-05-24.

At the time of research, the configured primary sources were Sidney & Matilda, Yellow Arch, Cafe No. 9, Jazz at The Lescar, The Greystones, Leadmill, and Corporation. The high-priority venues below were found as Sheffield live music sources that were not yet represented by repo-backed YAML files under `config/sources/`.

Implementation status: the high-priority sources in the next section are now represented by repo-backed YAML files `08` through `15` under `config/sources/`. Their original research notes are retained as decision history. Use the source YAML, parser tests, replay tests, and review-stage tests as implementation truth.

Treat the secondary and do-not-add sections as a decision backlog, not as implementation truth. Before adding any future source, fetch the primary page again, save a small fixture under `internal/ingest/testdata/`, and prove the chosen parser with an ingest test. Do not change stable source identity fields after import history exists.

### High-Priority Sources Implemented From This Research

#### Hallamshire Hotel

- Suggested source key: `hallamshire-hotel`
- Suggested venue slug: `hallamshire-hotel`
- Primary page: `https://hallamshirehotel.pub/`
- Venue evidence: official site for Hallamshire Hotel, 182 West Street, Sheffield.
- Event evidence observed: the homepage has an `Events` section backed by a public Google Calendar ICS URL in a hidden `cfgFilestring` element.
- ICS URL observed on 2026-05-24: `https://calendar.google.com/calendar/ical/c_3bc79a2475a0c9540838a74d401458962aedd23ae8ff89c01a88258efcd4972a%40group.calendar.google.com/public/basic.ics`
- Why it matters: reopened West Street venue with an official, machine-readable calendar. This is the simplest high-value addition.
- Likely ingest shape: `mode: linked_ics`, generic ICS parser, new small ICS link extractor for the Hallamshire page. Reuse the generic venue handling if ICS `LOCATION` data is usable; otherwise add a venue normalizer.
- Authority note: this can be an owned-venue authoritative source if the feed only lists Hallamshire events.
- Implementation status: implemented as `config/sources/08-hallamshire-hotel.yaml` with `mode: linked_ics`, `ics_parser: generic`, and `owned_venue_slug: hallamshire-hotel`. The hidden-calendar extractor prefers public Google Calendar ICS URLs before applying its limit. The parser does not fabricate venue evidence for missing ICS `LOCATION`.

#### The Washington

- Suggested source key: `the-washington`
- Suggested venue slug: `the-washington`
- Primary pages: `https://thewashington.pub/` and `https://thewashington.pub/events`
- Venue evidence: official site lists The Washington at 79 Fitzwilliam Street, Sheffield S1 4JP, with booking contact `bookings@thewashington.pub`.
- Event evidence observed: the official site has an `EVENTS` page. Initial text crawl exposed only venue/contact/opening-time content and image placeholders, but follow-up implementation found the page embeds `cal.html`, which initializes FullCalendar with a public Google Calendar ID and API key.
- Why it matters: longstanding city-centre live music pub with regular grassroots gigs and DJ nights. Treat as high priority despite source-shape uncertainty.
- Likely ingest shape: high priority discovery task before parser implementation. Inspect the rendered page, image metadata, scripts, social links, and any network/API calls to find the venue-owned event source behind the official `EVENTS` page. If no venue-owned structured source exists, document whether a venue-managed social/ticket source is acceptable before adding YAML.
- Authority note: do not use Sheffield Gigs, DesignMyNight, Gigseekr, or venue directories as the authoritative source. They are useful cross-checks only.
- Implementation status: implemented as `config/sources/09-the-washington.yaml` with `mode: linked_detail_pages`, `linked_page_link_extractor: the_washington_api_links`, `linked_page_parser: the_washington_api`, `venue_normalizer: the_washington`, and `owned_venue_slug: the-washington`.
- Source contract: The Washington's live official Google Calendar API feed currently omits per-event `location` fields. Requiring per-event venue evidence would make the official source produce no candidates. The implementation therefore treats only the known official embedded Washington calendar ID as source-level venue evidence. Location-less events from any other Google Calendar ID remain skipped.
- Identity contract: The parser uses the Google Calendar API `event.id` scoped to the calendar ID as the candidate UID. Google `iCalUID` is series-level for recurring events and must not be treated as one event occurrence.
- Risk note: this source depends on the official page's embedded FullCalendar/Google Calendar API shape. If the public API key, calendar ID, or embed structure changes, update fixtures and parser tests before relying on new output.

#### Network Sheffield

- Suggested source key: `network-sheffield`
- Suggested venue slug: `network-sheffield` if filtering to Network only.
- Primary pages: `https://www.networksheffield.co.uk/events/` and `https://www.networksheffield.co.uk/event/`
- Detail-page example from research: `https://www.networksheffield.co.uk/event/in-the-know-festival/`
- Venue evidence: official site lists Network Sheffield at 14 Matilda Street, Sheffield S1 4QD.
- Event evidence observed: current/future listings include date, time, title, buy-ticket link, and venue labels such as Network, Network 1, Network 2, Network 3, Earl's Yard, and The Arundel Emporium.
- Why it matters: large active city-centre live music and club venue with many listings.
- Likely ingest shape: `mode: linked_detail_pages`, custom archive/detail link extractor, and custom detail-page parser. The site looks WordPress-like and detail pages expose title, start/end time, venue, description, image, and address.
- Authority note: do not blindly set one `owned_venue_slug` if the parser includes offsite or adjacent venue labels such as The Arundel Emporium or Earl's Yard. Either filter to Network-owned rooms, model rooms under Network, or extend the catalog authority model before treating the whole feed as authoritative.
- Implementation status: implemented as `config/sources/10-network-sheffield.yaml` with `mode: linked_detail_pages` and `owned_venue_slug: network-sheffield`. The parser filters to Network/Network Sheffield/Network 1/2/3 evidence, emits room evidence for rooms 1-3, skips offsite/adjacent venues, and uses the official Network detail URL as source identity rather than external ticket URLs.

#### Alder

- Suggested source key: `alder`
- Suggested venue slug: `alder`
- Primary candidates: `https://alderbar.com/`, `https://alder-bar.business.site/`, and venue-managed `https://linktr.ee/alderbar`
- Venue evidence: public venue listings and the venue-managed Linktree identify Alder/Alder Bar at Unit 111-112 JC Albyn Complex, Percy Street, Neepsend, Sheffield S3 8BT.
- Event evidence observed: the venue-managed Linktree was visible in search results with current `What's on at Alder!` listings and outbound ticket links. Venue/culture pages describe Alder as a live music venue with recurring jazz, folk, experimental, karaoke, and Get Together Festival programming. The Linktree fetch itself was blocked by robots during research, so implementation needs a fresh browser/API investigation.
- Why it matters: active Neepsend/Kelham grassroots room with eclectic programming. Treat as high priority despite source-shape uncertainty.
- Likely ingest shape: high priority discovery task. Prefer a venue-owned source over third-party listings: first check whether `alderbar.com` or the Google business site exposes parseable events; if not, inspect the venue-managed Linktree and linked ticket providers. A practical first pass may need a small `source_page` parser over a stable venue-managed listing page plus outbound ticket URLs as detail links.
- Authority note: Alder programming appears to span music, film, comedy, workshops, and community events. Add explicit music filtering before auto-promoting, and do not treat all linked Linktree items as live music.
- Implementation status: implemented as `config/sources/11-alder.yaml` with `mode: linked_detail_pages` and `owned_venue_slug: alder`. The source uses venue-managed listing links plus delegated Eventbrite/Fatsoma/Ticketpass detail pages. Authority requires Alder label evidence plus Percy Street or S3 8BT address evidence, and the parser applies explicit music filtering.

#### Crookes Club

- Suggested source key: `crookes-club`
- Suggested venue slug: `crookes-club`
- Primary pages: `https://crookesclub.co.uk/` and `https://crookesclub.co.uk/lounge-live-music`
- Venue evidence: official site describes Crookes Club as a live music and social club with a 500-capacity concert room plus lounge/bar spaces at Mulehouse Road, Crookes, Sheffield S10 1TD.
- Event evidence observed: homepage has ticketed upcoming date blocks; lounge page lists free Saturday live music with dates and artists.
- Why it matters: established live room outside the city centre and useful neighbourhood coverage.
- Likely ingest shape: custom `source_page` parser. The homepage and lounge page are different enough that the first pass should choose one of these strategies:
  - add the homepage ticketed/concert-room feed first, then add lounge coverage later, or
  - add a custom link extractor that fetches both pages and emits room evidence (`concert-room` vs `lounge`).
- Authority note: if both pages are ingested, preserve room evidence. Lounge listings may have sparse metadata and recurring-style dates, so avoid over-normalizing.
- Implementation status: implemented as `config/sources/12-crookes-club.yaml` with `mode: source_page`, `source_page_parser: crookes_club`, and `source_page_link_extractor: crookes_club_secondary_pages`. It parses both homepage and lounge live-music evidence and emits room evidence for `concert-room` and `lounge`.

#### Delicious Clam

- Suggested source key: `delicious-clam`
- Suggested venue slug: `delicious-clam`
- Primary page: `https://www.deliciousclam.co.uk/events`
- Venue evidence: official Delicious Clam site; research snapshot showed venue identity and Sheffield postcode S2 5TS. Confirm the full street address before creating a validated venue seed.
- Event evidence observed: the events page lists future dated shows with titles and external ticket URLs, for example Skiddle links. The page also contains older Squarespace-style event blocks with `Google Calendar ICS` links.
- Why it matters: important DIY and grassroots live music source not covered by current primary venue feeds.
- Likely ingest shape: custom `source_page` parser for the current future-show list. Treat external ticket URLs as listing/ticket links, not as primary identity, unless the official page itself lacks stable per-event anchors.
- Authority note: use Delicious Clam's official page as authoritative for the venue. Do not promote third-party ticket pages to primary source status unless the venue explicitly delegates current listings there and no usable official listing is available.
- Implementation status: implemented as `config/sources/13-delicious-clam.yaml` with `mode: linked_detail_pages` and `owned_venue_slug: delicious-clam`. Official-page discovery gates delegated Skiddle detail pages. The extractor requires current/future listing dates, detail pages reject past dates, and venue proof uses body/address-style evidence rather than title text alone.

#### Hagglers Corner

- Suggested source key: `hagglers-corner`
- Suggested venue slug: `hagglers-corner`
- Primary pages: `https://hagglerscorner.co.uk/` and `https://hagglerscorner.co.uk/category/events-gigs/`
- Venue evidence: official site for Hagglers Corner, 586 Queens Road, Sheffield, with venue/music/festival positioning.
- Event evidence observed: the `Events & gigs` category contains event posts, including music and non-music/community listings.
- Why it matters: active grassroots/courtyard venue with recurring live music and club activity.
- Likely ingest shape: WordPress category/detail parser with music filtering. Start by extracting post title, post date, event date text, description, and outbound ticket link.
- Authority note: this source needs event-type filtering. Do not ingest quiz, market, workshop, or private-hire posts as live music without an explicit music signal.
- Implementation status: implemented as `config/sources/14-hagglers-corner.yaml` with `mode: linked_detail_pages` and `owned_venue_slug: hagglers-corner`. It extracts same-host detail posts, requires positive music evidence, rejects non-music title/body signals and aggregate posts, and skips explicit labelled venue/location/address values unless they resolve to Hagglers evidence.

#### University of Sheffield Performance Venues

- Suggested source key: `university-of-sheffield-performance-venues`
- Suggested venue slugs: `octagon-centre`, `firth-hall`, and `drama-studio`.
- Primary pages: `https://performancevenues.group.shef.ac.uk/whats-on/` and `https://performancevenues.group.shef.ac.uk/about-us/`
- Venue evidence: official Performance Venues site for the University of Sheffield's Octagon, Firth Hall, and Drama Studio.
- Event evidence observed: What's On page lists dated events and detail pages expose fields such as `Dates`, `Venue`, `Times`, and `Cost`. Research examples included music listings at the Octagon and concert listings at Firth Hall.
- Why it matters: covers significant concert and touring activity that is not in the current venue catalog.
- Likely ingest shape: custom `source_page` parser plus detail-page enrichment. Use the detail page venue field to create venue evidence.
- Authority note: this is a multi-venue official source. It uses `owned_venue_slugs` for Octagon Centre, Firth Hall, and Drama Studio. Authoritative fields are populated only when every candidate in a staged cluster resolves to the same configured University venue slug; cross-venue clusters remain staged without authoritative source fields.
- Implementation status: implemented as `config/sources/15-university-of-sheffield-performance-venues.yaml` with `mode: linked_detail_pages` and `owned_venue_slugs`. Detail-page parsing maps known venue labels, skips unknown/ambiguous venues and no-year pages, and requires a strict music signal after stripping page chrome/links.

### Secondary Missing Sources

#### FoundrySU

- Primary page: `https://foundrysu.com/events`
- Research note: official Events & Tickets page has a Live Music category, but the crawl did not expose enough event rows to confirm a simple static parser.
- Decision: worth revisiting if JavaScript/API discovery is acceptable. Lower priority than the high-priority backlog because parser effort is less clear.

#### The Irish Sheffield

- Suggested source key: `the-irish-sheffield`
- Suggested venue slug: `the-irish-sheffield`
- Primary page: `https://www.theirishsheffield.co.uk/`
- Venue evidence: official site lists The Irish at 301 Ecclesall Road, Sharrow, Sheffield S11 8NX.
- Event evidence observed: the official page explicitly describes live music and links to Facebook/Instagram, but says to keep an eye on socials for gig nights, open sessions, and special events. No stable parseable event list was observed on the site during research.
- Decision: secondary source candidate. Add only after finding a stable venue-managed listing feed or accepting venue-managed social posts as the primary listing source.
- Parser note: if added, likely starts as social/link discovery rather than a normal HTML event-list parser. Needs music-only filtering because the venue also promotes sport, run club, and private bookings.

#### Maida Vale Sheffield

- Suggested source key: `maida-vale-sheffield`
- Suggested venue slug: `maida-vale-sheffield`
- Primary page: `https://www.maidavalesheffield.com/`
- Venue evidence: official site lists Maida Vale at 88 West Street, Sheffield S1 4EP.
- Event evidence observed: the official homepage has a `Maida News & Events` area and states `Live music EVERY BANK HOLIDAY`, but no detailed dated event feed was observed in the text crawl.
- Decision: secondary source candidate. Useful for West Street live-cover-band coverage, but not enough machine-readable event data was found for a first parser pass.
- Parser note: recheck for a hidden events link or social/ticket feed before implementation. If only generic bank-holiday live music text exists, do not create recurring events without dated primary evidence.

#### Sheffield Cathedral

- Primary page: `https://www.sheffieldcathedral.org/whats-on/`
- Research note: official Squarespace-style What's On page exposes dated events, per-event pages, and `Google Calendar ICS` links. It includes many non-music events alongside classical, choral, candlelight, and worship-music events.
- Decision: add if cathedral concerts and classical/choral programming are in scope. Requires filtering to avoid family trails, talks, worship-only, and community events.

#### FORGE Warehouse

- Primary pages: `https://www.forgewarehouse.co.uk/events/` and `https://www.forgewarehouse.co.uk/event/`
- Research note: official site had an event archive and an events page that may show no upcoming events. The venue-managed Linktree at `https://linktr.ee/forge.sheffield` showed more current 2026 listings during research.
- Decision: do not add from Linktree alone unless the project accepts venue-managed landing pages as primary sources. Recheck the official site first.

#### Zephyr's

- Primary page: `https://www.zephyrsbar.co.uk/`
- Research note: official site describes a grassroots live music venue at Stag Works and includes a `What's On` block, but observed event dates were sparse and appeared to omit years.
- Decision: recheck freshness and date semantics before implementation. If current, likely a small custom source-page parser.

#### Record Junkee

- Primary candidates: `https://recordjunkee.co.uk/` and `https://www.fatsoma.com/p/record-junkee-sheffield`
- Research note: official/venue-owned event data looked sparse. Aggregators and ticketing sites show Record Junkee events, but they should not be used as primary sources unless the venue has delegated listings there.
- Decision: defer until a stable venue-owned event feed is found.

#### Sheffield City Hall and Utilita Arena Sheffield

- Primary pages: `https://www.sheffieldcityhall.co.uk/events/` and `https://www.utilitaarenasheffield.co.uk/events/`
- Research note: both are official high-volume venue calendars and include music, comedy, theatre, sport, and other large-format events.
- Decision: valid primary sources, but lower priority for a grassroots/live-board first pass. Add later if the product scope includes large multi-purpose venues.

### Do Not Add Unless Status Changes

#### Dorothy Pax

- Primary page: `https://dorothypax.com/`
- Research note: the official site still had legacy information, but external reporting in 2025 said the venue closed with immediate effect.
- Decision: do not add as a new live source unless an official channel confirms reopening and current events.

#### The Rocking Chair

- Research note: venue directories report it permanently closed.
- Decision: do not add unless an official reopening/current-listings source appears.
