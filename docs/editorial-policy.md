# Editorial policy

## Purpose

This site is a curated Sheffield live-music guide, not a scrape dump.

## Public policy

- keep the public data set small and legible
- prefer accurate, current venue names and links
- avoid duplicating the same event across multiple public slugs
- use one canonical slug per venue and event
- write descriptions that are factual and neutral
- retain the source link for each public event
- present freshness as source-check metadata, not as marketing copy
- keep seed, test, and dev records visibly tagged; live records are not tagged

## Public browsing

Public browsing covers:

- home
- events
- event detail
- venues
- venue detail

Each record shows provenance and freshness. Live browsing should make the source and currentness of a record clear without turning the page into a marketing layer.

## Review policy

Admin review exists for curated publication control. Duplicate reviews expose field-level choices, a canonical draft summary, persisted majority defaults, and may include a live canonical snapshot row for comparison. Review candidates also retain venue evidence from ingest (`venue_text` and `venue_location_raw`), and review summaries derive shared-venue context from deterministic venue matching over that evidence. Singleton new listings can be accepted or rejected when they are not auto-promoted first.

Resolving a duplicate review or accepting a singleton review publishes exactly one canonical public event. Rejecting a review does not publish anything.
Eligible singleton imports may also auto-publish before review when they are the first matching live event seen. Supporting-source first-seen publishes are stored internally as provisional until a review or authoritative update confirms them. Duplicate staging now has two extra closed-history paths: exact canonical duplicate auto-resolution against an existing live event, and unanimous staged-duplicate auto-resolution when every staged candidate agrees on the canonical fields.

The canonical event policy is:

- avoid duplicate public events
- prefer accurate venue names and links
- keep descriptions neutral and factual
- require a canonical start time
- allow a canonical end time to be unknown
- when present, require the canonical end time to be later than the start time
- keep one canonical slug per venue and event
- retain the source link
- treat freshness as source-check metadata rather than marketing

Supporting singleton auto-promotion is intentionally narrower than authoritative publish:

- it creates provisional events rather than reviewed ones
- it does not create authoritative source links
- it does not create secondary-source info rows
- it can create provisional venue rows immediately when the venue evidence is uniquely new
- it may fill blank live fields on later exact matches, but conflicting populated fields stay in review
- Jazz at The Lescar remains a non-authoritative, program-only source even when its first-seen singletons auto-publish

Canonical-backed duplicate resolution follows two precedence rules:

- authoritative source identity wins over canonical slug match when they disagree
- non-authoritative canonical-backed duplicate resolution updates the matched live event in place rather than publishing a second event

Venue matching and provisional venue creation follow these rules:

- when a selected staged `venue_slug` already names one existing venue, that exact slug wins even if `venue_text` or `venue_location_raw` point somewhere else
- when the selected venue evidence uniquely matches an existing venue, review resolution canonicalizes to that venue
- staging or non-authoritative singleton auto-promotion can create a provisional venue row immediately when venue evidence is uniquely new
- new provisional venue slugs prefer the location-head venue name over the full generic ICS `LOCATION` string
- when the selected venue evidence matches no existing venue, review resolution creates a provisional venue row and publishes against it in the same transaction if an earlier flow did not already do so
- when the selected venue evidence is ambiguous, review resolution fails closed rather than guessing

Reviews are held or rejected when the source is not good enough to publish:

- ambiguous venue names
- duplicate uncertainty
- cancelled or moved shows
- stale or bad source links
- incomplete source data

The site does not claim complete Sheffield coverage unless source breadth and import health justify it.
