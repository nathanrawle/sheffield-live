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
- keep test and dev records visibly tagged; curated bootstrap venues and live records are not tagged

## Public browsing

Public browsing covers:

- home
- events
- event detail
- venues
- venue detail

Each record shows provenance and freshness. Live browsing should make the source and currentness of a record clear without turning the page into a marketing layer.

## Review policy

Admin review exists for curated publication control. Event-review clusters are the active review model; legacy review groups are historical schema only and are not part of active editorial workflow. Clusters expose field-level choices, a canonical draft summary, persisted defaults, provenance, and any live canonical context needed to compare ingest evidence with the public event record.

Resolving an event-review cluster publishes or updates exactly one canonical public event when publication is appropriate, or records a durable no-publish separation decision when the correct editorial decision is "these are distinct events." Discarding a cluster does not publish anything. Superseding a cluster records that newer active evidence has replaced the older open review path. Discard and supersede are administrative escape hatches rather than terminal editorial decisions.

Eligible singleton imports may still auto-publish before review when they are the first matching live event seen. Supporting-source first-seen publishes are stored internally as provisional until a review or authoritative update confirms them, unless a supporting-only near-title guard keeps the candidate in review. Duplicate staging has two closed-history auto-resolution paths: exact canonical duplicate auto-resolution against an existing live event, and unanimous staged-duplicate auto-resolution when every staged candidate agrees on the canonical fields.

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
- before inserting a provisional event, it blocks on nearby same-venue live events whose title matches by exact clean title, aggressively normalized title, or normalized headliner
- it may fill blank live fields on later exact matches, but conflicting populated fields stay in review

Canonical-backed duplicate and cluster resolution follow these precedence rules:

- authoritative source identity wins over canonical slug match when they disagree
- non-authoritative canonical-backed duplicate resolution updates the matched live event in place rather than publishing a second event
- fresh ingest evidence wins over stale open cluster state
- terminal cluster history is immutable; conflicting fresh evidence creates a successor cluster rather than rewriting the terminal decision

Secondary-source evidence follows these retention rules:

- secondary-source genre and description rows are evidence attached to a canonical event, not replacement canonical fields
- non-authoritative resolved clusters treat secondary-source evidence as cumulative; a later resolved cluster that omits a previously seen secondary source does not invalidate or delete that source's stored information
- a staged candidate can contribute secondary-source evidence to the accepted event when its venue slug and start time match and its title matches after trimming, case folding, and whitespace normalization
- a non-authoritative secondary-source row is replaced only when the same source/event identity supplies a new non-empty value
- authoritative cluster resolution can reconcile the secondary-source rows supplied with that authoritative decision
- inferred event genres use the canonical description plus persisted secondary-source descriptions, so absence of a source in one later non-authoritative review is not treated as genre evidence retraction

Manual event-review resolution currently supports these terminal editorial decisions:

- import evidence can insert a new listing when no live target conflicts exist
- import evidence can be accepted as a supporting source for one existing reviewed or provisional event when exact identity, source identity, canonical/evidence target, slug, exact title/venue/start, or near-title evidence safely agree
- near-title false positives can record both event/evidence and event/event separations before inserting the incoming listing as a separate event
- authoritative import evidence uses source identity before slug or title matching, and can update an existing event or insert a new one
- title repair can apply a simple title/slug update, merge/update a slug-conflict duplicate while preserving the slug-owning event, or record that the slug-conflict event is separate
- historical duplicate review can apply stored live actions, override the chosen canonical/withhold actions with the same mutation guards, or keep all reviewed/provisional events separate with pairwise event separations
- unsupported conflict types stay open with an explicit blocker and no terminal resolver until the project has both a producer and an editorial policy for that conflict

Cluster evidence follows these audit rules:

- evidence linked only to open clusters may refresh while the cluster is still under review
- evidence linked to a resolved, discarded, or superseded cluster is frozen
- compatible fresh terminal evidence is retained through run-evidence provenance rather than active-link mutation
- changed evidence for a terminal decision creates a new evidence revision and, when it conflicts with the terminal outcome, a successor cluster

Venue matching and provisional venue creation follow these rules:

- when a selected staged `venue_slug` already names one existing venue, that exact slug wins even if `venue_text` or `venue_location_raw` point somewhere else
- when the selected venue evidence uniquely matches an existing venue, cluster resolution canonicalizes to that venue
- staging or non-authoritative singleton auto-promotion can create a provisional venue row immediately when venue evidence is uniquely new
- new provisional venue slugs and names prefer the derived location-head from source venue evidence, using escape-aware parsing for ICS `LOCATION` values
- when the selected venue evidence matches no existing venue, cluster resolution creates a provisional venue row and publishes against it in the same transaction if an earlier flow did not already do so
- when the selected venue evidence is ambiguous, cluster resolution fails closed rather than guessing

Clusters are held open or discarded when the source is not good enough to publish:

- ambiguous venue names
- duplicate uncertainty
- cancelled or moved shows
- stale or bad source links
- incomplete source data

The site does not claim complete Sheffield coverage unless source breadth and import health justify it.
