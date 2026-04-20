# Sources

## Current state

This increment uses code-seeded public venue and event data only.

The venue and event entries are illustrative and are not ingested from external systems yet. Phase 4A adds manual raw snapshotting and ICS parsing for review, but parsed candidates are report-only and are not written to public event tables.

## Phase 4A manual source check

Sidney & Matilda is the first manual ingestion source. The command fetches `https://www.sidneyandmatilda.com/`, stores a raw source-page snapshot, extracts anchors labelled "Google Calendar ICS", stores raw ICS snapshots, and prints a JSON report containing parsed candidates, skips, and errors.

Snapshot payloads use a JSON envelope in `snapshots.payload` with base64 body content, content metadata, captured-body SHA-256, and a truncation flag.

This path is intentionally fail-closed:

- zero ICS links is a report error
- non-2xx responses are still snapshotted when a response body is available
- all-day, cancelled, malformed, or incomplete events are skipped
- no venue or event rows are written

## First source candidates

Official venue listings should be reviewed first:

- The Leadmill
- Yellow Arch Studios
- Sidney & Matilda
- Corporation Sheffield
- FoundrySU
- The Greystones
- Crookes Social Club

Aggregators can help with coverage and cross-checking after official pages are understood:

- Welcome to Sheffield gigs
- Our Favourite Places music picks
- SheffieldGigs
- Sheffield Gig Guide

APIs should only be added where terms and value are clear. Ticketmaster Discovery and Eventbrite are plausible later candidates. Skiddle, Songkick, and Bandsintown need careful access and usage review before they are treated as reliable inputs.

## Phase 2 expectations

Before any scraper or API import is built:

- check robots.txt and terms
- prefer official feeds, APIs, or permission where available
- store the canonical source URL
- store fetch/import time
- keep raw source snapshots separate from canonical event data
- avoid copying long descriptions or images unless licensed
