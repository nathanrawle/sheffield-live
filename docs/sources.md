# Sources

## Current state

This increment uses code-seeded sample data only.

The venue and event entries are illustrative and are not ingested from external systems yet.

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
