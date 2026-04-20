# MVP

## Product Scope

The product is a small, readable Sheffield live-music browser that answers three questions quickly:

- what is coming up?
- where is it happening?
- where did this listing come from?

The current scope includes:

- SQLite persistence with seed bootstrap
- manual Sidney & Matilda ingest
- review staging
- review-to-public-event publishing
- server-rendered minimalist UI
- public browsing for home, events, event detail, venues, and venue detail
- visible provenance and freshness on records
- visibly tagged seed, test, and dev data
- live records that are not tagged
- one lightweight SQLite driver dependency

## Non-Goals

- broad source coverage
- automated publishing without review
- login or auth hardening
- a recurring scheduler
- image or media ingestion
- a claim of complete Sheffield coverage
- a rich JavaScript app or HTMX unless it earns its place

## Current Constraints

The site stays curated and source-led. Canonical public events are published only through review resolution, and the public surface stays simple and server-rendered.
