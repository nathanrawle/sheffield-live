# MVP

## Goal

Ship a small, readable Sheffield live-music browser that answers three questions quickly:

- what is coming up?
- where is it happening?
- where did this listing come from?

## Current increment

- one deployable Go server
- hand-seeded events and venues with source and freshness fields
- simple list/detail browsing
- static CSS only
- no login, admin, or write path
- no ingestion pipeline
- no external dependencies

## What this proves

- the route model is stable
- the content model can handle venues and events
- source provenance belongs in the event detail experience from the beginning
- the HTML layout is reusable
- the site can stay dependency-light

## Phase 2 boundary

Phase 2 should add persistence behind the same public routes. The first persistence pass should preserve the distinction between canonical event fields and source/provenance fields.
