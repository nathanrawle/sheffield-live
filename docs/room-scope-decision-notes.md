# Room Scope Decision Notes

## Background

The venue-room model currently stores concrete room assignments separately from source room text:

- `domain.Event` has `Rooms []VenueRoom` and `RoomText string`
- `review.Candidate` and `review.CandidateInput` have `Rooms []VenueRoom` and `RoomText string`
- SQLite stores concrete rooms in `event_rooms` and `review_candidate_rooms`
- SQLite stores source room text in `events.room_text` and `review_candidates.room_text`
- ingest replay JSON carries `room_text` and `rooms`

Sidney & Matilda can provide room evidence such as `FACTORY`, `BASEMENT`, `GALLERY`, `GALLERY + BASEMENT`, and `WHOLE VENUE`. Concrete labels map to room slugs. `WHOLE VENUE` is explicit source evidence, but it is not a concrete room assignment, so it currently stores as `RoomText: "WHOLE VENUE"` with no room slugs.

GitHub issue: https://github.com/nathanrawle/sheffield-live/issues/19

## Outstanding Issue

Explicit whole-venue evidence must not collapse to the same review value as absent room evidence.

The immediate bug is in review defaults and auto-resolution: room comparison historically reduced candidates to concrete room slugs only. A candidate with `RoomText: "WHOLE VENUE"` and no room slugs therefore compared the same as a candidate with no room evidence at all. That can let defaults and auto-resolution drop the intended whole-venue context.

## Current Assessment

Do not model `WHOLE VENUE` as a fake room such as `every-room`.

Reasons:

- `every-room` is not a physical or venue-defined room.
- It would leak into room admin, room detail pages, event room links, and matching unless special-cased.
- Whole-venue source text is not always equivalent to all currently modeled rooms. It may include unmodeled spaces, flexible layouts, or simply mean the source is not room-specific.
- If a venue later adds another room, historical `every-room` semantics become ambiguous.
- Treating whole-venue as a concrete room set could create false conflicts against one-room or partial-room evidence from another source.

A cleaner long-term model would likely add explicit room scope, with values along these lines:

- `unspecified`: source has no room or scope evidence
- `specific_rooms`: source identifies one or more concrete rooms
- `whole_venue`: source explicitly says the event is venue-wide

`RoomText` should still be preserved as source/display text.

## Why Explicit Scope Is A Larger Decision

Adding a column is straightforward. Defining and threading the semantics is the larger part.

A proper room-scope change likely touches:

- domain structs
- review structs and draft/default choice behavior
- ingest structs and replay JSON
- SQLite schema and backfill rules
- Sidney extraction and future source extraction
- review defaults and auto-resolution
- duplicate matching and secondary-source support matching
- event publishing/update paths
- public/admin display behavior
- tests and fixture expectations

## Unresolved Questions

1. What exact scopes are needed? Is `unspecified`, `specific_rooms`, and `whole_venue` enough?
2. Should `whole_venue` compare as conflicting with a specific room, compatible-but-not-identical, or source-preferred depending on source authority?
3. Should `whole_venue` compare as identical to all known venue rooms, or remain a distinct scope?
4. If all current rooms are selected (`factory + basement + gallery`), is that semantically different from `WHOLE VENUE`?
5. Should a source-provided label like `WHOLE VENUE` be display text only, scope only, or both?
6. How should historical rows be backfilled? Is `room_text = "WHOLE VENUE"` enough to infer `whole_venue`, and what about other text variants?
7. Should public UI display `Venue (Whole Venue)`, just `Venue`, or a different label?
8. Should admin room pages ever include whole-venue events, or only concrete room-linked events?
9. Should review choices expose scope separately from rooms, or keep one combined rooms/scope field?
10. How should source authority affect conflicts between blank, whole-venue, and room-specific evidence?

## Short-Term PR Fix

For the venue-room PR, keep the schema unchanged and make review room comparison distinguish explicit text-only room evidence from blank evidence.

That fixes the observed review/default/auto-resolution issue without committing to full room-scope semantics.

## Decision Status

Deferred. Revisit explicit room scope after the questions above are answered.
