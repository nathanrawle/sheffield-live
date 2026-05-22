# Duplicate Event Near-Title Guard Plan

## Context

The duplicate Joe Carnall event was created because the supporting Leadmill candidate did not match the existing Yellow Arch live event:

- Yellow Arch authoritative event: `2026-06-06T18:30:00Z`, title with an en dash.
- Leadmill supporting candidate: `2026-06-06T18:00:00Z`, title with a hyphen.

The existing exact identity checks did not match because both the time and exact clean title differed. The existing guarded near-match window was close enough on venue/time, but its title comparison only lowercases and collapses whitespace, so `-` and `–` remained different.

## Goals

1. Prevent supporting-source singleton promotion from inserting provisional duplicates when a near live event has an aggressively normalized matching title.
2. Keep durable exact identity behavior unchanged.
3. Keep authoritative-source near-update/import-review behavior conservative.
4. Preserve review visibility when a supporting candidate is blocked by a near-title match.

## Non-Goals

- Do not change `eventidentity.NormalizeCleanTitle` or existing `event_exact_identities` keys.
- Do not add heuristic title matching to the shared guarded near matcher used by authoritative paths.
- Do not silently merge or move source links for supporting candidates blocked by near-title matches.
- Do not add new dependencies.

## Implementation

1. Add a supporting-only near-title guard used by `promoteNonAuthoritativeSingletonReviewClusterIfMissing` after exact matching fails and before provisional insertion.
2. Keep the existing exact clean title near-match tier as the first guard.
3. Add an aggressively normalized title tier:
   - lowercase
   - Unicode letter/number aware
   - consecutive non-letter/non-number runs collapsed to a single `-`
   - leading and trailing separators trimmed
4. Add a normalized headliner-only tier using conservative separator handling:
   - `with`
   - `featuring`
   - `feat`
   - `ft`
   - `plus`
   - `vs`
   - `&`
   - `+`
   - `/`
5. Ignore empty, one-character, and punctuation-only heuristic keys.
6. Return match tiers for tests and diagnostics:
   - `clean_title_near`
   - `title_variant_near`
   - `headliner_near`
7. Preserve the existing supporting behavior when a near match is found:
   - do not insert the provisional event
   - do not move or attach a source link
   - keep staged/review evidence available
   - record conflict observations where the single-match path already supports it
8. Fix guarded near-match disabling centrally so disabled sources return no near records, even if callers ignore the enabled flag.

## Validation

1. Add pure title-tier tests for dash variants, headliner-only matches, accented titles, and unusable short/punctuation keys.
2. Add a regression test covering the Yellow Arch/Leadmill-style title and time mismatch so the supporting candidate is blocked instead of inserted.
3. Add coverage that disabled guarded-near matching returns no records.
4. Run focused store tests, then broader relevant package tests if the focused tests pass.
