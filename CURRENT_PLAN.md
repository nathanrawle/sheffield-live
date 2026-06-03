# Current Plan: Issue #47 Hallamshire Hotel All-Day ICS

Updated: 2026-06-02T22:50:58Z

## Agents

- Architect: `019e8a82-da3a-73d0-b7a2-d9fe461a7149`
- Reviewer: `019e8a82-dab9-7b01-b42f-866d87780b46`

## Objective

Fix GitHub issue #47 by making Hallamshire Hotel ingest keep current/future live music entries that appear as all-day ICS rows, enrich them from trusted linked detail pages where possible, and make any fallback inferred time explicit and review-safe.

## Decisions

- Keep the generic ICS parser conservative. All-day admission is Hallamshire-specific.
- Hallamshire all-day rows are admitted only when they are single-day events and look like live music by source-scoped title policy.
- Multi-day all-day rows and explicit non-music rows are skipped unless a future source-specific parser can prove a single concrete event occurrence.
- Hallamshire all-day fallback start is `19:30 Europe/London` on the `DTSTART` date, converted to UTC. All-day `DTEND` is not treated as a real event end time.
- Candidates carry explicit metadata for inferred starts (`StartAtInferred` and `StartAtBasis`) in report JSON and review evidence/provenance.
- Detail page time can replace an inferred fallback start when the linked page exposes reliable event time. Detail time must not override a real timed ICS start.
- Fallback-only inferred candidates are staged for review but must not auto-promote as reviewed or provisional events.
- Detail links are trusted only when the URL belongs to a provider/parser family we explicitly support for Hallamshire enrichment. Unsupported links remain candidate URLs only when useful for provenance, and failed detail fetches do not fail ingest.
- Hallamshire source identity remains based on the ICS calendar/UID. Detail URLs may become candidate/source display URLs, but their source-identity key is disabled so event-review evidence identity keys do not use delegated ticket/detail URLs.
- Initial Hallamshire all-day music title allowlist is case-insensitive and prefix-based: `GIG:`, `LIVE:`, `FREE ENTRY GIG:`, and `FREE GIG:`.
- Initial Hallamshire all-day title denylist is case-insensitive and prefix/exact based: `QUIZ:`, `FREE ENTRY QUIZ:`, `FREE ENTY QUIZ:`, `CLUB NIGHT:`, `CLUB EVENT:`, `FREE ENTRY CLUBNIGHT:`, `PRE/POST:`, exact `PRIVATE PARTY`, exact `TRAMLINES FRINGE`, and exact `FRINGE AT TRAMLINES`.
- Initial trusted Hallamshire detail providers are `fatsoma.com`, `leadmill.co.uk`, and `wegottickets.com`, because sampled pages expose reliable structured or bounded event detail data. Links to social/homepage/calendar/search pages are not fetched for enrichment.

## Checklist

- [x] Inspect issue #47 and relevant repo/source docs.
- [x] Start requested architect and reviewer agents.
- [x] Draft plan and request architect evaluation.
- [x] Incorporate architect feedback and get refined plan accepted.
- [x] Commit the approved plan file.
- [x] Implement explicit inferred-start metadata and review-stage payload/provenance/auto-promotion gate.
- [x] Ask reviewer to review metadata/auto-promotion diff; address actionworthy comments.
- [ ] Implement Hallamshire-specific ICS parser and config switch.
- [ ] Ask reviewer to review parser/config diff; address actionworthy comments.
- [ ] Implement trusted Hallamshire detail enrichment and detail-time merge.
- [ ] Ask reviewer to review enrichment diff; address actionworthy comments.
- [ ] Add/adjust focused tests for parser, enrichment, review staging, and replay parity.
- [ ] Ask reviewer to review test coverage and final diff; address actionworthy comments.
- [ ] Run focused validation (`go test ./internal/ingest` and relevant CLI/store tests).
- [ ] Run broader validation (`go test ./...`) when focused checks pass.
- [ ] Push branch and open a ready-to-review PR.
- [ ] Wait 5 minutes, then poll every 5 minutes for PR comments/reactions.
- [ ] Address actionworthy comments until reviewer/Codex bot has no more changes.
- [ ] Stop when the Codex bot gives a thumbs up. If the bot leaves no emoji, toggle draft and ready-for-review once.

## Test Plan

- Generic `ParseICS` still skips all-day `DTSTART;VALUE=DATE` rows.
- Hallamshire parser admits a single-day all-day music row with DST-correct fallback start, blank end, URL extraction, and inferred-start metadata.
- Hallamshire parser skips multi-day all-day rows and non-music all-day rows.
- Timed Hallamshire rows behave like the generic parser and are not marked inferred.
- Detail enrichment replaces inferred start/end/description/image/canonical URL when a trusted linked page exposes reliable data.
- Detail enrichment does not override real timed ICS starts.
- Unsupported or failed detail pages do not fail ingest and leave fallback candidates staged.
- Review-stage evidence/provenance exposes inferred-start metadata, preserves ICS UID/calendar identity, and prevents singleton auto-promotion for inferred fallback starts.
- Hallamshire enriched evidence identity keys include ICS UID/calendar identity and do not include delegated detail/ticket URLs.
- Replay with stored detail snapshots reproduces the enriched report.
