# Recurring Calendar UID Multi-Occurrence Plan

## Summary

Handle import-review clusters where a calendar source emits multiple valid event occurrences that share one recurring UID. The concrete walkthrough cases were clusters `#6`-`#8` from The Washington: each cluster grouped multiple separate dated performances under a repeated UID while each occurrence also had a distinct Google Calendar event URL.

## Dependencies

- Reuse existing event-review separation primitives where possible.
- Reuse import-review candidate parsing, source identity choice, and selected-candidate readiness helpers.
- Coordinate with the event-review detail summary work if it lands first, because this shape needs clear operator copy about repeated UID semantics.

## Key Changes

- [ ] Detect recurring calendar UID clusters where multiple active evidence rows share a source UID but differ by exact identity/date.
- [ ] Treat repeated UIDs as non-disambiguating for single-event identity when they span multiple candidate start dates.
- [ ] Decide and implement the operator path:
  - split into one cluster per occurrence,
  - accept all eligible occurrences as separate new listings in one terminal action,
  - record evidence/evidence or identity/evidence separations that prevent the repeated UID from binding the occurrences together,
  - or explicitly mark the repeated UID as non-disambiguating and use per-occurrence URLs as event-level source identities.
- [ ] Ensure resolving one occurrence cannot silently resolve the whole cluster while leaving other valid occurrences stranded.
- [ ] Show a clear admin summary when a source identity key is shared across multiple candidate dates.
- [ ] Keep existing near-title, title slug-conflict, and historical duplicate separation behavior unchanged.

## Test Plan

- [ ] Store/readiness coverage for clusters with multiple evidence rows sharing one source UID and distinct exact identity keys.
- [ ] Resolver coverage for the selected operator path, including all occurrences in clusters matching the `#6`-`#8` shape.
- [ ] Regression coverage proving a single selected-candidate accept cannot resolve a recurring UID cluster while leaving other active occurrences unhandled.
- [ ] Coverage that per-occurrence Google Calendar event URLs can remain valid source identities even when the repeated UID is non-disambiguating.
- [ ] Admin UI coverage for the repeated-UID summary/blocker/action surface.
- [ ] Existing separation and import-review tests continue to pass.

## Done Criteria

- [ ] Clusters shaped like `#6`, `#7`, and `#8` have a safe terminal editorial path or an explicit non-terminal blocker.
- [ ] The chosen path records enough state to stop the same recurring UID false grouping from reappearing unchanged.
- [ ] The operator UI explains that the repeated UID spans multiple dates and should not be treated as one event identity.
- [ ] Focused store and admin tests pass.
- [ ] Full relevant package tests pass.

## Walkthrough Evidence

- Cluster `#6`: `[DJ] FIESTA!`, evidence `#6`-`#9`, separate dates `26 May`, `2 Jun`, `9 Jun`, and `16 Jun 2026`, shared UID `uid:E93E4748-F137-4ABA-AC41-713AE8DABCDE`.
- Cluster `#7`: `[DJ] Asbo A Gogo`, evidence `#10`-`#13`, separate dates `27 May`, `3 Jun`, `10 Jun`, and `17 Jun 2026`, shared UID `uid:5E50DBD4-4BCC-4576-A32E-A0C1E84EAB77`.
- Cluster `#8`: `[DJ] Affirmative Alternative`, evidence `#14`-`#16`, separate dates `1 Jun`, `8 Jun`, and `15 Jun 2026`, shared UID `uid:0d0uhpmht5v60t5vfiv2j7kd13@google.com`.
