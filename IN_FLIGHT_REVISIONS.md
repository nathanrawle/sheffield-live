# In-Flight Revisions

Record any implementation-time plan changes here. When an entry is added, mark the affected `CURRENT_PLAN.md` section invalidated and point back to this file.

## CHUNK-7 Duplicate Auto-Resolution Transaction Boundary

- The original plan required staging/merge plus auto-resolution to happen in one transaction.
- Implementation keeps per-evidence event-review staging transactions and runs open-cluster restage cleanup plus duplicate auto-resolution in the store-owned finalization transaction.
- If final auto-resolution fails, already staged review evidence/cluster links may remain, but live event mutations, observations created by auto-resolution, cluster terminal status, and resolution snapshots roll back together.
- The command layer may trigger finalization, but duplicate eligibility, live side effects, cluster terminal update, and resolution snapshot insertion remain store-owned.

## CHUNK-9 Active Legacy Review-Group Store Cleanup

- Broad validation after CHUNK-8 showed active legacy review-group store tests still exercise `review_groups` branch columns that the review-cluster direction intentionally removed.
- Add a final cleanup chunk to retire active legacy review-group store APIs/tests while preserving shared low-level helper functions still used by event-review cluster resolution.
- This is not a data-preservation compatibility path; fresh review-cluster migrations remain decisive, and frozen historical schema references may remain only as inert migration/schema coverage.
