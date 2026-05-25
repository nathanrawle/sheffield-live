# Event Review Resolution Plan Parts

| plan-part-name | mini-summary | status | planner-agent-id | implementer-agent-id | reviewer-agent-id | plan-part-file |
| --- | --- | --- | --- | --- | --- | --- |
| import-supporting-source | Add existing-event supporting-source attachment and manual new-listing secondary-source persistence for import-review clusters. | implemented | top-level session | top-level session |  | `001_IMPORT_SUPPORTING_SOURCE_PLAN.md` |
| import-near-title-resolution | Add near-title same-event and false-positive separation/new-listing paths. | planned | top-level session |  |  | `002_IMPORT_NEAR_TITLE_RESOLUTION_PLAN.md` |
| import-authoritative-resolution | Add manual authoritative import resolution with source-first target semantics. | planned | top-level session |  |  | `003_IMPORT_AUTHORITATIVE_RESOLUTION_PLAN.md` |
| canonical-exact-provenance | Record supporting provenance during canonical exact auto-resolution. | planned | top-level session |  |  | `004_CANONICAL_EXACT_PROVENANCE_PLAN.md` |
| title-repair-slug-conflict | Resolve title-repair slug conflicts by merge/update or keep-separate. | planned | top-level session |  |  | `005_TITLE_REPAIR_SLUG_CONFLICT_PLAN.md` |
| historical-duplicate-keep-separate | Add all-keep false-positive resolution for historical duplicate clusters. | planned | top-level session |  |  | `006_HISTORICAL_DUPLICATE_KEEP_SEPARATE_PLAN.md` |
| historical-duplicate-action-override | Allow manual historical duplicate live-action override with validation. | planned | top-level session |  |  | `007_HISTORICAL_DUPLICATE_ACTION_OVERRIDE_PLAN.md` |
| unsupported-cluster-docs | Rolling docs checkpoint and unsupported-cluster blocker cleanup after shipped milestones. | planned | top-level session |  |  | `008_UNSUPPORTED_CLUSTER_DOCS_PLAN.md` |

## Status Values

- `planned`: Plan exists and is ready for implementation planning/review.
- `in-progress`: Implementation has started.
- `implemented`: Code changes are complete and local focused checks pass.
- `reviewed`: Reviewer has approved or no blocking findings remain.
- `blocked`: Implementation cannot proceed without a decision or prerequisite.
