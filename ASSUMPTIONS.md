---
title: Review Cluster Assumptions
description: One-line outline of assumptions made while refining the review-clusters cleanup plan.
---

# Review Cluster Assumptions

## Branch And Rollout

- This branch has not shipped to users.
- Rewriting branch-only migrations is acceptable.
- Breaking branch-local JSON report fields is acceptable.
- Existing local DBs that applied abandoned branch migrations do not need preservation.
- Operators can reset or recreate incompatible local DBs after a fail-fast error.

## Data And Schema

- Frozen historical `review_groups` tables may remain in older or base migrations.
- Frozen historical `review_groups` tables may remain in schema tests.
- Active runtime behavior can treat frozen historical `review_groups` tables as inert.
- New run-evidence provenance tables are acceptable schema additions.
- Keeping `fingerprint_version = 1` is preferable unless the full fingerprint algorithm changes.
- Encoding evidence revisions inside `evidence_fingerprint` is acceptable.
- Immutable terminal evidence is more important than minimizing evidence row count.
- Open-cluster evidence may stay mutable because open clusters are not final audit records.

## Product Behavior

- Review clusters are the decisive product direction.
- Legacy review-group UI compatibility is not required.
- Legacy staging flag compatibility is not required.
- Public event routes are not suitable for admin review context involving withheld records.
- Operators may navigate from staged report `cluster_id` values to the open review cluster.
- Historical duplicate repair discovery is out of scope as a CLI feature.
- Existing singleton auto-promotion remains desirable.
- Only the legacy duplicate auto-resolution cases `canonical_exact_match` and `unanimous_duplicate` are worth preserving.
- Fresh ingest evidence should override stale open manual or stored cluster state.
- Terminal audit history should remain trustworthy even when fresh ingest evidence changes later.

## Implementation Constraints

- No new external dependencies are needed.
- Existing SQLite migration style remains the migration mechanism.
- Existing Go test tooling is sufficient for validation.
- Existing repair-run and withhold primitives can be reused safely by cluster resolution.
- Cluster-native DTOs are worth the churn required to remove active `review.GroupInput` reliance.
- Active code naming should favor clarity over minimizing rename churn.
- Startup cleanup should remove legacy backfills and validators rather than keeping preservation paths.

## Validation

- Documentation cleanup scope is public docs plus `DECISIONS.md` and `ASSUMPTIONS.md`, not historical plan and chunk artifacts.
- `GOCACHE=/tmp/sheffield-live-gocache go test ./cmd/ingest ./internal/store/sqlite ./internal/web` is the smallest relevant focused validation.
- `GOCACHE=/tmp/sheffield-live-gocache go test ./...` remains the final broad validation.
- Migration tests can cover fresh pre-cluster migration and incompatible branch DB fail-fast behavior.
- Web tests can assert removed legacy routes after admin auth without keeping compatibility redirects.
