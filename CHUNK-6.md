# CHUNK-6: Terminal Evidence And Run-Evidence Provenance

## CURRENT_PLAN.md Coverage

- Terminal-linked evidence is immutable.
- Changed payload under a terminal-linked fingerprint creates a new evidence revision while keeping `fingerprint_version = 1`.
- Compatible fresh terminal evidence is persisted via run-evidence provenance and not active-linked to the immutable terminal cluster.
- Terminal reuse and exact replay matching search active terminal evidence and run-evidence provenance evidence.
- Successor idempotency follows staging key plus staging key version.

## Affected Files

- `internal/store/event_review.go`
- `internal/store/sqlite/event_review_staging.go`
- `internal/store/sqlite/event_review_repair_staging.go`
- `internal/store/sqlite/event_review_read.go`
- `internal/store/sqlite/event_review_staging_test.go`
- `internal/store/sqlite/event_review_repair_staging_test.go`
- `internal/store/sqlite/event_review_read_test.go`

## Implementation Plan

- Start only after CHUNK-4 provenance schema exists and CHUNK-5 deterministic open staging keys are in place.
- Reorder staging so evidence is classified before mutation:
  - look up existing evidence by fingerprint;
  - detect whether it is linked to any terminal cluster before updating payload, source, event, updated_at, or identity links;
  - compare payload/source/event/identity material;
  - decide exact replay, compatible provenance-only evidence, revised fingerprint, successor, or retryable conflict before writing.
- Wire run-evidence provenance inserts into import and repair staging paths.
- Every import/repair staging path that associates a run with evidence writes provenance, including normal open staging, terminal exact replay, compatible terminal reuse, and successor creation.
- Detect terminal-linked evidence before in-place refresh and create revised evidence fingerprints when payload changes.
- Evidence revision fingerprint format: encode original fingerprint, a hash of the full immutable evidence material being revised (`payload`, `source_id`, `event_id`, identity keys), and evidence revision algorithm version inside `evidence_fingerprint`; keep `fingerprint_version = 1`.
- Treat changed payload, source, event, or identity material under terminal-linked evidence as requiring revision/successor unless it is exact replay or compatible terminal reuse by the concrete compatibility rules.
- Cover resolved, discarded, and superseded terminal statuses; resolved-only lookup is insufficient.
- Define compatibility from immutable resolution snapshot data, canonical event identity, selected identity choices, and recorded live actions.
- Implement terminal exact replay, compatible reuse, conflicting successor, superseded lineage, and retryable separation behavior from the table.
- Ensure terminal successor creation writes deterministic immutable creation keys from fresh evidence identity set, previous cluster, conflict type, and reason.
- Ensure terminal successor creation does not mutate terminal `superseded_by_cluster_id`.
- Add store tests for provenance idempotency, FK behavior, terminal immutability, evidence revisions, provenance-only terminal reuse, successor idempotency, discarded/superseded statuses, and retryable separation rollback.

## Ownership Notes

- CHUNK-4 owns provenance table schema and constraints.
- CHUNK-5 owns open-cluster staging keys; this chunk owns terminal successor keys.
- This chunk may introduce typed constants/helpers for run-evidence provenance link reasons.

## Completion Criteria

- Terminal cluster active evidence and identity links are never mutated by fresh staging.
- Compatible terminal fresh observations are auditable through run-evidence provenance.
- Conflicting terminal fresh observations create/reuse the intended successor.
- Terminal exact replay preserves `payload`, `source_id`, `event_id`, `updated_at`, and evidence identity links.
- Changed terminal payload creates a new evidence revision and successor while keeping `fingerprint_version = 1`.
- Provenance-only evidence participates in later terminal matching.
- Same successor evidence reruns reuse the successor by staging key.
- Retryable separation conflict leaves no cluster/evidence/provenance mutation.

## Validation

- `GOCACHE=/tmp/sheffield-live-gocache go test ./internal/store/sqlite`
