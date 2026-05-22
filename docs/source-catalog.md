# Source Catalog

## Add a Template-Fit Source

1. Add a new YAML file under `config/sources/`.
2. Set the stable identity fields:
   - `key`
   - `name`
   - `url`
   - `review_stage_source_name`
3. Set ownership policy fields for either:
   - authoritative owned-venue behavior, or
   - compatibility metadata for legacy non-authoritative singleton behavior
4. Choose one `mode`.
5. Fill the matching runtime block with existing family names.
6. Add or update ingest tests that prove the selected family works for the new source.

If the source fits an existing family, no Go code should be required.

## Add a Custom-Family Source

1. Add the YAML source definition under `config/sources/`.
2. Add a new bounded family implementation in `internal/ingest`.
3. Register that family in the relevant runtime family map.
4. Add validation and fixture-backed tests for the new family.
5. Add replay or review-stage tests if the source changes identity, normalization, or authority behavior.

Add a new family only when the source cannot be expressed by an existing family without adding a mini DSL or source-key branching back into the runtime.

## Compatibility Notes

First-seen singleton auto-publish is now runtime behavior rather than catalog gating. Owned-source singletons publish as reviewed events with authoritative source identity; supporting-source singletons publish provisionally when they are the first matching live event seen and are not blocked by the supporting-only near-title guard. `owned_venue_slug` defines authoritative overwrite rights. `non_authoritative_singleton_venue_slug` remains as compatibility metadata and should not be used as the primary way to reason about singleton publication behavior.

In v1, these fields are replay-sensitive and review-sensitive:

- `key`
- `name`
- `url`
- `review_stage_source_name`

Treat them as immutable once a source has historical import runs or staged review data.
