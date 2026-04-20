# Sources

## Current Flow

The current manual source pipeline starts with Sidney & Matilda.

The ingest run fetches the source page, stores a raw source-page snapshot, extracts `Google Calendar ICS` links, fetches each ICS feed, stores raw ICS snapshots, and parses event candidates, skips, and parse errors.

Snapshots are kept as separate raw artifacts. They are not the same thing as canonical public event rows.

The ingest run writes to `sources`, `import_runs`, and `snapshots`, and it records the parsed report output rather than publishing public events directly.

## Snapshot Payloads

Snapshot payloads are stored as JSON envelopes that contain the response body in base64, response metadata, a captured-body SHA-256, and a truncation flag.

## Review Staging

`cmd/ingest` can stage review groups from a successful ingest report.

Review staging creates duplicate clusters and singleton new listings. Duplicate review groups support field-level canonical choices plus a canonical draft summary. Singleton review groups support accept or reject.

## Publish Rules

Resolving a duplicate group or accepting a singleton publishes exactly one canonical public event in the same SQLite transaction.

Rejecting either a duplicate or singleton review does not publish an event.

When a review group resolves:

- selected review fields map to `internal/domain.Event`
- source name and source URL fall back to the review-group source only when the selected field is blank
- the venue must already exist
- the source row is ensured transactionally
- the published event origin is `live`
- the slug is `live-<slug(name)>-<slug(venue)>-<YYYYMMDDHHMMSS UTC>`
- slug conflicts are handled with upsert semantics

## Source Strategy

Prefer official venue listings first.
Use aggregators later for coverage and cross-checking.
Add APIs only where terms and value are clear.
