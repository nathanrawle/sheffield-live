# Sources

## Current Flow

The current manual source pipeline supports Sidney & Matilda and Yellow Arch.

Sources are registered in code with source metadata plus a page-processing mode. That mode decides whether ingest expands the page into linked ICS fetches or parses candidates directly from the stored page snapshot.

Every ingest run fetches the source page and stores a raw source-page snapshot.

After that, parsing depends on the source:

- Sidney & Matilda extracts ICS export links from the source page, fetches each ICS feed, stores raw ICS snapshots, and parses candidates, skips, and parse errors from ICS.
- Yellow Arch parses candidates, skips, and parse errors directly from schema.org `Event` JSON-LD embedded in the source page. No secondary snapshots are fetched for that source.

Snapshots are kept as separate raw artifacts. They are not the same thing as canonical public event rows.

The ingest run writes to `sources`, `import_runs`, and `snapshots`, and it records the parsed report output rather than publishing public events directly.

Sidney & Matilda extraction accepts Squarespace `?format=ical` ICS links and legacy Google Calendar-style ICS labels.
Yellow Arch parsing accepts embedded JSON-LD arrays or graphs that contain schema.org `Event` objects.

## Snapshot Payloads

Snapshot payloads are stored as JSON envelopes that contain the response body in base64, response metadata, a captured-body SHA-256, and a truncation flag.

## Review Staging

`cmd/ingest` can stage review groups from a successful ingest report.

Review staging creates duplicate clusters and singleton new listings. Duplicate review groups support field-level canonical choices plus a canonical draft summary. Singleton review groups support accept or reject.
Review staging uses a durable key, so source metadata changes alone do not create a new group, and closed groups are not reopened.

Replay auto-detects the source from stored page snapshot metadata, reconstructs the same source-specific extraction path from stored snapshots, validates the snapshot envelope version and SHA-256, and refuses missing or ambiguous snapshot matches.

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
