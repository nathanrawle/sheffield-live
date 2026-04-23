# Command Reference

This repository has one Go monolith and two entrypoints:

- `./cmd/web` serves the site
- `./cmd/ingest` performs manual venue-source ingestion and review staging

## `./cmd/web`

Run:

```bash
go run ./cmd/web
```

Environment:

- `ADDR` defaults to `:8080`
- `DB_PATH` defaults to `./data/sheffield-live.db`

Behavior:

- opens and bootstraps the SQLite database on startup
- serves server-rendered HTML
- uses `modernc.org/sqlite`
- requires writable storage for the database path

Routes:

- `GET /` home page
- `GET /events` event list
- `GET /events/{slug}` event detail
- `GET /venues` venue list
- `GET /venues/{slug}` venue detail
- `GET /admin/review` open review queue
- `GET /admin/review/history` read-only resolved and rejected review history
- `GET /admin/review/{groupID}` review detail
- `GET /admin/import-runs` read-only import history
- `GET /admin/import-runs/{id}` read-only import run snapshot metadata
- `POST /admin/review/{groupID}` review actions
- `GET /healthz` plain-text health check
- `GET /static/site.css` embedded stylesheet

`/events` query parameters:

- `window=all|today|week`
- `venue={venue-slug}`

`/admin/review` and `/admin/review/{groupID}` flash query parameters:

- `saved=1`
- `resolved=1`
- `accepted=1`
- `rejected=1`

Review behavior:

- duplicate groups use field-by-field draft choices and a canonical draft summary
- the review queue shows a read-only link to the latest successful import when the store provides import history
- `action=save` stores draft choices for duplicate groups
- `action=resolved` confirms a duplicate and resolves it, publishing one canonical public event
- singleton groups use accept/reject actions
- `action=accept` resolves a singleton group and publishes one canonical public event
- `action=rejected` rejects a duplicate or singleton group without publishing
- closed groups are read-only and disappear from the open queue
- review history lists the 50 newest resolved and rejected groups
- import history and import run detail pages are read-only and available only when the store implements them
- import run detail pages show summary fields and snapshot metadata only; stored snapshot payload bodies are not rendered

## `./cmd/ingest`

Run:

```bash
go run ./cmd/ingest -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Defaults:

- `-source` defaults to `sidney-and-matilda`
- `-limit` defaults to `20`
- `-timeout` defaults to `10s`
- `-db` overrides `DB_PATH`

Validation:

- `-limit` applies to live ingest and replay, and must be between `1` and `50`
- `-timeout` must be positive
- live ingest requires a non-empty `-http-user-agent` or `-user-agent`
- replay does not require a user agent

Live ingest:

- primary flag: `-http-user-agent`
- alias: `-user-agent`
- supports `sidney-and-matilda` and `yellow-arch`
- fetches the selected source page
- Sidney & Matilda snapshots the source page and fetched ICS payloads
- Yellow Arch snapshots only the source page and parses embedded JSON-LD event data from that page
- parses candidates, skips, and errors
- writes `sources`, `import_runs`, and `snapshots`
- prints a JSON report to stdout

Replay:

- `-import-run-id <id> [-limit N] [-stage-review-groups]`
- network-free
- only replays finished succeeded runs
- validates the stored snapshot envelope version and body SHA-256
- refuses missing or ambiguous snapshot matches
- auto-detects the source from stored page snapshot metadata
- reconstructs source-specific extraction from stored source page snapshots
- Sidney & Matilda replays source-page extraction to ICS links and matching ICS snapshots by URL and final URL
- Yellow Arch replays candidate parsing directly from the stored source page snapshot

Stage review groups:

- primary flag: `-stage-review-groups`
- alias: `-stage-review`
- wraps the ingest report with `review_stage`
- creates duplicate review groups
- creates singleton review groups
- reports `groups_created` and `groups_reused`
- each staged group includes `result: created|reused`
- only runs after a successful ingest

Offline review fixture:

- primary flag: `-review-ics-fixture`
- alias: `-review-fixture`
- mutually exclusive with replay
- reads a local ICS file
- does not use the network
- parses candidates, skips, and errors
- creates one offline review group
- prints a JSON report with the fixture path, group ID, candidate count, skips, and errors

`-review-title` sets the review-group title used with `-review-ics-fixture`.
`-review-ics-fixture` remains non-idempotent.
