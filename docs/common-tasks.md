# Common Tasks

These recipes stay short on purpose. See [Command Reference](commands.md) for the full flag list and command details.

## Run the site

```bash
go run ./cmd/web
```

## Run on a different address or database

```bash
ADDR=:3000 DB_PATH=/tmp/sheffield-live.db go run ./cmd/web
```

## Inspect the local SQLite DB

```bash
sqlite3 ./data/sheffield-live.db
```

Use the SQLite CLI only if you already have it installed. Any read-only query tool you prefer is fine.

## Run a manual ingest

Sidney & Matilda is the default source:

```bash
go run ./cmd/ingest -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Yellow Arch uses the same command with an explicit source:

```bash
go run ./cmd/ingest -source yellow-arch -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Sidney & Matilda snapshots the source page plus linked ICS payloads. Yellow Arch snapshots only the source page and parses embedded JSON-LD event data from that page. `-limit` caps linked ICS fetches for Sidney & Matilda and parsed source-page candidates for Yellow Arch. Both commands print a JSON report.

## Stage review groups after ingest

```bash
go run ./cmd/ingest -http-user-agent "sheffield-live manual ingest (contact: you@example.com)" -stage-review-groups
```

This adds duplicate and singleton review groups after a successful ingest, and reruns reuse existing groups when the staged content matches.

## Replay a stored ingest run

```bash
go run ./cmd/ingest -import-run-id 42 -limit 20 -stage-review-groups
```

This rebuilds the report from stored snapshots without using the network. Reruns are safe and reuse existing groups when the staged content matches. Omit `-stage-review-groups` if you only want the replay report.
Replay auto-detects whether the stored run used linked ICS extraction or direct source-page parsing.

## Create an offline review group from a local ICS file

```bash
go run ./cmd/ingest -review-ics-fixture internal/ingest/testdata/sidney.ics
```

This is the no-network path for review data.

## Reset a disposable local database

Stop the app, then remove only your local development DB file and start again. This example uses the default `DB_PATH`; use your configured path if you changed it.

```bash
rm -f ./data/sheffield-live.db
go run ./cmd/web
```

Use this only for a disposable local database. Do not delete a shared or production DB.

## Work the review queue

Open `/admin/review` in the browser.

- duplicate groups use field choices and a canonical draft summary
- singleton groups use accept/reject
- resolving or accepting publishes one canonical public event
- rejecting does not publish
- open `/admin/review/history` for the 50 newest resolved and rejected groups
