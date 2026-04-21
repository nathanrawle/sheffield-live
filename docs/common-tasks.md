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

## Run a manual Sidney & Matilda ingest

```bash
go run ./cmd/ingest -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

This fetches the source page, snapshots the page and ICS payloads, and prints a JSON report.

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
