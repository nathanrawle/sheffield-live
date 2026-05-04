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

If local or global git config already has `user.email`, you can omit the flag and let the command derive a default user agent:

```bash
go run ./cmd/ingest
```

Yellow Arch uses the same command with an explicit source:

```bash
go run ./cmd/ingest -source yellow-arch -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Leadmill uses the same pattern:

```bash
go run ./cmd/ingest -source leadmill -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Jazz at The Lescar also uses the same pattern:

```bash
go run ./cmd/ingest -source jazz-at-the-lescar -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

The Greystones also uses the same pattern:

```bash
go run ./cmd/ingest -source the-greystones -http-user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Sidney & Matilda snapshots the source page, linked ICS payloads, and linked event detail pages; ICS remains authoritative for identity and times, while detail pages enrich blank descriptions. Cafe No. 9 snapshots the WeGotTickets organiser pages plus event detail pages and enriches descriptions from detail-page event information. Leadmill snapshots the source page plus linked ICS payloads. Yellow Arch and Jazz at The Lescar snapshot only the source page and parse candidates directly from that page. The Greystones and Corporation snapshot the source page plus linked detail pages. `-limit` caps linked ICS or linked detail-page fetches and parsed source-page candidates for direct source-page parsers. All commands print a JSON report.
Use `-contact you@example.com` to override the contact detail in the derived default user agent, or `-contact none` to suppress contact info entirely.

## Stage review groups after ingest

```bash
go run ./cmd/ingest -http-user-agent "sheffield-live manual ingest (contact: you@example.com)" -stage-review-groups
```

This stages duplicate groups and any singleton groups that were not auto-promoted after a successful ingest. Any singleton may auto-publish first when it is the first matching live event seen; authoritative sources can also upgrade existing provisional events in place, while supporting-source conflicts stay in review. Duplicate staging can also auto-resolve an exact canonical match or a unanimous staged duplicate and records those outcomes separately in `review_stage.duplicate_auto_resolved`. Reruns reuse existing staging keys when the staged content matches, refresh open-group canonical snapshot/default state, preserve manual draft choices, and record the new import-run link in the persisted provenance table.

## Replay a stored ingest run

```bash
go run ./cmd/ingest -import-run-id 42 -limit 20 -stage-review-groups
```

This rebuilds the report from stored snapshots without using the network. Reruns are safe and reuse existing groups when the staged content matches, eligible singletons may auto-promote instead of creating a review group, and eligible duplicate groups may auto-resolve into closed review history. Omit `-stage-review-groups` if you only want the replay report.
Replay auto-detects whether the stored run used linked ICS extraction or direct source-page parsing.

## Repair existing descriptions only

```bash
go run ./cmd/ingest -source cafe-no-9 -repair-descriptions
go run ./cmd/ingest -source sidney-and-matilda -repair-descriptions
go run ./cmd/ingest -import-run-id 42 -repair-descriptions
```

This updates only eligible existing event descriptions from live ingest or replayed snapshots. It does not stage review groups, auto-promote events, create new events, or mutate non-description event fields. Use this for owned authoritative sources when an earlier ingest left descriptions blank or filled with generated markup/CSS.

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
- open duplicate groups preselect persisted majority defaults when no manual draft choice exists
- duplicate groups may include a `Live canonical snapshot` comparison column
- singleton groups use accept/reject when they were not auto-promoted during staging
- resolving or accepting publishes one canonical public event
- rejecting does not publish
- open `/admin/review/history` for the 50 newest resolved and rejected groups
