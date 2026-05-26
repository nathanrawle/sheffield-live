# Common Tasks

These recipes stay short on purpose. See [Command Reference](commands.md) for the full flag list and command details.

## Run the site

```bash
ADMIN_AUTH_DISABLED=1 go run ./cmd/web
```

## Run on a different address or database

```bash
ADDR=:3000 DB_PATH=/tmp/sheffield-live.db ADMIN_AUTH_DISABLED=1 go run ./cmd/web
```

Local copied images are served from `MEDIA_ROOT` at `MEDIA_URL_PREFIX`. Defaults are `./data/media` and `/media`.

## Configure admin login

Generate a bcrypt hash for the admin passphrase:

```bash
printf '%s' 'your admin passphrase' | go run ./cmd/admin-password-hash
```

Run the site with `ADMIN_PASSWORD_HASH` set to that output. Keep `ADMIN_COOKIE_SECURE=true` for HTTPS deployments; set `ADMIN_COOKIE_SECURE=false` only for local HTTP testing with auth enabled.

## Inspect the local SQLite DB

```bash
sqlite3 ./data/sheffield-live.db
```

Use the SQLite CLI only if you already have it installed. Any read-only query tool you prefer is fine.

## Run manual ingest

By default, ingest runs every registered source and stages event reviews:

```bash
go run ./cmd/ingest -user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

If local or global git config already has `user.email`, you can omit the flag and let the command derive a default user agent:

```bash
go run ./cmd/ingest
```

Use `-source` to run one source:

```bash
go run ./cmd/ingest -source yellow-arch -user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Other source keys use the same pattern:

```bash
go run ./cmd/ingest -source leadmill -user-agent "sheffield-live manual ingest (contact: you@example.com)"
go run ./cmd/ingest -source jazz-at-the-lescar -user-agent "sheffield-live manual ingest (contact: you@example.com)"
go run ./cmd/ingest -source the-greystones -user-agent "sheffield-live manual ingest (contact: you@example.com)"
```

Sidney & Matilda snapshots the source page, linked ICS payloads, and linked event detail pages; ICS remains authoritative for identity and times, while detail pages enrich blank descriptions. Cafe No. 9 snapshots the WeGotTickets organiser pages plus event detail pages and enriches descriptions from detail-page event information. Yellow Arch snapshots the source page plus candidate detail pages, and uses detail-page event content to enrich descriptions without adding those detail URLs to report links. Leadmill snapshots the source page plus linked ICS payloads. Jazz at The Lescar snapshots only the source page and parses candidates directly from that page. The Greystones and Corporation snapshot the source page plus linked detail pages. Ingest also copies supported source images into local media storage when an event image URL is available and stores a best-effort focus point for card crops; set `MEDIA_ROOT` and `MEDIA_URL_PREFIX` to change the local storage path or public URL prefix. `-limit` caps linked ICS or linked detail-page fetches and parsed source-page candidates for direct source-page parsers. All commands print a JSON report.
Use `-contact you@example.com` to override the contact detail in the derived default user agent, or `-contact none` to suppress contact info entirely.

Use `-dry-run` to skip event-review staging while still writing import runs and snapshots:

```bash
go run ./cmd/ingest -dry-run
```

This stages duplicate event-review clusters and any singleton clusters that were not auto-promoted after a successful ingest. Any singleton may auto-publish first when it is the first matching live event seen; authoritative sources can also upgrade existing provisional events in place, while supporting-source conflicts stay in review. Supporting singleton candidates are also kept in review instead of inserted provisionally when a nearby same-venue live event matches by exact clean title, aggressively normalized title, or normalized headliner. Duplicate staging can also auto-resolve an exact canonical match or a unanimous staged duplicate and records those outcomes separately in `review_stage.event_review_clusters_auto_resolved`. Use `review_stage.event_review_clusters[].cluster_id` for open review follow-up; auto-resolved rows are terminal history/reporting rows. Reruns reuse existing staging keys when the staged content matches, refresh open-cluster canonical snapshot/default state, preserve manual draft choices, and record the new import-run link in the persisted provenance table.
Event-review staging can also create provisional venue rows immediately from uniquely new venue evidence, even when no event is published yet, so `/admin/venues` may show rows with `0` upcoming events.

## Replay a stored ingest run

```bash
go run ./cmd/ingest replay -limit 20 42
```

Omit the ID to replay the latest finished import run by ID:

```bash
go run ./cmd/ingest replay
```

This rebuilds the report from stored snapshots without using the network. Reruns are safe and reuse existing clusters when the staged content matches, eligible singletons may auto-promote instead of creating an event-review cluster, and eligible duplicate clusters may auto-resolve into closed review history. Add `-dry-run` if you only want the replay report and no event-review staging.
Replay auto-detects whether the stored run used linked ICS extraction or direct source-page parsing.
Replay reuses existing copied image-asset metadata by source URL and does not fetch remote image bytes.

## Repair image focus metadata

```bash
go run ./cmd/ingest fix image-focus
```

This reads existing copied image files from `MEDIA_ROOT`, recomputes focus metadata, and updates copied asset, review candidate, and event rows. Use it after migrating an existing local development database that already has copied images.

## Repair existing descriptions only

```bash
go run ./cmd/ingest fix descriptions
go run ./cmd/ingest fix descriptions -source cafe-no-9
go run ./cmd/ingest replay -descriptions 42
```

This updates only eligible existing event descriptions from live ingest or replayed snapshots. It does not stage event-review clusters, auto-promote events, create new events, or mutate non-description event fields. Use this for owned authoritative sources when an earlier ingest left descriptions blank or filled with generated markup/CSS.

## Repair existing event titles

```bash
go run ./cmd/ingest fix titles
go run ./cmd/ingest fix titles -source yellow-arch
go run ./cmd/ingest replay -titles 42
```

Title repair applies by default; add `-dry-run` to report changes without repair writes. Authoritative matches can update event names and slugs directly; non-authoritative matches do not overwrite authoritative event names and instead create or reuse an event-review cluster when there is one safe target.

## Reset a disposable local database

Stop the app, then remove only your local development DB file and start again. This example uses the default `DB_PATH`; use your configured path if you changed it.

```bash
rm -f ./data/sheffield-live.db
ADMIN_AUTH_DISABLED=1 go run ./cmd/web
```

Use this only for a disposable local database. Do not delete a shared or production DB.

## Work the review queue

Open `/admin/review` in the browser for the event-review cluster queue.

- event-review clusters use field choices and a canonical draft summary
- open event-review clusters preselect persisted majority defaults when no manual draft choice exists
- event-review clusters may include a `Live canonical snapshot` comparison column
- shared venue labels in review summaries come from deterministic matching over stored venue slug, venue text, and raw location evidence
- when venue writes are available, `/admin/venues/{slug}` lets you save provisional venue fields in place and then use the separate validate action to remove the venue from the queue
- when venue writes are unavailable, `/admin/venues/{slug}` remains read-only and hides save/validate controls
- cluster resolution publishes one canonical public event when the selected event-review action is eligible
- discard and supersede are administrative escape hatches, while eligible resolve forms are terminal editorial decisions
- import-review resolve forms can insert new listings, attach supporting evidence to an existing event, resolve authoritative imports, or record near-title false positives before inserting a separate listing
- title-repair resolve forms can apply simple repairs, merge/update slug conflicts, or keep the slug-conflict event separate
- historical-duplicate resolve forms can apply or override canonical/withhold actions, or record all-kept false positives as pairwise event separations
- unsupported open conflict types show a blocker and no terminal resolve form
- manual resolution reuses an existing venue when the selected evidence yields one unique match, otherwise it creates a provisional venue in the same transaction if staging or singleton auto-promotion did not already do so
- ambiguous venue evidence fails closed and leaves the cluster unresolved
- singleton auto-promotion can now create a provisional venue row immediately for a uniquely new venue
- resolving a duplicate can persist secondary-source genre and description evidence from matching non-selected candidates; matching uses venue slug, start time, and case/whitespace-normalized title
- non-authoritative secondary evidence is cumulative, so a missing source in a later resolved review record is not treated as deletion
- open `/admin/venues` to inspect provisional venue rows created from newly detected venue evidence
- open `/admin/venues/{slug}` to inspect one provisional venue and its upcoming linked events
- when venue writes are available, use `/admin/venues/{slug}` to edit provisional venue fields and then use the separate validate action to mark the venue validated and remove it from the queue
- open `/admin/rooms` to inspect provisional room rows created from newly detected room evidence
- open `/admin/rooms/{venueSlug}/{roomSlug}` to inspect or validate one provisional room
- open `/admin/configuration` to inspect or edit genre inference rules; saving or deleting a rule recomputes stored event genre rankings
- the provisional venue queue still does not support venue merge actions
- open `/admin/event-review/history` for the 50 newest terminal event-review clusters
- `/admin/legacy-review*` returns 404 after admin auth; use `/admin/review` for the active event-review cluster queue and `/admin/event-review/...` for history/detail
