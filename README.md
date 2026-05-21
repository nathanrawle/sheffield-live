# Sheffield Live

Sheffield Live is a single Go monolith for browsing live music in Sheffield with server-rendered HTML and SQLite persistence via `modernc.org/sqlite`.

Run it locally with admin auth disabled:

```bash
ADMIN_AUTH_DISABLED=1 go run ./cmd/web
```

For a public or shared deployment, keep admin auth enabled and provide a bcrypt hash:

```bash
printf '%s' 'your admin passphrase' | go run ./cmd/admin-password-hash
ADMIN_PASSWORD_HASH='paste generated hash here' go run ./cmd/web
```

Defaults: `ADDR=:8080`, `DB_PATH=./data/sheffield-live.db`, `LOG_LEVEL=info`, `LOG_FORMAT=text`, and `ADMIN_COOKIE_SECURE=true`.
Set `ADMIN_COOKIE_SECURE=false` only when testing login over local plain HTTP.

Current surface:

- home and `/events` event board, event detail, and venue list/detail pages
- passphrase-protected `/admin` tools for review, provisional venues and rooms, import history, and genre configuration
- `GET /healthz`
- `GET /readyz`
- embedded stylesheet at `/static/site.css`
- test and development records are labelled; curated bootstrap venues and live records are untagged
- `/events` defaults to a Tonight/Tomorrow/next-seven-days board and still supports `window=today|tonight|week|weekend|all`, `venue=...`, and `area=...` query URLs

Manual ingest supports live ingest, snapshot replay, and event-review cluster staging. See [Command reference](docs/commands.md) or [Common tasks](docs/common-tasks.md) for the short versions.
Live ingest currently supports `sidney-and-matilda`, `yellow-arch`, `cafe-no-9`, `jazz-at-the-lescar`, `the-greystones`, `leadmill`, and `corporation`.
Source definitions now live in repo-backed YAML files under `config/sources/`, with bounded runtime families selected by config and custom Go adapters retained for irregular sources.
Replay auto-detects the stored source from page snapshot metadata and reuses that source's ingest path.
Review staging is idempotent by durable staging key, so reruns reuse existing event-review clusters instead of duplicating them, and each staged or reused cluster persists a link to the current import run.
Duplicate event-review clusters can include a live canonical snapshot column, persist majority defaults separately from manual draft choices, and auto-resolve in narrow duplicate cases without leaving an open review.
Venue coverage is stored as data. Most venues use full-venue coverage; The Lescar is marked program-only and shows a coverage note in venue and event detail pages.

## Admin login

Set `ADMIN_PASSWORD_HASH` for public or shared deployments. Use `ADMIN_AUTH_DISABLED=1` only for disposable local development, and do not commit passphrases or generated hashes to the repository.

Docs:

- [Command reference](docs/commands.md)
- [Common tasks](docs/common-tasks.md)
- [Logging](docs/logging.md)
- [Source catalog](docs/source-catalog.md)
- [Architecture](docs/architecture.md) and [sources](docs/sources.md)
