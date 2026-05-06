# Sheffield Live

Sheffield Live is a single Go monolith for browsing live music in Sheffield with server-rendered HTML and SQLite persistence via `modernc.org/sqlite`.

Run it:

```bash
go run ./cmd/web
```

Defaults: `ADDR=:8080` and `DB_PATH=./data/sheffield-live.db`.

Current surface:

- home, event list/detail, and venue list/detail pages
- admin review queue, provisional venue queue, review history, and review detail pages for staged ingest work
- `GET /healthz`
- `GET /readyz`
- embedded stylesheet at `/static/site.css`
- seed, test, and development records are labelled; live records are untagged
- `/events` supports `window=today|tonight|week|weekend|all`, `venue=...`, and `area=...`

Manual ingest supports live ingest, snapshot replay, and fixture-based offline review data. See [Command reference](docs/commands.md) or [Common tasks](docs/common-tasks.md) for the short versions.
Live ingest currently supports `sidney-and-matilda`, `yellow-arch`, `cafe-no-9`, `jazz-at-the-lescar`, `the-greystones`, `leadmill`, and `corporation`.
Source definitions now live in repo-backed YAML files under `config/sources/`, with bounded runtime families selected by config and custom Go adapters retained for irregular sources.
Replay auto-detects the stored source from page snapshot metadata and reuses that source's ingest path.
Review staging is idempotent by durable staging key, so reruns reuse existing review groups instead of duplicating them, and each staged or reused group persists a link to the current import run.
Duplicate review groups can include a live canonical snapshot column, persist majority defaults separately from manual draft choices, and auto-resolve in narrow duplicate cases without leaving an open review.
Venue coverage is stored as data. Most venues use full-venue coverage; The Lescar is marked program-only and shows a coverage note in venue and event detail pages.

Docs:

- [Command reference](docs/commands.md)
- [Common tasks](docs/common-tasks.md)
- [Source catalog](docs/source-catalog.md)
- [Architecture](docs/architecture.md) and [sources](docs/sources.md)
