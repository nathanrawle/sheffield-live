# Sheffield Live

Sheffield Live is a single Go monolith for browsing live music in Sheffield with server-rendered HTML and SQLite persistence via `modernc.org/sqlite`.

Run it:

```bash
go run ./cmd/web
```

Defaults: `ADDR=:8080` and `DB_PATH=./data/sheffield-live.db`.

Current surface:

- home, event list/detail, and venue list/detail pages
- admin review queue and review detail pages for staged ingest work
- `GET /healthz`
- embedded stylesheet at `/static/site.css`
- seed, test, and development records are labelled; live records are untagged

Docs:

- [Command reference](docs/commands.md)
- [Common tasks](docs/common-tasks.md)
- [Architecture](docs/architecture.md) and [sources](docs/sources.md)
