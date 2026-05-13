# Logging

Sheffield Live uses Go's standard `log/slog` package for structured operational logs.

Logs are written to stderr. This matters for `cmd/ingest`: stdout is reserved for the command's JSON report so shell scripts can keep piping or parsing stdout safely.

## Configure Logs

Both entrypoints support the same logging environment variables:

- `LOG_LEVEL` defaults to `info`
- `LOG_FORMAT` defaults to `text`

Supported levels:

- `debug`
- `info`
- `warn`
- `error`

Supported formats:

- `text`
- `json`

Examples:

```bash
ADMIN_AUTH_DISABLED=1 LOG_LEVEL=info LOG_FORMAT=text go run ./cmd/web
```

```bash
ADMIN_AUTH_DISABLED=1 LOG_LEVEL=debug LOG_FORMAT=json go run ./cmd/web
```

```bash
LOG_FORMAT=json go run ./cmd/ingest -review-ics-fixture internal/ingest/testdata/sidney.ics >report.json
```

If `LOG_LEVEL` or `LOG_FORMAT` is invalid, the command exits before starting and writes the configuration error to stderr.

## Web Logs

`cmd/web` logs:

- startup configuration
- listen address
- HTTP requests
- readiness failures
- internal handler failures
- template render failures
- SQLite close failures during shutdown
- fatal startup or serve errors before exit

Request logs include:

- `method`
- `path`
- `status`
- `bytes`
- `duration`
- `remote_addr`
- `user_agent`

Successful `/healthz` and `/readyz` requests are not logged. Failed readiness checks are logged because they indicate an operational problem.

Example text log:

```text
time=2026-05-11T09:00:00.000Z level=INFO msg="http request" method=GET path=/events status=200 bytes=12345 duration=4.2ms remote_addr=127.0.0.1:59321 user_agent=curl/8.0.1
```

Example JSON log:

```json
{"time":"2026-05-11T09:00:00.000Z","level":"INFO","msg":"http request","method":"GET","path":"/events","status":200,"bytes":12345,"duration":4200000,"remote_addr":"127.0.0.1:59321","user_agent":"curl/8.0.1"}
```

## Ingest Logs

`cmd/ingest` logs:

- ingest start
- ingest finish
- SQLite close failures
- per-source summaries for `-all-sources`
- fatal command errors before exit

Ingest start logs include:

- `mode`
- `source`
- `all_sources`
- `import_run_id`
- `stage_review_groups`
- `db_path`

Ingest finish logs include:

- `mode`
- `source`
- `all_sources`
- `import_run_id`
- `status`
- `duration`
- `links`
- `snapshots`
- `candidates`
- `skips`
- `errors`

When `-all-sources` is used, the finish log also includes:

- `sources`
- `failed_sources`

When `-stage-review-groups` is used, the finish log also includes:

- `review_groups_created`
- `review_groups_reused`
- `auto_promoted`
- `duplicate_auto_resolved`

Ingest modes:

- `live`
- `all_sources`
- `replay`
- `description_repair_live`
- `description_repair_replay`
- `review_fixture`
- `image_focus_backfill`

Example:

```bash
go run ./cmd/ingest -source leadmill -http-user-agent "sheffield-live ingest (contact: you@example.com)" >report.json 2>ingest.log
```

`report.json` contains only the JSON report. `ingest.log` contains the structured operational logs.

## Privacy And Payloads

Logs are intended for operations, not data export or replay.

The application does not log:

- fetched page bodies
- snapshot payloads
- request bodies
- query strings
- form bodies
- admin passphrases
- session or CSRF tokens

The web server logs request paths, remote addresses, user agents, and startup auth status without logging admin secrets. Ingest logs include the database path and source names. Treat logs as operational data and avoid posting production logs publicly without review.

## Manual QA

Use [Logging QA Script](qa/logging.md) for a step-by-step manual check of text logs, JSON logs, ingest stdout/stderr separation, health-check noise, and invalid logging configuration.
