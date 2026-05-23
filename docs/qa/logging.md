# Logging QA Script

Run this from the repository root.

## Setup

```bash
cd /Users/nathan/code/sheffield-live/feat/logging

WEB_DB=/tmp/sheffield-live-logging-web.db
INGEST_DB=/tmp/sheffield-live-logging-ingest.db
WEB_LOG=/tmp/sheffield-live-web.log
INGEST_LOG=/tmp/sheffield-live-ingest.log
INGEST_JSON=/tmp/sheffield-live-ingest.json

rm -f "$WEB_DB" "$INGEST_DB" "$WEB_LOG" "$INGEST_LOG" "$INGEST_JSON"
```

## Web Text Logs

Start the web app:

```bash
ADDR=:8098 DB_PATH="$WEB_DB" ADMIN_AUTH_DISABLED=1 LOG_LEVEL=info LOG_FORMAT=text go run ./cmd/web 2>"$WEB_LOG"
```

In another terminal:

```bash
curl -fsS -A "qa-agent" http://127.0.0.1:8098/events >/dev/null
curl -fsS http://127.0.0.1:8098/healthz
curl -fsS -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8098/does-not-exist
```

Expected in `$WEB_LOG`:

```text
msg="web starting"
msg="web listening"
msg="http request" method=GET path=/events status=200
user_agent=qa-agent
msg="http request" method=GET path=/does-not-exist status=404
```

Successful health checks should not be logged:

```bash
grep 'path=/healthz' "$WEB_LOG"
```

This should print nothing.

## Web Internal Error Log

Trigger an admin POST validation/store error:

```bash
curl -fsS -o /tmp/genre-response.txt -w "%{http_code}\n" \
  -X POST http://127.0.0.1:8098/admin/configuration \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data 'action=save&key=broken&name=Broken&match_type=regex&pattern=[&enabled=1&sort_order=999'
```

Expected:

- Curl prints `400`.
- `/tmp/genre-response.txt` contains `invalid regex`.
- `$WEB_LOG` contains:

```text
msg="save genre rule"
error="invalid regex
path=/admin/configuration
status=400
```

## Web JSON Logs

Stop the first server, then run:

```bash
ADDR=:8099 DB_PATH=/tmp/sheffield-live-logging-json.db ADMIN_AUTH_DISABLED=1 LOG_FORMAT=json go run ./cmd/web 2>/tmp/sheffield-live-web-json.log
```

In another terminal:

```bash
curl -fsS http://127.0.0.1:8099/events >/dev/null
```

Expected in `/tmp/sheffield-live-web-json.log`:

```text
"msg":"web starting"
"msg":"web listening"
"msg":"http request"
"path":"/events"
"status":200
```

## Ingest Stdout/Stderr Separation

Run a staged ingest:

```bash
DB_PATH="$INGEST_DB" LOG_LEVEL=info LOG_FORMAT=text \
  go run ./cmd/ingest \
  -source sidney-and-matilda \
  >"$INGEST_JSON" 2>"$INGEST_LOG"
```

Expected stdout JSON:

```bash
python3 -m json.tool "$INGEST_JSON" >/dev/null
```

Expected logs in `$INGEST_LOG`:

```text
msg="ingest starting"
mode=live
msg="ingest finished"
status=succeeded
```

Expected no logs in stdout:

```bash
grep -E 'ingest starting|ingest finished' "$INGEST_JSON"
```

This should print nothing.

## Invalid Logging Config

```bash
LOG_LEVEL=trace go run ./cmd/ingest -source sidney-and-matilda >/tmp/ignored.json
```

Expected:

- Command exits non-zero.
- Stderr says `unsupported LOG_LEVEL`.

```bash
ADMIN_AUTH_DISABLED=1 LOG_FORMAT=xml go run ./cmd/web
```

Expected:

- Command exits non-zero.
- Stderr says `unsupported LOG_FORMAT`.

## Cleanup

Stop any running `go run ./cmd/web` processes, then:

```bash
rm -f /tmp/sheffield-live-logging-*.db /tmp/sheffield-live-*.log /tmp/sheffield-live-ingest.json /tmp/genre-response.txt
```
