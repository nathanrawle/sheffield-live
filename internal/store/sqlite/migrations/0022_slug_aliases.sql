CREATE TABLE IF NOT EXISTS slug_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alias_slug TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_event_id INTEGER REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  target_venue_id INTEGER REFERENCES venues(id) ON DELETE CASCADE ON UPDATE CASCADE,
  repair_run_id INTEGER REFERENCES repair_runs(id) ON DELETE SET NULL ON UPDATE CASCADE,
  reason TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(target_kind, alias_slug),
  CHECK(TRIM(alias_slug) <> ''),
  CHECK(target_kind IN ('event', 'venue')),
  CHECK(
    (target_kind = 'event' AND target_event_id IS NOT NULL AND target_venue_id IS NULL)
    OR
    (target_kind = 'venue' AND target_venue_id IS NOT NULL AND target_event_id IS NULL)
  )
);
