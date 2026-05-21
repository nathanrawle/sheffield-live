CREATE TABLE IF NOT EXISTS event_exact_identities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  identity_key TEXT NOT NULL,
  key_version INTEGER NOT NULL DEFAULT 1,
  venue_slug TEXT NOT NULL,
  utc_start_at TEXT NOT NULL,
  clean_title TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deactivated_at TEXT,
  deactivated_reason TEXT NOT NULL DEFAULT '',
  repair_run_id INTEGER REFERENCES repair_runs(id) ON DELETE SET NULL,
  CHECK(TRIM(identity_key) <> ''),
  CHECK(TRIM(venue_slug) <> ''),
  CHECK(TRIM(clean_title) <> ''),
  CHECK(key_version > 0),
  CHECK(active IN (0, 1)),
  CHECK(
    (active = 1 AND deactivated_at IS NULL AND TRIM(deactivated_reason) = '')
    OR
    (active = 0 AND deactivated_at IS NOT NULL AND TRIM(deactivated_reason) <> '')
  ),
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_exact_identities_active_identity_key
ON event_exact_identities(identity_key)
WHERE active = 1;

CREATE INDEX IF NOT EXISTS idx_event_exact_identities_event_id
ON event_exact_identities(event_id, active, identity_key);

CREATE INDEX IF NOT EXISTS idx_event_exact_identities_repair_run_id
ON event_exact_identities(repair_run_id);
