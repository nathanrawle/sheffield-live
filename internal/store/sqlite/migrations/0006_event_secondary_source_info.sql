CREATE TABLE IF NOT EXISTS event_secondary_source_info (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  venue_slug TEXT NOT NULL,
  event_name TEXT NOT NULL,
  start_at TEXT NOT NULL,
  info_type TEXT NOT NULL,
  value TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(source_id, venue_slug, event_name, start_at, info_type),
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_event_secondary_source_info_event_id
ON event_secondary_source_info(event_id);
