CREATE TABLE IF NOT EXISTS event_source_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  event_id INTEGER NOT NULL,
  source_event_key TEXT NOT NULL,
  is_authoritative INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(source_id, source_event_key),
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_event_source_links_event_id
ON event_source_links(event_id);
