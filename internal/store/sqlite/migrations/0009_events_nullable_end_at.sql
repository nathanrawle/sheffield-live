ALTER TABLE events RENAME TO events_old;
DROP INDEX IF EXISTS idx_events_venue_id_start_slug;
DROP INDEX IF EXISTS idx_events_start_slug;

CREATE TABLE events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slug TEXT NOT NULL,
  venue_id INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  start_at TEXT NOT NULL,
  end_at TEXT,
  genre TEXT NOT NULL,
  status TEXT NOT NULL,
  description TEXT NOT NULL,
  last_checked_at TEXT NOT NULL,
  origin TEXT NOT NULL,
  UNIQUE(slug),
  FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE RESTRICT ON UPDATE CASCADE,
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE RESTRICT ON UPDATE CASCADE
);

INSERT INTO events (
  id,
  slug,
  venue_id,
  source_id,
  name,
  start_at,
  end_at,
  genre,
  status,
  description,
  last_checked_at,
  origin
)
SELECT
  id,
  slug,
  venue_id,
  source_id,
  name,
  start_at,
  CASE
    WHEN TRIM(end_at) = '' THEN NULL
    ELSE end_at
  END,
  genre,
  status,
  description,
  last_checked_at,
  origin
FROM events_old;

ALTER TABLE event_source_links RENAME TO event_source_links_old;
DROP INDEX IF EXISTS idx_event_source_links_event_id;

CREATE TABLE event_source_links (
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

INSERT INTO event_source_links (
  id,
  source_id,
  event_id,
  source_event_key,
  is_authoritative,
  created_at,
  updated_at
)
SELECT
  id,
  source_id,
  event_id,
  source_event_key,
  is_authoritative,
  created_at,
  updated_at
FROM event_source_links_old;

ALTER TABLE event_secondary_source_info RENAME TO event_secondary_source_info_old;
DROP INDEX IF EXISTS idx_event_secondary_source_info_event_id;

CREATE TABLE event_secondary_source_info (
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

INSERT INTO event_secondary_source_info (
  id,
  event_id,
  source_id,
  venue_slug,
  event_name,
  start_at,
  info_type,
  value,
  created_at,
  updated_at
)
SELECT
  id,
  event_id,
  source_id,
  venue_slug,
  event_name,
  start_at,
  info_type,
  value,
  created_at,
  updated_at
FROM event_secondary_source_info_old;

DROP TABLE event_source_links_old;
DROP TABLE event_secondary_source_info_old;
DROP TABLE events_old;

CREATE INDEX idx_events_venue_id_start_slug ON events(venue_id, start_at, slug);
CREATE INDEX idx_events_start_slug ON events(start_at, slug);
CREATE INDEX idx_event_source_links_event_id ON event_source_links(event_id);
CREATE INDEX idx_event_secondary_source_info_event_id ON event_secondary_source_info(event_id);
