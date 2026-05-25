CREATE TABLE IF NOT EXISTS event_source_images (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  source_identity_key TEXT NOT NULL DEFAULT '',
  image_url TEXT NOT NULL,
  image_source_url TEXT NOT NULL DEFAULT '',
  image_alt TEXT NOT NULL DEFAULT '',
  image_width INTEGER NOT NULL DEFAULT 0,
  image_height INTEGER NOT NULL DEFAULT 0,
  image_focus_x INTEGER NOT NULL DEFAULT 50,
  image_focus_y INTEGER NOT NULL DEFAULT 50,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK(TRIM(image_url) <> ''),
  UNIQUE(event_id, source_id),
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_event_source_images_event_id
ON event_source_images(event_id);

CREATE INDEX IF NOT EXISTS idx_event_source_images_image_source_url
ON event_source_images(image_source_url);

CREATE INDEX IF NOT EXISTS idx_event_source_images_image_url
ON event_source_images(image_url);
