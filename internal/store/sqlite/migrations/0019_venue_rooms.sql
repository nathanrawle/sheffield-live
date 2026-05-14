CREATE TABLE IF NOT EXISTS venue_rooms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  venue_id INTEGER NOT NULL,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  sort_order INTEGER NOT NULL DEFAULT 0,
  validation_state TEXT NOT NULL DEFAULT 'validated',
  origin TEXT NOT NULL,
  UNIQUE(venue_id, slug),
  FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE CASCADE ON UPDATE CASCADE
);

ALTER TABLE events ADD COLUMN room_text TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS event_rooms (
  event_id INTEGER NOT NULL,
  room_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  PRIMARY KEY(event_id, room_id),
  UNIQUE(event_id, position),
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(room_id) REFERENCES venue_rooms(id) ON DELETE RESTRICT ON UPDATE CASCADE
);

ALTER TABLE review_candidates ADD COLUMN room_text TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS review_candidate_rooms (
  candidate_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  room_slug TEXT NOT NULL,
  room_name TEXT NOT NULL,
  PRIMARY KEY(candidate_id, room_slug),
  UNIQUE(candidate_id, position),
  FOREIGN KEY(candidate_id) REFERENCES review_candidates(id) ON DELETE CASCADE
);

INSERT OR IGNORE INTO venue_rooms (venue_id, slug, name, sort_order, validation_state, origin)
SELECT id, 'factory', 'Factory', 1, 'validated', 'live'
FROM venues
WHERE slug = 'sidney-and-matilda';

INSERT OR IGNORE INTO venue_rooms (venue_id, slug, name, sort_order, validation_state, origin)
SELECT id, 'basement', 'Basement', 2, 'validated', 'live'
FROM venues
WHERE slug = 'sidney-and-matilda';

INSERT OR IGNORE INTO venue_rooms (venue_id, slug, name, sort_order, validation_state, origin)
SELECT id, 'gallery', 'Gallery', 3, 'validated', 'live'
FROM venues
WHERE slug = 'sidney-and-matilda';

CREATE INDEX IF NOT EXISTS idx_venue_rooms_venue_slug ON venue_rooms(venue_id, slug);
CREATE INDEX IF NOT EXISTS idx_venue_rooms_validation ON venue_rooms(validation_state, venue_id, sort_order, slug);
CREATE INDEX IF NOT EXISTS idx_event_rooms_event_position ON event_rooms(event_id, position);
CREATE INDEX IF NOT EXISTS idx_event_rooms_room ON event_rooms(room_id);
CREATE INDEX IF NOT EXISTS idx_review_candidate_rooms_candidate_position ON review_candidate_rooms(candidate_id, position);
