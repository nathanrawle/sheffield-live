CREATE TABLE IF NOT EXISTS review_groups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  status TEXT NOT NULL,
  notes TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_candidates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  external_id TEXT NOT NULL DEFAULT '',
  name TEXT NOT NULL,
  venue_slug TEXT NOT NULL,
  start_at TEXT NOT NULL,
  end_at TEXT NOT NULL,
  genre TEXT NOT NULL,
  status TEXT NOT NULL,
  description TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  provenance TEXT NOT NULL DEFAULT '',
  UNIQUE(group_id, position),
  UNIQUE(group_id, id),
  FOREIGN KEY(group_id) REFERENCES review_groups(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS review_draft_choices (
  group_id INTEGER NOT NULL,
  field TEXT NOT NULL,
  candidate_id INTEGER NOT NULL,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(group_id, field),
  FOREIGN KEY(group_id) REFERENCES review_groups(id) ON DELETE CASCADE,
  FOREIGN KEY(group_id, candidate_id) REFERENCES review_candidates(group_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_review_groups_status_updated ON review_groups(status, updated_at, id);
CREATE INDEX IF NOT EXISTS idx_review_candidates_group_position ON review_candidates(group_id, position);
CREATE INDEX IF NOT EXISTS idx_review_draft_choices_group ON review_draft_choices(group_id);
