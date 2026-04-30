ALTER TABLE review_candidates ADD COLUMN canonical_event_id INTEGER;

CREATE UNIQUE INDEX IF NOT EXISTS idx_review_candidates_group_canonical_event
ON review_candidates(group_id)
WHERE canonical_event_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS review_field_defaults (
  group_id INTEGER NOT NULL,
  field TEXT NOT NULL,
  candidate_id INTEGER NOT NULL,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(group_id, field),
  FOREIGN KEY(group_id) REFERENCES review_groups(id) ON DELETE CASCADE,
  FOREIGN KEY(group_id, candidate_id) REFERENCES review_candidates(group_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_review_field_defaults_group ON review_field_defaults(group_id);
