CREATE TABLE IF NOT EXISTS event_source_attribute_observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_scope TEXT NOT NULL,
  source_id INTEGER NOT NULL,
  source_identity_key TEXT NOT NULL,
  source_authority TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  event_id INTEGER,
  review_group_id INTEGER,
  field_name TEXT NOT NULL,
  incoming_raw TEXT NOT NULL DEFAULT '',
  incoming_normalized TEXT NOT NULL DEFAULT '',
  canonical_before_raw TEXT NOT NULL DEFAULT '',
  canonical_before_normalized TEXT NOT NULL DEFAULT '',
  outcome TEXT NOT NULL DEFAULT '',
  is_conflict INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK(TRIM(run_scope) <> ''),
  CHECK(
    (
      SUBSTR(run_scope, 1, 7) = 'import:'
      AND LENGTH(run_scope) > 7
      AND SUBSTR(run_scope, 8, 1) BETWEEN '1' AND '9'
      AND SUBSTR(run_scope, 8) NOT GLOB '*[^0-9]*'
    )
    OR
    (
      SUBSTR(run_scope, 1, 7) = 'repair:'
      AND LENGTH(run_scope) > 7
      AND SUBSTR(run_scope, 8, 1) BETWEEN '1' AND '9'
      AND SUBSTR(run_scope, 8) NOT GLOB '*[^0-9]*'
    )
  ),
  CHECK(TRIM(source_identity_key) <> ''),
  CHECK(TRIM(field_name) <> ''),
  CHECK(source_authority IN ('authoritative', 'supporting')),
  CHECK(target_kind IN ('event', 'review_group')),
  CHECK(is_conflict IN (0, 1)),
  CHECK(
    (target_kind = 'event' AND event_id IS NOT NULL AND review_group_id IS NULL)
    OR
    (target_kind = 'review_group' AND review_group_id IS NOT NULL AND event_id IS NULL)
  ),
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(review_group_id) REFERENCES review_groups(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_source_attribute_observations_event_identity
ON event_source_attribute_observations(run_scope, event_id, source_identity_key, field_name)
WHERE target_kind = 'event';

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_source_attribute_observations_review_group_identity
ON event_source_attribute_observations(run_scope, review_group_id, source_identity_key, field_name)
WHERE target_kind = 'review_group';
