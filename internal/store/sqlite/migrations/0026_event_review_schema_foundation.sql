CREATE TABLE IF NOT EXISTS event_review_clusters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  status TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  superseded_by_cluster_id INTEGER REFERENCES event_review_clusters(id) ON DELETE SET NULL ON UPDATE CASCADE,
  previous_cluster_id INTEGER REFERENCES event_review_clusters(id) ON DELETE SET NULL ON UPDATE CASCADE,
  canonical_event_id INTEGER REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE,
  conflict_type TEXT NOT NULL DEFAULT '',
  conflict_reason TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK(status IN ('open', 'resolved', 'discarded', 'superseded')),
  CHECK(version > 0),
  CHECK(
    (status = 'superseded' AND superseded_by_cluster_id IS NOT NULL)
    OR
    (status <> 'superseded' AND superseded_by_cluster_id IS NULL)
  ),
  CHECK(previous_cluster_id IS NULL OR previous_cluster_id <> id),
  CHECK(superseded_by_cluster_id IS NULL OR superseded_by_cluster_id <> id)
);

CREATE TABLE IF NOT EXISTS event_review_evidence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  event_id INTEGER REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE,
  evidence_fingerprint TEXT NOT NULL,
  fingerprint_version INTEGER NOT NULL DEFAULT 1,
  payload TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK(source_id > 0),
  CHECK(TRIM(evidence_fingerprint) = evidence_fingerprint AND TRIM(evidence_fingerprint) <> ''),
  CHECK(fingerprint_version = 1),
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_evidence_fingerprint
ON event_review_evidence(evidence_fingerprint, fingerprint_version);

CREATE TABLE IF NOT EXISTS event_review_cluster_evidence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id INTEGER NOT NULL,
  evidence_id INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  linked_at TEXT NOT NULL,
  unlinked_at TEXT,
  link_reason TEXT NOT NULL DEFAULT '',
  CHECK(active IN (0, 1)),
  CHECK((active = 1 AND unlinked_at IS NULL) OR (active = 0 AND unlinked_at IS NOT NULL)),
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(evidence_id) REFERENCES event_review_evidence(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_cluster_evidence_active_evidence
ON event_review_cluster_evidence(evidence_id)
WHERE active = 1;

CREATE INDEX IF NOT EXISTS idx_event_review_cluster_evidence_cluster_active
ON event_review_cluster_evidence(cluster_id, active, evidence_id);

CREATE TABLE IF NOT EXISTS event_review_identity_keys (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  identity_key_hash TEXT NOT NULL,
  key_kind TEXT NOT NULL,
  key_version INTEGER NOT NULL DEFAULT 1,
  normalized_key TEXT NOT NULL,
  created_at TEXT NOT NULL,
  CHECK(TRIM(identity_key_hash) = identity_key_hash AND TRIM(identity_key_hash) <> ''),
  CHECK(key_kind IN ('source', 'exact', 'near', 'manual')),
  CHECK(key_version > 0),
  CHECK(TRIM(normalized_key) = normalized_key AND TRIM(normalized_key) <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_identity_keys_hash
ON event_review_identity_keys(identity_key_hash);

CREATE TABLE IF NOT EXISTS event_review_evidence_identity_keys (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  evidence_id INTEGER NOT NULL,
  identity_key_id INTEGER NOT NULL,
  source_id INTEGER,
  role TEXT NOT NULL,
  CHECK(role IN ('observed', 'derived', 'exact')),
  CHECK(source_id IS NULL OR source_id > 0),
  FOREIGN KEY(evidence_id) REFERENCES event_review_evidence(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(identity_key_id) REFERENCES event_review_identity_keys(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_event_review_evidence_identity_keys_identity_key
ON event_review_evidence_identity_keys(identity_key_id, evidence_id, role, source_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_evidence_identity_keys_unique_null_source
ON event_review_evidence_identity_keys(evidence_id, identity_key_id, role)
WHERE source_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_evidence_identity_keys_unique_source
ON event_review_evidence_identity_keys(evidence_id, identity_key_id, source_id, role)
WHERE source_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS event_review_cluster_identity_keys (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id INTEGER NOT NULL,
  identity_key_id INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  linked_at TEXT NOT NULL,
  unlinked_at TEXT,
  CHECK(active IN (0, 1)),
  CHECK((active = 1 AND unlinked_at IS NULL) OR (active = 0 AND unlinked_at IS NOT NULL)),
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(identity_key_id) REFERENCES event_review_identity_keys(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_cluster_identity_keys_active_cluster
ON event_review_cluster_identity_keys(cluster_id, identity_key_id)
WHERE active = 1;

CREATE INDEX IF NOT EXISTS idx_event_review_cluster_identity_keys_identity_key_active
ON event_review_cluster_identity_keys(identity_key_id, active, cluster_id);

CREATE INDEX IF NOT EXISTS idx_event_review_cluster_identity_keys_cluster_active
ON event_review_cluster_identity_keys(cluster_id, active, identity_key_id);

CREATE TABLE IF NOT EXISTS event_review_canonical_choices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id INTEGER NOT NULL,
  field_name TEXT NOT NULL,
  choice_kind TEXT NOT NULL,
  event_id INTEGER,
  evidence_id INTEGER,
  value TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL,
  CHECK(TRIM(field_name) = field_name AND TRIM(field_name) <> ''),
  CHECK(choice_kind IN ('event', 'evidence', 'manual')),
  CHECK(
    (choice_kind = 'event' AND event_id IS NOT NULL AND evidence_id IS NULL)
    OR
    (choice_kind = 'evidence' AND evidence_id IS NOT NULL AND event_id IS NULL)
    OR
    (choice_kind = 'manual' AND event_id IS NULL AND evidence_id IS NULL)
  ),
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(evidence_id) REFERENCES event_review_evidence(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_canonical_choices_cluster_field
ON event_review_canonical_choices(cluster_id, field_name);

CREATE TABLE IF NOT EXISTS event_review_draft_choices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id INTEGER NOT NULL,
  field_name TEXT NOT NULL,
  choice_kind TEXT NOT NULL,
  event_id INTEGER,
  evidence_id INTEGER,
  value TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL,
  CHECK(TRIM(field_name) = field_name AND TRIM(field_name) <> ''),
  CHECK(choice_kind IN ('event', 'evidence', 'manual')),
  CHECK(
    (choice_kind = 'event' AND event_id IS NOT NULL AND evidence_id IS NULL)
    OR
    (choice_kind = 'evidence' AND evidence_id IS NOT NULL AND event_id IS NULL)
    OR
    (choice_kind = 'manual' AND event_id IS NULL AND evidence_id IS NULL)
  ),
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(evidence_id) REFERENCES event_review_evidence(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_draft_choices_cluster_field
ON event_review_draft_choices(cluster_id, field_name);

CREATE TABLE IF NOT EXISTS event_review_live_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id INTEGER NOT NULL,
  event_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK(action IN ('withhold_duplicate', 'keep_separate')),
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE(cluster_id, event_id)
);

CREATE TABLE IF NOT EXISTS event_review_source_identity_choices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  source_identity_key TEXT NOT NULL,
  selected INTEGER NOT NULL DEFAULT 0,
  selection_reason TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL,
  CHECK(TRIM(source_identity_key) = source_identity_key AND TRIM(source_identity_key) <> ''),
  CHECK(selected IN (0, 1)),
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE(cluster_id, source_id, source_identity_key)
);

CREATE INDEX IF NOT EXISTS idx_event_review_source_identity_choices_source_identity_key
ON event_review_source_identity_choices(source_identity_key, cluster_id, source_id);

CREATE TABLE IF NOT EXISTS event_review_separations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  endpoint_a_kind TEXT NOT NULL,
  endpoint_a_key TEXT NOT NULL,
  endpoint_a_event_id INTEGER REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE,
  endpoint_a_evidence_id INTEGER REFERENCES event_review_evidence(id) ON DELETE SET NULL ON UPDATE CASCADE,
  endpoint_a_identity_key_id INTEGER REFERENCES event_review_identity_keys(id) ON DELETE SET NULL ON UPDATE CASCADE,
  endpoint_a_canonical_event_id INTEGER REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE,
  endpoint_b_kind TEXT NOT NULL,
  endpoint_b_key TEXT NOT NULL,
  endpoint_b_event_id INTEGER REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE,
  endpoint_b_evidence_id INTEGER REFERENCES event_review_evidence(id) ON DELETE SET NULL ON UPDATE CASCADE,
  endpoint_b_identity_key_id INTEGER REFERENCES event_review_identity_keys(id) ON DELETE SET NULL ON UPDATE CASCADE,
  endpoint_b_canonical_event_id INTEGER REFERENCES events(id) ON DELETE SET NULL ON UPDATE CASCADE,
  active INTEGER NOT NULL DEFAULT 1,
  reason TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK(endpoint_a_kind IN ('event', 'evidence', 'identity_key')),
  CHECK(endpoint_b_kind IN ('event', 'evidence', 'identity_key')),
  CHECK(TRIM(endpoint_a_key) = endpoint_a_key AND TRIM(endpoint_a_key) <> ''),
  CHECK(TRIM(endpoint_b_key) = endpoint_b_key AND TRIM(endpoint_b_key) <> ''),
  CHECK(endpoint_a_key < endpoint_b_key),
  CHECK(active IN (0, 1)),
  CHECK(
    (endpoint_a_kind = 'event' AND endpoint_a_event_id IS NOT NULL AND endpoint_a_evidence_id IS NULL AND endpoint_a_identity_key_id IS NULL AND endpoint_a_key = 'event:' || endpoint_a_event_id)
    OR
    (endpoint_a_kind = 'evidence' AND endpoint_a_event_id IS NULL AND endpoint_a_evidence_id IS NOT NULL AND endpoint_a_identity_key_id IS NULL AND endpoint_a_key LIKE 'evidence:%')
    OR
    (endpoint_a_kind = 'identity_key' AND endpoint_a_event_id IS NULL AND endpoint_a_evidence_id IS NULL AND endpoint_a_identity_key_id IS NOT NULL AND endpoint_a_key LIKE 'identity:%')
  ),
  CHECK(
    (endpoint_b_kind = 'event' AND endpoint_b_event_id IS NOT NULL AND endpoint_b_evidence_id IS NULL AND endpoint_b_identity_key_id IS NULL AND endpoint_b_key = 'event:' || endpoint_b_event_id)
    OR
    (endpoint_b_kind = 'evidence' AND endpoint_b_event_id IS NULL AND endpoint_b_evidence_id IS NOT NULL AND endpoint_b_identity_key_id IS NULL AND endpoint_b_key LIKE 'evidence:%')
    OR
    (endpoint_b_kind = 'identity_key' AND endpoint_b_event_id IS NULL AND endpoint_b_evidence_id IS NULL AND endpoint_b_identity_key_id IS NOT NULL AND endpoint_b_key LIKE 'identity:%')
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_separations_active_pair
ON event_review_separations(endpoint_a_key, endpoint_b_key)
WHERE active = 1;

CREATE INDEX IF NOT EXISTS idx_event_review_separations_endpoint_a
ON event_review_separations(endpoint_a_key, active, id);

CREATE INDEX IF NOT EXISTS idx_event_review_separations_endpoint_b
ON event_review_separations(endpoint_b_key, active, id);

CREATE TRIGGER IF NOT EXISTS event_review_separations_validate_endpoint_keys_insert
BEFORE INSERT ON event_review_separations
BEGIN
  SELECT RAISE(ABORT, 'event review separation endpoint_a_key must match referenced evidence fingerprint')
  WHERE NEW.endpoint_a_kind = 'evidence'
    AND NEW.endpoint_a_evidence_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM event_review_evidence e
      WHERE e.id = NEW.endpoint_a_evidence_id
        AND NEW.endpoint_a_key <> 'evidence:' || e.evidence_fingerprint
    );
  SELECT RAISE(ABORT, 'event review separation endpoint_b_key must match referenced evidence fingerprint')
  WHERE NEW.endpoint_b_kind = 'evidence'
    AND NEW.endpoint_b_evidence_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM event_review_evidence e
      WHERE e.id = NEW.endpoint_b_evidence_id
        AND NEW.endpoint_b_key <> 'evidence:' || e.evidence_fingerprint
    );
  SELECT RAISE(ABORT, 'event review separation endpoint_a_key must match referenced identity hash')
  WHERE NEW.endpoint_a_kind = 'identity_key'
    AND NEW.endpoint_a_identity_key_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM event_review_identity_keys i
      WHERE i.id = NEW.endpoint_a_identity_key_id
        AND NEW.endpoint_a_key <> 'identity:' || i.identity_key_hash
    );
  SELECT RAISE(ABORT, 'event review separation endpoint_b_key must match referenced identity hash')
  WHERE NEW.endpoint_b_kind = 'identity_key'
    AND NEW.endpoint_b_identity_key_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM event_review_identity_keys i
      WHERE i.id = NEW.endpoint_b_identity_key_id
        AND NEW.endpoint_b_key <> 'identity:' || i.identity_key_hash
    );
END;

CREATE TRIGGER IF NOT EXISTS event_review_separations_validate_endpoint_keys_update
BEFORE UPDATE ON event_review_separations
BEGIN
  SELECT RAISE(ABORT, 'event review separation endpoint_a_key must match referenced evidence fingerprint')
  WHERE NEW.endpoint_a_kind = 'evidence'
    AND NEW.endpoint_a_evidence_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM event_review_evidence e
      WHERE e.id = NEW.endpoint_a_evidence_id
        AND NEW.endpoint_a_key <> 'evidence:' || e.evidence_fingerprint
    );
  SELECT RAISE(ABORT, 'event review separation endpoint_b_key must match referenced evidence fingerprint')
  WHERE NEW.endpoint_b_kind = 'evidence'
    AND NEW.endpoint_b_evidence_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM event_review_evidence e
      WHERE e.id = NEW.endpoint_b_evidence_id
        AND NEW.endpoint_b_key <> 'evidence:' || e.evidence_fingerprint
    );
  SELECT RAISE(ABORT, 'event review separation endpoint_a_key must match referenced identity hash')
  WHERE NEW.endpoint_a_kind = 'identity_key'
    AND NEW.endpoint_a_identity_key_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM event_review_identity_keys i
      WHERE i.id = NEW.endpoint_a_identity_key_id
        AND NEW.endpoint_a_key <> 'identity:' || i.identity_key_hash
    );
  SELECT RAISE(ABORT, 'event review separation endpoint_b_key must match referenced identity hash')
  WHERE NEW.endpoint_b_kind = 'identity_key'
    AND NEW.endpoint_b_identity_key_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM event_review_identity_keys i
      WHERE i.id = NEW.endpoint_b_identity_key_id
        AND NEW.endpoint_b_key <> 'identity:' || i.identity_key_hash
    );
END;

CREATE TABLE IF NOT EXISTS event_review_resolutions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  snapshot TEXT NOT NULL,
  discard_reason TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK(status IN ('resolved', 'discarded', 'superseded')),
  CHECK(TRIM(snapshot) = snapshot AND TRIM(snapshot) <> ''),
  CHECK(
    (status = 'discarded' AND TRIM(discard_reason) = discard_reason AND TRIM(discard_reason) <> '')
    OR
    (status <> 'discarded' AND TRIM(discard_reason) = discard_reason AND TRIM(discard_reason) = '')
  ),
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE(cluster_id)
);

CREATE TRIGGER IF NOT EXISTS event_review_resolutions_protect_update
BEFORE UPDATE ON event_review_resolutions
BEGIN
  SELECT RAISE(ABORT, 'event review resolutions are immutable');
END;

CREATE TRIGGER IF NOT EXISTS event_review_resolutions_protect_delete
BEFORE DELETE ON event_review_resolutions
BEGIN
  SELECT RAISE(ABORT, 'event review resolutions are immutable');
END;

CREATE TABLE IF NOT EXISTS import_run_event_review_clusters (
  import_run_id INTEGER NOT NULL,
  cluster_id INTEGER NOT NULL,
  linked_at TEXT NOT NULL,
  PRIMARY KEY(import_run_id, cluster_id),
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_import_run_event_review_clusters_cluster_id
ON import_run_event_review_clusters(cluster_id, linked_at, import_run_id);

CREATE TABLE IF NOT EXISTS repair_run_event_review_clusters (
  repair_run_id INTEGER NOT NULL,
  cluster_id INTEGER NOT NULL,
  linked_at TEXT NOT NULL,
  PRIMARY KEY(repair_run_id, cluster_id),
  FOREIGN KEY(repair_run_id) REFERENCES repair_runs(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_repair_run_event_review_clusters_cluster_id
ON repair_run_event_review_clusters(cluster_id, linked_at, repair_run_id);

ALTER TABLE event_source_attribute_observations RENAME TO event_source_attribute_observations_old;
DROP INDEX IF EXISTS idx_event_source_attribute_observations_event_identity;
DROP INDEX IF EXISTS idx_event_source_attribute_observations_review_group_identity;
DROP INDEX IF EXISTS idx_event_source_attribute_observations_event_review_cluster_identity;

CREATE TABLE event_source_attribute_observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_scope TEXT NOT NULL,
  source_id INTEGER NOT NULL,
  source_identity_key TEXT NOT NULL,
  source_authority TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  event_id INTEGER,
  review_group_id INTEGER,
  event_review_cluster_id INTEGER,
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
  CHECK(target_kind IN ('event', 'review_group', 'event_review_cluster')),
  CHECK(is_conflict IN (0, 1)),
  CHECK(
    (target_kind = 'event' AND event_id IS NOT NULL AND review_group_id IS NULL AND event_review_cluster_id IS NULL)
    OR
    (target_kind = 'review_group' AND review_group_id IS NOT NULL AND event_id IS NULL AND event_review_cluster_id IS NULL)
    OR
    (target_kind = 'event_review_cluster' AND event_review_cluster_id IS NOT NULL AND event_id IS NULL AND review_group_id IS NULL)
  ),
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(review_group_id) REFERENCES review_groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(event_review_cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO event_source_attribute_observations (
  id,
  run_scope,
  source_id,
  source_identity_key,
  source_authority,
  target_kind,
  event_id,
  review_group_id,
  event_review_cluster_id,
  field_name,
  incoming_raw,
  incoming_normalized,
  canonical_before_raw,
  canonical_before_normalized,
  outcome,
  is_conflict,
  created_at,
  updated_at
)
SELECT
  id,
  run_scope,
  source_id,
  source_identity_key,
  source_authority,
  target_kind,
  event_id,
  review_group_id,
  NULL,
  field_name,
  incoming_raw,
  incoming_normalized,
  canonical_before_raw,
  canonical_before_normalized,
  outcome,
  is_conflict,
  created_at,
  updated_at
FROM event_source_attribute_observations_old;

DROP TABLE event_source_attribute_observations_old;

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_source_attribute_observations_event_identity
ON event_source_attribute_observations(run_scope, event_id, source_id, source_identity_key, field_name)
WHERE target_kind = 'event';

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_source_attribute_observations_review_group_identity
ON event_source_attribute_observations(run_scope, review_group_id, source_id, source_identity_key, field_name)
WHERE target_kind = 'review_group';

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_source_attribute_observations_event_review_cluster_identity
ON event_source_attribute_observations(run_scope, event_review_cluster_id, source_id, source_identity_key, field_name)
WHERE target_kind = 'event_review_cluster';
