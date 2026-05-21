ALTER TABLE event_review_clusters ADD COLUMN staging_key TEXT;
ALTER TABLE event_review_clusters ADD COLUMN staging_key_version INTEGER NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_review_clusters_staging_key_version
ON event_review_clusters(staging_key, staging_key_version)
WHERE staging_key IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS event_review_clusters_staging_key_validate_insert
BEFORE INSERT ON event_review_clusters
WHEN (
	NEW.staging_key IS NULL AND COALESCE(NEW.staging_key_version, 0) <> 0
)
OR (
	NEW.staging_key IS NOT NULL AND (
		TRIM(NEW.staging_key) <> NEW.staging_key
		OR TRIM(NEW.staging_key) = ''
		OR COALESCE(NEW.staging_key_version, 0) <= 0
	)
)
BEGIN
	SELECT RAISE(ABORT, 'invalid event review cluster staging key');
END;

CREATE TRIGGER IF NOT EXISTS event_review_clusters_staging_key_validate_update
BEFORE UPDATE OF staging_key, staging_key_version ON event_review_clusters
WHEN NEW.staging_key IS NOT OLD.staging_key
	OR NEW.staging_key_version <> OLD.staging_key_version
BEGIN
	SELECT RAISE(ABORT, 'event review cluster staging key is immutable');
END;

CREATE TABLE IF NOT EXISTS import_run_event_review_evidence (
  import_run_id INTEGER NOT NULL,
  cluster_id INTEGER NOT NULL,
  evidence_id INTEGER NOT NULL,
  linked_at TEXT NOT NULL,
  link_reason TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(import_run_id, cluster_id, evidence_id),
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(evidence_id) REFERENCES event_review_evidence(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_import_run_event_review_evidence_cluster_evidence
ON import_run_event_review_evidence(cluster_id, evidence_id, linked_at, import_run_id);

CREATE TABLE IF NOT EXISTS repair_run_event_review_evidence (
  repair_run_id INTEGER NOT NULL,
  cluster_id INTEGER NOT NULL,
  evidence_id INTEGER NOT NULL,
  linked_at TEXT NOT NULL,
  link_reason TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(repair_run_id, cluster_id, evidence_id),
  FOREIGN KEY(repair_run_id) REFERENCES repair_runs(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(cluster_id) REFERENCES event_review_clusters(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(evidence_id) REFERENCES event_review_evidence(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_repair_run_event_review_evidence_cluster_evidence
ON repair_run_event_review_evidence(cluster_id, evidence_id, linked_at, repair_run_id);
