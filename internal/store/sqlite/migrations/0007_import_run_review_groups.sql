CREATE TABLE IF NOT EXISTS import_run_review_groups (
  import_run_id INTEGER NOT NULL,
  review_group_id INTEGER NOT NULL,
  linked_at TEXT NOT NULL,
  PRIMARY KEY(import_run_id, review_group_id),
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id) ON DELETE CASCADE,
  FOREIGN KEY(review_group_id) REFERENCES review_groups(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_import_run_review_groups_review_group_id
ON import_run_review_groups(review_group_id, linked_at, import_run_id);
