ALTER TABLE review_groups ADD COLUMN staging_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_review_groups_staging_key
ON review_groups(staging_key);
