DROP INDEX IF EXISTS idx_event_source_attribute_observations_event_identity;
DROP INDEX IF EXISTS idx_event_source_attribute_observations_review_group_identity;

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_source_attribute_observations_event_identity
ON event_source_attribute_observations(run_scope, event_id, source_id, source_identity_key, field_name)
WHERE target_kind = 'event';

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_source_attribute_observations_review_group_identity
ON event_source_attribute_observations(run_scope, review_group_id, source_id, source_identity_key, field_name)
WHERE target_kind = 'review_group';
