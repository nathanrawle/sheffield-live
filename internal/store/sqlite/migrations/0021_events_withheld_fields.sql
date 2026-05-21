ALTER TABLE events ADD COLUMN withheld_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN canonical_event_id INTEGER REFERENCES events(id) ON DELETE SET NULL;
ALTER TABLE events ADD COLUMN withheld_repair_run_id INTEGER REFERENCES repair_runs(id) ON DELETE SET NULL;
