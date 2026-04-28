ALTER TABLE venues ADD COLUMN coverage_kind TEXT NOT NULL DEFAULT 'venue';
ALTER TABLE venues ADD COLUMN coverage_note TEXT NOT NULL DEFAULT '';

UPDATE venues
SET coverage_kind = 'program',
    coverage_note = 'Current coverage follows the Jazz at The Lescar programme rather than every event at The Lescar.'
WHERE slug = 'lescar';
