ALTER TABLE events ADD COLUMN official_listing_url TEXT NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN calendar_url TEXT NOT NULL DEFAULT '';
ALTER TABLE review_candidates ADD COLUMN calendar_url TEXT NOT NULL DEFAULT '';
