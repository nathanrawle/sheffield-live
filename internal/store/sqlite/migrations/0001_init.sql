CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  UNIQUE(name, url)
);

CREATE TABLE IF NOT EXISTS venues (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  address TEXT NOT NULL,
  neighbourhood TEXT NOT NULL,
  description TEXT NOT NULL,
  website TEXT NOT NULL,
  origin TEXT NOT NULL,
  UNIQUE(slug)
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slug TEXT NOT NULL,
  venue_id INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  start_at TEXT NOT NULL,
  end_at TEXT NOT NULL,
  genre TEXT NOT NULL,
  status TEXT NOT NULL,
  description TEXT NOT NULL,
  last_checked_at TEXT NOT NULL,
  origin TEXT NOT NULL,
  UNIQUE(slug),
  FOREIGN KEY(venue_id) REFERENCES venues(id) ON DELETE RESTRICT ON UPDATE CASCADE,
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS import_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL,
  notes TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_run_id INTEGER NOT NULL,
  source_id INTEGER,
  captured_at TEXT NOT NULL,
  payload TEXT NOT NULL,
  FOREIGN KEY(import_run_id) REFERENCES import_runs(id) ON DELETE CASCADE,
  FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_sources_name_url ON sources(name, url);
CREATE INDEX IF NOT EXISTS idx_venues_slug ON venues(slug);
CREATE INDEX IF NOT EXISTS idx_events_venue_id_start_slug ON events(venue_id, start_at, slug);
CREATE INDEX IF NOT EXISTS idx_events_start_slug ON events(start_at, slug);
CREATE INDEX IF NOT EXISTS idx_snapshots_import_run_id ON snapshots(import_run_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_source_id ON snapshots(source_id);
