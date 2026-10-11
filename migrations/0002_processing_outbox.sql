CREATE TABLE IF NOT EXISTS processing_outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  metadata_id INTEGER NOT NULL UNIQUE REFERENCES metadata(id) ON DELETE CASCADE,
  file_key TEXT NOT NULL,
  request_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'READY' CHECK (status IN ('READY', 'ENQUEUED')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  dispatched_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_processing_outbox_ready
  ON processing_outbox(status, created_at);
