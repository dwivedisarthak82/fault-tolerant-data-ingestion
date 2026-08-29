const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const dataDir = path.join(__dirname, 'data');
fs.mkdirSync(dataDir, { recursive: true });

const db = new Database(path.join(dataDir, 'events.db'));
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// Store the original event beside the normalized columns. This keeps ingestion auditable
// while giving aggregation a stable, typed shape to query.
db.exec(`
  CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT UNIQUE NOT NULL,
    client_id TEXT,
    raw_payload TEXT NOT NULL,
    metric TEXT,
    amount REAL,
    timestamp TEXT,
    status TEXT NOT NULL CHECK (status IN ('processed', 'rejected')),
    reject_reason TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  );

  CREATE INDEX IF NOT EXISTS idx_events_client_id ON events(client_id);
  CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
  CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
`);

module.exports = db;
