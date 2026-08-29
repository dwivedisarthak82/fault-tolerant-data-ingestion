const crypto = require('crypto');
const express = require('express');
const path = require('path');

const db = require('./db');
const { getAggregate } = require('./aggregation');
const { FIELD_ALIASES, normalizeEvent } = require('./normalize');

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json({ limit: '1mb' }));
app.use((error, req, res, next) => {
  if (error instanceof SyntaxError && 'body' in error) {
    res.status(400).json({
      error: 'Request body must be valid JSON.'
    });
    return;
  }

  next(error);
});
app.use(express.static(path.join(__dirname, '..', 'frontend')));

const insertProcessed = db.prepare(`
  INSERT OR IGNORE INTO events (
    idempotency_key, client_id, raw_payload, metric, amount, timestamp, status, reject_reason, created_at
  ) VALUES (
    @idempotency_key, @client_id, @raw_payload, @metric, @amount, @timestamp, 'processed', NULL, @created_at
  )
`);

const insertRejected = db.prepare(`
  INSERT OR IGNORE INTO events (
    idempotency_key, client_id, raw_payload, metric, amount, timestamp, status, reject_reason, created_at
  ) VALUES (
    @idempotency_key, @client_id, @raw_payload, @metric, @amount, @timestamp, 'rejected', @reject_reason, @created_at
  )
`);

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function isTruthy(value) {
  return value === true || value === 'true' || value === '1' || value === 1;
}

function processedKey(event) {
  // The idempotency key is derived from the normalized business fields so a retried event
  // maps to the same unique row even if the client sends the original loose schema again.
  return sha256(`${event.client_id}${event.metric}${event.amount}${event.timestamp}`);
}

function rejectedKey(rawPayload, reason) {
  return sha256(`rejected|${JSON.stringify(rawPayload)}|${reason}`);
}

function rawClientId(body) {
  for (const field of FIELD_ALIASES.client_id) {
    if (typeof body?.[field] === 'string' && body[field].trim() !== '') {
      return body[field].trim();
    }
  }

  return null;
}

function nowIso() {
  return new Date().toISOString();
}

// The transaction is the boundary for retry safety: a failure rolls back the whole
// attempt, while a committed duplicate is ignored by SQLite's unique idempotency key.
const ingestEvent = db.transaction((body, simulateFailure) => {
  const normalization = normalizeEvent(body);
  const rawPayload = JSON.stringify(body || {});

  if (!normalization.ok) {
    const idempotency_key = rejectedKey(body || {}, normalization.reason);

    insertRejected.run({
      idempotency_key,
      client_id: rawClientId(body),
      raw_payload: rawPayload,
      metric: null,
      amount: null,
      timestamp: null,
      reject_reason: normalization.reason,
      created_at: nowIso()
    });

    return {
      statusCode: 202,
      body: {
        status: 'rejected',
        note: 'Event was stored as rejected instead of crashing the ingestion flow.',
        reason: normalization.reason,
        idempotency_key
      }
    };
  }

  const event = normalization.data;
  const idempotency_key = processedKey(event);

  if (simulateFailure) {
    // This happens inside the transaction before the insert, so the client can retry
    // without risking a half-written row or double-counted aggregate.
    throw new Error('Simulated failure after validation and before commit');
  }

  const result = insertProcessed.run({
    idempotency_key,
    client_id: event.client_id,
    raw_payload: rawPayload,
    metric: event.metric,
    amount: event.amount,
    timestamp: event.timestamp,
    created_at: nowIso()
  });

  return {
    statusCode: 202,
    body: {
      status: 'processed',
      note: result.changes === 0
        ? 'Duplicate event ignored safely.'
        : 'Event processed successfully.',
      duplicate: result.changes === 0,
      idempotency_key,
      event
    }
  };
});

app.post('/events', (req, res) => {
  try {
    const simulateFailure = isTruthy(req.query.simulateFailure) || isTruthy(req.body?.simulateFailure);
    const response = ingestEvent(req.body, simulateFailure);
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    res.status(500).json({
      error: 'Ingestion failed before commit. Retry the same event safely.',
      detail: error.message
    });
  }
});

app.get('/events', (req, res) => {
  const where = [];
  const params = {};

  if (req.query.client_id) {
    where.push('client_id = @client_id');
    params.client_id = req.query.client_id;
  }

  const rows = db.prepare(`
    SELECT *
    FROM events
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
    ORDER BY id DESC
  `).all(params);

  res.json({
    processed: rows.filter((row) => row.status === 'processed'),
    rejected: rows.filter((row) => row.status === 'rejected')
  });
});

app.get('/aggregate', (req, res) => {
  try {
    res.json({
      filters: {
        client_id: req.query.client_id || null,
        from: req.query.from || null,
        to: req.query.to || null
      },
      results: getAggregate(db, req.query)
    });
  } catch (error) {
    res.status(400).json({
      error: 'Invalid aggregate filter.',
      detail: error.message
    });
  }
});

app.listen(port, () => {
  console.log(`Fault-tolerant ingestion app listening at http://localhost:${port}`);
});
