# Fault-Tolerant Data Ingestion

## What assumptions did you make?

- Incoming events are JSON objects shaped like `{ "source": "client_A", "payload": { ... } }`.
- `source` is the client identifier unless `client_id` or `clientId` is provided.
- The accepted metric aliases are `metric`, `event_type`, and `type`.
- The accepted amount aliases are `amount` and `value`; numeric strings are accepted.
- The accepted timestamp aliases are `timestamp`, `date`, and `time`; values are converted to ISO 8601.
- Extra payload fields are stored in `raw_payload` for debugging/auditing but ignored by the normalizer so new client fields do not break ingestion.
- The normalizer is intentionally strict about the four canonical fields and tolerant about aliases/types. That keeps bad data out of aggregates without rejecting harmless schema drift.
- This is a single-process demo, so SQLite and synchronous `better-sqlite3` queries are acceptable. The code is still split into `backend` and `frontend` folders to keep responsibilities clear.

## How does your system prevent double counting?

Valid events are normalized first, then the app computes an idempotency key from the canonical business fields:

```text
sha256(client_id + metric + amount + timestamp)
```

The `events.idempotency_key` column is unique, and processed inserts use `INSERT OR IGNORE`. If the same event is resent, SQLite ignores the duplicate row, so aggregation still counts only the first accepted event. Aggregation also reads only rows where `status = 'processed'`, so rejected data never affects totals.

## What happens if the database fails mid-request?

The ingestion write runs inside a SQLite transaction. The `simulateFailure` option throws after validation but before the processed insert, so the transaction rolls back and no partial event is committed. The API returns an error telling the client the request can be retried safely. On retry without the simulated failure, the same normalized event produces the same idempotency key and is inserted once. If validation itself fails, the event is stored as `rejected` with a reason and the API returns `202` instead of crashing.

## What would break first at scale?

The single Express process and local SQLite database would be the first limits. Large write volume would need a queue, worker pool, stronger backpressure, and a database designed for concurrent writes. The alias-mapping normalizer would also need a more configurable schema registry as clients and formats grow. At higher scale, aggregation would likely move to precomputed summary tables or background jobs instead of querying raw event rows every time.
