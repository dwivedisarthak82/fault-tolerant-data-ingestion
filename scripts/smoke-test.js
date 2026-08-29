const assert = require('assert');
const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const port = 3100 + Math.floor(Math.random() * 500);
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ingestion-smoke-'));
const dbPath = path.join(tempDir, 'events.db');

const server = spawn(process.execPath, ['backend/server.js'], {
  cwd: path.join(__dirname, '..'),
  env: {
    ...process.env,
    PORT: String(port),
    DB_PATH: dbPath
  },
  stdio: ['ignore', 'pipe', 'pipe']
});

let output = '';
server.stdout.on('data', (chunk) => {
  output += chunk.toString();
});
server.stderr.on('data', (chunk) => {
  output += chunk.toString();
});

function url(pathname) {
  return `http://localhost:${port}${pathname}`;
}

async function waitForServer() {
  const started = Date.now();

  while (Date.now() - started < 8000) {
    try {
      const response = await fetch(url('/'));
      if (response.ok) {
        return;
      }
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 150));
    }
  }

  throw new Error(`Server did not start. Output:\n${output}`);
}

async function postEvent(body) {
  const response = await fetch(url('/events'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });

  const data = await response.json();
  return { response, data };
}

async function getJson(pathname) {
  const response = await fetch(url(pathname));
  const data = await response.json();
  assert.equal(response.ok, true, `${pathname} should return 2xx`);
  return data;
}

async function run() {
  await waitForServer();

  const frontend = await fetch(url('/'));
  assert.equal(frontend.status, 200, 'frontend should be served');

  const baseEvent = {
    source: 'client_smoke',
    payload: {
      metric: 'sales',
      amount: '1200',
      timestamp: '2024/01/01',
      extra_field: 'ignored but preserved in raw_payload'
    }
  };

  const first = await postEvent(baseEvent);
  assert.equal(first.response.status, 202);
  assert.equal(first.data.status, 'processed');
  assert.equal(first.data.duplicate, false);
  assert.equal(first.data.event.amount, 1200);
  assert.equal(first.data.event.timestamp, '2024-01-01T00:00:00.000Z');

  const duplicate = await postEvent(baseEvent);
  assert.equal(duplicate.response.status, 202);
  assert.equal(duplicate.data.duplicate, true);
  assert.equal(duplicate.data.idempotency_key, first.data.idempotency_key);

  const aliasEvent = await postEvent({
    source: 'client_smoke',
    payload: {
      type: 'usage',
      value: '25',
      time: '2024-01-01T13:30:00Z'
    }
  });
  assert.equal(aliasEvent.data.status, 'processed');

  const rejected = await postEvent({
    clientId: 'client_smoke',
    payload: {
      event_type: 'sales',
      date: '2024-01-01'
    }
  });
  assert.equal(rejected.response.status, 202);
  assert.equal(rejected.data.status, 'rejected');
  assert.match(rejected.data.reason, /amount\/value/);

  const simulatedFailure = await postEvent({
    source: 'client_smoke',
    payload: {
      metric: 'sales',
      amount: '99',
      timestamp: '2024-01-02'
    },
    simulateFailure: true
  });
  assert.equal(simulatedFailure.response.status, 500);
  assert.match(simulatedFailure.data.error, /Retry/);

  const retryAfterFailure = await postEvent({
    source: 'client_smoke',
    payload: {
      metric: 'sales',
      amount: '99',
      timestamp: '2024-01-02'
    }
  });
  assert.equal(retryAfterFailure.response.status, 202);
  assert.equal(retryAfterFailure.data.duplicate, false);

  const events = await getJson('/events?client_id=client_smoke');
  assert.equal(events.processed.length, 3);
  assert.equal(events.rejected.length, 1);

  const aggregate = await getJson('/aggregate?client_id=client_smoke&from=2024-01-01&to=2024-01-02');
  const sales = aggregate.results.find((row) => row.metric === 'sales');
  const usage = aggregate.results.find((row) => row.metric === 'usage');

  assert.equal(sales.count, 2);
  assert.equal(sales.total_amount, 1299);
  assert.equal(usage.count, 1);
  assert.equal(usage.total_amount, 25);

  console.log('Smoke test passed: ingestion, dedupe, rejection, failure retry, frontend, and aggregation all work.');
}

function stopServer() {
  return new Promise((resolve) => {
    if (server.exitCode !== null) {
      resolve();
      return;
    }

    const timeout = setTimeout(resolve, 2500);
    server.once('exit', () => {
      clearTimeout(timeout);
      resolve();
    });
    server.kill();
  });
}

run()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await stopServer();
    fs.rmSync(tempDir, {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 200
    });
  });
