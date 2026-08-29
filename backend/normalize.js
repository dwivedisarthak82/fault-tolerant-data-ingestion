const FIELD_ALIASES = {
  client_id: ['client_id', 'clientId', 'source'],
  metric: ['metric', 'event_type', 'type'],
  amount: ['amount', 'value'],
  timestamp: ['timestamp', 'date', 'time']
};

function firstPresent(source, names) {
  for (const name of names) {
    if (source[name] !== undefined && source[name] !== null && source[name] !== '') {
      return source[name];
    }
  }
  return undefined;
}

function parseAmount(value) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : undefined;
  }

  if (typeof value === 'string') {
    const normalized = value.replace(/,/g, '').trim();
    if (normalized === '') {
      return undefined;
    }

    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

function parseTimestamp(value) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }

  if (typeof value === 'number') {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? undefined : parsed.toISOString();
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') {
      return undefined;
    }

    const dateOnly = trimmed.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
    if (dateOnly) {
      const [, year, month, day] = dateOnly;
      const parsed = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
      return Number.isNaN(parsed.getTime()) ? undefined : parsed.toISOString();
    }

    const parsed = new Date(trimmed);
    return Number.isNaN(parsed.getTime()) ? undefined : parsed.toISOString();
  }

  return undefined;
}

function normalizeEvent(input) {
  const payload = input && typeof input.payload === 'object' && input.payload !== null
    ? input.payload
    : {};

  // Keep all schema drift handling in one small mapping layer. If a new client sends
  // "eventName" instead of "metric", this config can grow without changing ingestion.
  const clientId = firstPresent(input || {}, FIELD_ALIASES.client_id);
  const metric = firstPresent(payload, FIELD_ALIASES.metric);
  const rawAmount = firstPresent(payload, FIELD_ALIASES.amount);
  const rawTimestamp = firstPresent(payload, FIELD_ALIASES.timestamp);

  const amount = parseAmount(rawAmount);
  const timestamp = parseTimestamp(rawTimestamp);
  const missing = [];

  if (!clientId || typeof clientId !== 'string') {
    missing.push('client_id/source');
  }

  if (!metric || typeof metric !== 'string') {
    missing.push('metric/event_type/type');
  }

  if (amount === undefined) {
    missing.push('amount/value');
  }

  if (!timestamp) {
    missing.push('timestamp/date/time');
  }

  if (missing.length > 0) {
    return {
      ok: false,
      reason: `Missing or malformed required field(s): ${missing.join(', ')}`
    };
  }

  return {
    ok: true,
    data: {
      client_id: clientId.trim(),
      metric: metric.trim(),
      amount,
      timestamp
    }
  };
}

module.exports = {
  FIELD_ALIASES,
  normalizeEvent
};
