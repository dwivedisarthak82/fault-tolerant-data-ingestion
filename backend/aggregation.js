function parseFilterDate(value, endOfDay = false) {
  const trimmed = String(value).trim();
  const dateOnly = trimmed.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);

  if (dateOnly) {
    const [, year, month, day] = dateOnly;
    const hours = endOfDay ? 23 : 0;
    const minutes = endOfDay ? 59 : 0;
    const seconds = endOfDay ? 59 : 0;
    const ms = endOfDay ? 999 : 0;
    return new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), hours, minutes, seconds, ms)).toISOString();
  }

  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid date filter: ${value}`);
  }

  return parsed.toISOString();
}

function getAggregate(db, filters = {}) {
  const where = ["status = 'processed'"];
  const params = {};

  if (filters.client_id) {
    where.push('client_id = @client_id');
    params.client_id = filters.client_id;
  }

  if (filters.from) {
    where.push('timestamp >= @from');
    params.from = parseFilterDate(filters.from);
  }

  if (filters.to) {
    where.push('timestamp <= @to');
    params.to = parseFilterDate(filters.to, true);
  }

  return db.prepare(`
    SELECT
      metric,
      COUNT(*) AS count,
      ROUND(SUM(amount), 4) AS total_amount
    FROM events
    WHERE ${where.join(' AND ')}
    GROUP BY metric
    ORDER BY metric ASC
  `).all(params);
}

module.exports = {
  getAggregate
};
