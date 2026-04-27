export function coerceColumnValue(
  value: string | number | boolean,
  columnType: string,
  enumValues?: string[]    // if provided, validate against this list
): string | number | boolean | null {
  
  // Already the right type — return as-is (idempotent)
  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  
  const raw = String(value).trim();
  const t = columnType.toUpperCase();

  if (
    t === 'INT' || t === 'INTEGER' || t === 'SERIAL' ||
    t === 'BIGINT' || t.startsWith('NUMERIC') || t === 'REAL'
  ) {
    const n = Number(raw);
    if (isNaN(n)) throw new Error(
      `Expected number for type ${columnType}, got: "${raw}"` 
    );
    return n;
  }

  if (t === 'BOOLEAN') {
    const v = raw.toLowerCase();
    if (v === 'true'  || v === '1' || v === 'yes') return true;
    if (v === 'false' || v === '0' || v === 'no')  return false;
    throw new Error(`Expected boolean, got: "${raw}"`);
  }

  if (t === 'TIMESTAMPTZ' || t === 'DATE') {
    if (raw === 'NOW()' || raw === 'CURRENT_TIMESTAMP') return raw;
    if (!isNaN(Date.parse(raw))) return raw;
    throw new Error(`Expected date/timestamp, got: "${raw}"`);
  }

  // After coercion, if enumValues provided, validate
  if (enumValues && enumValues.length > 0) {
    const coerced = raw;
    const lower = coerced.toLowerCase();
    
    // Case-insensitive match, return the canonical casing from schema
    const match = enumValues.find(v => v.toLowerCase() === lower);
    if (!match) {
      throw new Error(
        `Invalid value "${coerced}" for enum column. ` +
        `Valid values: ${enumValues.map(v => `'${v}'`).join(', ')}`
      );
    }
    return match;  // return canonical value e.g. 'open' not 'Open'
  }

  // TEXT, JSONB, INET, etc — return as string
  return raw;
}
