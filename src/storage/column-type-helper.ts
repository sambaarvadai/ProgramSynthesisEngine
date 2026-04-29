import { crmSchema } from '../schema/crm-schema.js';

export function isTextColumn(
  tableName: string,
  fieldName: string
): boolean {
  const colDef = (crmSchema as any).parsed.tables
    .get(tableName)?.columns.get(fieldName);
  if (!colDef) return true;  // default conservative: treat as text
  const t = (colDef.type ?? 'TEXT').toUpperCase();
  return (
    t === 'TEXT' ||
    t === 'VARCHAR' ||
    t === 'CHAR' ||
    t.startsWith('CHARACTER') ||
    t === 'INET'   // treat INET as text for comparison purposes
  );
}

export function normalizeStringParam(value: any): any {
  if (typeof value === 'string') return value.toLowerCase();
  if (Array.isArray(value)) return value.map(normalizeStringParam);
  return value;
}
