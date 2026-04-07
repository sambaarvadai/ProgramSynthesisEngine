// Defines row data structures and row operations

import type { Value, RowSet } from './value.js';
import type { RowSchema } from './schema.js';

export interface Row {
  [key: string]: Value;
}

export type RowType = Row;

export type RowBatch = {
  rows: RowType[];
  schema: RowSchema;
};

/**
 * Normalize a Value to a RowSet.
 *
 * - If value is already a RowSet (has .rows and .schema): return as-is
 * - If value is an array of RowSets: flatten into single RowSet
 * - If value is an array of Records (loop collect output):
 *   wrap into a RowSet with inferred schema
 * - If value is an empty array: return empty RowSet
 * - Otherwise: wrap in single-row RowSet with a 'value' field
 */
export function normalizeToRowSet(value: Value): RowSet {
  // Already a RowSet
  if (value && typeof value === 'object' && !Array.isArray(value)
      && Array.isArray((value as any).rows) && typeof (value as any).schema === 'object') {
    return value as RowSet;
  }

  // Array of RowSets — flatten into single RowSet
  if (Array.isArray(value) && value.length > 0
      && value[0] && typeof value[0] === 'object'
      && Array.isArray((value[0] as any).rows)) {
    const allRows = value.flatMap((rs: RowSet) => (rs as any).rows ?? []);
    const schema = value.find((rs: RowSet) => (rs as any).schema?.columns?.length > 0)?.schema as any
      ?? { columns: [] };
    return { schema, rows: allRows };
  }

  // Array of plain Records — treat each as a Row
  if (Array.isArray(value) && value.length > 0
      && typeof value[0] === 'object' && !Array.isArray(value[0])) {
    const columns = Object.keys(value[0]).map(name => ({
      name,
      type: { kind: 'any' } as any,
      nullable: true
    }));
    return { schema: { columns }, rows: value };
  }

  // Empty array
  if (Array.isArray(value) && value.length === 0) {
    return { schema: { columns: [] }, rows: [] };
  }

  // Scalar — wrap in single row
  return {
    schema: { columns: [{ name: 'value', type: { kind: 'any' } as any, nullable: true }] },
    rows: [{ value }]
  };
}
