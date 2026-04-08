// Defines row data structures and row operations
// Note: DataValue conversion is now handled by toTabular() in data-value.ts

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
