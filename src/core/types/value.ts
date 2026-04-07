// Defines value types and value handling mechanisms

import type { RowSchema } from './schema.js';

export interface RowSet {
  schema: RowSchema;
  rows: Row[];
}

export interface Row {
  [key: string]: Value;
}

// Use a recursive interface to break the circular reference
export interface ArrayValue extends ReadonlyArray<Value> {}
export interface ObjectValue extends Readonly<Record<string, Value>> {}

export type Value =
  | string
  | number
  | boolean
  | null
  | ArrayValue
  | ObjectValue
  | RowSet;
