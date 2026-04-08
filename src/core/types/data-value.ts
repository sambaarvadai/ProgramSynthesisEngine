// Top-level type for values flowing along pipeline edges
// Wraps existing types without replacing them

import type { Value, RowSet, Row } from './value.js';
import type { RowSchema } from './schema.js';
import type { EngineType } from './engine-type.js';

// Scalar — single primitive value
type Scalar = string | number | boolean | null;

// DataValueKind — discriminator for all DataValue kinds
type DataValueKind =
  | 'tabular'      // table-shaped: RowSet
  | 'record'       // single row: Row
  | 'scalar'       // single primitive: Scalar
  | 'collection'   // ordered list of DataValues
  | 'void';        // no output (sink nodes)

// DataValue — discriminated union, flows along edges
type DataValue =
  | { kind: 'tabular';    data: RowSet;       schema: RowSchema }
  | { kind: 'record';     data: Row;          schema: RowSchema }
  | { kind: 'scalar';     data: Scalar;       type: EngineType }
  | { kind: 'collection'; data: DataValue[];  itemKind: DataValueKind }
  | { kind: 'void' };

// DataType — compile-time type descriptor for edges
// Used by NodeDefinition.inputPorts and outputPorts
type DataType =
  | { kind: 'tabular' }
  | { kind: 'record' }
  | { kind: 'scalar';     type: EngineType }
  | { kind: 'collection'; itemKind: DataValueKind }
  | { kind: 'void' }
  | { kind: 'any' };     // escape hatch — accepts anything

// Error class for coercion failures
export class DataValueCoercionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DataValueCoercionError';
  }
}

// Constructor helpers — make DataValues without object literal noise
export function tabular(data: RowSet, schema: RowSchema): DataValue {
  return { kind: 'tabular', data, schema };
}

export function record(data: Row, schema: RowSchema): DataValue {
  return { kind: 'record', data, schema };
}

export function scalar(data: Scalar, type: EngineType): DataValue {
  return { kind: 'scalar', data, type };
}

export function collection(data: DataValue[], itemKind: DataValueKind): DataValue {
  return { kind: 'collection', data, itemKind };
}

export const void_: DataValue = { kind: 'void' };

// Type guards
export function isTabular(v: DataValue): v is { kind: 'tabular'; data: RowSet; schema: RowSchema } {
  return v.kind === 'tabular';
}

export function isRecord(v: DataValue): v is { kind: 'record'; data: Row; schema: RowSchema } {
  return v.kind === 'record';
}

export function isScalar(v: DataValue): v is { kind: 'scalar'; data: Scalar; type: EngineType } {
  return v.kind === 'scalar';
}

export function isCollection(v: DataValue): v is { kind: 'collection'; data: DataValue[]; itemKind: DataValueKind } {
  return v.kind === 'collection';
}

export function isVoid(v: DataValue): v is { kind: 'void' } {
  return v.kind === 'void';
}

// Coercion helpers - convert between kinds when semantically safe
export function toTabular(v: DataValue): RowSet {
  if (isTabular(v)) return v.data
  
  if (isRecord(v)) {
    return { schema: v.schema, rows: [v.data] }
  }
  
  if (isScalar(v)) {
    return {
      schema: { columns: [{ name: 'value', type: v.type, nullable: true }] },
      rows: [{ value: v.data }]
    }
  }
  
  if (isVoid(v)) {
    return { schema: { columns: [] }, rows: [] }
  }
  
  if (isCollection(v)) {
    if (v.data.length === 0) {
      return { schema: { columns: [] }, rows: [] }
    }
    
    // Inspect actual items to determine how to flatten
    const allRows: Row[] = []
    let schema: RowSchema = { columns: [] }
    
    for (const item of v.data) {
      if (isVoid(item)) continue                    // skip void items
      
      const itemTable = toTabular(item)             // recursive coercion
      
      if (itemTable.rows.length > 0) {
        allRows.push(...itemTable.rows)
        // Use the schema from the first non-empty item
        if (schema.columns.length === 0 && itemTable.schema.columns.length > 0) {
          schema = itemTable.schema
        }
      }
    }
    
    return { schema, rows: allRows }
  }
  
  // Fallback - never throw, always return a valid RowSet
  return { schema: { columns: [] }, rows: [] }
}

export function toCollection(v: DataValue): DataValue[] {
  switch (v.kind) {
    case 'collection':
      return v.data;
    case 'tabular':
      return v.data.rows.map(row => ({ kind: 'record', data: row, schema: v.schema } as DataValue));
    case 'record':
      return [v];
    case 'scalar':
      return [v];
    case 'void':
      return [];
    default:
      throw new DataValueCoercionError(`Cannot coerce ${(v as DataValue).kind} to collection`);
  }
}

export function toScalar(v: DataValue): Scalar {
  switch (v.kind) {
    case 'scalar':
      return v.data;
    case 'tabular': {
      const rows = v.data.rows;
      const columns = v.data.schema.columns;
      if (rows.length === 1 && columns.length === 1) {
        return rows[0][columns[0].name] as Scalar ?? null;
      }
      throw new DataValueCoercionError(`Cannot coerce tabular with ${rows.length} rows and ${columns.length} columns to scalar`);
    }
    case 'record': {
      const keys = Object.keys(v.data);
      if (keys.length === 1) {
        const value = v.data[keys[0]];
        if (isScalarValue(value)) {
          return value;
        }
      }
      throw new DataValueCoercionError(`Cannot coerce record with ${keys.length} fields to scalar`);
    }
    default:
      throw new DataValueCoercionError(`Cannot coerce ${v.kind} to scalar`);
  }
}

function isScalarValue(v: unknown): v is Scalar {
  return typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null;
}

// Type compatibility check — used by edge validator
export function isCompatible(output: DataType, input: DataType): boolean {
  // 'any' accepts everything
  if (input.kind === 'any') return true;

  // Exact kind match
  if (output.kind === input.kind) {
    // For scalar, also check type compatibility
    if (output.kind === 'scalar' && input.kind === 'scalar') {
      // Allow any scalar type to flow into any scalar type for now
      // This could be refined with proper type hierarchy checks
      return true;
    }
    // For collection, check itemKind compatibility
    if (output.kind === 'collection' && input.kind === 'collection') {
      // Item kinds must be compatible - collection of records can go to collection of records, etc.
      return output.itemKind === input.itemKind || input.itemKind === 'any' as DataValueKind;
    }
    return true;
  }

  // tabular ← record: true (record coerces to single-row tabular)
  if (input.kind === 'tabular' && output.kind === 'record') {
    return true;
  }

  // collection ← tabular: true (tabular coerces to collection of records)
  if (input.kind === 'collection' && output.kind === 'tabular') {
    return input.itemKind === 'record' || input.itemKind === 'any' as DataValueKind;
  }

  // collection ← record: true (single record can be treated as collection of one)
  if (input.kind === 'collection' && output.kind === 'record') {
    return input.itemKind === 'record' || input.itemKind === 'any' as DataValueKind;
  }

  // Everything else: false
  return false;
}

// Re-export types for external use
export type { Scalar, DataValueKind, DataValue, DataType };
