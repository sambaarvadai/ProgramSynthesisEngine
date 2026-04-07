// Defines core engine type system and type definitions

import type { PrimitiveType } from './primitive.js';
import type { RowSchema } from './schema.js';

export type EngineType =
  // Primitive types
  | { kind: 'string' }
  | { kind: 'number' }
  | { kind: 'boolean' }
  | { kind: 'null' }
  | { kind: 'datetime' }
  | { kind: 'json' }
  // Complex types
  | { kind: 'array'; item: EngineType }
  | { kind: 'record'; fields: Record<string, EngineType> }
  | { kind: 'rowset'; schema: RowSchema }
  | { kind: 'any' };
