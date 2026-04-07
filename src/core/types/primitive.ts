// Defines primitive data types and their handling

export type PrimitiveType =
  | { kind: 'string' }
  | { kind: 'number' }
  | { kind: 'boolean' }
  | { kind: 'null' }
  | { kind: 'datetime' }
  | { kind: 'json' };
