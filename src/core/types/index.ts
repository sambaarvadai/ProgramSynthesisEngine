// Core type system exports

export type { PrimitiveType } from './primitive.js';
export type { EngineType } from './engine-type.js';
export type { Value, Row, RowSet, ArrayValue, ObjectValue } from './value.js';
export type { RowSchema } from './schema.js';
export type { RowType, RowBatch } from './row.js';
export type { ValidationError, ValidationResult } from './validation.js';
export { validationOk, validationFail } from './validation.js';

// DataValue exports
export type { Scalar, DataValueKind, DataValue, DataType } from './data-value.js';
export {
  DataValueCoercionError,
  tabular,
  record,
  scalar,
  collection,
  void_,
  isTabular,
  isRecord,
  isScalar,
  isCollection,
  isVoid,
  toTabular,
  toCollection,
  toScalar,
  isCompatible
} from './data-value.js';
