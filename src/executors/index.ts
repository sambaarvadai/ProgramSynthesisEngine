// Executors module exports

export type { PhysicalOperatorState, PhysicalOperator } from './physical-operator.js';
export { BasePhysicalOperator, BudgetExceededError, collectAll } from './physical-operator.js';
export { ExprEvaluator, FieldNotFoundError, TypeMismatchError, CastError, NotImplementedError } from './expr-evaluator.js';
export { ScanOperator, FilterOperator, ProjectOperator, JoinOperator, AggOperator, SortOperator, LimitOperator } from './operators/index.js';
export type { ProjectionSpec, SortDirection, NullsOrder, SortKey, JoinKind } from './operators/index.js';
export {
  QueryExecutor,
  type QueryExecutorConfig,
  type QueryResult,
  QueryValidationError
} from './query-executor.js';
