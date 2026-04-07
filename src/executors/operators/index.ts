// Physical operators module exports

export { ScanOperator } from './scan-operator.js';
export { FilterOperator } from './filter-operator.js';
export { ProjectOperator } from './project-operator.js';
export { JoinOperator } from './join-operator.js';
export { AggOperator } from './agg-operator.js';
export { SortOperator } from './sort-operator.js';
export { LimitOperator } from './limit-operator.js';

export type { ProjectionSpec } from './project-operator.js';
export type { JoinKind } from './join-operator.js';
export type { SortDirection, NullsOrder, SortKey } from './sort-operator.js';
