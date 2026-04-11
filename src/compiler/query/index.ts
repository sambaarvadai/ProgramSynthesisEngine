// Re-export QueryAST types from query-ast folder
export type {
  OrderByNode,
  ProjectionNode,
  ScanNode,
  JoinNode,
  QueryAST,
  QueryDAGNodeId,
  QueryDAGNode,
  QueryPlan
} from '../query-ast/query-ast.js';

export type {
  QueryIntentColumn,
  QueryIntentFilter,
  QueryIntentJoin,
  QueryIntentOrderBy,
  QueryIntent
} from '../query-ast/query-intent.js';

// Re-export QueryAST components
export { QueryASTBuilder } from '../query-ast/query-ast-builder.js'
export { QueryPlanner } from '../query-ast/query-planner.js'
export type { QueryASTBuildResult } from '../query-ast/query-ast-builder.js'

// Re-export Substrait components
export { SubstraitTranslator } from '../substrait/substrait-translator.js';

// Remaining query components
import { OperatorTreeBuilder } from './operator-tree-builder.js';

import {
  TablePreSelector,
  type TablePreSelectorConfig,
  type PreSelectionResult
} from './table-pre-selector.js';

export {
  QueryIntentGenerator,
  type QueryIntentGeneratorConfig
} from './query-intent-generator.js';

export { OperatorTreeBuilder };
export { TablePreSelector };
export type { TablePreSelectorConfig, PreSelectionResult };
