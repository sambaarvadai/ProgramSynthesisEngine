// Core compiler components
export { PipelineCompiler } from './pipeline/pipeline-compiler.js'
export { PipelineEngine } from '../pipeline-engine.js'

// Query compilation
export { QueryASTBuilder, QueryPlanner } from './query-ast/index.js'
export type { QueryPlan, QueryDAGNode, QueryDAGNodeId, QueryIntent } from './query-ast/index.js'

// Substrait support
export { SubstraitTranslator } from './substrait/index.js'

// Schema configuration
export { crmSchema } from '../config/crm-schema.js'
export type { SchemaConfig } from './schema/schema-config.js'
