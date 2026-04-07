// ProgramExecutionEngine root

import { NodeRegistry } from './core/registry/index.js';
import { FunctionRegistry } from './core/registry/index.js';
import type { ExecutionBudget } from './core/context/execution-budget.js';
import type { StorageBackend, TempStore } from './core/storage/index.js';
import { Scheduler } from './scheduler/index.js';
import { registerAllNodes } from './nodes/index.js';
import { PostgresBackend, SQLiteTempStore } from './storage/index.js';
import { QueryExecutor } from './executors/index.js';
import { ExprEvaluator } from './executors/index.js';
import { TablePreSelector } from './compiler/query/index.js';
import { QueryIntentGenerator } from './compiler/query/index.js';
import { defaultBudget } from './core/context/execution-budget.js';

// Singleton registries
export const nodeRegistry = new NodeRegistry();
export const functionRegistry = new FunctionRegistry();

// Engine version
export const ENGINE_VERSION = '0.1.0';

// Re-export all core types so consumers import from 'src/index.ts' only
export * from './core/types/index.js';
export * from './core/ast/index.js';
export * from './core/scope/index.js';
export * from './core/graph/index.js';
export * from './core/context/index.js';
export * from './core/registry/index.js';
export * from './core/storage/index.js';
export * from './executors/index.js';
export * from './nodes/index.js';
export { Scheduler, SchedulerValidationError, NodeTimeoutError } from './scheduler/index.js';
export * from './scheduler/types.js';
export * from './scheduler/graph-utils.js';
export {
  QueryASTBuilder,
  QueryPlanner,
  OperatorTreeBuilder,
  TablePreSelector,
  QueryIntentGenerator,
  type QueryAST,
  type QueryDAGNode,
  type QueryPlan,
  type QueryIntent,
  type QueryIntentColumn,
  type QueryIntentFilter,
  type QueryIntentJoin,
  type QueryIntentOrderBy,
  type TablePreSelectorConfig,
  type PreSelectionResult,
  type QueryIntentGeneratorConfig,
  type QueryASTBuildResult
} from './compiler/query/index.js';
export * from './compiler/pipeline/index.js';
export { PipelineEngine, PipelineCompilationError } from './pipeline-engine.js';
export type { PipelineEngineConfig, PlanResult, RunResult } from './pipeline-engine.js';

// ============================================================================
// Engine Factory
// ============================================================================

export type ProgramExecutionEngineConfig = {
  postgresUrl: string;
  anthropicApiKey: string;
  maxParallelBranches?: number;
  defaultBatchSize?: number;
  budget?: Partial<ExecutionBudget>;
};

export async function createEngine(config: ProgramExecutionEngineConfig): Promise<{
  scheduler: Scheduler;
  queryExecutor: QueryExecutor;
  intentGenerator: QueryIntentGenerator;
  backend: StorageBackend;
  tempStore: TempStore;
  registry: NodeRegistry;
  fnRegistry: FunctionRegistry;
  dispose: () => Promise<void>;
}> {
  // Create storage backend and connect
  const backend = new PostgresBackend(config.postgresUrl);
  await backend.connect();

  // Create temp store
  const tempStore = new SQLiteTempStore(':memory:');

  // Create registries
  const registry = new NodeRegistry();
  const fnRegistry = new FunctionRegistry();

  // Register all node definitions
  registerAllNodes(registry);

  // Create evaluator
  const evaluator = new ExprEvaluator(fnRegistry);

  // Create query executor
  const queryExecutor = new QueryExecutor({
    schema: { tables: new Map(), foreignKeys: [], version: '1' },
    backend,
    tempStore,
    evaluator,
    batchSize: config.defaultBatchSize || 100
  });

  // Create scheduler
  const scheduler = new Scheduler({
    nodeRegistry: registry,
    queryExecutor,
    evaluator,
    maxParallelBranches: config.maxParallelBranches || 4,
    defaultBatchSize: config.defaultBatchSize || 100
  });

  // Create table pre-selector
  const preSelector = new TablePreSelector({
    anthropicApiKey: config.anthropicApiKey
  });

  // Create query intent generator
  const intentGenerator = new QueryIntentGenerator({
    anthropicApiKey: config.anthropicApiKey,
    preSelector
  });

  // Dispose function
  const dispose = async () => {
    await backend.disconnect();
    await tempStore.close();
  };

  return {
    scheduler,
    queryExecutor,
    intentGenerator,
    backend,
    tempStore,
    registry,
    fnRegistry,
    dispose
  };
}
