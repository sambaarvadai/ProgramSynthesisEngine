// Scheduler layer types
// Pure types only, no logic

import type { NodeRegistry } from '../core/registry/node-registry.js';
import type { QueryExecutor } from '../executors/query-executor.js';
import type { ExprEvaluator } from '../executors/expr-evaluator.js';
import type { PipelineGraph } from '../core/graph/pipeline-graph.js';
import type { NodeId } from '../core/graph/node.js';
import type { EdgeId } from '../core/graph/edge.js';
import type { ExecutionContext } from '../core/context/execution-context.js';
import type { ExecutionTrace } from '../core/context/execution-trace.js';
import type { ExecutionBudget } from '../core/context/execution-budget.js';
import type { Value } from '../core/types/value.js';

// ============================================================================
// Scheduler Configuration
// ============================================================================

export type SchedulerConfig = {
  nodeRegistry: NodeRegistry;
  queryExecutor: QueryExecutor;
  evaluator: ExprEvaluator;
  maxParallelBranches: number;
  defaultBatchSize: number;
};

// ============================================================================
// Node Execution State
// ============================================================================

export type NodeExecutionStatus =
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'skipped';

export type NodeExecutionState = {
  nodeId: NodeId;
  status: NodeExecutionStatus;
  startedAt?: number;
  completedAt?: number;
  output?: Value;
  error?: Error;
  retryCount: number;
};

// ============================================================================
// Execution State
// ============================================================================

export type ExecutionState = {
  graph: PipelineGraph;
  nodeStates: Map<NodeId, NodeExecutionState>;
  ctx: ExecutionContext;
  activatedEdges: Set<EdgeId>;
  deactivatedEdges: Set<EdgeId>;
};

// ============================================================================
// Scheduler Events
// ============================================================================

export type SchedulerEvent =
  | { kind: 'node_start'; nodeId: NodeId; timestamp: number }
  | { kind: 'node_complete'; nodeId: NodeId; output: Value; timestamp: number }
  | { kind: 'node_skip'; nodeId: NodeId; reason: string; timestamp: number }
  | { kind: 'node_error'; nodeId: NodeId; error: Error; willRetry: boolean; timestamp: number }
  | { kind: 'branch_fork'; parentNodeId: NodeId; branchNodeIds: NodeId[]; timestamp: number }
  | { kind: 'branch_merge'; mergeNodeId: NodeId; branchCount: number; timestamp: number }
  | { kind: 'pipeline_complete'; outputs: Map<string, Value>; timestamp: number }
  | { kind: 'pipeline_error'; error: Error; timestamp: number };

export type SchedulerEventHandler = (event: SchedulerEvent) => void;

// ============================================================================
// Execution Result
// ============================================================================

export type ExecutionResult = {
  status: 'success' | 'partial' | 'failed';
  outputs: Map<string, Value>;
  trace: ExecutionTrace;
  budgetUsed: ExecutionBudget;
  nodeStates: Map<NodeId, NodeExecutionState>;
  error?: Error;
};
