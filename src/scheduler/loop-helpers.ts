// Loop execution helpers

import type { ExecutionState } from './types.js';
import type { ExecutionContext } from '../core/context/execution-context.js';
import type { Value, RowSet } from '../core/types/value.js';
import type { NodeId } from '../core/graph/index.js';
import { contextPushScope } from '../core/context/execution-context.js';
import { scopeSet } from '../core/scope/scope.js';

/**
 * Fork execution state for a loop iteration.
 * 
 * The key insight: body nodes expect RowSet on data edges, but iteration items
 * may be scalars or individual Rows. We separate "what flows on the data edge" from "what is bound in scope".
 * 
 * - Body nodes receive the current iteration item (wrapped as a RowSet) via the loop node's data edge
 * - The iterVar is bound in scope per iteration to the current item/row/count
 * 
 * @param state - Parent execution state
 * @param iterCtx - Forked execution context with iterVar bound
 * @param bodyNodeIds - IDs of nodes in the loop body
 * @param loopNodeId - ID of the loop node
 * @param iterItem - Current iteration item (will be wrapped as RowSet for data edge)
 * @returns Forked execution state for this iteration
 */
export function forkStateForIteration(
  state: ExecutionState,
  iterCtx: ExecutionContext,
  bodyNodeIds: NodeId[],
  loopNodeId: NodeId,
  iterItem: Value
): ExecutionState {
  // Create fresh nodeStates for body nodes (all pending)
  const nodeStates = new Map<NodeId, any>();
  for (const nodeId of bodyNodeIds) {
    nodeStates.set(nodeId, {
      nodeId,
      status: 'pending',
      retryCount: 0
    });
  }

  // Add synthetic 'completed' state for the loop node itself
  // This allows body nodes reading from the loop's data edge to get the current iteration item
  nodeStates.set(loopNodeId, {
    nodeId: loopNodeId,
    status: 'completed',
    output: wrapAsRowSet(iterItem),
    retryCount: 0,
    completedAt: Date.now()
  });

  return {
    graph: state.graph,
    nodeStates,
    ctx: iterCtx,
    activatedEdges: new Set(), // Fresh Set for each iteration
    deactivatedEdges: new Set() // Fresh Set for each iteration
  };
}

/**
 * Wrap a value as a RowSet for data edge transmission.
 * 
 * - If value is already a RowSet: return as-is
 * - If value is a Row (Record): wrap in single-row RowSet with inferred schema
 * - If value is a scalar: wrap in single-row RowSet with 'value' column
 */
function wrapAsRowSet(item: Value): RowSet {
  if (isRowSet(item)) return item; // already a RowSet: use as-is
  
  if (typeof item === 'object' && item !== null && !Array.isArray(item)) {
    // It's a Row (Record<string, Value>): wrap in single-row RowSet
    const columns = Object.keys(item as Record<string, Value>).map(name => ({
      name,
      type: { kind: 'any' } as any,
      nullable: true
    }));
    return {
      schema: { columns },
      rows: [item as Record<string, Value>]
    };
  }
  
  // Scalar: wrap in single-row RowSet with 'value' column
  return {
    schema: { columns: [{ name: 'value', type: { kind: 'any' } as any, nullable: true }] },
    rows: [{ value: item }]
  };
}

/**
 * Convert a value to an iterable array.
 * 
 * Handles:
 * - RowSet: iterate over rows (each row is a Record<string, Value>)
 * - array: iterate elements as-is
 * - single Value (non-array, non-RowSet): wrap in [value] (single iteration)
 * - null: return [] (zero iterations)
 * 
 * @param value - Value to convert
 * @returns Array of items to iterate over
 */
export function toIterableArray(value: Value): any[] {
  if (value === null || value === undefined) {
    return [];
  }

  // Check if it's a RowSet
  if (isRowSet(value)) {
    return value.rows;
  }

  // Check if it's an array
  if (Array.isArray(value)) {
    return value;
  }

  // Single value - wrap in array for single iteration
  return [value];
}

/**
 * Type guard for RowSet
 */
function isRowSet(value: Value): value is RowSet {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Array.isArray((value as any).rows) &&
    typeof (value as any).schema === 'object'
  );
}

/**
 * Initialize accumulator based on accumulator definition.
 */
export function initAccumulator(def?: any): Value {
  if (!def || def.kind === 'collect') {
    return [];
  }
  if (def.kind === 'merge') {
    return {};
  }
  if (def.kind === 'reduce') {
    return def.initial ?? null;
  }
  return [];
}

/**
 * Update accumulator with a new value.
 */
export function updateAccumulator(
  def: any,
  acc: Value,
  item: Value,
  ctx: ExecutionContext,
  evaluator: any
): Value {
  if (!def || def.kind === 'collect') {
    const accArray = Array.isArray(acc) ? acc : [];
    accArray.push(item);
    return accArray;
  }

  if (def.kind === 'merge') {
    const accObj = typeof acc === 'object' && acc !== null && !Array.isArray(acc) ? acc : {};
    if (typeof item === 'object' && item !== null && !Array.isArray(item)) {
      Object.assign(accObj, item);
    }
    return accObj;
  }

  if (def.kind === 'reduce') {
    // Set acc and item in scope for reducer evaluation
    scopeSet(ctx.scope, 'acc', acc);
    scopeSet(ctx.scope, 'item', item);
    const reducerExpr = def.reducer;
    if (reducerExpr) {
      return evaluator.evaluate(reducerExpr, ctx.scope);
    }
    return item;
  }

  return acc;
}

/**
 * Get the output from a subgraph execution.
 * Finds nodes with no outgoing data edges TO OTHER BODY NODES (exit nodes) and returns the output
 * from the first completed exit node.
 * 
 * @param bodyNodeIds - IDs of nodes in the body (passed from caller)
 * @param graph - Pipeline graph
 * @param state - Execution state
 * @returns Output value from the first completed exit node
 */
export function getSubgraphOutput(
  bodyNodeIds: NodeId[],
  graph: any,
  state: ExecutionState
): Value | undefined {
  const bodySet = new Set(bodyNodeIds);
  const edges = Array.from(graph.edges.values()) as any[];
  
  // Find exit nodes: body nodes with no outgoing data edges to other body nodes
  const exitNodeIds = bodyNodeIds.filter(id => {
    const outgoing = edges.filter(e => e.from === id && e.kind === 'data');
    return outgoing.every(e => !bodySet.has(e.to));
  });

  if (exitNodeIds.length === 0) {
    // No exit node found - return the output of the last executed node
    // This is a fallback for single-node bodies
    for (const nodeId of bodyNodeIds) {
      const nodeState = state.nodeStates.get(nodeId);
      if (nodeState?.status === 'completed' && nodeState.output !== undefined) {
        return nodeState.output;
      }
    }
    return undefined;
  }

  // Return output from first completed exit node
  for (const exitId of exitNodeIds) {
    const nodeState = state.nodeStates.get(exitId);
    if (nodeState?.status === 'completed' && nodeState.output !== undefined) {
      return nodeState.output;
    }
  }

  return undefined;
}
