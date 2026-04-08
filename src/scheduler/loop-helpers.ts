// Loop execution helpers

import type { ExecutionState } from './types.js';
import type { ExecutionContext } from '../core/context/execution-context.js';
import type { RowSet, Row } from '../core/types/value.js';
import type { DataValue, DataValueKind } from '../core/types/data-value.js';
import { tabular, record, scalar, collection, void_, isCollection, isRecord, isTabular, isScalar } from '../core/types/data-value.js';
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
/**
 * Wrap a DataValue as a DataValue for data edge transmission (loop node output).
 * 
 * - If value is already a DataValue: return as-is
 * - If value is a RowSet: wrap as tabular
 * - If value is a Row (Record): wrap as record
 * - If value is a scalar: wrap as scalar
 */
function wrapAsDataValue(item: unknown): DataValue {
  // Already DataValue
  if (item && typeof item === 'object' && 'kind' in item) {
    const kind = (item as any).kind;
    if (['tabular', 'record', 'scalar', 'collection', 'void'].includes(kind)) {
      return item as DataValue;
    }
  }

  // RowSet (has rows and schema)
  if (item && typeof item === 'object' && !Array.isArray(item) && 'rows' in item && 'schema' in item) {
    const rowSet = item as RowSet;
    return tabular(rowSet, rowSet.schema);
  }

  // Scalar
  if (typeof item === 'string' || typeof item === 'number' || typeof item === 'boolean' || item === null) {
    return scalar(item, { kind: item === null ? 'null' : typeof item as 'string' | 'number' | 'boolean' });
  }

  // Array → collection
  if (Array.isArray(item)) {
    return collection(item.map(wrapAsDataValue), 'any' as DataValueKind);
  }

  // Plain Record → record
  if (item && typeof item === 'object' && !Array.isArray(item)) {
    const schema = {
      columns: Object.keys(item).map(name => ({
        name,
        type: { kind: 'any' as const } as import('../core/types/engine-type.js').EngineType,
        nullable: true
      }))
    };
    return record(item as import('../core/types/value.js').Row, schema);
  }

  // undefined/null → void
  return void_;
}

export function forkStateForIteration(
  state: ExecutionState,
  iterCtx: ExecutionContext,
  bodyNodeIds: NodeId[],
  loopNodeId: NodeId,
  iterItem: DataValue
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
  // iterItem is a DataValue; tabular flows through, others get wrapped as record for data edge
  let loopOutput: DataValue;
  if (isTabular(iterItem)) {
    loopOutput = iterItem;
  } else if (isRecord(iterItem)) {
    loopOutput = iterItem;
  } else {
    // Wrap scalar/collection/void as a record for data edge transmission
    const rowData = isScalar(iterItem) ? { value: iterItem.data }
      : isCollection(iterItem) ? { items: iterItem.data }
      : {};
    loopOutput = record(rowData as Row, { columns: [] });
  }
  nodeStates.set(loopNodeId, {
    nodeId: loopNodeId,
    status: 'completed',
    output: loopOutput,
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
 * Convert a DataValue to an iterable array.
 * 
 * Handles:
 * - tabular: iterate over rows (each row wrapped as record DataValue)
 * - collection: iterate over items
 * - record: return [value]
 * - scalar: return [value]
 * - void/null: return [] (zero iterations)
 * 
 * @param value - DataValue to convert
 * @returns Array of DataValues to iterate over
 */
export function toIterableArray(value: DataValue): DataValue[] {
  if (value.kind === 'void') {
    return [];
  }

  // tabular: iterate over rows as record DataValues
  if (value.kind === 'tabular') {
    return value.data.rows.map((row: Row) => record(row, value.schema));
  }

  // collection: iterate items
  if (value.kind === 'collection') {
    return value.data;
  }

  // record, scalar: single-item array
  return [value];
}


/**
 * Initialize accumulator based on accumulator definition.
 */
export function initAccumulator(def?: any): DataValue {
  if (!def || def.kind === 'collect') {
    // Start with empty collection of 'any' - will be refined when first item is added
    return collection([], 'any' as DataValueKind);
  }
  if (def.kind === 'merge') {
    return record({}, { columns: [] });
  }
  if (def.kind === 'reduce') {
    return scalar(def?.initial ?? null, { kind: 'null' });
  }
  return collection([], 'any' as DataValueKind);
}

/**
 * Merge two schemas, preserving column definitions.
 */
function mergeSchemas(acc: DataValue, item: DataValue): { columns: Array<{ name: string; type: import('../core/types/engine-type.js').EngineType; nullable: boolean }> } {
  const accCols = isRecord(acc) ? acc.schema.columns 
                : isTabular(acc) ? acc.schema.columns 
                : [];
  const itemCols = isRecord(item) ? item.schema.columns 
                   : isTabular(item) ? item.schema.columns 
                   : [];
  
  // Combine columns, avoiding duplicates by name
  const seen = new Set<string>();
  const columns: Array<{ name: string; type: import('../core/types/engine-type.js').EngineType; nullable: boolean }> = [];
  
  for (const col of accCols) {
    if (!seen.has(col.name)) {
      seen.add(col.name);
      columns.push(col);
    }
  }
  for (const col of itemCols) {
    if (!seen.has(col.name)) {
      seen.add(col.name);
      columns.push(col);
    }
  }
  
  return { columns };
}

/**
 * Update accumulator with a new value.
 */
export function updateAccumulator(
  def: any,
  acc: DataValue,
  item: DataValue,
  ctx: ExecutionContext,
  evaluator: any
): DataValue {
  if (!def || def.kind === 'collect') {
    // Collect into collection, using item's kind
    const arr = isCollection(acc) ? acc.data : [];
    // Convert void to empty tabular before collecting
    const safeItem = item.kind === 'void'
      ? tabular({ schema: { columns: [] }, rows: [] }, { columns: [] })
      : item;
    return collection([...arr, safeItem], safeItem.kind);
  }

  if (def.kind === 'merge') {
    // Merge rows: accRow + itemRow
    const accRow = isRecord(acc) ? acc.data 
                 : isTabular(acc) ? (acc.data.rows[0] ?? {}) 
                 : {};
    const itemRow = isRecord(item) ? item.data 
                  : isTabular(item) ? (item.data.rows[0] ?? {})
                  : {};
    return record({ ...accRow, ...itemRow }, mergeSchemas(acc, item));
  }

  if (def.kind === 'reduce') {
    // Set acc and item in scope for reducer evaluation
    scopeSet(ctx.scope, 'acc', isScalar(acc) ? acc.data : acc);
    scopeSet(ctx.scope, 'item', isScalar(item) ? item.data : item);
    const result = evaluator.evaluate(def!.reducer, ctx.scope);
    return scalar(result, { kind: 'any' });
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
 * @returns Output DataValue from the first completed exit node
 */
export function getSubgraphOutput(
  bodyNodeIds: NodeId[],
  graph: any,
  state: ExecutionState
): DataValue | undefined {
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
