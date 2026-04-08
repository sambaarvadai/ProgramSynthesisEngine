// Graph analysis utilities
// Pure functions for graph analysis. No execution logic.

import type { PipelineGraph, NodeId, EdgeId, PipelineEdge } from '../core/graph/index.js';
import type { ExecutionState, NodeExecutionState, NodeExecutionStatus } from './types.js';
import type { DataValue, DataValueKind } from '../core/types/data-value.js';
import { tabular, record, scalar, collection, void_ } from '../core/types/data-value.js';
import type { RowSet } from '../core/types/value.js';
import type { ValidationResult, ValidationError } from '../core/types/validation.js';
import { validationOk, validationFail } from '../core/types/validation.js';

export class CycleDetectedError extends Error {
  cycle: NodeId[];

  constructor(cycle: NodeId[]) {
    super(`Cycle detected in graph: ${cycle.join(' -> ')}`);
    this.name = 'CycleDetectedError';
    this.cycle = cycle;
  }
}

/**
 * Kahn's algorithm for topological sort (BFS-based, not recursive)
 * Considers only data edges for ordering
 * Throws CycleDetectedError if cycle found
 * Returns nodes in execution order (dependencies before dependents)
 */
export function topologicalSort(graph: PipelineGraph): NodeId[] {
  // Count incoming data edges for each node
  const inDegree = new Map<NodeId, number>();
  const dataEdges = Array.from(graph.edges.values()).filter(e => e.kind === 'data');

  // Initialize in-degree for all nodes
  for (const nodeId of graph.nodes.keys()) {
    inDegree.set(nodeId, 0);
  }

  // Count incoming data edges
  for (const edge of dataEdges) {
    inDegree.set(edge.to, (inDegree.get(edge.to) || 0) + 1);
  }

  // Queue of nodes with no incoming data edges
  const queue: NodeId[] = [];
  for (const [nodeId, degree] of inDegree.entries()) {
    if (degree === 0) {
      queue.push(nodeId);
    }
  }

  const result: NodeId[] = [];
  const visited = new Set<NodeId>();

  while (queue.length > 0) {
    const nodeId = queue.shift()!;
    if (visited.has(nodeId)) {
      continue;
    }
    visited.add(nodeId);
    result.push(nodeId);

    // Find all outgoing data edges from this node
    const outgoingEdges = dataEdges.filter(e => e.from === nodeId);
    for (const edge of outgoingEdges) {
      const newDegree = (inDegree.get(edge.to) || 0) - 1;
      inDegree.set(edge.to, newDegree);

      if (newDegree === 0) {
        queue.push(edge.to);
      }
    }
  }

  // If we didn't visit all nodes, there's a cycle
  if (result.length !== graph.nodes.size) {
    // Find the cycle (nodes not in result)
    const cycle = Array.from(graph.nodes.keys()).filter(id => !result.includes(id));
    throw new CycleDetectedError(cycle);
  }

  return result;
}

/**
 * Returns all edges where edge.to === nodeId && edge.kind === 'data'
 */
export function getIncomingDataEdges(graph: PipelineGraph, nodeId: NodeId): PipelineEdge[] {
  return Array.from(graph.edges.values()).filter(
    e => e.to === nodeId && e.kind === 'data'
  );
}

/**
 * Returns all edges where edge.from === nodeId && edge.kind === 'data'
 */
export function getOutgoingDataEdges(graph: PipelineGraph, nodeId: NodeId): PipelineEdge[] {
  return Array.from(graph.edges.values()).filter(
    e => e.from === nodeId && e.kind === 'data'
  );
}

/**
 * Returns all edges where edge.from === nodeId && edge.kind === 'control'
 */
export function getOutgoingControlEdges(graph: PipelineGraph, nodeId: NodeId): PipelineEdge[] {
  return Array.from(graph.edges.values()).filter(
    e => e.from === nodeId && e.kind === 'control'
  );
}

/**
 * Returns all edges where edge.to === nodeId && edge.kind === 'control'
 */
export function getIncomingControlEdges(graph: PipelineGraph, nodeId: NodeId): PipelineEdge[] {
  return Array.from(graph.edges.values()).filter(
    e => e.to === nodeId && e.kind === 'control'
  );
}

/**
 * Checks if a node's control path is active:
 * - If node has no incoming control edges: return true (always reachable)
 * - If ALL incoming control edges are in deactivatedEdges: return false (skip this node)
 * - If AT LEAST ONE incoming control edge is in activatedEdges: return true
 * - If incoming control edges exist but none are activated or deactivated yet:
 *     return true (waiting — scheduler will handle ordering)
 */
export function isControlPathActive(nodeId: NodeId, graph: PipelineGraph, state: ExecutionState): boolean {
  const incomingControlEdges = getIncomingControlEdges(graph, nodeId);

  // No incoming control edges means always reachable
  if (incomingControlEdges.length === 0) {
    return true;
  }

  // Check if ALL incoming control edges are deactivated
  const allDeactivated = incomingControlEdges.every(edge => state.deactivatedEdges.has(edge.id));
  if (allDeactivated) {
    return false;
  }

  // Check if AT LEAST ONE incoming control edge is activated
  const atLeastOneActivated = incomingControlEdges.some(edge => state.activatedEdges.has(edge.id));
  if (atLeastOneActivated) {
    return true;
  }

  // Control edges exist but none are activated or deactivated yet
  // Return true (waiting for scheduler to handle ordering)
  return true;
}

/**
 * Checks if a node should be skipped based on control path activation
 */
export function shouldSkipNode(nodeId: NodeId, graph: PipelineGraph, state: ExecutionState): boolean {
  return !isControlPathActive(nodeId, graph, state);
}

/**
 * Activates a control edge in the execution state
 */
export function activateEdge(state: ExecutionState, edgeId: EdgeId): void {
  state.activatedEdges.add(edgeId);
}

/**
 * Deactivates a control edge in the execution state
 */
export function deactivateEdge(state: ExecutionState, edgeId: EdgeId): void {
  state.deactivatedEdges.add(edgeId);
}

/**
 * Gets all nodes on inactive branches from a conditional node:
 * - Find all deactivated control edges from the conditional node
 * - BFS from those edges' target nodes
 * - Stop BFS at MergeNode (it's shared by both branches)
 * - Return all node ids found (excluding MergeNode itself)
 */
export function getNodesOnInactiveBranches(
  graph: PipelineGraph,
  conditionalNodeId: NodeId,
  state: ExecutionState
): NodeId[] {
  const inactiveNodes = new Set<NodeId>();
  const queue: NodeId[] = [];

  // Find all deactivated control edges from this conditional node
  const outgoingControlEdges = getOutgoingControlEdges(graph, conditionalNodeId);
  for (const edge of outgoingControlEdges) {
    if (state.deactivatedEdges.has(edge.id)) {
      queue.push(edge.to);
    }
  }

  // BFS from deactivated edge targets
  while (queue.length > 0) {
    const nodeId = queue.shift()!;
    if (inactiveNodes.has(nodeId)) {
      continue;
    }

    const node = graph.nodes.get(nodeId);
    if (!node) {
      continue;
    }

    // Stop at MergeNode (it's shared by both branches)
    if (node.kind === 'merge') {
      continue;
    }

    inactiveNodes.add(nodeId);

    // Add all downstream nodes
    const outgoingEdges = Array.from(graph.edges.values()).filter(e => e.from === nodeId);
    for (const edge of outgoingEdges) {
      queue.push(edge.to);
    }
  }

  return Array.from(inactiveNodes);
}

/**
 * Checks if a node is ready to execute:
 * - All incoming data edge sources have status 'completed'
 * - Node itself has status 'pending'
 * - At least one incoming control edge is 'activated' OR no control edges
 */
export function isReadyToExecute(nodeId: NodeId, state: ExecutionState): boolean {
  const nodeState = state.nodeStates.get(nodeId);
  if (!nodeState || nodeState.status !== 'pending') {
    return false;
  }

  // Check all incoming data edges have completed sources
  const incomingDataEdges = getIncomingDataEdges(state.graph, nodeId);
  for (const edge of incomingDataEdges) {
    const sourceState = state.nodeStates.get(edge.from);
    if (!sourceState || sourceState.status !== 'completed') {
      return false;
    }
  }

  // Check control path is active
  if (!isControlPathActive(nodeId, state.graph, state)) {
    return false;
  }

  return true;
}

/**
 * Gets inputs for a node from the execution state:
 * - For each incoming data edge, get source node's output
 * - Key = edge.inputKey ?? 'input'
 * - Value = source node's output (possibly sliced by edge.outputKey)
 */
/**
 * Wraps a legacy Value in DataValue if not already wrapped.
 * Compatibility shim for existing nodes that return Value instead of DataValue.
 */
function wrapLegacyValue(v: unknown): DataValue {
  // Already DataValue
  if (v && typeof v === 'object' && 'kind' in v) {
    const kind = (v as any).kind;
    if (['tabular', 'record', 'scalar', 'collection', 'void'].includes(kind)) {
      return v as DataValue;
    }
  }

  // RowSet (has rows and schema)
  if (v && typeof v === 'object' && !Array.isArray(v) && 'rows' in v && 'schema' in v) {
    const rowSet = v as RowSet;
    return tabular(rowSet, rowSet.schema);
  }

  // Scalar
  if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null) {
    return scalar(v, { kind: v === null ? 'null' : typeof v as 'string' | 'number' | 'boolean' });
  }

  // Array → collection
  if (Array.isArray(v)) {
    return collection(v.map(wrapLegacyValue), 'any' as DataValueKind);
  }

  // Plain Record → record
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    // Infer simple schema from object keys
    const schema = {
      columns: Object.keys(v).map(name => ({
        name,
        type: { kind: 'any' as const } as import('../core/types/engine-type.js').EngineType,
        nullable: true
      }))
    };
    return record(v as import('../core/types/value.js').Row, schema);
  }

  // undefined/null → void
  return void_;
}


export function getNodeInputs(
  nodeId: NodeId,
  graph: PipelineGraph,
  state: ExecutionState
): Record<string, DataValue> {
  const inputs: Record<string, DataValue> = {};
  const incomingDataEdges = getIncomingDataEdges(graph, nodeId);

  for (const edge of incomingDataEdges) {
    const sourceState = state.nodeStates.get(edge.from);
    if (!sourceState || sourceState.output === undefined) {
      continue;
    }

    const inputKey = edge.inputKey || 'input';
    let value = sourceState.output;

    // Slice by outputKey if specified
    if (edge.outputKey && value.kind === 'record') {
      const rowValue = value.data;
      if (edge.outputKey in rowValue) {
        const extracted = rowValue[edge.outputKey];
        value = wrapLegacyValue(extracted);
      }
    }

    inputs[inputKey] = value;
  }

  return inputs;
}

/**
 * Validates a pipeline graph:
 * - Cycle detection (topologicalSort catches this)
 * - Every node kind exists in NodeRegistry (deferred to caller - need registry)
 * - Every data edge references valid node ids
 * - Every control edge references valid node ids
 * - Entry node exists and has no incoming data edges
 * - Exit nodes exist and have no outgoing data edges
 * - No orphaned nodes (every non-entry node has at least one incoming edge)
 */
export function validateGraph(graph: PipelineGraph, nodeRegistry?: any): ValidationResult {
  const errors: ValidationError[] = [];

  // Cycle detection
  try {
    topologicalSort(graph);
  } catch (e) {
    if (e instanceof CycleDetectedError) {
      errors.push({
        code: 'CYCLE_DETECTED',
        message: `Cycle detected in graph: ${e.cycle.join(' -> ')}`
      });
    } else {
      errors.push({
        code: 'CYCLE_DETECTION_FAILED',
        message: `Failed to detect cycles: ${(e as Error).message}`
      });
    }
  }

  // Check entry node exists
  if (!graph.nodes.has(graph.entryNode)) {
    errors.push({
      code: 'ENTRY_NODE_MISSING',
      message: `Entry node '${graph.entryNode}' does not exist in graph`
    });
  }

  // Check entry node has no incoming data edges
  const entryIncomingData = getIncomingDataEdges(graph, graph.entryNode);
  if (entryIncomingData.length > 0) {
    errors.push({
      code: 'ENTRY_NODE_HAS_INPUTS',
      message: `Entry node '${graph.entryNode}' should have no incoming data edges`,
      nodeId: graph.entryNode
    });
  }

  // Check exit nodes exist
  for (const exitNodeId of graph.exitNodes) {
    if (!graph.nodes.has(exitNodeId)) {
      errors.push({
        code: 'EXIT_NODE_MISSING',
        message: `Exit node '${exitNodeId}' does not exist in graph`
      });
    }
  }

  // Special check for _output node: it should have no outgoing data edges
  const outputNode = graph.nodes.get('_output');
  if (outputNode) {
    const outputOutgoingData = getOutgoingDataEdges(graph, '_output');
    if (outputOutgoingData.length > 0) {
      errors.push({
        code: 'EXIT_NODE_HAS_OUTPUTS',
        message: `Exit node '_output' should have no outgoing data edges`,
        nodeId: '_output'
      });
    }
  }

  // Check all edge references valid node ids
  for (const [edgeId, edge] of graph.edges) {
    if (!graph.nodes.has(edge.from)) {
      errors.push({
        code: 'EDGE_SOURCE_INVALID',
        message: `Edge '${edgeId}' references non-existent source node '${edge.from}'`
      });
    }

    if (!graph.nodes.has(edge.to)) {
      errors.push({
        code: 'EDGE_TARGET_INVALID',
        message: `Edge '${edgeId}' references non-existent target node '${edge.to}'`
      });
    }
  }

  // Check for orphaned nodes (every non-entry node has at least one incoming edge)
  for (const [nodeId, node] of graph.nodes) {
    if (nodeId === graph.entryNode) {
      continue; // Entry node can have no incoming edges
    }

    const incomingEdges = Array.from(graph.edges.values()).filter(e => e.to === nodeId);
    if (incomingEdges.length === 0) {
      errors.push({
        code: 'ORPHANED_NODE',
        message: `Node '${nodeId}' has no incoming edges (orphaned)`,
        nodeId
      });
    }
  }

  // Check node kinds exist in registry (if provided)
  if (nodeRegistry) {
    for (const [nodeId, node] of graph.nodes) {
      if (!nodeRegistry.has(node.kind)) {
        errors.push({
          code: 'UNKNOWN_NODE_KIND',
          message: `Node '${nodeId}' has unknown kind '${node.kind}'`,
          nodeId
        });
      }
    }
  }

  if (errors.length > 0) {
    return validationFail(errors);
  }

  return validationOk();
}
