// Pipeline Scheduler
// Manages execution of pipeline graphs with parallel branch support

import type { SchedulerConfig, ExecutionState, NodeExecutionState, SchedulerEvent, ExecutionResult } from './types.js';
import type { PipelineGraph, NodeId, PipelineNode } from '../core/graph/index.js';
import type { ExecutionContext } from '../core/context/execution-context.js';
import type { DataValue, DataValueKind } from '../core/types/data-value.js';
import { tabular, record, scalar, collection, void_, isCollection, isVoid } from '../core/types/data-value.js';
import type { RowSet, Row } from '../core/types/value.js';
import type { NodeDefinition } from '../core/registry/node-registry.js';
import { validateGraph, topologicalSort, getNodeInputs, getOutgoingDataEdges, getOutgoingControlEdges, getIncomingDataEdges, isControlPathActive, shouldSkipNode, activateEdge, deactivateEdge, getNodesOnInactiveBranches } from './graph-utils.js';
import { forkStateForIteration, toIterableArray, updateAccumulator, initAccumulator, getSubgraphOutput } from './loop-helpers.js';
import { validationOk, validationFail, ValidationError } from '../core/types/validation.js';
import { isBudgetExceeded, budgetRemaining } from '../core/context/execution-budget.js';
import { contextPushScope } from '../core/context/execution-context.js';
import { scopeSet } from '../core/scope/scope.js';

export class SchedulerValidationError extends Error {
  errors: ValidationError[];

  constructor(errors: ValidationError[]) {
    super(`Graph validation failed: ${errors.map(e => e.message).join('; ')}`);
    this.name = 'SchedulerValidationError';
    this.errors = errors;
  }
}

export class BudgetExceededError extends Error {
  limit: string;

  constructor(limit: string) {
    super(`Budget exceeded: ${limit}`);
    this.name = 'BudgetExceededError';
    this.limit = limit;
  }
}

export class NodeTimeoutError extends Error {
  nodeId: NodeId;
  timeoutMs: number;

  constructor(nodeId: NodeId, timeoutMs: number) {
    super(`Node ${nodeId} timed out after ${timeoutMs}ms`);
    this.name = 'NodeTimeoutError';
    this.nodeId = nodeId;
    this.timeoutMs = timeoutMs;
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

function markNodeRunning(state: ExecutionState, nodeId: NodeId): void {
  const nodeState = state.nodeStates.get(nodeId);
  if (nodeState) {
    nodeState.status = 'running';
    nodeState.startedAt = Date.now();
  }
}

function markNodeCompleted(state: ExecutionState, nodeId: NodeId, output: DataValue): void {
  const nodeState = state.nodeStates.get(nodeId);
  if (nodeState) {
    nodeState.status = 'completed';
    nodeState.completedAt = Date.now();
    nodeState.output = output;
  }
}

function markNodeFailed(state: ExecutionState, nodeId: NodeId, error: Error): void {
  const nodeState = state.nodeStates.get(nodeId);
  if (nodeState) {
    nodeState.status = 'failed';
    nodeState.completedAt = Date.now();
    nodeState.error = error;
  }
}

function markNodeSkipped(state: ExecutionState, nodeId: NodeId, reason: string): void {
  const nodeState = state.nodeStates.get(nodeId);
  if (nodeState) {
    nodeState.status = 'skipped';
    nodeState.completedAt = Date.now();
  }
}

/**
 * Wraps a legacy Value in DataValue if not already wrapped.
 * Compatibility shim for nodes that return Value instead of DataValue.
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

async function executeWithRetry(
  node: PipelineNode,
  def: NodeDefinition<any, any, any>,
  inputs: any,
  ctx: ExecutionContext,
  config: SchedulerConfig,
  state: ExecutionState
): Promise<DataValue> {
  const maxRetries = node.errorPolicy.maxRetries || 0;
  const retryDelayMs = node.errorPolicy.retryDelayMs || 1000;
  let lastError: Error | undefined;
  const nodeState = state.nodeStates.get(node.id);

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const result = await def.execute(node.payload, inputs, ctx);
      // Wrap legacy Value outputs in DataValue for backward compatibility
      return wrapLegacyValue(result);
    } catch (error) {
      lastError = error as Error;
      // Increment retry count
      if (nodeState && attempt < maxRetries) {
        nodeState.retryCount = attempt + 1;
      }
      if (attempt < maxRetries) {
        // Exponential backoff
        const delay = retryDelayMs * Math.pow(2, attempt);
        await sleep(delay);
      }
    }
  }

  throw lastError || new Error('Execution failed after retries');
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function checkBudget(ctx: ExecutionContext, nodeKind: string): void {
  // Check overall budget
  if (isBudgetExceeded(ctx.budget)) {
    const remaining = budgetRemaining(ctx.budget);
    if (remaining.timeMs <= 0) {
      throw new BudgetExceededError('Pipeline timeout exceeded');
    }
    if (remaining.llmCalls <= 0) {
      throw new BudgetExceededError('LLM call budget exhausted');
    }
    if (remaining.iterations <= 0) {
      throw new BudgetExceededError('Iteration budget exhausted');
    }
  }

  // Specific LLM check
  if (nodeKind === 'llm') {
    if (ctx.budget.llmCallsUsed >= ctx.budget.maxLLMCalls) {
      throw new BudgetExceededError('LLM call budget exhausted');
    }
  }

  // Timeout check
  const elapsed = Date.now() - ctx.budget.startedAt;
  if (elapsed >= ctx.budget.timeoutMs) {
    throw new BudgetExceededError('Pipeline timeout exceeded');
  }
}

function incrementLLMBudget(ctx: ExecutionContext): void {
  ctx.budget.llmCallsUsed++;
}

function incrementIterationBudget(ctx: ExecutionContext): void {
  ctx.budget.iterationsUsed++;
}

async function executeWithTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  nodeId: NodeId
): Promise<T> {
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => {
      reject(new NodeTimeoutError(nodeId, timeoutMs));
    }, timeoutMs);
  });

  return Promise.race([promise, timeoutPromise]);
}

// ============================================================================
// Scheduler Class
// ============================================================================

export class Scheduler {
  private eventHandlers: ((event: SchedulerEvent) => void)[] = [];
  private topoCache = new WeakMap<PipelineGraph, NodeId[]>();

  constructor(private config: SchedulerConfig) {}

  on(handler: (event: SchedulerEvent) => void): void {
    this.eventHandlers.push(handler);
  }

  private emit(event: SchedulerEvent): void {
    for (const handler of this.eventHandlers) {
      try {
        handler(event);
      } catch (error) {
        console.error('Error in event handler:', error);
      }
    }
  }

  private getTopoOrder(graph: PipelineGraph): NodeId[] {
    if (this.topoCache.has(graph)) return this.topoCache.get(graph)!;
    const order = topologicalSort(graph);
    this.topoCache.set(graph, order);
    return order;
  }

  async execute(
    graph: PipelineGraph,
    ctx: ExecutionContext
  ): Promise<ExecutionResult> {
    // 1. Validate graph
    const validation = validateGraph(graph, this.config.nodeRegistry);
    if (!validation.ok) {
      throw new SchedulerValidationError(validation.errors);
    }

    // 2. Initialize ExecutionState
    const nodeStates = new Map<NodeId, NodeExecutionState>();
    for (const nodeId of graph.nodes.keys()) {
      nodeStates.set(nodeId, {
        nodeId,
        status: 'pending',
        retryCount: 0
      });
    }

    const state: ExecutionState = {
      graph,
      nodeStates,
      ctx,
      activatedEdges: new Set(),
      deactivatedEdges: new Set()
    };

    // 3. Get execution order
    const order = this.getTopoOrder(graph);

    // 4. Execute nodes in order
    for (const nodeId of order) {
      const nodeState = state.nodeStates.get(nodeId);
      
      // Skip if already completed or skipped (e.g., by parallel execution)
      if (nodeState?.status === 'completed' || nodeState?.status === 'skipped') {
        continue;
      }

      // Skip if control path is inactive
      if (shouldSkipNode(nodeId, state.graph, state)) {
        markNodeSkipped(state, nodeId, 'Control path inactive');
        this.emit({
          kind: 'node_skip',
          nodeId,
          reason: 'Control path inactive',
          timestamp: Date.now()
        });
        continue;
      }

      // Check if ready to execute
      // (should always be true with correct topo sort, but guard anyway)
      const incomingDataEdges = getNodeInputs(nodeId, graph, state);
      if (!this.isReadyToExecute(nodeId, state)) {
        markNodeSkipped(state, nodeId, 'Dependencies not met');
        this.emit({
          kind: 'node_skip',
          nodeId,
          reason: 'Dependencies not met',
          timestamp: Date.now()
        });
        continue;
      }

      await this.executeNode(nodeId, graph, state);
    }

    // 5. Collect outputs
    const outputs = new Map<string, DataValue>();
    const outputNode = graph.nodes.get('_output');
    const outputNodeExecuted = outputNode && state.nodeStates.get('_output')?.status === 'completed';

    // Check if _output node has actual data
    let outputNodeHasData = false;
    if (outputNodeExecuted) {
      const outputState = state.nodeStates.get('_output');
      outputNodeHasData = outputState?.output !== undefined && 
                         outputState.output.kind !== 'void';
    }

    // If _output node was executed and has data, collect its output
    if (outputNodeExecuted && outputNodeHasData) {
      const outputState = state.nodeStates.get('_output');
      if (outputState?.output !== undefined) {
        outputs.set('_output', outputState.output);
      }
    } else {
      // Otherwise, collect from all exit nodes (including _output if it exists but has no data)
      for (const exitNodeId of graph.exitNodes) {
        const exitState = state.nodeStates.get(exitNodeId);
                if (exitState?.output !== undefined) {
          outputs.set(exitNodeId, exitState.output);
        }
      }
      
      // Special case: if _output node exists but wasn't executed, collect from its inputs
      if (outputNode && outputNode.kind === 'output') {
        const incomingEdges = getIncomingDataEdges(graph, '_output');
        for (const edge of incomingEdges) {
          const sourceState = state.nodeStates.get(edge.from);
          if (sourceState?.output !== undefined) {
            // Use the edge's outputKey if specified, otherwise use 'result'
            const outputKey = (outputNode.payload as any)?.outputKey ?? 'result';
            outputs.set(outputKey, sourceState.output);
            break; // Only take the first input
          }
        }
      }
    }

    // 6. Return ExecutionResult
    const failedNodes = Array.from(state.nodeStates.values()).filter(s => s.status === 'failed');
    const status = failedNodes.length === 0 ? 'success' : 
                   failedNodes.length < state.nodeStates.size ? 'partial' : 'failed';

    const result: ExecutionResult = {
      status,
      outputs,
      trace: ctx.trace,
      budgetUsed: ctx.budget,
      nodeStates: state.nodeStates,
      error: failedNodes.length > 0 ? failedNodes[0].error : undefined
    };

    this.emit({
      kind: 'pipeline_complete',
      outputs,
      timestamp: Date.now()
    });

    return result;
  }

  private isReadyToExecute(nodeId: NodeId, state: ExecutionState): boolean {
    const nodeState = state.nodeStates.get(nodeId);
    if (!nodeState || nodeState.status !== 'pending') {
      return false;
    }

    // Check all incoming data edges have completed or skipped sources
    const inputs = getNodeInputs(nodeId, state.graph, state);
    // getNodeInputs only returns inputs from completed sources
    // So if we get all expected inputs, we're ready
    const incomingEdges = Array.from(state.graph.edges.values())
      .filter(e => e.to === nodeId && e.kind === 'data');
    
    for (const edge of incomingEdges) {
      const sourceState = state.nodeStates.get(edge.from);
      if (!sourceState || (sourceState.status !== 'completed' && sourceState.status !== 'skipped')) {
        return false;
      }
    }

    // Check control path is active
    if (!isControlPathActive(nodeId, state.graph, state)) {
      return false;
    }

    return true;
  }

  private async executeNode(
    nodeId: NodeId,
    graph: PipelineGraph,
    state: ExecutionState
  ): Promise<void> {
    const node = graph.nodes.get(nodeId);
    if (!node) {
      throw new Error(`Node ${nodeId} not found in graph`);
    }

    // Check budget before dispatching
    checkBudget(state.ctx, node.kind);

    const def = this.config.nodeRegistry.get(node.kind);

    this.emit({
      kind: 'node_start',
      nodeId,
      timestamp: Date.now()
    });

    // Special handling for known scheduler-owned node kinds
    if (node.kind === 'parallel') {
      await this.executeParallel(nodeId, graph, state);
      return;
    }

    if (node.kind === 'conditional') {
      await this.executeConditional(nodeId, graph, state);
      return;
    }

    if (node.kind === 'loop') {
      await this.executeLoop(nodeId, graph, state);
      return;
    }

    if (node.kind === 'merge') {
      await this.executeMerge(nodeId, graph, state);
      return;
    }

    // Special case: QueryNode uses QueryExecutor directly
    if (node.kind === 'query') {
      const inputs = getNodeInputs(nodeId, graph, state);
      const payload = node.payload as any;
      
      // Collect additional fields from previous transform steps
      const additionalFields = this.collectAdditionalFields(nodeId, graph, state);
      
      // Create execution context
      const ctx: ExecutionContext = {
        executionId: state.ctx.executionId,
        pipelineId: state.ctx.pipelineId || '',
        sessionId: state.ctx.sessionId || '',
        trace: state.ctx.trace,
        budget: state.ctx.budget,
        scope: { id: 'query', kind: 'global', bindings: new Map(), parent: null },
        nodeOutputs: new Map(),
        params: {},
      };
      
      // QueryNode doesn't use input, but we pass it for consistency
      const queryResult = await executeWithTimeout(
        this.config.queryExecutor.execute(payload.intent, state.ctx, additionalFields),
        30000, // 30 second timeout
        nodeId
      );
      
      // Wrap QueryResult as DataValue
      const wrappedResult: DataValue = wrapLegacyValue(queryResult);
      
      markNodeCompleted(state, nodeId, wrappedResult);
      this.emit({
        kind: 'node_complete',
        nodeId,
        output: wrappedResult,
        timestamp: Date.now()
      });
      return;
    }

    // Standard node execution
    try {
      const inputs = getNodeInputs(nodeId, graph, state);
      
      // Debug: Log node execution start
      console.log(`[Scheduler] Executing node ${nodeId} (${node.kind}) with inputs:`, {
        inputKeys: Object.keys(inputs),
        inputSample: inputs['input'] ? 
          (inputs['input'].kind === 'tabular' ? 
            `${inputs['input'].data.rows.length} rows` : 
            inputs['input'].kind) : 
          'none'
      });
      
      markNodeRunning(state, nodeId);

      // Calculate timeout for this node
      const remaining = budgetRemaining(state.ctx.budget);
      const timeoutMs = Math.min(remaining.timeMs, 30000); // Cap at 30s per node

      // Determine actual input to pass to node
      // If node has single input port named 'input', pass that value directly
      // Otherwise, pass the Record
      let actualInput: any;
      if (def.inputPorts.length === 1 && def.inputPorts[0].key === 'input') {
        actualInput = inputs['input'];
      } else {
        actualInput = inputs;
      }

      // Debug: For write nodes, log payload details
      if (node.kind === 'write') {
        console.log(`[Scheduler] WriteNode ${nodeId} payload:`, JSON.stringify(node.payload, null, 2));
        console.log(`[Scheduler] WriteNode ${nodeId} input type:`, actualInput?.kind);
        if (actualInput?.kind === 'tabular') {
          console.log(`[Scheduler] WriteNode ${nodeId} input rows:`, actualInput.data.rows.length);
        }
      }

      const output = await executeWithTimeout(
        executeWithRetry(node, def, actualInput, state.ctx, this.config, state),
        timeoutMs,
        nodeId
      );

      // Debug: Log successful completion
      console.log(`[Scheduler] Node ${nodeId} completed successfully, output type:`, output?.kind);

      // Increment LLM budget after successful LLM execution
      if (node.kind === 'llm') {
        incrementLLMBudget(state.ctx);
      }

      markNodeCompleted(state, nodeId, output);
      this.emit({
        kind: 'node_complete',
        nodeId,
        output,
        timestamp: Date.now()
      });

    } catch (error) {
      console.error(`[Scheduler] Node ${nodeId} failed with error:`, (error as Error).message);
      console.error(`[Scheduler] Node ${nodeId} error stack:`, (error as Error).stack);
      await this.handleNodeError(node, error as Error, graph, state);
    }
  }

  private async executeParallel(
    nodeId: NodeId,
    graph: PipelineGraph,
    state: ExecutionState
  ): Promise<void> {
    const node = graph.nodes.get(nodeId);
    if (!node) {
      throw new Error(`Node ${nodeId} not found`);
    }

    const payload = node.payload as any;
    const branches = getOutgoingDataEdges(graph, nodeId);

    this.emit({
      kind: 'branch_fork',
      parentNodeId: nodeId,
      branchNodeIds: branches.map(b => b.to),
      timestamp: Date.now()
    });

    // Get input to pass to all branches
    const input = getNodeInputs(nodeId, graph, state)['input'];

    // Execute branches in parallel with concurrency cap
    const cap = Math.min(payload.maxConcurrency, this.config.maxParallelBranches);

    // Process in batches of cap
    for (let i = 0; i < branches.length; i += cap) {
      const batch = branches.slice(i, i + cap);
      await Promise.all(batch.map(edge =>
        this.executeSubgraph(edge.to, graph, state, input)
      ));
    }

    markNodeCompleted(state, nodeId, input);
  }

  private async executeSubgraph(
    startNodeId: NodeId,
    graph: PipelineGraph,
    state: ExecutionState,
    input?: DataValue
  ): Promise<void> {
    // Set input for start node if provided
    if (input !== undefined) {
      const startNodeState = state.nodeStates.get(startNodeId);
      if (startNodeState) {
        startNodeState.output = input;
        startNodeState.status = 'completed';
        startNodeState.completedAt = Date.now();
      }
    }

    // Get execution order
    const order = this.getTopoOrder(graph);

    // Filter to reachable nodes that are not yet completed
    const reachableNodes = this.getReachableNodes(startNodeId, graph);
    const nodesToExecute = order.filter(id => 
      reachableNodes.has(id) && state.nodeStates.get(id)?.status === 'pending'
    );

    // Execute each reachable node
    for (const nodeId of nodesToExecute) {
      // Re-check status — may have changed since filter (e.g. marked skipped by conditional)
      const nodeState = state.nodeStates.get(nodeId);
      if (nodeState?.status !== 'pending') {
        continue;
      }
      
      // Re-check control path — may have been deactivated by conditional
      if (shouldSkipNode(nodeId, graph, state)) {
        markNodeSkipped(state, nodeId, 'Control path inactive');
        this.emit({
          kind: 'node_skip',
          nodeId,
          reason: 'Control path inactive',
          timestamp: Date.now()
        });
        continue;
      }
      
      await this.executeNode(nodeId, graph, state);
    }
  }

  private getReachableNodes(startNodeId: NodeId, graph: PipelineGraph): Set<NodeId> {
    const reachable = new Set<NodeId>();
    const queue: NodeId[] = [startNodeId];

    while (queue.length > 0) {
      const nodeId = queue.shift()!;
      if (reachable.has(nodeId)) {
        continue;
      }
      reachable.add(nodeId);

      // Add all downstream nodes via data edges only
      // Control edges determine branching, not reachability
      const outgoingEdges = Array.from(graph.edges.values())
        .filter(e => e.from === nodeId && e.kind === 'data');
      for (const edge of outgoingEdges) {
        queue.push(edge.to);
      }
    }

    return reachable;
  }

  private getBodyNodeIds(loopNodeId: NodeId, graph: PipelineGraph): NodeId[] {
    const bodyNodeIds: NodeId[] = [];
    const visited = new Set<NodeId>();
    const queue: NodeId[] = [];

    // Start from loop's outgoing data edge targets
    const startEdges = getOutgoingDataEdges(graph, loopNodeId);
    for (const edge of startEdges) {
      queue.push(edge.to);
    }

    while (queue.length > 0) {
      const nodeId = queue.shift()!;
      if (visited.has(nodeId)) continue;
      visited.add(nodeId);
      bodyNodeIds.push(nodeId);

      // Follow outgoing data edges BUT stop if the target node
      // has an incoming data edge from OUTSIDE the body
      // (i.e., from a node not in visited and not the loopNodeId)
      const outEdges = getOutgoingDataEdges(graph, nodeId);
      for (const edge of outEdges) {
        const targetIncomingEdges = getIncomingDataEdges(graph, edge.to);
        const hasExternalIncoming = targetIncomingEdges.some(
          e => e.from !== loopNodeId && !visited.has(e.from) && !queue.includes(e.from)
        );
        // If target has external incoming edges, it's outside the body (e.g. MergeNode after loop)
        if (!hasExternalIncoming) {
          queue.push(edge.to);
        }
      }
    }

    return bodyNodeIds;
  }

  private async executeConditional(
    nodeId: NodeId,
    graph: PipelineGraph,
    state: ExecutionState
  ): Promise<void> {
    const node = graph.nodes.get(nodeId);
    if (!node) {
      throw new Error(`Node ${nodeId} not found`);
    }

    // 1. Get inputs for this node
    const inputs = getNodeInputs(nodeId, graph, state);
    const input = inputs['input'];

    
    // Extract current row context for predicate evaluation
    // Input is now a DataValue - extract the row data
    let currentRow: Row | undefined = undefined;
    if (input && typeof input === 'object' && 'kind' in input) {
      const dv = input as DataValue;
      if (dv.kind === 'tabular') {
        // Use first row as predicate context
        currentRow = dv.data.rows.length > 0 ? dv.data.rows[0] : undefined;
      } else if (dv.kind === 'record') {
        // Use record data directly
        currentRow = dv.data;
      } else if (dv.kind === 'scalar') {
        // Wrap scalar in a row
        currentRow = { value: dv.data };
      }
    } else if (input && typeof input === 'object' && !Array.isArray(input)) {
      // Legacy: plain Row object
      currentRow = input as Row;
    }

    // 2. Get the node definition and execute predicate with row context
    const payload = node.payload as any;
    const result = this.config.evaluator.evaluate(payload.predicate, state.ctx.scope, currentRow);
    
    // Debug: Log conditional evaluation
    console.log(`Conditional ${nodeId}: predicate=${JSON.stringify(payload.predicate)} result=${result} for row=${JSON.stringify(currentRow)?.slice(0,50)}`);
    
    if (typeof result !== 'boolean') {
      throw new TypeError(`ConditionalNode predicate must return boolean, got ${typeof result}`);
    }

    // 3. Get outgoing control edges and activate/deactivate based on result
    const controlEdges = getOutgoingControlEdges(graph, nodeId);
    
    for (const edge of controlEdges) {
      if (edge.condition === 'always') {
        activateEdge(state, edge.id);
      } else if (result === true && edge.condition === 'true') {
        activateEdge(state, edge.id);
      } else if (result === false && edge.condition === 'false') {
        activateEdge(state, edge.id);
      } else if (edge.condition === 'error') {
        deactivateEdge(state, edge.id); // error edges only fire on exceptions
      } else {
        deactivateEdge(state, edge.id); // wrong branch
      }
    }

    // 4. Pass input through on outgoing data edges (both branches receive same data)
    markNodeCompleted(state, nodeId, input);
    this.emit({
      kind: 'node_complete',
      nodeId,
      output: input,
      timestamp: Date.now()
    });

    // 5. Mark nodes on inactive branches as skipped
    const inactiveBranchNodes = getNodesOnInactiveBranches(graph, nodeId, state);
    for (const skippedNodeId of inactiveBranchNodes) {
      const skippedNodeState = state.nodeStates.get(skippedNodeId);
      if (skippedNodeState?.status === 'pending') {
        markNodeSkipped(state, skippedNodeId, 'inactive conditional branch');
        this.emit({
          kind: 'node_skip',
          nodeId: skippedNodeId,
          reason: 'inactive conditional branch',
          timestamp: Date.now()
        });
      }
    }
  }

  private async executeLoop(
    nodeId: NodeId,
    graph: PipelineGraph,
    state: ExecutionState
  ): Promise<void> {
    const node = graph.nodes.get(nodeId);
    if (!node) {
      throw new Error(`Node ${nodeId} not found`);
    }

    const payload = node.payload as any;

    // Get loop input and bind it in scope for payload.over resolution
    const inputs = getNodeInputs(nodeId, graph, state);
    const loopInput = inputs['input'] ?? inputs[Object.keys(inputs)[0]] ?? null;
    scopeSet(state.ctx.scope, 'input', loopInput);

    // Enforce maxIterations hard cap
    const maxIter = Math.min(payload.maxIterations, state.ctx.budget.maxIterations);

    // Get the loop body: outgoing data edge target(s)
    const bodyEdges = getOutgoingDataEdges(graph, nodeId);
    if (bodyEdges.length === 0) {
      throw new Error('LoopNode has no body (no outgoing data edges)');
    }
    const bodyStartNodeId = bodyEdges[0].to;

    // Get all body node IDs (nodes in the loop body)
    // Stop BFS when reaching nodes with external incoming edges (e.g. MergeNode after loop)
    const bodyNodeIds = this.getBodyNodeIds(nodeId, graph);

    // Initialize accumulator as DataValue
    let accumulated: DataValue = wrapLegacyValue(initAccumulator(payload.accumulator));
    let iterCount = 0;

    if (payload.mode === 'forEach') {
      // Resolve the iterable
      const iterable = this.config.evaluator.evaluate(payload.over, state.ctx.scope);
      const items = toIterableArray(wrapLegacyValue(iterable));
      
      console.log(`[Loop] forEach mode: ${items.length} items to iterate`);
      if (items.length === 0) {
        console.log(`[Loop] No items to iterate, loop will not execute body`);
      }

      for (const item of items) {
        if (iterCount >= maxIter) {
          // Emit warning trace event
          console.warn(`Loop hit maxIterations cap (${maxIter})`);
          break;
        }

        try {
          // Fork scope for this iteration
          const iterCtx = contextPushScope(state.ctx, 'loop');
          scopeSet(iterCtx.scope, payload.iterVar, item);
          if (payload.indexVar) {
            scopeSet(iterCtx.scope, payload.indexVar, iterCount);
          }

          // Execute body subgraph with forked context
          // Pass current item (wrapped as RowSet) for data edge
          const iterState = forkStateForIteration(state, iterCtx, bodyNodeIds, nodeId, item);
          await this.executeSubgraph(bodyStartNodeId, graph, iterState);

          // Collect body output
          const bodyOutput = getSubgraphOutput(bodyNodeIds, graph, iterState);

          // Debug: Trace what bodyOutput is for each iteration
          console.log(`iter ${iterCount}: bodyOutput=${JSON.stringify(bodyOutput)?.slice(0,80)} accumulated.length=${isCollection(accumulated) ? (accumulated as any).data.length : 'N/A'}`);

          // Update accumulator (always accumulate even if bodyOutput is undefined)
          accumulated = updateAccumulator(payload.accumulator, accumulated, bodyOutput ?? void_, iterCtx, this.config.evaluator);

          incrementIterationBudget(state.ctx);
          iterCount++;
        } catch (err) {
          console.error(`iter ${iterCount} error:`, (err as Error).message);
          // For now: rethrow. Later: respect errorPolicy.
          throw err;
        }
      }
    } else if (payload.mode === 'while') {
      // Get loop input for while mode (processes full input each iteration)
      const inputs = getNodeInputs(nodeId, graph, state);
      const loopInput = inputs['input'] ?? void_;
      
      while (true) {
        if (iterCount >= maxIter) {
          // Emit warning trace event
          console.warn(`Loop hit maxIterations cap (${maxIter})`);
          break;
        }

        // Bind iterVar in outer scope so condition can see it
        // This accumulates iterVar bindings across iterations (acceptable for while mode)
        if (payload.iterVar) {
          scopeSet(state.ctx.scope, payload.iterVar, iterCount);
        }
        if (payload.indexVar) {
          scopeSet(state.ctx.scope, payload.indexVar, iterCount);
        }

        // Evaluate condition using outer scope (which now has iterVar bound)
        const condResult = this.config.evaluator.evaluate(payload.condition, state.ctx.scope);
        if (typeof condResult !== 'boolean') {
          throw new TypeError('while condition must return boolean');
        }
        if (!condResult) break;

        // Fork scope for body execution (inherits from outer scope which has iterVar)
        const iterCtx = contextPushScope(state.ctx, 'loop');
        // Re-bind iterVar in iterCtx for body nodes that explicitly reference it
        if (payload.iterVar) {
          scopeSet(iterCtx.scope, payload.iterVar, iterCount);
        }

        // While mode processes the full input each iteration
        const iterState = forkStateForIteration(state, iterCtx, bodyNodeIds, nodeId, loopInput);
        await this.executeSubgraph(bodyStartNodeId, graph, iterState);

        const bodyOutput = getSubgraphOutput(bodyNodeIds, graph, iterState);
        // Update accumulator (always accumulate even if bodyOutput is undefined)
        accumulated = updateAccumulator(payload.accumulator, accumulated, bodyOutput ?? void_, iterCtx, this.config.evaluator);

        incrementIterationBudget(state.ctx);
        iterCount++;
      }
    }

    // Mark all body nodes as completed in the outer state to prevent re-execution
    // After the loop finishes, the scheduler's top-level execute will try to execute
    // nodes in topological order. Body nodes should not be executed again.
    for (const bodyNodeId of bodyNodeIds) {
      state.nodeStates.set(bodyNodeId, {
        nodeId: bodyNodeId,
        status: 'completed',
        output: undefined,
        retryCount: 0,
        completedAt: Date.now()
      });
    }

    markNodeCompleted(state, nodeId, accumulated);
    this.emit({
      kind: 'node_complete',
      nodeId,
      output: accumulated,
      timestamp: Date.now()
    });
  }

  private collectAdditionalFields(nodeId: NodeId, graph: PipelineGraph, state: ExecutionState): Map<string, { name: string; type: any }[]> {
    const additionalFields = new Map<string, { name: string; type: any }[]>();
    
    // Find all incoming edges to this node
    for (const edge of graph.edges.values()) {
      if (edge.to === nodeId && edge.kind === 'data') {
        const fromNode = graph.nodes.get(edge.from);
        if (fromNode?.kind === 'transform') {
          // This is a transform step - collect its added fields
          const transformPayload = fromNode.payload as any;
          if (transformPayload?.operations) {
            const addedFields: { name: string; type: any }[] = [];
            for (const op of transformPayload.operations) {
              if (op.kind === 'addField') {
                addedFields.push({
                  name: op.name,
                  type: { kind: 'string' } // Default type, could be improved
                });
              }
            }
            if (addedFields.length > 0) {
              additionalFields.set(edge.from, addedFields);
            }
          }
        }
      }
    }
    
    return additionalFields;
  }

  private async executeMerge(
    nodeId: NodeId,
    graph: PipelineGraph,
    state: ExecutionState
  ): Promise<void> {
    const node = graph.nodes.get(nodeId)!;
    const inputs = getNodeInputs(nodeId, graph, state);
    const payload = node.payload as any;

    // Collect inputs from all incoming data edges
    const incomingEdges = getIncomingDataEdges(graph, nodeId);
    
    const inputValues: DataValue[] = [];
    console.log(`Merge ${nodeId}: processing ${incomingEdges.length} incoming edges`);
    for (const edge of incomingEdges) {
      const sourceState = state.nodeStates.get(edge.from);
      if (!sourceState) {
        console.log(`Merge ${nodeId}: edge ${edge.from} has no state`);
        continue;
      }

      console.log(`Merge ${nodeId}: edge ${edge.from} status=${sourceState.status} hasOutput=${!!sourceState.output}`);
      if (sourceState.status === 'completed' && sourceState.output) {
        console.log(`Merge ${nodeId}: adding input from ${edge.from} with ${sourceState.output.kind} containing ${isCollection(sourceState.output) ? (sourceState.output as any).data.length : 'N/A'} items`);
        inputValues.push(sourceState.output);
      } else if (sourceState.status === 'skipped') {
        // skipped branch - don't include, don't error
        console.log(`Merge ${nodeId}: skipping edge ${edge.from} (branch skipped)`);
        continue;
      } else if (payload.waitForAll) {
        // waitForAll is true but node hasn't completed (pending/running/failed)
        throw new Error(`MergeNode waiting for node ${edge.from} but it is ${sourceState.status}`);
      }
    }

    if (inputValues.length === 0) {
      throw new Error('MergeNode received no inputs — all branches skipped or failed');
    }

    // Single input (one branch taken): pass through directly
    if (inputValues.length === 1) {
      const output = inputValues[0];
      // Never return void from a merge - if the single input is void,
      // return an empty tabular instead
      const safeOutput = isVoid(output as DataValue)
        ? tabular({ schema: { columns: [] }, rows: [] }, { columns: [] })
        : output;
      
      markNodeCompleted(state, nodeId, safeOutput);
      this.emit({
        kind: 'node_complete',
        nodeId,
        output: safeOutput,
        timestamp: Date.now()
      });
      return;
    }

    // Multiple inputs: apply merge strategy
    // Wrap inputs in collection DataValue for merge node
    const collectionInput = { kind: 'collection', data: inputValues };
    const def = this.config.nodeRegistry.get(node.kind);
    markNodeRunning(state, nodeId);

    const output = await def.execute(payload, collectionInput, state.ctx);
    
    markNodeCompleted(state, nodeId, output);
    this.emit({
      kind: 'node_complete',
      nodeId,
      output,
      timestamp: Date.now()
    });

    this.emit({
      kind: 'branch_merge',
      mergeNodeId: nodeId,
      branchCount: inputValues.length,
      timestamp: Date.now()
    });
  }

  private async handleNodeError(
    node: PipelineNode,
    error: Error,
    graph: PipelineGraph,
    state: ExecutionState
  ): Promise<void> {
    const def = this.config.nodeRegistry.get(node.kind);
    const policy = node.errorPolicy.onError;
    const nodeId = node.id;
    const nodeState = state.nodeStates.get(nodeId);

    switch (policy) {
      case 'fail':
        markNodeFailed(state, nodeId, error);
        this.emit({
          kind: 'node_error',
          nodeId,
          error,
          willRetry: false,
          timestamp: Date.now()
        });
        this.emit({
          kind: 'pipeline_error',
          error,
          timestamp: Date.now()
        });
        throw error;

      case 'skip':
        // Pass through input as output
        const inputs = getNodeInputs(nodeId, graph, state);
        let inputToPass: DataValue;
        if (def.inputPorts.length === 1 && def.inputPorts[0].key === 'input') {
          inputToPass = inputs['input'] ?? void_;
        } else {
          // Wrap the Record<string, DataValue> as a collection
          inputToPass = collection(Object.values(inputs), 'any' as DataValueKind);
        }
        markNodeSkipped(state, nodeId, error.message);
        if (nodeState) {
          nodeState.output = inputToPass;
        }
        this.emit({
          kind: 'node_skip',
          nodeId,
          reason: error.message,
          timestamp: Date.now()
        });
        break;

      case 'retry':
        // Retries are handled by executeWithRetry
        // If we reach here, retries were exhausted, fall through to fail
        markNodeFailed(state, nodeId, error);
        this.emit({
          kind: 'node_error',
          nodeId,
          error,
          willRetry: false,
          timestamp: Date.now()
        });
        this.emit({
          kind: 'pipeline_error',
          error,
          timestamp: Date.now()
        });
        throw error;

      case 'fallback':
        markNodeSkipped(state, nodeId, 'Using fallback');
        this.emit({
          kind: 'node_skip',
          nodeId,
          reason: 'Using fallback',
          timestamp: Date.now()
        });
        
        const fallbackNodeId = node.errorPolicy.fallbackNodeId;
        if (fallbackNodeId) {
          await this.executeNode(fallbackNodeId, graph, state);
        }
        break;

      default:
        markNodeFailed(state, nodeId, error);
        this.emit({
          kind: 'node_error',
          nodeId,
          error,
          willRetry: false,
          timestamp: Date.now()
        });
        throw error;
    }
  }
}
