// Pipeline Scheduler
// Manages execution of pipeline graphs with parallel branch support

import type { SchedulerConfig, ExecutionState, NodeExecutionState, SchedulerEvent, ExecutionResult } from './types.js';
import type { PipelineGraph, NodeId, PipelineNode } from '../core/graph/index.js';
import type { ExecutionContext } from '../core/context/execution-context.js';
import type { Value, RowSet, Row } from '../core/types/value.js';
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

function markNodeCompleted(state: ExecutionState, nodeId: NodeId, output: Value): void {
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

async function executeWithRetry(
  node: PipelineNode,
  def: NodeDefinition<any, any, any>,
  inputs: any,
  ctx: ExecutionContext,
  config: SchedulerConfig,
  state: ExecutionState
): Promise<Value> {
  const maxRetries = node.errorPolicy.maxRetries || 0;
  const retryDelayMs = node.errorPolicy.retryDelayMs || 1000;
  let lastError: Error | undefined;
  const nodeState = state.nodeStates.get(node.id);

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await def.execute(node.payload, inputs, ctx);
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
    const outputs = new Map<string, Value>();
    for (const exitNodeId of graph.exitNodes) {
      const exitState = state.nodeStates.get(exitNodeId);
      if (exitState?.output !== undefined) {
        outputs.set(exitNodeId, exitState.output);
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
      
      // Calculate timeout for this node
      const remaining = budgetRemaining(state.ctx.budget);
      const timeoutMs = Math.min(remaining.timeMs, 30000); // Cap at 30s per node
      
      // QueryNode doesn't use input, but we pass it for consistency
      const result = await executeWithTimeout(
        this.config.queryExecutor.execute(payload.intent, state.ctx),
        timeoutMs,
        nodeId
      );
      
      markNodeCompleted(state, nodeId, result);
      this.emit({
        kind: 'node_complete',
        nodeId,
        output: result,
        timestamp: Date.now()
      });
      return;
    }

    // Standard node execution
    try {
      const inputs = getNodeInputs(nodeId, graph, state);
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

      const output = await executeWithTimeout(
        executeWithRetry(node, def, actualInput, state.ctx, this.config, state),
        timeoutMs,
        nodeId
      );

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
    input?: Value
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
      // Skip if target is an exit node of the graph
      if (graph.exitNodes.includes(edge.to)) continue;
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
        // Skip graph.exitNodes - never include exit nodes in body
        if (graph.exitNodes.includes(edge.to)) continue;

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
    // If input is a RowSet with one row, use that row
    // If input is a single Row (Record), use it directly
    let currentRow: Row | undefined = undefined;
    if (input && typeof input === 'object' && !Array.isArray(input)) {
      if ('rows' in input && 'schema' in input) {
        // It's a RowSet — use first row as predicate context
        const rs = input as RowSet;
        currentRow = rs.rows.length > 0 ? rs.rows[0] : undefined;
      } else {
        // It's a plain Record — use directly as row
        currentRow = input as Row;
      }
    }

    // 2. Get the node definition and execute predicate with row context
    const payload = node.payload as any;
    const result = this.config.evaluator.evaluate(payload.predicate, state.ctx.scope, currentRow);
    
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

    // Initialize accumulator
    let accumulated: Value = initAccumulator(payload.accumulator);
    let iterCount = 0;

    if (payload.mode === 'forEach') {
      // Resolve the iterable
      const iterable = this.config.evaluator.evaluate(payload.over, state.ctx.scope);
      const items = toIterableArray(iterable);

      for (const item of items) {
        if (iterCount >= maxIter) {
          // Emit warning trace event
          console.warn(`Loop hit maxIterations cap (${maxIter})`);
          break;
        }

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

        // Update accumulator (always accumulate even if bodyOutput is null/undefined)
        accumulated = updateAccumulator(payload.accumulator, accumulated, bodyOutput ?? null, iterCtx, this.config.evaluator);

        incrementIterationBudget(state.ctx);
        iterCount++;
      }
    } else if (payload.mode === 'while') {
      // Get loop input for while mode (processes full input each iteration)
      const inputs = getNodeInputs(nodeId, graph, state);
      const loopInput = inputs['input'] ?? null;
      
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
        // Update accumulator (always accumulate even if bodyOutput is null/undefined)
        accumulated = updateAccumulator(payload.accumulator, accumulated, bodyOutput ?? null, iterCtx, this.config.evaluator);

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

  private async executeMerge(
    nodeId: NodeId,
    graph: PipelineGraph,
    state: ExecutionState
  ): Promise<void> {
    const node = graph.nodes.get(nodeId);
    if (!node) {
      throw new Error(`Node ${nodeId} not found`);
    }

    const payload = node.payload as any;

    // Collect inputs from all incoming data edges
    const incomingEdges = getIncomingDataEdges(graph, nodeId);
    
    const inputs: Value[] = [];
    for (const edge of incomingEdges) {
      const sourceState = state.nodeStates.get(edge.from);
      if (!sourceState) {
        continue;
      }

      if (sourceState.status === 'completed') {
        inputs.push(sourceState.output!);
      } else if (sourceState.status === 'skipped') {
        // skipped branch — don't include, don't error
        continue;
      } else if (payload.waitForAll) {
        // waitForAll is true but node hasn't completed (pending/running/failed)
        throw new Error(`MergeNode waiting for node ${edge.from} but it is ${sourceState.status}`);
      }
    }

    if (inputs.length === 0) {
      throw new Error('MergeNode received no inputs — all branches skipped or failed');
    }

    // Single input (one branch taken): pass through directly
    if (inputs.length === 1) {
      markNodeCompleted(state, nodeId, inputs[0]);
      this.emit({
        kind: 'node_complete',
        nodeId,
        output: inputs[0],
        timestamp: Date.now()
      });
      return;
    }

    // Multiple inputs: apply merge strategy
    // Delegate to merge-node definition's execute
    const def = this.config.nodeRegistry.get(node.kind);
    markNodeRunning(state, nodeId);

    const output = await def.execute(payload, inputs, state.ctx);
    
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
      branchCount: inputs.length,
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
        const inputToPass = def.inputPorts.length === 1 && def.inputPorts[0].key === 'input' 
          ? inputs['input'] 
          : inputs;
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
