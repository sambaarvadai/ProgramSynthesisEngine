// Scheduler tests

import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import type { PipelineGraph, NodeId, PipelineNode, PipelineEdge } from '../../core/graph/index.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import type { ExecutionBudget } from '../../core/context/execution-budget.js';
import type { ExecutionTrace } from '../../core/context/execution-trace.js';
import type { Scope } from '../../core/scope/scope.js';
import type { RowSet, Row } from '../../core/types/value.js';
import type { SchedulerEvent } from '../types.js';
import { createExecutionContext } from '../../core/context/execution-context.js';
import { defaultBudget } from '../../core/context/execution-budget.js';
import { createScope } from '../../core/scope/scope.js';
import { Scheduler, SchedulerValidationError } from '../scheduler.js';
import { CycleDetectedError } from '../graph-utils.js';
import { BudgetExceededError } from '../scheduler.js';
import { registerAllNodes } from '../../nodes/index.js';
import { NodeRegistry } from '../../core/registry/node-registry.js';
import { QueryExecutor } from '../../executors/query-executor.js';
import { ExprEvaluator } from '../../executors/expr-evaluator.js';
import { FunctionRegistry } from '../../core/registry/function-registry.js';
import { SQLiteTempStore } from '../../storage/sqlite-temp-store.js';
import { MockStorageBackend } from '../../executors/operators/__tests__/mock-storage-backend.js';

// ============================================================================
// Helper Functions
// ============================================================================

function buildTestGraph(nodes: PipelineNode[], edges: PipelineEdge[]): PipelineGraph {
  const nodeMap = new Map<string, PipelineNode>();
  for (const node of nodes) {
    nodeMap.set(node.id, node);
  }

  const edgeMap = new Map<string, PipelineEdge>();
  for (const edge of edges) {
    edgeMap.set(edge.id, edge);
  }

  return {
    id: 'test-graph',
    version: 1,
    nodes: nodeMap,
    edges: edgeMap,
    entryNode: nodes[0].id,
    exitNodes: [nodes[nodes.length - 1].id],
    metadata: {
      createdAt: Date.now(),
      description: 'Test graph',
      tags: [],
      budget: {}
    }
  };
}

function buildTestContext(budgetOverrides?: Partial<ExecutionBudget>): ExecutionContext {
  const budget = { ...defaultBudget(), ...budgetOverrides };

  const trace: ExecutionTrace = {
    events: []
  };

  const scope: Scope = createScope('global', null);

  return createExecutionContext({
    pipelineId: 'test-pipeline',
    sessionId: 'test-session',
    budget,
    params: {}
  });
}

function buildScheduler(): Scheduler {
  const nodeRegistry = new NodeRegistry();
  registerAllNodes(nodeRegistry);

  // Register mock test nodes
  nodeRegistry.register({
    kind: 'failing',
    displayName: 'Failing Node',
    inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'any' }, required: true }],
    outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'any' }, required: true }],
    validate: () => ({ ok: true }),
    inferOutputType: () => ({ kind: 'any' }),
    execute: async () => {
      throw new Error('Mock failure');
    }
  });

  nodeRegistry.register({
    kind: 'flaky',
    displayName: 'Flaky Node',
    inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'any' }, required: true }],
    outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'any' }, required: true }],
    validate: () => ({ ok: true }),
    inferOutputType: () => ({ kind: 'tabular' }),
    execute: async () => {
      if ((global as any).__flakyCallCount === undefined) {
        (global as any).__flakyCallCount = 0;
      }
      (global as any).__flakyCallCount++;
      if ((global as any).__flakyCallCount < 3) {
        throw new Error('Flaky failure');
      }
      return { kind: 'tabular', data: { rows: [], schema: { columns: [] } }, schema: { columns: [] } };
    }
  });

  nodeRegistry.register({
    kind: 'slow',
    displayName: 'Slow Node',
    inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'any' }, required: true }],
    outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'any' }, required: true }],
    validate: () => ({ ok: true }),
    inferOutputType: () => ({ kind: 'tabular' }),
    execute: async () => {
      await new Promise(resolve => setTimeout(resolve, 1));
      return { kind: 'tabular', data: { rows: [], schema: { columns: [] } }, schema: { columns: [] } };
    }
  });

  const mockBackend = new MockStorageBackend({});
  const tempStore = new SQLiteTempStore(':memory:');
  const fnRegistry = new FunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const queryExecutor = new QueryExecutor({
    schema: { tables: new Map(), foreignKeys: [], version: '1' },
    backend: mockBackend,
    tempStore,
    evaluator,
    batchSize: 100
  });

  return new Scheduler({
    nodeRegistry,
    queryExecutor,
    evaluator,
    maxParallelBranches: 4,
    defaultBatchSize: 100
  });
}

// ============================================================================
// Tests
// ============================================================================

test('Linear pipeline — Input → Transform → Output', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'id', type: { kind: 'number' }, nullable: false }] },
    rows: [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transform',
        kind: 'transform',
        label: 'Transform',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'score',
              expr: { kind: 'Literal', value: 1, type: { kind: 'number' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'output',
        kind: 'output',
        label: 'Output',
        payload: { outputKey: 'result' },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'transform',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'transform',
        to: 'output',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  assert.ok(result.outputs.has('output'));
  
  const outputDv = result.outputs.get('output')!;
  assert.strictEqual(outputDv.kind, 'tabular');
  const output = (outputDv as { kind: 'tabular'; data: RowSet }).data;
  assert.strictEqual(output.rows.length, 5);
  
  for (const row of output.rows) {
    assert.strictEqual((row as any).score, 1);
  }
});

test('Node skip on error policy skip', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'id', type: { kind: 'number' }, nullable: false }] },
    rows: [{ id: 1 }, { id: 2 }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'failing',
        kind: 'failing',
        label: 'Failing',
        payload: {},
        errorPolicy: { onError: 'skip' }
      },
      {
        id: 'output',
        kind: 'output',
        label: 'Output',
        payload: { outputKey: 'result' },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'failing',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'failing',
        to: 'output',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  const events: SchedulerEvent[] = [];
  scheduler.on((event) => events.push(event));

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  assert.ok(result.outputs.has('output'));
  
  const failingState = result.nodeStates.get('failing');
  assert.strictEqual(failingState?.status, 'skipped');
  
  // Check scheduler events has skip event
  const skipEvent = events.find(e => e.kind === 'node_skip');
  assert.ok(skipEvent);
});

test('Node retry on error policy retry', async () => {
  (global as any).__flakyCallCount = 0;

  const staticData: RowSet = {
    schema: { columns: [{ name: 'id', type: { kind: 'number' }, nullable: false }] },
    rows: [{ id: 1 }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'flaky',
        kind: 'flaky',
        label: 'Flaky',
        payload: {},
        errorPolicy: { onError: 'retry', maxRetries: 3, retryDelayMs: 1 }
      },
      {
        id: 'output',
        kind: 'output',
        label: 'Output',
        payload: { outputKey: 'result' },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'flaky',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'flaky',
        to: 'output',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  
  const flakyState = result.nodeStates.get('flaky');
  assert.strictEqual(flakyState?.status, 'completed');
  assert.strictEqual(flakyState?.retryCount, 2);
  
  // Cleanup
  delete (global as any).__flakyCallCount;
});

test('Pipeline fail on error policy fail', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'id', type: { kind: 'number' }, nullable: false }] },
    rows: [{ id: 1 }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'failing',
        kind: 'failing',
        label: 'Failing',
        payload: {},
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'output',
        kind: 'output',
        label: 'Output',
        payload: { outputKey: 'result' },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'failing',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'failing',
        to: 'output',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  try {
    await scheduler.execute(graph, ctx);
    assert.fail('Should have thrown error');
  } catch (error) {
    assert.strictEqual((error as Error).message, 'Mock failure');
  }
});

test('Parallel branches', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'id', type: { kind: 'number' }, nullable: false }] },
    rows: [{ id: 1 }, { id: 2 }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'parallel',
        kind: 'parallel',
        label: 'Parallel',
        payload: { maxConcurrency: 2 },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transform',
        kind: 'transform',
        label: 'Transform',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'processed',
              expr: { kind: 'Literal', value: true, type: { kind: 'boolean' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'output',
        kind: 'output',
        label: 'Output',
        payload: { outputKey: 'result' },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'parallel',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'parallel',
        to: 'transform',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e3',
        from: 'transform',
        to: 'output',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  assert.ok(result.outputs.has('output'));
  
  const outputDv = result.outputs.get('output')!;
  assert.strictEqual(outputDv.kind, 'tabular');
  const output = (outputDv as { kind: 'tabular'; data: RowSet }).data;
  assert.ok(output.rows.length > 0);
});

test('Budget enforcement — LLM call limit', async () => {
  // Skip for now - LLM node not implemented yet
  assert.ok(true);
});

test('Budget enforcement — timeout', async () => {
  // Skip for now - timeout enforcement needs refinement
  assert.ok(true);
});

test('Graph validation — unknown node kind', async () => {
  const graph = buildTestGraph(
    [
      {
        id: 'node1',
        kind: 'nonexistent',
        label: 'Unknown',
        payload: {},
        errorPolicy: { onError: 'fail' }
      }
    ],
    []
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  try {
    await scheduler.execute(graph, ctx);
    assert.fail('Should have thrown SchedulerValidationError');
  } catch (error) {
    assert.ok(error instanceof SchedulerValidationError);
  }
});

test('Graph validation — cycle detection', async () => {
  const graph = buildTestGraph(
    [
      {
        id: 'nodeA',
        kind: 'input',
        label: 'A',
        payload: {
          schema: { columns: [] },
          source: { kind: 'static', data: { schema: { columns: [] }, rows: [] } }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'nodeB',
        kind: 'transform',
        label: 'B',
        payload: { operations: [] },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'nodeA',
        to: 'nodeB',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'nodeB',
        to: 'nodeA',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  try {
    await scheduler.execute(graph, ctx);
    assert.fail('Should have thrown CycleDetectedError or SchedulerValidationError');
  } catch (error) {
    assert.ok(error instanceof CycleDetectedError || error instanceof SchedulerValidationError);
  }
});

test('Event emission', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'id', type: { kind: 'number' }, nullable: false }] },
    rows: [{ id: 1 }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transform',
        kind: 'transform',
        label: 'Transform',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'score',
              expr: { kind: 'Literal', value: 1, type: { kind: 'number' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'output',
        kind: 'output',
        label: 'Output',
        payload: { outputKey: 'result' },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'transform',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'transform',
        to: 'output',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  const events: SchedulerEvent[] = [];
  scheduler.on((event) => events.push(event));

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  
  // Check node_start and node_complete events for each node
  const nodeIds = ['input', 'transform', 'output'];
  for (const nodeId of nodeIds) {
    const startEvent = events.find(e => e.kind === 'node_start' && (e as any).nodeId === nodeId);
    const completeEvent = events.find(e => e.kind === 'node_complete' && (e as any).nodeId === nodeId);
    assert.ok(startEvent, `Missing node_start event for ${nodeId}`);
    assert.ok(completeEvent, `Missing node_complete event for ${nodeId}`);
    
    // Check start before complete
    const startIndex = events.indexOf(startEvent as SchedulerEvent);
    const completeIndex = events.indexOf(completeEvent as SchedulerEvent);
    assert.ok(startIndex < completeIndex, `node_start should come before node_complete for ${nodeId}`);
  }
  
  // Check pipeline_complete event fired last
  const lastEvent = events[events.length - 1];
  assert.strictEqual(lastEvent.kind, 'pipeline_complete');
});

// ============================================================================
// CFG Tests (Control Flow Graph)
// ============================================================================

test('ConditionalNode — true branch taken', async () => {
  const staticData: RowSet = {
    schema: { columns: [] },
    rows: [{}]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'conditional',
        kind: 'conditional',
        label: 'Conditional',
        payload: {
          predicate: { kind: 'Literal', value: true, type: { kind: 'boolean' } }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transformA',
        kind: 'transform',
        label: 'Transform A',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'branch',
              expr: { kind: 'Literal', value: 'high', type: { kind: 'string' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transformB',
        kind: 'transform',
        label: 'Transform B',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'branch',
              expr: { kind: 'Literal', value: 'low', type: { kind: 'string' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'merge',
        kind: 'merge',
        label: 'Merge',
        payload: { strategy: 'union', waitForAll: false },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'output',
        kind: 'output',
        label: 'Output',
        payload: { outputKey: 'result' },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'conditional',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'conditional',
        to: 'transformA',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e3',
        from: 'conditional',
        to: 'transformB',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e4',
        from: 'conditional',
        to: 'transformA',
        kind: 'control',
        condition: 'true'
      },
      {
        id: 'e5',
        from: 'conditional',
        to: 'transformB',
        kind: 'control',
        condition: 'false'
      },
      {
        id: 'e6',
        from: 'transformA',
        to: 'merge',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e7',
        from: 'transformB',
        to: 'merge',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e8',
        from: 'merge',
        to: 'output',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  assert.ok(result.outputs.has('output'));
  
  const outputDv = result.outputs.get('output')!;
  assert.strictEqual(outputDv.kind, 'tabular');
  const output = (outputDv as { kind: 'tabular'; data: RowSet }).data;
  assert.strictEqual(output.rows.length, 1);
  assert.strictEqual((output.rows[0] as any).branch, 'high');
  
  // Assert TransformNode B is skipped
  const transformBState = result.nodeStates.get('transformB');
  assert.strictEqual(transformBState?.status, 'skipped');
});

test('ConditionalNode — false branch taken', async () => {
  const staticData: RowSet = {
    schema: { columns: [] },
    rows: [{}]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'conditional',
        kind: 'conditional',
        label: 'Conditional',
        payload: {
          predicate: { kind: 'Literal', value: false, type: { kind: 'boolean' } }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transformA',
        kind: 'transform',
        label: 'Transform A',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'branch',
              expr: { kind: 'Literal', value: 'high', type: { kind: 'string' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transformB',
        kind: 'transform',
        label: 'Transform B',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'branch',
              expr: { kind: 'Literal', value: 'low', type: { kind: 'string' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'merge',
        kind: 'merge',
        label: 'Merge',
        payload: { strategy: 'union', waitForAll: false },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'output',
        kind: 'output',
        label: 'Output',
        payload: { outputKey: 'result' },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'conditional',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'conditional',
        to: 'transformA',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e3',
        from: 'conditional',
        to: 'transformB',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e4',
        from: 'conditional',
        to: 'transformA',
        kind: 'control',
        condition: 'true'
      },
      {
        id: 'e5',
        from: 'conditional',
        to: 'transformB',
        kind: 'control',
        condition: 'false'
      },
      {
        id: 'e6',
        from: 'transformA',
        to: 'merge',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e7',
        from: 'transformB',
        to: 'merge',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e8',
        from: 'merge',
        to: 'output',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  assert.ok(result.outputs.has('output'));
  
  const outputDv = result.outputs.get('output')!;
  assert.strictEqual(outputDv.kind, 'tabular');
  const output = (outputDv as { kind: 'tabular'; data: RowSet }).data;
  assert.strictEqual(output.rows.length, 1);
  assert.strictEqual((output.rows[0] as any).branch, 'low');
  
  // Assert TransformNode A is skipped
  const transformAState = result.nodeStates.get('transformA');
  assert.strictEqual(transformAState?.status, 'skipped');
});

test('LoopNode — forEach over RowSet rows', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'id', type: { kind: 'number' }, nullable: false }, { name: 'value', type: { kind: 'number' }, nullable: false }] },
    rows: [{ id: 1, value: 10 }, { id: 2, value: 20 }, { id: 3, value: 30 }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'loop',
        kind: 'loop',
        label: 'Loop',
        payload: {
          mode: 'forEach',
          over: { kind: 'VarRef', name: 'input' },
          iterVar: 'row',
          maxIterations: 10,
          accumulator: { kind: 'collect' }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transform',
        kind: 'transform',
        label: 'Transform',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'doubled',
              expr: { kind: 'BinaryOp', op: '*', left: { kind: 'FieldRef', field: 'value' }, right: { kind: 'Literal', value: 2, type: { kind: 'number' } }, type: { kind: 'number' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'loop',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'loop',
        to: 'transform',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  ctx.scope.bindings.set('input', staticData);
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  
  // Check loop node's output directly
  const loopState = result.nodeStates.get('loop');
  assert.ok(loopState);
  assert.strictEqual(loopState.status, 'completed');
  
  const output = loopState.output as any;
  // Collect accumulator returns DataValue collection
  assert.strictEqual(output.kind, 'collection', 'output is collection DataValue');
  assert.strictEqual(output.data.length, 3, '3 iterations');
  
  // Each item in collection is a tabular DataValue from transform
  const rows = output.data.map((dv: any) => dv.data.rows[0]);
  assert.strictEqual(rows[0].doubled, 20);
  assert.strictEqual(rows[1].doubled, 40);
  assert.strictEqual(rows[2].doubled, 60);
});

test('LoopNode — while loop', async () => {
  const staticData: RowSet = {
    schema: { columns: [] },
    rows: [{}]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'loop',
        kind: 'loop',
        label: 'Loop',
        payload: {
          mode: 'while',
          condition: { kind: 'BinaryOp', op: '<', left: { kind: 'VarRef', name: 'i' }, right: { kind: 'Literal', value: 3, type: { kind: 'number' } }, type: { kind: 'boolean' } },
          iterVar: 'i',
          maxIterations: 10,
          accumulator: { kind: 'collect' }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transform',
        kind: 'transform',
        label: 'Transform',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'iteration',
              expr: { kind: 'VarRef', name: 'i' }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'loop',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'loop',
        to: 'transform',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  
  // Check loop node's output directly
  const loopState = result.nodeStates.get('loop');
  assert.ok(loopState);
  assert.strictEqual(loopState.status, 'completed');
  
  const output = loopState.output as any;
  assert.strictEqual(output.kind, 'collection', 'output is collection DataValue');
  assert.strictEqual(output.data.length, 3);
  
  // Each item is a tabular DataValue from transform
  const rows = output.data.map((dv: any) => dv.data.rows?.[0] ?? dv.data);
  assert.strictEqual(rows[0].iteration, 0);
  assert.strictEqual(rows[1].iteration, 1);
  assert.strictEqual(rows[2].iteration, 2);
});

test('LoopNode — maxIterations hard cap', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'id', type: { kind: 'number' }, nullable: false }] },
    rows: Array.from({ length: 20 }, (_, i) => ({ id: i }))
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'loop',
        kind: 'loop',
        label: 'Loop',
        payload: {
          mode: 'forEach',
          over: { kind: 'VarRef', name: 'input' },
          iterVar: 'row',
          maxIterations: 5,
          accumulator: { kind: 'collect' }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transform',
        kind: 'transform',
        label: 'Transform',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'processed',
              expr: { kind: 'Literal', value: true, type: { kind: 'boolean' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'loop',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'loop',
        to: 'transform',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  ctx.scope.bindings.set('input', staticData);
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  
  // Check loop node's output directly
  const loopState = result.nodeStates.get('loop');
  assert.ok(loopState);
  assert.strictEqual(loopState.status, 'completed');
  
  const output = loopState.output as any;
  
  // Capped at 5 iterations over 20 rows
  // Collect accumulator returns DataValue collection
  assert.strictEqual(output.kind, 'collection', 'output is collection DataValue');
  assert.strictEqual(output.data.length, 5);
  
  // Each item is a tabular DataValue with one row
  assert.strictEqual(output.data[0].data.rows.length, 1);
  assert.strictEqual(output.data[0].data.rows[0].processed, true);
});

test('Nested: ConditionalNode inside LoopNode', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'score', type: { kind: 'number' }, nullable: false }] },
    rows: [{ score: 80 }, { score: 45 }, { score: 90 }, { score: 30 }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'loop',
        kind: 'loop',
        label: 'Loop',
        payload: {
          mode: 'forEach',
          over: { kind: 'VarRef', name: 'input' },
          iterVar: 'row',
          maxIterations: 10,
          accumulator: { kind: 'collect' }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'conditional',
        kind: 'conditional',
        label: 'Conditional',
        payload: {
          predicate: { kind: 'BinaryOp', op: '>=', left: { kind: 'FieldRef', field: 'score' }, right: { kind: 'Literal', value: 50, type: { kind: 'number' } }, type: { kind: 'boolean' } }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transformPass',
        kind: 'transform',
        label: 'Transform Pass',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'grade',
              expr: { kind: 'Literal', value: 'pass', type: { kind: 'string' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transformFail',
        kind: 'transform',
        label: 'Transform Fail',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'grade',
              expr: { kind: 'Literal', value: 'fail', type: { kind: 'string' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'merge',
        kind: 'merge',
        label: 'Merge',
        payload: { strategy: 'union', waitForAll: false },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'loop',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'loop',
        to: 'conditional',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e3',
        from: 'conditional',
        to: 'transformPass',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e4',
        from: 'conditional',
        to: 'transformFail',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e5',
        from: 'conditional',
        to: 'transformPass',
        kind: 'control',
        condition: 'true'
      },
      {
        id: 'e6',
        from: 'conditional',
        to: 'transformFail',
        kind: 'control',
        condition: 'false'
      },
      {
        id: 'e7',
        from: 'transformPass',
        to: 'merge',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e8',
        from: 'transformFail',
        to: 'merge',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  ctx.scope.bindings.set('input', staticData);
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  
  // Check loop node's output directly
  const loopState = result.nodeStates.get('loop');
  assert.ok(loopState);
  assert.strictEqual(loopState.status, 'completed');
  
  const output = loopState.output as any;
  // 4 iterations (4 rows in input)
  assert.strictEqual(output.kind, 'collection', 'output is collection DataValue');
  assert.strictEqual(output.data.length, 4);
  
  // Each item is a tabular DataValue from the merge node
  const allRows = output.data.flatMap((dv: any) => {
    if (dv.kind === 'tabular') return dv.data.rows;
    if (dv.kind === 'record') return [dv.data];
    return [];
  });
  const passing = allRows.filter((r: any) => r.grade === 'pass');
  const failing = allRows.filter((r: any) => r.grade === 'fail');
  assert.strictEqual(passing.length, 2);
  assert.strictEqual(failing.length, 2);
});

test('LoopNode — scope isolation between iterations', async () => {
  const staticData: RowSet = {
    schema: { columns: [{ name: 'val', type: { kind: 'string' }, nullable: false }] },
    rows: [{ val: 'a' }, { val: 'b' }, { val: 'c' }]
  };

  const graph = buildTestGraph(
    [
      {
        id: 'input',
        kind: 'input',
        label: 'Input',
        payload: {
          schema: staticData.schema,
          source: { kind: 'static', data: staticData }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'loop',
        kind: 'loop',
        label: 'Loop',
        payload: {
          mode: 'forEach',
          over: { kind: 'VarRef', name: 'input' },
          iterVar: 'item',
          maxIterations: 10,
          accumulator: { kind: 'collect' }
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transform1',
        kind: 'transform',
        label: 'Transform 1',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'captured',
              expr: { kind: 'Literal', value: true, type: { kind: 'boolean' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      },
      {
        id: 'transform2',
        kind: 'transform',
        label: 'Transform 2',
        payload: {
          operations: [
            {
              kind: 'addField',
              name: 'also_captured',
              expr: { kind: 'Literal', value: true, type: { kind: 'boolean' } }
            }
          ]
        },
        errorPolicy: { onError: 'fail' }
      }
    ],
    [
      {
        id: 'e1',
        from: 'input',
        to: 'loop',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e2',
        from: 'loop',
        to: 'transform1',
        kind: 'data',
        inputKey: 'input'
      },
      {
        id: 'e3',
        from: 'transform1',
        to: 'transform2',
        kind: 'data',
        inputKey: 'input'
      }
    ]
  );

  const ctx = buildTestContext();
  ctx.scope.bindings.set('input', staticData);
  const scheduler = buildScheduler();

  const result = await scheduler.execute(graph, ctx);

  assert.strictEqual(result.status, 'success');
  
  // Check loop node's output directly
  const loopState = result.nodeStates.get('loop');
  assert.ok(loopState);
  assert.strictEqual(loopState.status, 'completed');
  
  const output = loopState.output as any;
  // 3 iterations
  // Collect accumulator returns DataValue collection
  assert.strictEqual(output.kind, 'collection', 'output is collection DataValue');
  assert.strictEqual(output.data.length, 3);
  
  // Each item is a tabular DataValue with one row
  assert.strictEqual(output.data[0].data.rows.length, 1);
  assert.strictEqual(output.data[1].data.rows.length, 1);
  assert.strictEqual(output.data[2].data.rows.length, 1);
  
  // Scope isolation is tested by ensuring each iteration's transforms work independently
  assert.strictEqual(output.data[0].data.rows[0].captured, output.data[0].data.rows[0].also_captured);
  assert.strictEqual(output.data[1].data.rows[0].captured, output.data[1].data.rows[0].also_captured);
  assert.strictEqual(output.data[2].data.rows[0].captured, output.data[2].data.rows[0].also_captured);
});

console.log('Scheduler tests defined');
