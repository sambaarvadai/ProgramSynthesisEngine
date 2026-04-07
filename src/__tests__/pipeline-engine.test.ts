import { test } from 'node:test';
import assert from 'node:assert';
import { PipelineCompiler } from '../compiler/pipeline/index.js';
import { PipelineEngine } from '../pipeline-engine.js';
import type { PlanResult } from '../pipeline-engine.js';
import type { PipelineIntent } from '../compiler/pipeline/index.js';

test('PipelineCompiler: linear intent compiles correctly', () => {
  const compiler = new PipelineCompiler({
    tables: new Map(),
    foreignKeys: [],
    version: '1',
  });

  const intent = {
    description: 'Fetch customers and output them',
    steps: [
      { id: 'fetch', kind: 'query', description: 'Fetch all customers' },
      {
        id: 'out',
        kind: 'transform',
        description: 'Pass through',
        dependsOn: ['fetch'],
      },
    ],
  } as PipelineIntent;

  const { graph, errors } = compiler.compile(intent);

  assert.strictEqual(errors.length, 0);
  assert.ok(graph.nodes.has('fetch'));
  assert.ok(graph.nodes.has('out'));
  assert.ok(graph.nodes.has('_input'));
  assert.ok(graph.nodes.has('_output'));
});

test('PipelineCompiler: conditional intent compiles correctly', () => {
  const compiler = new PipelineCompiler({
    tables: new Map(),
    foreignKeys: [],
    version: '1',
  });

  const intent = {
    description: 'Conditional workflow',
    steps: [
      { id: 'fetch', kind: 'query', description: 'Fetch data' },
      {
        id: 'check',
        kind: 'conditional',
        description: 'Check condition',
        dependsOn: ['fetch'],
        trueBranch: 'transform_a',
        falseBranch: 'transform_b',
        mergeStep: 'merge',
      },
      {
        id: 'transform_a',
        kind: 'transform',
        description: 'Transform for true branch',
      },
      {
        id: 'transform_b',
        kind: 'transform',
        description: 'Transform for false branch',
      },
      {
        id: 'merge',
        kind: 'merge',
        description: 'Merge branches',
        mergeFrom: ['transform_a', 'transform_b'],
      },
    ],
  } as PipelineIntent;

  const { graph, errors } = compiler.compile(intent);

  assert.strictEqual(errors.length, 0);
  assert.ok(graph.nodes.has('check'));
  assert.ok(graph.nodes.has('transform_a'));
  assert.ok(graph.nodes.has('transform_b'));
  assert.ok(graph.nodes.has('merge'));

  // Check control edges from conditional
  const controlEdges = Array.from(graph.edges.values()).filter(
    e => e.kind === 'control',
  );
  assert.ok(
    controlEdges.some(e => e.from === 'check' && e.to === 'transform_a'),
  );
  assert.ok(
    controlEdges.some(e => e.from === 'check' && e.to === 'transform_b'),
  );

  // Check data edges to merge
  const mergeEdges = Array.from(graph.edges.values()).filter(
    e => e.to === 'merge' && e.kind === 'data',
  );
  assert.ok(mergeEdges.some(e => e.from === 'transform_a'));
  assert.ok(mergeEdges.some(e => e.from === 'transform_b'));
});

test('PipelineCompiler: loop intent compiles correctly', () => {
  const compiler = new PipelineCompiler({
    tables: new Map(),
    foreignKeys: [],
    version: '1',
  });

  const intent = {
    description: 'Loop workflow',
    steps: [
      { id: 'fetch', kind: 'query', description: 'Fetch data' },
      {
        id: 'loop',
        kind: 'loop',
        description: 'Loop over data',
        dependsOn: ['fetch'],
        loopMode: 'forEach',
        loopBody: ['transform'],
      },
      {
        id: 'transform',
        kind: 'transform',
        description: 'Transform each item',
      },
    ],
  } as PipelineIntent;

  const { graph, errors } = compiler.compile(intent);

  assert.strictEqual(errors.length, 0);
  assert.ok(graph.nodes.has('loop'));
  assert.ok(graph.nodes.has('transform'));

  // Check data edge from loop to transform
  const loopToTransform = Array.from(graph.edges.values()).find(
    e => e.from === 'loop' && e.to === 'transform' && e.kind === 'data',
  );
  assert.ok(loopToTransform);
});

test('PipelineCompiler: validation catches duplicate step ids', () => {
  const compiler = new PipelineCompiler({
    tables: new Map(),
    foreignKeys: [],
    version: '1',
  });

  const intent = {
    description: 'Duplicate ids',
    steps: [
      { id: 'step1', kind: 'query', description: 'First step' },
      { id: 'step1', kind: 'transform', description: 'Duplicate id' },
    ],
  } as PipelineIntent;

  const { errors } = compiler.compile(intent);

  assert.ok(errors.length > 0);
  assert.ok(errors.some(e => e.code === 'DUPLICATE_STEP_ID'));
});

test('PipelineCompiler: validation catches missing branch reference', () => {
  const compiler = new PipelineCompiler({
    tables: new Map(),
    foreignKeys: [],
    version: '1',
  });

  const intent = {
    description: 'Missing branch',
    steps: [
      {
        id: 'check',
        kind: 'conditional',
        description: 'Check condition',
        trueBranch: 'nonexistent',
        falseBranch: 'also_nonexistent',
      },
    ],
  } as PipelineIntent;

  const { errors } = compiler.compile(intent);

  assert.ok(errors.length > 0);
  assert.ok(
    errors.some(e => e.code === 'INVALID_TRUE_BRANCH'),
    'Should catch invalid trueBranch',
  );
  assert.ok(
    errors.some(e => e.code === 'INVALID_FALSE_BRANCH'),
    'Should catch invalid falseBranch',
  );
});

test('PipelineEngine: formatPlan output', () => {
  const engine = new PipelineEngine({
    anthropicApiKey: 'test-key',
  });

  const mockPlanResult: PlanResult = {
    intent: {
      description: 'Test pipeline',
      steps: [
        { id: 'step1', kind: 'query', description: 'Fetch data' },
        {
          id: 'step2',
          kind: 'transform',
          description: 'Transform data',
          dependsOn: ['step1'],
        },
        {
          id: 'step3',
          kind: 'llm',
          description: 'Analyze with LLM',
          dependsOn: ['step2'],
          outputFields: ['result'],
        },
      ],
    },
    graph: {
      id: 'test-graph',
      version: 1,
      nodes: new Map(),
      edges: new Map(),
      entryNode: '_input',
      exitNodes: ['_output'],
      metadata: {
        description: 'Test pipeline',
        createdAt: Date.now(),
        tags: [],
        budget: {},
      },
    },
    compilationErrors: [],
    intentRaw: '{"description":"Test pipeline"}',
  };

  const formatted = engine.formatPlan(mockPlanResult);

  assert.ok(formatted.includes('Test pipeline'));
  assert.ok(formatted.includes('Fetch data'));
  assert.ok(formatted.includes('Transform data'));
  assert.ok(formatted.includes('Analyze with LLM'));
  assert.ok(formatted.includes('[query]'));
  assert.ok(formatted.includes('[transform]'));
  assert.ok(formatted.includes('[llm]'));
});
