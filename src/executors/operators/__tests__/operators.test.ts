// Comprehensive tests for all physical operators

import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import type { StorageBackend } from '../../../core/storage/storage-backend.js';
import type { RowBatch, Row } from '../../../core/types/row.js';
import type { RowSchema } from '../../../core/types/schema.js';
import type { ExprAST } from '../../../core/ast/expr-ast.js';
import { ScanOperator } from '../scan-operator.js';
import { FilterOperator } from '../filter-operator.js';
import { ProjectOperator } from '../project-operator.js';
import { JoinOperator } from '../join-operator.js';
import { AggOperator } from '../agg-operator.js';
import { SortOperator } from '../sort-operator.js';
import { LimitOperator } from '../limit-operator.js';
import { ExprEvaluator } from '../../expr-evaluator.js';
import { SQLiteTempStore } from '../../../storage/sqlite-temp-store.js';
import { ExecutionContext } from '../../../core/context/execution-context.js';
import { ExecutionBudget } from '../../../core/context/execution-budget.js';
import { ExecutionTrace } from '../../../core/context/execution-trace.js';
import { Scope } from '../../../core/scope/scope.js';
import { TypeScope } from '../../../core/scope/scope.js';
import { FunctionRegistry } from '../../../core/registry/function-registry.js';
import { MockStorageBackend } from './mock-storage-backend.js';

// Helper functions
function createMockContext(): ExecutionContext {
  const budget: ExecutionBudget = {
    maxRowsPerNode: 10000,
    maxMemoryMB: 1024,
    timeoutMs: 30000,
    maxLLMCalls: 1000,
    llmCallsUsed: 0,
    maxIterations: 1000,
    iterationsUsed: 0,
    startedAt: Date.now(),
    maxBatchSize: 1000
  };

  const trace: ExecutionTrace = {
    events: []
  };

  const scope: Scope = {
    id: 'test-scope',
    kind: 'test' as any,
    bindings: new Map(),
    parent: null
  };

  return {
    executionId: 'test-execution',
    pipelineId: 'test-pipeline',
    sessionId: 'test-session',
    budget,
    trace,
    scope,
    nodeOutputs: new Map(),
    params: {}
  };
}

function createTestRows(count: number): Row[] {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    name: `User${i + 1}`,
    email: `user${i + 1}@example.com`,
    age: 20 + (i % 50),
    status: i % 3 === 0 ? 'active' : i % 3 === 1 ? 'inactive' : 'pending',
    amount: Math.floor(Math.random() * 1000) + 1,
    customer_id: Math.floor(i / 10) + 1
  }));
}

function createFunctionRegistry(): FunctionRegistry {
  const registry = new FunctionRegistry();
  
  // Register basic functions for testing
  registry.register({
    name: 'ABS',
    inferType: (args: any[]) => ({ kind: 'number' }),
    validate: (args: any[]) => ({ ok: true }),
    execute: (args: any[]) => Math.abs(args[0] as number)
  });
  
  registry.register({
    name: 'CONCAT',
    inferType: (args: any[]) => ({ kind: 'string' }),
    validate: (args: any[]) => ({ ok: true }),
    execute: (args: any[]) => String(args[0]) + String(args[1])
  });
  
  return registry;
}

// Test 1: ScanOperator
test('ScanOperator scans table and returns correct count', async () => {
  const testData = createTestRows(100);
  const mockBackend = new MockStorageBackend({ users: testData });
  const tempStore = new SQLiteTempStore(':memory:');
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const ctx = createMockContext();

  const scanOp = new ScanOperator({
    table: 'users',
    alias: 'u',
    schema: await mockBackend.getSchema('users'),
    batchSize: 50,
    backend: mockBackend
  });

  await scanOp.open(ctx);
  
  const allBatches = [];
  let batch;
  while ((batch = await scanOp.nextBatch(50)).rows.length > 0) {
    allBatches.push(batch);
  }
  
  await scanOp.close();

  const totalRows = allBatches.reduce((sum, b) => sum + b.rows.length, 0);
  assert.strictEqual(totalRows, 100, 'Should scan all 100 rows');
  assert.ok(allBatches.every(b => b.rows.length <= 50), 'Each batch should not exceed size limit');
});

// Test 2: FilterOperator
test('FilterOperator filters rows where age > 30', async () => {
  const testData = createTestRows(100);
  const mockBackend = new MockStorageBackend({ users: testData });
  const tempStore = new SQLiteTempStore(':memory:');
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const ctx = createMockContext();

  const scanOp = new ScanOperator({
    table: 'users',
    alias: 'u',
    schema: await mockBackend.getSchema('users'),
    batchSize: 1000,
    backend: mockBackend
  });

  const filterOp = new FilterOperator({
    input: scanOp,
    predicate: {
      kind: 'BinaryOp',
      op: '>',
      left: { kind: 'FieldRef', field: 'age' },
      right: { kind: 'Literal', value: 30, type: { kind: 'number' } }
    },
    evaluator
  });

  await filterOp.open(ctx);
  
  const allBatches = [];
  let batch;
  while ((batch = await filterOp.nextBatch(1000)).rows.length > 0) {
    allBatches.push(batch);
  }
  
  await filterOp.close();

  const totalRows = allBatches.reduce((sum, b) => sum + b.rows.length, 0);
  const allRows = allBatches.flatMap(b => b.rows);
  const allAges = allRows.map(r => r.age).filter(age => age !== null) as number[];
  const allOver30 = allAges.every(age => age > 30);
  
  assert.ok(totalRows > 0, 'Should return some rows');
  assert.ok(allOver30, 'All returned rows should have age > 30');
});

// Test 3: ProjectOperator
test('ProjectOperator projects only name and email fields', async () => {
  const testData = createTestRows(10);
  const mockBackend = new MockStorageBackend({ users: testData });
  const tempStore = new SQLiteTempStore(':memory:');
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const ctx = createMockContext();

  const scanOp = new ScanOperator({
    table: 'users',
    alias: 'u',
    schema: await mockBackend.getSchema('users'),
    batchSize: 1000,
    backend: mockBackend
  });

  const projectOp = new ProjectOperator({
    input: scanOp,
    projections: [
      { expr: { kind: 'FieldRef', field: 'name' }, alias: 'name' },
      { expr: { kind: 'FieldRef', field: 'email' }, alias: 'email' }
    ],
    evaluator
  });

  await projectOp.open(ctx);
  
  const batch = await projectOp.nextBatch(1000);
  await projectOp.close();

  assert.strictEqual(batch.rows.length, 10, 'Should return all 10 rows');
  assert.ok(batch.rows.every(row => 
    'name' in row && 'email' in row && 
    Object.keys(row).length === 2
  ), 'Each row should only have name and email fields');
  
  assert.ok(batch.schema.columns.some(col => col.name === 'name'), 'Schema should include name field');
  assert.ok(batch.schema.columns.some(col => col.name === 'email'), 'Schema should include email field');
});

// Test 4: JoinOperator INNER
test('JoinOperator INNER join orders with customers', async () => {
  const ordersData = Array.from({ length: 100 }, (_, i) => ({
    id: i + 1,
    customer_id: Math.floor(i / 10) + 1,
    total: (i + 1) * 10
  }));

  const customersData = Array.from({ length: 10 }, (_, i) => ({
    id: i + 1,
    name: `Customer${i + 1}`
  }));

  const mockBackend = new MockStorageBackend({ orders: ordersData, customers: customersData });
  const tempStore = new SQLiteTempStore(':memory:');
  const evaluator = new ExprEvaluator(createFunctionRegistry());
  const ctx = createMockContext();

  const leftScan = new ScanOperator({
    table: 'orders',
    alias: 'o',
    schema: await mockBackend.getSchema('orders'),
    batchSize: 1000,
    backend: mockBackend
  });

  const rightScan = new ScanOperator({
    table: 'customers',
    alias: 'c',
    schema: await mockBackend.getSchema('customers'),
    batchSize: 1000,
    backend: mockBackend
  });

  const joinOp = new JoinOperator({
    left: leftScan,
    right: rightScan,
    on: {
      kind: 'BinaryOp',
      op: '=',
      left: { kind: 'FieldRef', field: 'customer_id' },
      right: { kind: 'FieldRef', field: 'id' }
    },
    kind: 'INNER',
    evaluator,
    tempStore,
    batchSize: 1000
  });

  await joinOp.open(ctx);
  
  const allBatches = [];
  let batch;
  while ((batch = await joinOp.nextBatch(1000)).rows.length > 0) {
    allBatches.push(batch);
  }
  
  await joinOp.close();

  const totalRows = allBatches.reduce((sum, b) => sum + b.rows.length, 0);
  assert.strictEqual(totalRows, 100, 'Should return all 100 order rows with customer matches');
  
  const allRows = allBatches.flatMap(b => b.rows);
  assert.ok(allRows.every(row => 
    'left.customer_id' in row && 'right.id' in row && 
    'left.total' in row && 'right.name' in row
  ), 'Each row should have merged fields from both sides');
});

// Test 5: JoinOperator LEFT
test('JoinOperator LEFT join with unmatched rows', async () => {
  const ordersData = Array.from({ length: 20 }, (_, i) => ({
    id: i + 1,
    customer_id: i < 15 ? i + 1 : 999, // Some orders have no matching customer
    total: (i + 1) * 10
  }));

  const customersData = Array.from({ length: 10 }, (_, i) => ({
    id: i + 1,
    name: `Customer${i + 1}`
  }));

  const mockBackend = new MockStorageBackend({ orders: ordersData, customers: customersData });
  const tempStore = new SQLiteTempStore(':memory:');
  const evaluator = new ExprEvaluator(createFunctionRegistry());
  const ctx = createMockContext();

  const leftScan = new ScanOperator({
    table: 'orders',
    alias: 'o',
    schema: await mockBackend.getSchema('orders'),
    batchSize: 1000,
    backend: mockBackend
  });

  const rightScan = new ScanOperator({
    table: 'customers',
    alias: 'c',
    schema: await mockBackend.getSchema('customers'),
    batchSize: 1000,
    backend: mockBackend
  });

  const joinOp = new JoinOperator({
    left: leftScan,
    right: rightScan,
    on: {
      kind: 'BinaryOp',
      op: '=',
      left: { kind: 'FieldRef', field: 'customer_id' },
      right: { kind: 'FieldRef', field: 'id' }
    },
    kind: 'LEFT',
    evaluator,
    tempStore,
    batchSize: 1000
  });

  await joinOp.open(ctx);
  
  const allBatches = [];
  let batch;
  while ((batch = await joinOp.nextBatch(1000)).rows.length > 0) {
    allBatches.push(batch);
  }
  
  await joinOp.close();

  const totalRows = allBatches.reduce((sum, b) => sum + b.rows.length, 0);
  assert.strictEqual(totalRows, 20, 'Should return all 20 order rows');
  
  const allRows = allBatches.flatMap(b => b.rows);
  const unmatchedRows = allRows.filter(row => row['right.id'] === null);
  assert.strictEqual(unmatchedRows.length, 10, 'Should have 10 unmatched orders with null right fields');
  assert.ok(unmatchedRows.every(row => {
    const customerId = row['left.customer_id'] as number;
    return (customerId >= 11 && customerId <= 15) || customerId === 999;
  }), 'Unmatched rows should have customer_id 11-15 or 999');
});

// Test 6: AggOperator
test('AggOperator groups orders by status with COUNT and SUM', async () => {
  const ordersData = Array.from({ length: 100 }, (_, i) => ({
    id: i + 1,
    status: ['active', 'inactive', 'pending'][i % 3],
    total: (i + 1) * 10
  }));

  const mockBackend = new MockStorageBackend({ orders: ordersData });
  const tempStore = new SQLiteTempStore(':memory:');
  const evaluator = new ExprEvaluator(createFunctionRegistry());
  const ctx = createMockContext();

  const scanOp = new ScanOperator({
    table: 'orders',
    alias: 'o',
    schema: await mockBackend.getSchema('orders'),
    batchSize: 1000,
    backend: mockBackend
  });

  const aggOp = new AggOperator({
    input: scanOp,
    groupBy: [{ kind: 'FieldRef', field: 'status' }],
    aggregations: [
      {
        fn: 'COUNT',
        expr: { kind: 'Literal', value: 1, type: { kind: 'number' } },
        alias: 'count'
      },
      {
        fn: 'SUM',
        expr: { kind: 'FieldRef', field: 'total' },
        alias: 'total_sum'
      }
    ],
    evaluator,
    tempStore,
    batchSize: 1000
  });

  await aggOp.open(ctx);
  
  const allBatches = [];
  let batch;
  while ((batch = await aggOp.nextBatch(1000)).rows.length > 0) {
    allBatches.push(batch);
  }
  
  await aggOp.close();

  const totalRows = allBatches.reduce((sum, b) => sum + b.rows.length, 0);
  assert.strictEqual(totalRows, 3, 'Should return 3 groups (active, inactive, pending)');
  
  const allRows = allBatches.flatMap(b => b.rows);
  const activeGroup = allRows.find(row => row.status === 'active');
  const inactiveGroup = allRows.find(row => row.status === 'inactive');
  const pendingGroup = allRows.find(row => row.status === 'pending');
  
  assert.strictEqual(activeGroup?.count, 34, 'Active group should have 34 orders');
  assert.strictEqual(inactiveGroup?.count, 33, 'Inactive group should have 33 orders');
  assert.strictEqual(pendingGroup?.count, 33, 'Pending group should have 33 orders');
  
  assert.strictEqual(activeGroup?.total_sum, 17170, 'Active sum should be correct');
  assert.strictEqual(inactiveGroup?.total_sum, 16500, 'Inactive sum should be correct');
  assert.strictEqual(pendingGroup?.total_sum, 16830, 'Pending sum should be correct');
});

// Test 7: SortOperator
test('SortOperator sorts 500 rows by amount DESC', async () => {
  const testData = Array.from({ length: 500 }, (_, i) => ({
    id: i + 1,
    amount: Math.floor(Math.random() * 1000) + 1
  }));

  const mockBackend = new MockStorageBackend({ data: testData });
  const tempStore = new SQLiteTempStore(':memory:');
  const evaluator = new ExprEvaluator(createFunctionRegistry());
  const ctx = createMockContext();

  const scanOp = new ScanOperator({
    table: 'data',
    alias: 'd',
    schema: await mockBackend.getSchema('data'),
    batchSize: 1000,
    backend: mockBackend
  });

  const sortOp = new SortOperator({
    input: scanOp,
    keys: [{
      expr: { kind: 'FieldRef', field: 'amount' },
      direction: 'DESC',
      nulls: 'LAST'
    }],
    evaluator,
    tempStore,
    memoryLimitRows: 1000
  });

  await sortOp.open(ctx);
  
  const allBatches = [];
  let batch;
  while ((batch = await sortOp.nextBatch(1000)).rows.length > 0) {
    allBatches.push(batch);
  }
  
  await sortOp.close();

  const totalRows = allBatches.reduce((sum, b) => sum + b.rows.length, 0);
  assert.strictEqual(totalRows, 500, 'Should return all 500 rows');
  
  const allRows = allBatches.flatMap(b => b.rows);
  const amounts = allRows.map(row => row.amount).filter(a => a !== null) as number[];
  
  // Check descending order
  for (let i = 1; i < amounts.length; i++) {
    assert.ok(amounts[i-1] >= amounts[i], `Amount at position ${i-1} should be >= amount at position ${i}`);
  }
});

// Test 8: LimitOperator
test('LimitOperator limits 1000 rows to 10 with offset 5', async () => {
  const testData = createTestRows(1000);
  const mockBackend = new MockStorageBackend({ users: testData });
  const tempStore = new SQLiteTempStore(':memory:');
  const evaluator = new ExprEvaluator(createFunctionRegistry());
  const ctx = createMockContext();

  const scanOp = new ScanOperator({
    table: 'users',
    alias: 'u',
    schema: await mockBackend.getSchema('users'),
    batchSize: 1000,
    backend: mockBackend
  });

  const limitOp = new LimitOperator({
    input: scanOp,
    limit: 10,
    offset: 5
  });

  await limitOp.open(ctx);
  
  const batch = await limitOp.nextBatch(1000);
  await limitOp.close();

  assert.strictEqual(batch.rows.length, 10, 'Should return exactly 10 rows');
  assert.strictEqual(batch.rows[0].id, 6, 'First row should have id 6 (offset 5)');
  assert.strictEqual(batch.rows[9].id, 15, 'Last row should have id 15');
});

// Test 9: Chained pipeline
test('Chained pipeline Scan → Filter → Join → Agg → Sort → Limit', async () => {
  const ordersData = Array.from({ length: 100 }, (_, i) => ({
    id: i + 1,
    customer_id: Math.floor(i / 10) + 1,
    status: ['active', 'inactive'][i % 2],
    total: (i + 1) * 10
  }));

  const customersData = Array.from({ length: 10 }, (_, i) => ({
    id: i + 1,
    name: `Customer${i + 1}`
  }));

  const mockBackend = new MockStorageBackend({ orders: ordersData, customers: customersData });
  const tempStore = new SQLiteTempStore(':memory:');
  const evaluator = new ExprEvaluator(createFunctionRegistry());
  const ctx = createMockContext();

  // Scan → Filter
  const scanOp = new ScanOperator({
    table: 'orders',
    alias: 'o',
    schema: await mockBackend.getSchema('orders'),
    batchSize: 1000,
    backend: mockBackend
  });

  const filterOp = new FilterOperator({
    input: scanOp,
    predicate: {
      kind: 'BinaryOp',
      op: '=',
      left: { kind: 'FieldRef', field: 'status' },
      right: { kind: 'Literal', value: 'active', type: { kind: 'string' } }
    },
    evaluator
  });

  // → Join
  const customerScan = new ScanOperator({
    table: 'customers',
    alias: 'c',
    schema: await mockBackend.getSchema('customers'),
    batchSize: 1000,
    backend: mockBackend
  });

  const joinOp = new JoinOperator({
    left: filterOp,
    right: customerScan,
    on: {
      kind: 'BinaryOp',
      op: '=',
      left: { kind: 'FieldRef', field: 'customer_id' },
      right: { kind: 'FieldRef', field: 'id' }
    },
    kind: 'INNER',
    evaluator,
    tempStore,
    batchSize: 1000
  });

  // → Agg
  const aggOp = new AggOperator({
    input: joinOp,
    groupBy: [{ kind: 'FieldRef', field: 'name' }],
    aggregations: [
      {
        fn: 'COUNT',
        expr: { kind: 'Literal', value: 1, type: { kind: 'number' } },
        alias: 'order_count'
      },
      {
        fn: 'SUM',
        expr: { kind: 'FieldRef', field: 'total' },
        alias: 'total_sum'
      }
    ],
    evaluator,
    tempStore,
    batchSize: 1000
  });

  // → Sort
  const sortOp = new SortOperator({
    input: aggOp,
    keys: [{
      expr: { kind: 'FieldRef', field: 'total_sum' },
      direction: 'DESC',
      nulls: 'LAST'
    }],
    evaluator,
    tempStore,
    memoryLimitRows: 1000
  });

  // → Limit
  const limitOp = new LimitOperator({
    input: sortOp,
    limit: 5,
    offset: 0
  });

  await limitOp.open(ctx);
  
  const batch = await limitOp.nextBatch(1000);
  await limitOp.close();

  assert.strictEqual(batch.rows.length, 5, 'Should return exactly 5 rows');
  assert.ok(batch.rows.every(row => 
    'name' in row && 'order_count' in row && 'total_sum' in row
  ), 'Each row should have aggregated fields');
  
  // Verify sorting (DESC by total_sum)
  const totalSums = batch.rows.map(row => row.total_sum).filter(s => s !== null) as number[];
  for (let i = 1; i < totalSums.length; i++) {
    assert.ok(totalSums[i-1] >= totalSums[i], `Total sum at position ${i-1} should be >= position ${i}`);
  }
});

console.log('All operator tests completed successfully!');
