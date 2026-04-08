// Query Pipeline Tests - Testing QueryASTBuilder, QueryPlanner, OperatorTreeBuilder, QueryExecutor

import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import type { SchemaConfig } from '../../schema/schema-config.js';
import type { QueryIntent } from '../query-intent.js';
import { QueryASTBuilder } from '../query-ast-builder.js';
import { QueryPlanner } from '../query-planner.js';
import { OperatorTreeBuilder } from '../operator-tree-builder.js';
import { QueryExecutor } from '../../../executors/query-executor.js';
import { ExprEvaluator } from '../../../executors/expr-evaluator.js';
import { FunctionRegistry } from '../../../core/registry/function-registry.js';
import { SQLiteTempStore } from '../../../storage/sqlite-temp-store.js';
import { MockStorageBackend } from '../../../executors/operators/__tests__/mock-storage-backend.js';
import { ExecutionContext } from '../../../core/context/execution-context.js';
import { ExecutionBudget } from '../../../core/context/execution-budget.js';
import { ExecutionTrace } from '../../../core/context/execution-trace.js';
import { Scope } from '../../../core/scope/scope.js';
import { ProjectOperator } from '../../../executors/operators/project-operator.js';
import { JoinOperator } from '../../../executors/operators/join-operator.js';
import { ScanOperator } from '../../../executors/operators/scan-operator.js';
import type { Row } from '../../../core/types/row.js';

// ============================================================================
// CRM Schema Configuration
// ============================================================================

const crmSchema: SchemaConfig = {
  tables: new Map([
    ['customers', {
      name: 'customers',
      alias: 'c',
      columns: [
        { name: 'id', type: { kind: 'number' }, nullable: false },
        { name: 'name', type: { kind: 'string' }, nullable: false },
        { name: 'email', type: { kind: 'string' }, nullable: false },
        { name: 'segment', type: { kind: 'string' }, nullable: false },
        { name: 'created_at', type: { kind: 'string' }, nullable: false }
      ],
      primaryKey: ['id']
    }],
    ['orders', {
      name: 'orders',
      alias: 'o',
      columns: [
        { name: 'id', type: { kind: 'number' }, nullable: false },
        { name: 'customer_id', type: { kind: 'number' }, nullable: false },
        { name: 'status', type: { kind: 'string' }, nullable: false },
        { name: 'total', type: { kind: 'number' }, nullable: false },
        { name: 'created_at', type: { kind: 'string' }, nullable: false }
      ],
      primaryKey: ['id']
    }],
    ['order_items', {
      name: 'order_items',
      alias: 'oi',
      columns: [
        { name: 'id', type: { kind: 'number' }, nullable: false },
        { name: 'order_id', type: { kind: 'number' }, nullable: false },
        { name: 'product_id', type: { kind: 'number' }, nullable: false },
        { name: 'quantity', type: { kind: 'number' }, nullable: false },
        { name: 'unit_price', type: { kind: 'number' }, nullable: false }
      ],
      primaryKey: ['id']
    }],
    ['products', {
      name: 'products',
      alias: 'p',
      columns: [
        { name: 'id', type: { kind: 'number' }, nullable: false },
        { name: 'name', type: { kind: 'string' }, nullable: false },
        { name: 'category', type: { kind: 'string' }, nullable: false },
        { name: 'price', type: { kind: 'number' }, nullable: false }
      ],
      primaryKey: ['id']
    }]
  ]),
  foreignKeys: [
    { fromTable: 'orders', fromColumn: 'customer_id', toTable: 'customers', toColumn: 'id' },
    { fromTable: 'order_items', fromColumn: 'order_id', toTable: 'orders', toColumn: 'id' },
    { fromTable: 'order_items', fromColumn: 'product_id', toTable: 'products', toColumn: 'id' }
  ],
  version: '1.0',
  description: 'CRM Schema with customers, orders, order_items, and products'
};

// ============================================================================
// Test Data Generators
// ============================================================================

function generateCustomers(count: number): Row[] {
  const segments = ['enterprise', 'smb'];
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    name: `Customer${i + 1}`,
    email: `customer${i + 1}@example.com`,
    segment: segments[i % 2],
    created_at: new Date(2023, 0, 1 + i).toISOString()
  }));
}

function generateOrders(count: number): Row[] {
  const statuses = ['pending', 'completed', 'cancelled'];
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    customer_id: (i % 20) + 1,
    status: statuses[i % 3],
    total: (i + 1) * 50,
    created_at: new Date(2023, 1, 1 + (i % 28)).toISOString()
  }));
}

function generateOrderItems(count: number): Row[] {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    order_id: (i % 100) + 1,
    product_id: (i % 10) + 1,
    quantity: (i % 5) + 1,
    unit_price: ((i % 10) + 1) * 10
  }));
}

function generateProducts(count: number): Row[] {
  const categories = ['electronics', 'clothing', 'food'];
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    name: `Product${i + 1}`,
    category: categories[i % 3],
    price: (i + 1) * 10
  }));
}

function createMockStorageWithCRMData(): MockStorageBackend {
  return new MockStorageBackend({
    customers: generateCustomers(20),
    orders: generateOrders(100),
    order_items: generateOrderItems(300),
    products: generateProducts(10)
  });
}

// ============================================================================
// Helper Functions
// ============================================================================

function createFunctionRegistry(): FunctionRegistry {
  const registry = new FunctionRegistry();
  
  // Register aggregate functions
  registry.register({
    name: 'COUNT',
    inferType: () => ({ kind: 'number' }),
    validate: () => ({ ok: true }),
    execute: (args) => args[0] as number
  });
  
  registry.register({
    name: 'SUM',
    inferType: () => ({ kind: 'number' }),
    validate: () => ({ ok: true }),
    execute: (args) => args[0] as number
  });
  
  return registry;
}

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

// ============================================================================
// Test 1: QueryASTBuilder - simple query
// ============================================================================

test('QueryASTBuilder - simple query', () => {
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const builder = new QueryASTBuilder(crmSchema, evaluator);

  const intent: QueryIntent = {
    table: 'customers',
    columns: [{ field: 'name' }, { field: 'email' }],
    filters: [{ field: 'segment', operator: '=', value: 'enterprise' }]
  };

  const result = builder.build(intent);

  assert.strictEqual(result.validation.isValid, true, 'Build should succeed');
  assert.ok(result.ast, 'AST should exist');
  assert.strictEqual(result.ast.kind, 'Select', 'AST should be Select');
  assert.strictEqual(result.ast.from.table, 'customers', 'Primary table should be customers');
  assert.strictEqual(result.ast.columns.length, 2, 'Should have 2 columns');
  assert.ok(result.ast.where, 'Should have WHERE clause');
});

// ============================================================================
// Test 2: QueryASTBuilder - auto-join derivation
// ============================================================================

test('QueryASTBuilder - auto-join derivation', () => {
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const builder = new QueryASTBuilder(crmSchema, evaluator);

  const intent: QueryIntent = {
    table: 'orders',
    columns: [
      { field: 'id', table: 'orders' },
      { field: 'total', table: 'orders' },
      { field: 'name', table: 'customers' }
    ],
    joins: [{ table: 'customers' }]
  };

  const result = builder.build(intent);

  assert.strictEqual(result.validation.isValid, true, 'Build should succeed');
  assert.strictEqual(result.ast.joins.length, 1, 'Should have 1 join');
  
  const joinNode = result.ast.joins[0];
  assert.strictEqual(joinNode.table, 'customers', 'Join table should be customers');
  
  // Check that join condition was derived from foreign key
  assert.ok(joinNode.on, 'Join should have ON clause');
  assert.strictEqual(joinNode.on.kind, 'BinaryOp', 'ON should be a binary operation');
  assert.strictEqual((joinNode.on as any).op, '=', 'Should be equality');
  
  // The join should be orders.customer_id = customers.id
  const leftField = (joinNode.on as any).left.field;
  const rightField = (joinNode.on as any).right.field;
  assert.ok(
    leftField.includes('customer_id') || leftField.includes('id'),
    'Left field should reference customer_id or id'
  );
  assert.ok(
    rightField.includes('customer_id') || rightField.includes('id'),
    'Right field should reference customer_id or id'
  );
});

// ============================================================================
// Test 3: QueryASTBuilder - validation errors
// ============================================================================

test('QueryASTBuilder - validation errors', () => {
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const builder = new QueryASTBuilder(crmSchema, evaluator);

  const intent: QueryIntent = {
    table: 'nonexistent',
    columns: []
  };

  const result = builder.build(intent);

  assert.strictEqual(result.validation.isValid, false, 'Validation should fail');
  assert.ok(result.validation.errors.length > 0, 'Should have errors');
  const hasUnknownTableError = result.validation.errors.some(e => 
    e.toLowerCase().includes('nonexistent') || e.toLowerCase().includes('does not exist')
  );
  assert.ok(hasUnknownTableError, 'Should have UNKNOWN_TABLE error');
});

// ============================================================================
// Test 4: QueryPlanner - predicate pushdown
// ============================================================================

test('QueryPlanner - predicate pushdown', () => {
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const astBuilder = new QueryASTBuilder(crmSchema, evaluator);
  const planner = new QueryPlanner(crmSchema);

  // Intent with filter on orders table
  const intent: QueryIntent = {
    table: 'orders',
    columns: [
      { field: 'id', table: 'orders' },
      { field: 'total', table: 'orders' }
    ],
    filters: [{ field: 'status', table: 'orders', operator: '=', value: 'completed' }]
  };

  const { ast } = astBuilder.build(intent);
  const plan = planner.plan(ast);

  // Verify plan has nodes
  assert.ok(plan.nodes.size > 0, 'Plan should have nodes');
  assert.ok(plan.root, 'Plan should have root node');
  
  // The plan should have optimizations array
  assert.ok(Array.isArray(plan.optimizations), 'Plan should have optimizations array');
  
  // Find scan and filter nodes (after predicate pushdown, filter may be in scan)
  let hasScan = false;
  let hasFilter = false;
  let scanHasPredicate = false;
  for (const node of plan.nodes.values()) {
    if (node.kind === 'Scan') {
      hasScan = true;
      if ((node.payload as any).predicate) {
        scanHasPredicate = true;
      }
    }
    if (node.kind === 'Filter') hasFilter = true;
  }
  assert.ok(hasScan, 'Plan should have Scan node');
  // After predicate pushdown optimization, filter should be in scan node
  assert.ok(scanHasPredicate || hasFilter, 'Plan should have filter (either as Filter node or in Scan predicate)');
});

// ============================================================================
// Test 5: OperatorTreeBuilder - correct operator types
// ============================================================================

test('OperatorTreeBuilder - correct operator types', async () => {
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const astBuilder = new QueryASTBuilder(crmSchema, evaluator);
  const planner = new QueryPlanner(crmSchema);
  const mockBackend = createMockStorageWithCRMData();
  const tempStore = new SQLiteTempStore(':memory:');
  const treeBuilder = new OperatorTreeBuilder(mockBackend, tempStore, evaluator, 100);

  // Simple query with filter
  const intent: QueryIntent = {
    table: 'orders',
    columns: [{ field: 'id' }, { field: 'total' }],
    filters: [{ field: 'status', operator: '=', value: 'completed' }]
  };

  const { ast } = astBuilder.build(intent);
  const plan = planner.plan(ast);
  const root = treeBuilder.build(plan);

  // Root should be a valid operator
  assert.ok(root, 'Root should exist');
  assert.ok(root.kind, 'Root should have a kind');
  
  // Verify we can get the operator kind
  const expectedKinds = ['Project', 'Scan', 'Filter', 'Join', 'Agg', 'Sort', 'Limit'];
  assert.ok(expectedKinds.includes(root.kind), `Root kind '${root.kind}' should be a valid operator type`);
});

// ============================================================================
// Test 6: QueryExecutor - full pipeline
// ============================================================================

test('QueryExecutor - full pipeline', async () => {
  const mockBackend = createMockStorageWithCRMData();
  const tempStore = new SQLiteTempStore(':memory:');
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const ctx = createMockContext();

  const executor = new QueryExecutor({
    schema: crmSchema,
    backend: mockBackend,
    tempStore,
    evaluator,
    batchSize: 100
  });

  const intent: QueryIntent = {
    table: 'orders',
    columns: [{ field: 'id' }, { field: 'status' }, { field: 'total' }],
    filters: [{ field: 'status', operator: '=', value: 'completed' }],
    limit: 10
  };

  const result = await executor.execute(intent, ctx);

  // Result should have correct schema
  assert.ok(result.schema, 'Result should have schema');
  assert.ok(result.schema.columns, 'Schema should have columns');
  
  // Result should have rows
  assert.ok(Array.isArray(result.rows), 'Result should have rows array');
  
  // Should have at most 10 rows due to limit
  assert.ok(result.rows.length <= 10, `Should have at most 10 rows, got ${result.rows.length}`);
  
  // All rows should have status = 'completed'
  result.rows.forEach((row: any, index: number) => {
    const status = row.status || row['o.status'] || row['orders.status'];
    assert.ok(status === 'completed', `Row ${index} should have status='completed', got ${JSON.stringify(status)}`);
  });
});

// ============================================================================
// Test 7: QueryExecutor - explain
// ============================================================================

test('QueryExecutor - explain', async () => {
  const mockBackend = createMockStorageWithCRMData();
  const tempStore = new SQLiteTempStore(':memory:');
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);

  const executor = new QueryExecutor({
    schema: crmSchema,
    backend: mockBackend,
    tempStore,
    evaluator,
    batchSize: 100
  });

  const intent: QueryIntent = {
    table: 'orders',
    columns: [{ field: 'id' }, { field: 'total' }],
    filters: [{ field: 'status', operator: '=', value: 'completed' }]
  };

  const explanation = await executor.explain(intent);

  // Explanation should be non-empty string
  assert.ok(typeof explanation === 'string', 'Explain should return a string');
  assert.ok(explanation.length > 0, 'Explanation should not be empty');
  
  // Should mention table names from intent
  assert.ok(
    explanation.includes('orders'),
    'Explanation should include table names from intent'
  );
  
  // Should have plan structure
  assert.ok(
    explanation.includes('Plan') || explanation.includes('Execution') || explanation.includes('Scan'),
    'Explanation should describe execution plan'
  );
});

// ============================================================================
// Test 8: Chained - customers with most orders
// ============================================================================

test('Chained: customers with most orders', async () => {
  const mockBackend = createMockStorageWithCRMData();
  const tempStore = new SQLiteTempStore(':memory:');
  const fnRegistry = createFunctionRegistry();
  const evaluator = new ExprEvaluator(fnRegistry);
  const ctx = createMockContext();

  const executor = new QueryExecutor({
    schema: crmSchema,
    backend: mockBackend,
    tempStore,
    evaluator,
    batchSize: 100
  });

  // Query: group orders by customer_id, count orders, sorted desc, limit 5
  const intent: QueryIntent = {
    table: 'orders',
    columns: [
      { field: 'customer_id' },
      { field: 'id', alias: 'order_count', agg: 'COUNT' }
    ],
    groupBy: ['customer_id'],
    aggregations: [{ fn: 'COUNT', expr: 'id', alias: 'order_count' }],
    orderBy: [{ field: 'order_count', direction: 'DESC' }],
    limit: 5
  };

  const result = await executor.execute(intent, ctx);

  // Should return up to 5 rows (might be fewer if less than 5 customers)
  assert.ok(result.rows.length <= 5, 'Should return at most 5 rows');
  assert.ok(result.rows.length > 0, 'Should have at least one row');
  
  // Each row should have customer_id and order_count fields
  result.rows.forEach((row: any, index: number) => {
    const hasCustomerId = 'customer_id' in row || Object.keys(row).some(k => k.includes('customer_id'));
    const hasCount = 'order_count' in row || Object.keys(row).some(k => k.includes('order_count'));
    assert.ok(hasCustomerId, `Row ${index} should have customer_id field`);
    assert.ok(hasCount, `Row ${index} should have order_count field`);
  });
  
  // Order should be descending by order_count
  if (result.rows.length >= 2) {
    const first = (result.rows[0] as any).order_count || 0;
    const second = (result.rows[1] as any).order_count || 0;
    assert.ok(first >= second, 'Results should be sorted in descending order');
  }
});

console.log('Query pipeline tests defined');
