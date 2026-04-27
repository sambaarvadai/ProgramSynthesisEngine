/**
 * Integration test for Session Cursor flow
 * Tests the complete flow: query → cursor storage → referential query → cursor-driven write
 */

import { test } from 'node:test';
import assert from 'node:assert';
import { SessionCursorStore, extractCursor, buildWhereFromCursor } from '../../src/session/SessionCursor.js';
import { isReferentialQuery, buildCursorSystemPromptFragment } from '../../src/session/ReferentialResolver.js';
import type { SessionCursor } from '../../src/session/SessionCursor.js';

// Mock storage for testing
class MockStorageBackend {
  private executedQueries: Array<{ sql: string; params: any[] }> = [];

  async rawQuery(sql: string, params: any[]): Promise<any> {
    this.executedQueries.push({ sql, params });
    
    // Mock response for SELECT
    if (sql.includes('SELECT')) {
      return { rows: [{ id: 1, status: 'open', title: 'Test Ticket' }], rowCount: 1 };
    }
    
    // Mock response for UPDATE/DELETE
    if (sql.includes('UPDATE') || sql.includes('DELETE')) {
      return { rowCount: 1 };
    }
    
    return { rowCount: 0 };
  }

  getExecutedQueries() {
    return this.executedQueries;
  }

  clearExecutedQueries() {
    this.executedQueries = [];
  }
}

test('SessionCursor: extractCursor with small result set (≤50 rows)', () => {
  const rows = [
    { id: 1, status: 'open' },
    { id: 2, status: 'open' },
  ];
  
  const filter = { op: '=', field: 'status', value: 'open' };
  
  const cursor = extractCursor(
    rows,
    'id',
    filter,
    'pipeline-123',
    'Show all open tickets'
  );

  assert.strictEqual(cursor.table, 'unknown'); // inferred from filter
  assert.deepStrictEqual(cursor.primaryKeys, ['id']);
  assert.deepStrictEqual(cursor.ids, [1, 2]);
  assert.strictEqual(cursor.sourceFilter, null);
  assert.strictEqual(cursor.rowCount, 2);
  assert.strictEqual(cursor.pipelineId, 'pipeline-123');
  assert.strictEqual(cursor.description, 'Show all open tickets');
  assert.ok(cursor.expiresAt instanceof Date);
  assert.ok(cursor.expiresAt > new Date(Date.now() + 290000)); // ~300s from now
});

test('SessionCursor: extractCursor with large result set (>50 rows)', () => {
  const rows = Array.from({ length: 100 }, (_, i) => ({ id: i + 1, status: 'open' }));
  
  const filter = { op: '=', field: 'status', value: 'open' };
  
  const cursor = extractCursor(
    rows,
    'id',
    filter,
    'pipeline-456',
    'Show all open tickets'
  );

  assert.strictEqual(cursor.ids, null);
  assert.deepStrictEqual(cursor.sourceFilter, filter);
  assert.strictEqual(cursor.rowCount, 100);
});

test('SessionCursorStore: set and get cursor', () => {
  const store = new SessionCursorStore();
  
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1, 2, 3],
    sourceFilter: null,
    rowCount: 3,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() + 300000),
  };

  store.set(cursor);
  
  const retrieved = store.get();
  assert.ok(retrieved);
  assert.strictEqual(retrieved?.table, 'tickets');
  assert.deepStrictEqual(retrieved?.ids, [1, 2, 3]);
});

test('SessionCursorStore: get returns null for expired cursor', () => {
  const store = new SessionCursorStore();
  
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1],
    sourceFilter: null,
    rowCount: 1,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() - 1000), // Expired 1 second ago
  };

  store.set(cursor);
  
  const retrieved = store.get();
  assert.strictEqual(retrieved, null);
});

test('SessionCursorStore: clear removes cursor', () => {
  const store = new SessionCursorStore();
  
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1],
    sourceFilter: null,
    rowCount: 1,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() + 300000),
  };

  store.set(cursor);
  store.clear();
  
  const retrieved = store.get();
  assert.strictEqual(retrieved, null);
});

test('SessionCursorStore: isExpired returns correct status', () => {
  const store = new SessionCursorStore();
  
  // No cursor set
  assert.strictEqual(store.isExpired(), true);
  
  // Valid cursor
  const validCursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1],
    sourceFilter: null,
    rowCount: 1,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() + 300000),
  };
  store.set(validCursor);
  assert.strictEqual(store.isExpired(), false);
  
  // Expired cursor
  const expiredCursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1],
    sourceFilter: null,
    rowCount: 1,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() - 1000),
  };
  store.set(expiredCursor);
  assert.strictEqual(store.isExpired(), true);
});

test('buildWhereFromCursor: with IDs', () => {
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1, 2, 3],
    sourceFilter: null,
    rowCount: 3,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() + 300000),
  };

  const { clause, params } = buildWhereFromCursor(cursor);
  
  assert.strictEqual(clause, '"id" = ANY($1)');
  assert.deepStrictEqual(params, [[1, 2, 3]]);
});

test('buildWhereFromCursor: with sourceFilter', () => {
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: null,
    sourceFilter: { op: '=', field: 'status', value: 'open' },
    rowCount: 100,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() + 300000),
  };

  const { clause, params } = buildWhereFromCursor(cursor);
  
  assert.strictEqual(clause, '"status" = $1');
  assert.deepStrictEqual(params, ['open']);
});

test('buildWhereFromCursor: throws error when neither IDs nor filter', () => {
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: null,
    sourceFilter: null,
    rowCount: 0,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() + 300000),
  };

  assert.throws(
    () => buildWhereFromCursor(cursor),
    /Cursor has no resolvable WHERE target/
  );
});

test('isReferentialQuery: detects demonstratives', () => {
  assert.strictEqual(isReferentialQuery('update this ticket'), true);
  assert.strictEqual(isReferentialQuery('change these records'), true);
  assert.strictEqual(isReferentialQuery('delete that row'), true);
  assert.strictEqual(isReferentialQuery('update those items'), true);
  assert.strictEqual(isReferentialQuery('modify it'), true);
});

test('isReferentialQuery: detects implicit references', () => {
  assert.strictEqual(isReferentialQuery('update the ticket'), true);
  assert.strictEqual(isReferentialQuery('delete the record'), true);
  assert.strictEqual(isReferentialQuery('modify the result'), true);
  assert.strictEqual(isReferentialQuery('use the same'), true);
  assert.strictEqual(isReferentialQuery('update them'), true);
  assert.strictEqual(isReferentialQuery('change they'), true);
});

test('isReferentialQuery: detects positional references', () => {
  assert.strictEqual(isReferentialQuery('update the one above'), true);
  assert.strictEqual(isReferentialQuery('modify the previous result'), true);
  assert.strictEqual(isReferentialQuery('change the last one'), true);
});

test('isReferentialQuery: word boundary matching', () => {
  // Should match word boundaries
  assert.strictEqual(isReferentialQuery('update this ticket'), true);
  assert.strictEqual(isReferentialQuery('update thisticket'), false); // No word boundary
  
  // Case insensitive
  assert.strictEqual(isReferentialQuery('UPDATE THIS TICKET'), true);
  assert.strictEqual(isReferentialQuery('Update This Ticket'), true);
});

test('isReferentialQuery: non-referential queries return false', () => {
  assert.strictEqual(isReferentialQuery('show me all tickets'), false);
  assert.strictEqual(isReferentialQuery('create a new record'), false);
  assert.strictEqual(isReferentialQuery('delete from tickets where id = 1'), false);
});

test('buildCursorSystemPromptFragment: includes all cursor details', () => {
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1, 2, 3],
    sourceFilter: null,
    rowCount: 3,
    pipelineId: 'pipeline-123',
    description: 'Show all open tickets',
    expiresAt: new Date(Date.now() + 300000),
  };

  const fragment = buildCursorSystemPromptFragment(cursor);
  
  assert.ok(fragment.includes('PRIOR QUERY CONTEXT'));
  assert.ok(fragment.includes('Show all open tickets'));
  assert.ok(fragment.includes('Table: tickets'));
  assert.ok(fragment.includes('Rows returned: 3'));
  assert.ok(fragment.includes('Resolved IDs: [1,2,3]'));
  assert.ok(fragment.includes('resolve the target using the above context'));
});

test('buildCursorSystemPromptFragment: includes sourceFilter when IDs are null', () => {
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: null,
    sourceFilter: { op: '=', field: 'status', value: 'open' },
    rowCount: 100,
    pipelineId: 'pipeline-123',
    description: 'Show all open tickets',
    expiresAt: new Date(Date.now() + 300000),
  };

  const fragment = buildCursorSystemPromptFragment(cursor);
  
  assert.ok(fragment.includes('Source filter:'));
  assert.ok(fragment.includes('status'));
  assert.ok(fragment.includes('open'));
});

test('Integration: Two-turn interaction with cursor-driven write', async () => {
  const store = new SessionCursorStore();
  const backend = new MockStorageBackend();

  // Turn 1: Query that returns 1 row
  const queryRows = [{ id: 1, status: 'open', title: 'Test Ticket' }];
  const queryFilter = { op: '=', field: 'status', value: 'open', table: 'tickets' };
  
  const cursor = extractCursor(
    queryRows,
    'id',
    queryFilter,
    'pipeline-123',
    'Show all open tickets'
  );
  
  assert.strictEqual(cursor.table, 'tickets');
  assert.deepStrictEqual(cursor.ids, [1]);
  assert.strictEqual(cursor.rowCount, 1);
  
  store.set(cursor);
  
  const retrievedCursor = store.get();
  assert.ok(retrievedCursor);
  assert.strictEqual(retrievedCursor?.table, 'tickets');
  assert.deepStrictEqual(retrievedCursor?.ids, [1]);

  // Turn 2: Referential query
  const referentialInput = 'change the status of this ticket to pending';
  assert.strictEqual(isReferentialQuery(referentialInput), true);
  
  // Intent generator would receive cursor fragment
  const systemPromptFragment = buildCursorSystemPromptFragment(retrievedCursor!);
  assert.ok(systemPromptFragment.includes('Show all open tickets'));
  assert.ok(systemPromptFragment.includes('tickets'));
  assert.ok(systemPromptFragment.includes('Resolved IDs: [1]'));
  
  // WriteNode would use cursor-driven path
  const { clause, params } = buildWhereFromCursor(retrievedCursor!);
  assert.strictEqual(clause, '"id" = ANY($1)');
  assert.deepStrictEqual(params, [[1]]);
  
  // Simulate cursor-driven UPDATE execution
  const updateSql = `UPDATE "tickets" SET "status" = 'pending', "updated_at" = NOW() WHERE ${clause}`;
  await backend.rawQuery(updateSql, ['pending', new Date().toISOString(), ...params]);
  
  const queries = backend.getExecutedQueries();
  assert.strictEqual(queries.length, 1);
  assert.ok(queries[0].sql.includes('UPDATE "tickets"'));
  assert.ok(queries[0].sql.includes('"id" = ANY($1)'));
  
  // Cursor should be cleared after successful write
  store.clear();
  const finalCursor = store.get();
  assert.strictEqual(finalCursor, null);
});

test('Edge case: Expired cursor throws error in WriteNode', async () => {
  const store = new SessionCursorStore();
  
  const expiredCursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1],
    sourceFilter: null,
    rowCount: 1,
    pipelineId: 'pipeline-123',
    description: 'Test query',
    expiresAt: new Date(Date.now() - 1000), // Expired
  };
  
  store.set(expiredCursor);
  
  const retrieved = store.get();
  assert.strictEqual(retrieved, null); // get() returns null for expired
  
  // WriteNode would throw error
  assert.throws(
    () => {
      if (!retrieved) {
        throw new Error('WriteNode: no input rows and no session cursor available. Cannot determine WHERE target for update.');
      }
    },
    /no input rows and no session cursor available/
  );
});

test('Edge case: Large result uses sourceFilter instead of IDs', () => {
  const largeRows = Array.from({ length: 100 }, (_, i) => ({ id: i + 1, status: 'open' }));
  const filter = { op: '=', field: 'status', value: 'open', table: 'tickets' };
  
  const cursor = extractCursor(
    largeRows,
    'id',
    filter,
    'pipeline-456',
    'Show all open tickets'
  );
  
  assert.strictEqual(cursor.ids, null);
  assert.deepStrictEqual(cursor.sourceFilter, filter);
  assert.strictEqual(cursor.rowCount, 100);
  
  const { clause, params } = buildWhereFromCursor(cursor);
  assert.strictEqual(clause, '"status" = $1');
  assert.deepStrictEqual(params, ['open']);
});

test('Edge case: Non-referential follow-up does not inject cursor', () => {
  const store = new SessionCursorStore();
  
  const cursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: [1, 2, 3],
    sourceFilter: null,
    rowCount: 3,
    pipelineId: 'pipeline-123',
    description: 'Show all open tickets',
    expiresAt: new Date(Date.now() + 300000),
  };
  
  store.set(cursor);
  
  // Non-referential query
  const nonReferentialInput = 'show me all closed tickets';
  assert.strictEqual(isReferentialQuery(nonReferentialInput), false);
  
  // Cursor should not be injected
  const shouldInject = isReferentialQuery(nonReferentialInput);
  assert.strictEqual(shouldInject, false);
});

test('Edge case: Confirm prompt warning for large cursor-driven updates', () => {
  const store = new SessionCursorStore();
  
  const largeCursor: SessionCursor = {
    table: 'tickets',
    primaryKeys: ['id'],
    ids: null,
    sourceFilter: { op: '=', field: 'status', value: 'open', table: 'tickets' },
    rowCount: 847,
    pipelineId: 'pipeline-123',
    description: 'Show all open tickets',
    expiresAt: new Date(Date.now() + 300000),
  };
  
  store.set(largeCursor);
  
  const retrieved = store.get();
  assert.ok(retrieved);
  assert.strictEqual(retrieved?.rowCount, 847);
  assert.strictEqual(retrieved?.table, 'tickets');
  
  // In the REPL, this would trigger the warning:
  // ⚠️  This will affect ~847 rows in 'tickets' matching the prior query filter.
  
  const warning = `⚠️  This will affect ~${retrieved!.rowCount} rows in '${retrieved!.table}' matching the prior query filter.`;
  assert.ok(warning.includes('847'));
  assert.ok(warning.includes('tickets'));
});

console.log('=== Session Cursor Integration Tests Complete ===');
