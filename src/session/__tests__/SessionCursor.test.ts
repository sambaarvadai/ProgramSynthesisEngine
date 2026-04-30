/**
 * Tests for SessionCursor - cursor store and buildWhereFromCursor
 */

import { describe, test } from 'node:test';
import * as assert from 'node:assert';
import { extractCursor, buildWhereFromCursor, type SessionCursor } from '../SessionCursor.js';

describe('SessionCursor', () => {

  describe('extractCursor', () => {
    test('Small result (≤50 rows) — IDs stored', () => {
      const rows = [{ id: 1 }, { id: 2 }];
      const cursor = extractCursor(
        rows,
        'id',
        null,
        'pipeline-1',
        'test query',
        'leads'
      );

      assert.deepStrictEqual(cursor.ids, [1, 2]);
      assert.strictEqual(cursor.sourceFilter, null);
      assert.strictEqual(cursor.rowCount, 2);
      assert.strictEqual(cursor.table, 'leads');
    });

    test('Large result (>50 rows) — filter stored', () => {
      const rows = Array.from({ length: 51 }, (_, i) => ({ id: i + 1 }));
      const filter = [{ field: 'status', operator: '=', value: 'open' }];
      const cursor = extractCursor(
        rows,
        'id',
        filter,
        'pipeline-1',
        'test query',
        'leads'
      );

      assert.strictEqual(cursor.ids, null);
      assert.deepStrictEqual(cursor.sourceFilter, filter);
      assert.strictEqual(cursor.rowCount, 51);
      assert.strictEqual(cursor.table, 'leads');
    });

    test('Exactly 50 rows — IDs stored (threshold is exclusive)', () => {
      const rows = Array.from({ length: 50 }, (_, i) => ({ id: i + 1 }));
      const cursor = extractCursor(
        rows,
        'id',
        null,
        'pipeline-1',
        'test query',
        'leads'
      );

      assert.strictEqual(cursor.ids?.length, 50);
      assert.strictEqual(cursor.sourceFilter, null);
    });
  });

  describe('buildWhereFromCursor', () => {
    test('IDs path — single ID', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: [1],
        sourceFilter: null,
        rowCount: 1,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"id" = $1');
      assert.deepStrictEqual(result.params, [1]);
      assert.strictEqual(result.isBulk, false);
    });

    test('IDs path — multiple IDs', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: [1, 2, 3],
        sourceFilter: null,
        rowCount: 3,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"id" = ANY($1)');
      assert.deepStrictEqual(result.params, [[1, 2, 3]]);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — simple equality', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'status', operator: '=', value: 'open' }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"status" = $1');
      assert.deepStrictEqual(result.params, ['open']);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — case-insensitive equality', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'name', operator: '=', value: 'John', caseInsensitive: true }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, 'LOWER("name") = LOWER($1)');
      assert.deepStrictEqual(result.params, ['John']);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — IN operator', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'status', operator: 'IN', value: ['open', 'pending'] }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"status" IN ($1, $2)');
      assert.deepStrictEqual(result.params, ['open', 'pending']);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — NOT IN operator', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'status', operator: 'NOT IN', value: ['closed', 'archived'] }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"status" NOT IN ($1, $2)');
      assert.deepStrictEqual(result.params, ['closed', 'archived']);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — comparison operators', () => {
      const operators = ['>', '<', '>=', '<=', '!='];
      for (const op of operators) {
        const cursor: SessionCursor = {
          table: 'leads',
          primaryKeys: ['id'],
          ids: null,
          sourceFilter: [{ field: 'value', operator: op as any, value: 100 }],
          rowCount: 100,
          pipelineId: 'pipeline-1',
          description: 'test query',
          expiresAt: new Date(Date.now() + 300000)
        };

        const result = buildWhereFromCursor(cursor, 1);
        assert.strictEqual(result.clause, `"value" ${op} $1`);
        assert.deepStrictEqual(result.params, [100]);
        assert.strictEqual(result.isBulk, true);
      }
    });

    test('Filter path — IS NULL', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'deleted_at', operator: 'IS NULL' }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"deleted_at" IS NULL');
      assert.deepStrictEqual(result.params, []);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — IS NOT NULL', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'email', operator: 'IS NOT NULL' }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"email" IS NOT NULL');
      assert.deepStrictEqual(result.params, []);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — BETWEEN', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'created_at', operator: 'BETWEEN', value: ['2024-01-01', '2024-12-31'] }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"created_at" BETWEEN $1 AND $2');
      assert.deepStrictEqual(result.params, ['2024-01-01', '2024-12-31']);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — LIKE', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'name', operator: 'LIKE', value: '%John%' }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"name" ILIKE $1');
      assert.deepStrictEqual(result.params, ['%John%']);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — multiple filters (AND)', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [
          { field: 'status', operator: '=', value: 'open' },
          { field: 'priority', operator: '>', value: 5 }
        ],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 1);
      assert.strictEqual(result.clause, '"status" = $1 AND "priority" > $2');
      assert.deepStrictEqual(result.params, ['open', 5]);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — custom startIndex', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'status', operator: '=', value: 'open' }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      const result = buildWhereFromCursor(cursor, 5);
      assert.strictEqual(result.clause, '"status" = $5');
      assert.deepStrictEqual(result.params, ['open']);
      assert.strictEqual(result.isBulk, true);
    });

    test('Filter path — unknown operator skips with warning', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: [{ field: 'status', operator: 'UNKNOWN' as any, value: 'open' }],
        rowCount: 100,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      // Should throw because no valid clauses were produced
      assert.throws(
        () => buildWhereFromCursor(cursor, 1),
        /sourceFilter produced no WHERE clauses/
      );
    });

    test('Cursor with neither ids nor sourceFilter throws', () => {
      const cursor: SessionCursor = {
        table: 'leads',
        primaryKeys: ['id'],
        ids: null,
        sourceFilter: null,
        rowCount: 0,
        pipelineId: 'pipeline-1',
        description: 'test query',
        expiresAt: new Date(Date.now() + 300000)
      };

      assert.throws(
        () => buildWhereFromCursor(cursor, 1),
        /cursor has neither ids nor sourceFilter/
      );
    });
  });
});
