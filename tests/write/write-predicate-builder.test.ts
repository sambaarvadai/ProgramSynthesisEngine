import { test } from 'node:test';
import assert from 'node:assert';
import { buildWritePredicate, predicateToSQL } from '../../src/nodes/definitions/write-predicate-builder.js';

test('buildWritePredicate - Shape 1: array of IDs', () => {
  const result = buildWritePredicate({ id: [1, 2] });
  assert.deepStrictEqual(result, { kind: 'any', column: 'id', values: [1, 2] });
});

test('buildWritePredicate - Shape 2: scalar filter', () => {
  const result = buildWritePredicate({ status: 'working' });
  assert.deepStrictEqual(result, { kind: 'eq', column: 'status', value: 'working' });
});

test('buildWritePredicate - Shape 2b: multiple scalar conditions', () => {
  const result = buildWritePredicate({ status: 'open', account_id: 7 });
  assert.deepStrictEqual(result, {
    kind: 'and',
    left: { kind: 'eq', column: 'status', value: 'open' },
    right: { kind: 'eq', column: 'account_id', value: 7 }
  });
});

test('buildWritePredicate - Shape 1 single element', () => {
  const result = buildWritePredicate({ id: [11] });
  assert.deepStrictEqual(result, { kind: 'eq', column: 'id', value: 11 });
});

test('buildWritePredicate - Empty where', () => {
  const result = buildWritePredicate({});
  assert.strictEqual(result, null);
});

test('buildWritePredicate - Null value (IS NULL)', () => {
  const result = buildWritePredicate({ deleted_at: null });
  assert.deepStrictEqual(result, { kind: 'isNull', column: 'deleted_at' });
});

test('buildWritePredicate - Empty array', () => {
  const result = buildWritePredicate({ id: [] });
  assert.strictEqual(result, null);
});

test('predicateToSQL - eq predicate', () => {
  const result = predicateToSQL({ kind: 'eq', column: 'status', value: 'working' });
  assert.deepStrictEqual(result, {
    sql: '"status" = $1',
    params: ['working'],
    nextIndex: 2
  });
});

test('predicateToSQL - any predicate', () => {
  const result = predicateToSQL({ kind: 'any', column: 'id', values: [1, 2] });
  assert.deepStrictEqual(result, {
    sql: '"id" = ANY($1)',
    params: [[1, 2]],
    nextIndex: 2
  });
});

test('predicateToSQL - in predicate', () => {
  const result = predicateToSQL({ kind: 'in', column: 'status', values: ['open', 'closed'] });
  assert.deepStrictEqual(result, {
    sql: '"status" IN ($1, $2)',
    params: ['open', 'closed'],
    nextIndex: 3
  });
});

test('predicateToSQL - range predicate', () => {
  const result = predicateToSQL({ kind: 'range', column: 'price', min: 10, max: 100 });
  assert.deepStrictEqual(result, {
    sql: '"price" BETWEEN $1 AND $2',
    params: [10, 100],
    nextIndex: 3
  });
});

test('predicateToSQL - isNull predicate', () => {
  const result = predicateToSQL({ kind: 'isNull', column: 'deleted_at' });
  assert.deepStrictEqual(result, {
    sql: '"deleted_at" IS NULL',
    params: [],
    nextIndex: 1
  });
});

test('predicateToSQL - notNull predicate', () => {
  const result = predicateToSQL({ kind: 'notNull', column: 'deleted_at' });
  assert.deepStrictEqual(result, {
    sql: '"deleted_at" IS NOT NULL',
    params: [],
    nextIndex: 1
  });
});

test('predicateToSQL - and predicate', () => {
  const result = predicateToSQL({
    kind: 'and',
    left: { kind: 'eq', column: 'status', value: 'open' },
    right: { kind: 'eq', column: 'account_id', value: 7 }
  });
  assert.deepStrictEqual(result, {
    sql: '("status" = $1 AND "account_id" = $2)',
    params: ['open', 7],
    nextIndex: 3
  });
});

test('predicateToSQL - or predicate', () => {
  const result = predicateToSQL({
    kind: 'or',
    left: { kind: 'eq', column: 'status', value: 'open' },
    right: { kind: 'eq', column: 'status', value: 'closed' }
  });
  assert.deepStrictEqual(result, {
    sql: '("status" = $1 OR "status" = $2)',
    params: ['open', 'closed'],
    nextIndex: 3
  });
});

test('predicateToSQL - raw predicate', () => {
  const result = predicateToSQL({ kind: 'raw', sql: 'id > 10 AND id < 100', params: [] });
  assert.deepStrictEqual(result, {
    sql: 'id > 10 AND id < 100',
    params: [],
    nextIndex: 1
  });
});

test('predicateToSQL - custom startIndex', () => {
  const result = predicateToSQL({ kind: 'eq', column: 'status', value: 'working' }, 5);
  assert.deepStrictEqual(result, {
    sql: '"status" = $5',
    params: ['working'],
    nextIndex: 6
  });
});

test('predicateToSQL - nested and with custom startIndex', () => {
  const result = predicateToSQL({
    kind: 'and',
    left: { kind: 'eq', column: 'status', value: 'open' },
    right: { kind: 'eq', column: 'account_id', value: 7 }
  }, 3);
  assert.deepStrictEqual(result, {
    sql: '("status" = $3 AND "account_id" = $4)',
    params: ['open', 7],
    nextIndex: 5
  });
});

test('Integration - Shape 1: array of IDs -> SQL', () => {
  const predicate = buildWritePredicate({ id: [1, 2] });
  assert.deepStrictEqual(predicate, { kind: 'any', column: 'id', values: [1, 2] });

  const sql = predicateToSQL(predicate!);
  assert.deepStrictEqual(sql, {
    sql: '"id" = ANY($1)',
    params: [[1, 2]],
    nextIndex: 2
  });
});

test('Integration - Shape 2: scalar filter -> SQL', () => {
  const predicate = buildWritePredicate({ status: 'working' });
  assert.deepStrictEqual(predicate, { kind: 'eq', column: 'status', value: 'working' });

  const sql = predicateToSQL(predicate!);
  assert.deepStrictEqual(sql, {
    sql: '"status" = $1',
    params: ['working'],
    nextIndex: 2
  });
});

test('Integration - Shape 2b: multiple scalar conditions -> SQL', () => {
  const predicate = buildWritePredicate({ status: 'open', account_id: 7 });
  assert.deepStrictEqual(predicate, {
    kind: 'and',
    left: { kind: 'eq', column: 'status', value: 'open' },
    right: { kind: 'eq', column: 'account_id', value: 7 }
  });

  const sql = predicateToSQL(predicate!);
  assert.deepStrictEqual(sql, {
    sql: '("status" = $1 AND "account_id" = $2)',
    params: ['open', 7],
    nextIndex: 3
  });
});

test('Integration - Shape 1 single element -> SQL', () => {
  const predicate = buildWritePredicate({ id: [11] });
  assert.deepStrictEqual(predicate, { kind: 'eq', column: 'id', value: 11 });

  const sql = predicateToSQL(predicate!);
  assert.deepStrictEqual(sql, {
    sql: '"id" = $1',
    params: [11],
    nextIndex: 2
  });
});

test('Integration - Null value -> SQL', () => {
  const predicate = buildWritePredicate({ deleted_at: null });
  assert.deepStrictEqual(predicate, { kind: 'isNull', column: 'deleted_at' });

  const sql = predicateToSQL(predicate!);
  assert.deepStrictEqual(sql, {
    sql: '"deleted_at" IS NULL',
    params: [],
    nextIndex: 1
  });
});
