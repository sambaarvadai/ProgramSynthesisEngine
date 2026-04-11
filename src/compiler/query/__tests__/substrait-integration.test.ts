import { test } from 'node:test'
import * as assert from 'node:assert'
import { SubstraitTranslator } from '../../substrait/substrait-translator.js'
import { SubstraitTestHarness, loadCRMTestData } from './substrait-test-harness.js'
import type { QueryPlan, QueryDAGNode, QueryDAGNodeId } from '../../query-ast/query-ast.js'

test('SubstraitTranslator - basic integration with Python bridge', async () => {
  const translator = new SubstraitTranslator()
  const harness = new SubstraitTestHarness()

  try {
    // Load test data
    await loadCRMTestData(harness)

    // Create a simple QueryPlan (this would normally come from QueryPlanner)
    const queryPlan: QueryPlan = {
      root: 'scan_1',
      nodes: new Map([
        ['scan_1', {
          id: 'scan_1',
          kind: 'Scan',
          payload: {
            table: 'customers',
            alias: 'c',
            schema: {
              columns: [
                { name: 'id', type: { kind: 'number' }, nullable: false },
                { name: 'name', type: { kind: 'string' }, nullable: false },
                { name: 'email', type: { kind: 'string' }, nullable: false },
                { name: 'segment', type: { kind: 'string' }, nullable: false },
                { name: 'created_at', type: { kind: 'string' }, nullable: false }
              ]
            }
          }
        }]
      ]),
      optimizations: []
    }

    // Translate to Substrait
    const substraitBinary = await translator.translate(queryPlan)
    
    // Verify we got binary data
    assert.ok(substraitBinary instanceof Uint8Array, 'Should return Uint8Array')
    assert.ok(substraitBinary.length > 0, 'Should have non-zero length')

    // Execute with DuckDB via Substrait
    const results = await harness.executeSubstrait(substraitBinary)
    
    // Verify results
    assert.ok(Array.isArray(results), 'Should return array of results')
    assert.ok(results.length > 0, 'Should have results')

    // Compare with direct SQL execution
    const sqlResults = await harness.executeSQL('SELECT * FROM customers')
    
    // Results should be similar (may differ in ordering)
    assert.strictEqual(results.length, sqlResults.length, 'Substrait and SQL should return same number of rows')

    console.log(`Substrait integration test passed! Got ${results.length} rows`)

  } finally {
    await harness.close()
  }
})

test('SubstraitTranslator - filter integration', async () => {
  const translator = new SubstraitTranslator()
  const harness = new SubstraitTestHarness()

  try {
    // Load test data
    await loadCRMTestData(harness)

    // Create a QueryPlan with filter
    const queryPlan: QueryPlan = {
      root: 'filter_1',
      nodes: new Map([
        ['scan_1', {
          id: 'scan_1',
          kind: 'Scan',
          payload: {
            table: 'customers',
            alias: 'c',
            schema: {
              columns: [
                { name: 'id', type: { kind: 'number' }, nullable: false },
                { name: 'name', type: { kind: 'string' }, nullable: false },
                { name: 'email', type: { kind: 'string' }, nullable: false },
                { name: 'segment', type: { kind: 'string' }, nullable: false },
                { name: 'created_at', type: { kind: 'string' }, nullable: false }
              ]
            }
          }
        }],
        ['filter_1', {
          id: 'filter_1',
          kind: 'Filter',
          input: 'scan_1',
          predicate: {
            kind: 'BinaryOp',
            op: '=',
            left: {
              kind: 'FieldRef',
              field: 'segment'
            },
            right: {
              kind: 'Literal',
              value: 'enterprise',
              type: { kind: 'string' }
            }
          }
        }]
      ]),
      optimizations: []
    }

    // Translate to Substrait
    const substraitBinary = await translator.translate(queryPlan)
    
    // Execute with DuckDB via Substrait
    const results = await harness.executeSubstrait(substraitBinary)
    
    // Verify results
    assert.ok(Array.isArray(results), 'Should return array of results')
    assert.ok(results.length > 0, 'Should have filtered results')

    // All results should have segment = 'enterprise'
    for (const row of results) {
      const segment = row.segment || row.c_segment || row['c.segment']
      assert.strictEqual(segment, 'enterprise', 'All rows should have enterprise segment')
    }

    // Compare with direct SQL execution
    const sqlResults = await harness.executeSQL("SELECT * FROM customers WHERE segment = 'enterprise'")
    
    assert.strictEqual(results.length, sqlResults.length, 'Substrait and SQL should return same number of filtered rows')

    console.log(`Substrait filter test passed! Got ${results.length} enterprise customers`)

  } finally {
    await harness.close()
  }
})
