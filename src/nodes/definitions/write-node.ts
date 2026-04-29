/**
 * WriteNode — writes rows back to the database.
 *
 * Supported modes:
 *   insert        — INSERT, fails on conflict
 *   insert_ignore — INSERT ON CONFLICT DO NOTHING
 *   update        — UPDATE with WHERE clause (requires whereColumns)
 *   upsert        — INSERT ON CONFLICT DO UPDATE SET (requires conflictColumns)
 *   delete        — DELETE with WHERE clause (requires whereColumns)
 *
 * WritePayload must be updated in payloads.ts to include:
 *   whereColumns?: string[]    — columns used in WHERE clause for update/delete
 *   updateColumns?: string[]   — columns to SET in update/upsert (defaults to non-conflict cols)
 *   conflictColumns?: string[] — columns in ON CONFLICT clause for upsert
 *   returning?: string[]       — columns to RETURNING (defaults to all)
 *   batchSize?: number         — rows per INSERT batch (default 100)
 */

import type { NodeDefinition } from '../../core/registry/node-registry.js'
import type { WritePayload } from '../payloads.js'
import type { DataValue } from '../../core/types/data-value.js'
import type { StorageBackend } from '../../core/storage/storage-backend.js'
import type { SchemaConfig } from '../../compiler/schema/schema-config.js'
import type { CalciteClient } from '../../compiler/calcite/index.js'
import { validationOk, validationFail } from '../../core/types/validation.js'
import type { SessionCursorStore } from '../../session/SessionCursor.js'
import { buildWhereFromCursor } from '../../session/SessionCursor.js'
import {
  classifyAllColumns,
  getBlockedOnUpdate,
  type SessionContext
} from '../../schema/ColumnClassifier.js'
import { crmSchema } from '../../schema/crm-schema.js'
import { buildWritePredicate, predicateToSQL } from './write-predicate-builder.js'

// ─── Types ───────────────────────────────────────────────────────────────────

type Row = Record<string, unknown>

interface WriteResult {
  rowsAffected: number
  rows: Row[]          // RETURNING rows when available
}

// ─── Factory ─────────────────────────────────────────────────────────────────

export function createWriteNodeDefinition(
  backend: StorageBackend,
  schema: SchemaConfig,
  calciteClient?: CalciteClient,
  sessionCursorStore?: SessionCursorStore
): NodeDefinition<WritePayload, DataValue, DataValue> {
  return {
    kind: 'write',
    displayName: 'Write to Database',
    icon: '💾',
    color: '#F59E0B',

    inputPorts: [{
      key: 'input',
      label: 'Input',
      dataType: { kind: 'tabular' },
      required: true,
    }],
    outputPorts: [{
      key: 'output',
      label: 'Output',
      dataType: { kind: 'tabular' },
      required: true,
    }],

    // ── Validation ────────────────────────────────────────────────────────────
    validate(payload: unknown) {
      const p = payload as WritePayload
      const errs: Array<{ code: string; message: string }> = []

      if (!p?.table)
        errs.push({ code: 'MISSING_TABLE', message: 'WriteNode requires table' })

      if (!p?.mode)
        errs.push({ code: 'MISSING_MODE', message: 'WriteNode requires mode (insert|insert_ignore|update|upsert|delete)' })

      if (!p?.columns?.length && p?.mode !== 'delete')
        errs.push({ code: 'MISSING_COLUMNS', message: 'WriteNode requires columns (except for delete mode)' })

      if ((p?.mode === 'update' || p?.mode === 'delete') && !p?.whereColumns?.length)
        errs.push({
          code: 'MISSING_WHERE_COLUMNS',
          message: `WriteNode mode '${p.mode}' requires whereColumns to identify which rows to affect`,
        })

      if (p?.mode === 'upsert' && !p?.conflictColumns?.length)
        errs.push({
          code: 'MISSING_CONFLICT_COLUMNS',
          message: "WriteNode mode 'upsert' requires conflictColumns for ON CONFLICT clause",
        })

      return errs.length ? validationFail(errs) : validationOk()
    },

    inferOutputType(_payload, inputType) { return inputType },

    // ── Execution ─────────────────────────────────────────────────────────────
    async execute(payload: WritePayload, input: DataValue, _ctx): Promise<DataValue> {
      console.log(`[WriteNode] Starting execution for table: ${payload.table}, mode: ${payload.mode}`);

      // ColumnClassifier: strip immutable columns from UPDATE
      if (payload.mode === 'update' && payload.table && (crmSchema as any).parsed?.tables.has(payload.table)) {
        const sessionCtx: SessionContext = {
          userId: _ctx.userId ? parseInt(_ctx.userId, 10) : 1,  // Use userId from execution context
          anchorIds: { workspaces: Number(1) }
        };
        const classifications = classifyAllColumns(payload.table, crmSchema, sessionCtx, 'update');
        const blocked = getBlockedOnUpdate(classifications);
        
        if (blocked.length > 0) {
          console.log(`[WriteNode] Stripping immutable columns from UPDATE: ${blocked.join(', ')}`);
          for (const col of blocked) {
            if (payload.staticValues) {
              delete payload.staticValues[col];
            }
          }
        }
      }

      // Normalise input to a flat row array
      const inputRows = extractRows(input)
      console.log(`[WriteNode] Extracted ${inputRows.length} rows from input`);

      // Check for empty input rows - try cursor-driven path for UPDATE/DELETE with whereColumns
      const isEmptyInput = inputRows.length === 0 || inputRows.every(r => Object.keys(r).length === 0);
      if (isEmptyInput && payload.whereColumns && payload.whereColumns.length > 0) {
        const cursor = sessionCursorStore?.get();
        if (cursor) {
          console.log(`[WriteNode] Using cursor-driven WHERE for ${payload.mode} on ${payload.table}`);

          // Emit warning for large cursor-driven updates
          if (cursor.rowCount > 50) {
            console.warn(
              `[WriteNode] Cursor-driven update will affect ~${cursor.rowCount} rows. ` +
              `Table: ${cursor.table}. Proceeding.`
            );
          }

          const { clause, params } = buildWhereFromCursor(cursor);
          console.log(`[WriteNode] Using cursor-driven WHERE: ${clause}`);

          // Build SET clause for UPDATE mode
          if (payload.mode === 'update') {
            const setColumns = payload.columns.filter(col => !payload.whereColumns?.includes(col));
            const staticValues = payload.staticValues ?? {};

            if (setColumns.length === 0 && Object.keys(staticValues).length === 0) {
              throw new Error('WriteNode UPDATE: no columns to SET');
            }

            // Combine static values and dynamic columns for SET clause
            const allSetColumns = [...new Set([...setColumns, ...Object.keys(staticValues)])];
            const setClause = allSetColumns.map((col, i) => `"${col}" = $${i + 1}`).join(', ');
            const setParams = allSetColumns.map(col => staticValues[col] ?? null);

            const sql = `UPDATE "${payload.table}" SET ${setClause} WHERE ${clause}`;
            const allParams = [...setParams, ...params];

            console.log(`[WriteNode] Cursor-driven UPDATE SQL: ${sql}`);
            await backend.rawQuery(sql, allParams);

            // Clear cursor after successful execution
            sessionCursorStore?.clear();
            console.log(`[WriteNode] Cursor consumed and cleared`);

            return createWriteSummaryResult(payload.table, payload.mode, cursor.rowCount, undefined, staticValues);
          } else if (payload.mode === 'delete') {
            const sql = `DELETE FROM "${payload.table}" WHERE ${clause}`;
            console.log(`[WriteNode] Cursor-driven DELETE SQL: ${sql}`);
            await backend.rawQuery(sql, params);

            // Clear cursor after successful execution
            sessionCursorStore?.clear();
            console.log(`[WriteNode] Cursor consumed and cleared`);

            return createWriteSummaryResult(payload.table, payload.mode, cursor.rowCount, undefined, undefined);
          } else {
            throw new Error(`WriteNode: cursor-driven execution not supported for mode '${payload.mode}'`);
          }
        } else {
          throw new Error(
            'WriteNode: no input rows and no session cursor available. ' +
            'Cannot determine WHERE target for update.'
          );
        }
      }

      if (!inputRows.length) {
        console.log(`[WriteNode] No input rows, returning early`);
        return input
      }

      // Debug: Log what we're working with
      console.log('[WriteNode] Payload:', JSON.stringify(payload, null, 2))
      console.log('[WriteNode] Input rows sample:', JSON.stringify(inputRows.slice(0, 2), null, 2))
      console.log('[WriteNode] Payload whereColumns:', JSON.stringify(payload.whereColumns, null, 2))

      // Try CalciteClient if available
      if (calciteClient) {
        try {
          let compiled: import('../../compiler/calcite/index.js').CalciteCompileResult
          
          switch (payload.mode) {
            case 'insert':
            case 'insert_ignore':
            case 'upsert':
              compiled = await calciteClient.compileInsert(payload, schema)
              break
            case 'update':
              compiled = await calciteClient.compileUpdate(payload, schema)
              break
            case 'delete':
              compiled = await calciteClient.compileDelete(payload, schema)
              break
            default:
              throw new Error(`Unknown mode: ${payload.mode}`)
          }
          
          console.log(`[WriteNode] Calcite SQL: ${compiled.sql}`)
          console.log(`[WriteNode] paramColumns: ${compiled.paramColumns}`)
          console.log(`[WriteNode] staticParams: ${JSON.stringify(compiled.staticParams)}`)
          
          const pool = (backend as any).pool
          let rowsAffected = 0
          
          // For UPDATE/DELETE with staticWhere only (no dynamic whereColumns)
          // Execute once, not once per row
          const hasOnlyStaticWhere = 
            (payload.staticWhere && Object.keys(payload.staticWhere).length > 0) &&
            (!payload.whereColumns || payload.whereColumns.length === 0)
          
          if (hasOnlyStaticWhere && (payload.mode === 'update' || payload.mode === 'delete')) {
            // Execute once - static WHERE doesn't need per-row iteration
            const dynamicParams = compiled.paramColumns.map(col => {
              // paramColumns are SET columns - get from staticValues or first row with alias resolution
              const firstRow = inputRows[0] ?? {}
              return payload.staticValues?.[col] ?? resolveColumnValue(col, firstRow, payload.columnAliases) ?? null
            })
            const allParams = [...dynamicParams, ...compiled.staticParams]
            const result = await pool.query(compiled.sql, allParams)
            console.log(`[WriteNode] Calcite ${payload.mode} on ${payload.table}: ${result.rowCount} rows affected`)

            const rowsAffected = result.rowCount ?? result.rowsAffected ?? 0;

            const filteredStaticValues = Object.fromEntries(
              Object.entries(payload.staticValues ?? {})
                .filter(([k]) => k !== 'workspace_id')
            );

            return createWriteSummaryResult(payload.table, payload.mode, rowsAffected, payload.staticWhere, filteredStaticValues);
          }
          
          // Per-row iteration (dynamic WHERE from input rows)
          for (const row of inputRows) {
            // Dynamic params from row fields - handle both SET columns and WHERE columns
            const dynamicParams = compiled.paramColumns.map(col => 
              resolveColumnValue(col, row, payload.columnAliases)
            )
            // Static params (literals) appended after
            const allParams = [...dynamicParams, ...compiled.staticParams]
            
            console.log(`[WriteNode] Executing with params:`, allParams)
            const result = await pool.query(compiled.sql, allParams)
            rowsAffected += result.rowCount ?? 0
          }
          
          console.log(`[WriteNode] Calcite ${payload.mode} on ${payload.table}: ${rowsAffected} rows affected`)
          
          // Return summary result for UPDATE/DELETE operations
          if (payload.mode === 'update' || payload.mode === 'delete') {
            return createWriteSummaryResult(payload.table, payload.mode, rowsAffected, payload.staticWhere, payload.staticValues)
          }
          
          return input
          
        } catch (err) {
          console.warn(
            `[WriteNode] Calcite failed, falling back to manual SQL:`,
            (err as Error).message
          )
          // Fall through to existing implementation
        }
      }

      // For write nodes with template expressions, we need to resolve them
      // Get the original step config to access template expressions
      const stepConfig = (_ctx as any).step?.config || {}
      const fields = stepConfig.fields as Record<string, any>
      
      if (fields) {
        console.log('WriteNode Debug - stepConfig fields:', JSON.stringify(fields, null, 2))
        
        // Resolve template expressions for each input row
        const resolvedRows = inputRows.map(row => {
          const resolvedRow: Row = {}
          for (const [targetField, template] of Object.entries(fields)) {
            if (typeof template === 'string' && template.startsWith('{{') && template.endsWith('}}')) {
              // Template expression like "{{order_id}}"
              const sourceField = template.slice(2, -2).trim()
              resolvedRow[targetField] = row[sourceField] ?? null
            } else if (typeof template === 'string' && template === 'NOW()') {
              // Special case for NOW()
              resolvedRow[targetField] = new Date().toISOString()
            } else {
              // Literal value
              resolvedRow[targetField] = template
            }
          }
          return resolvedRow
        })
        
        console.log('WriteNode Debug - resolvedRows (first 2):', JSON.stringify(resolvedRows.slice(0, 2), null, 2))
        
        // Use resolved rows for insertion
        const { dataRows, effectiveColumns } = prepareRows(resolvedRows, {
          ...payload,
          columns: Object.keys(fields)
        })
        
        if (!effectiveColumns.length && payload.mode !== 'delete') return input
        
        console.log('WriteNode Debug - final effectiveColumns:', effectiveColumns)
        console.log('WriteNode Debug - final dataRows (first 2):', JSON.stringify(dataRows.slice(0, 2), null, 2))
        
        // Execute with resolved data
        const batchSize = payload.batchSize ?? 100
        console.log(`[WriteNode] Executing ${payload.mode} with ${dataRows.length} rows, ${effectiveColumns.length} columns, batchSize: ${batchSize}`);
        
        try {
          switch (payload.mode) {
            case 'insert':
              console.log(`[WriteNode] Running INSERT on ${payload.table}`);
              await runInsert(backend, payload.table, effectiveColumns, dataRows, false, batchSize, payload.returning, payload.staticValues)
              console.log(`[WriteNode] INSERT completed successfully`);
              break
            case 'insert_ignore':
              console.log(`[WriteNode] Running INSERT IGNORE on ${payload.table}`);
              await runInsert(backend, payload.table, effectiveColumns, dataRows, true, batchSize, payload.returning, payload.staticValues)
              console.log(`[WriteNode] INSERT IGNORE completed successfully`);
              break
            case 'update':
              console.log(`[WriteNode] Running UPDATE on ${payload.table}`);
              await runUpdate(backend, payload.table, effectiveColumns, dataRows, payload.whereColumns!, payload.returning, payload)
              console.log(`[WriteNode] UPDATE completed successfully`);
              break
            case 'upsert':
              console.log(`[WriteNode] Running UPSERT on ${payload.table}`);
              await runUpsert(
                backend,
                payload.table,
                effectiveColumns,
                dataRows,
                payload.conflictColumns!,
                payload.updateColumns,
                payload.returning,
              )
              console.log(`[WriteNode] UPSERT completed successfully`);
              break
            case 'delete':
              console.log(`[WriteNode] Running DELETE on ${payload.table}`);
              await runDelete(backend, payload.table, dataRows, payload.whereColumns!, payload.returning)
              console.log(`[WriteNode] DELETE completed successfully`);
              break
            default:
              throw new Error(`WriteNode: unknown mode '${(payload as any).mode}'`)
          }
        } catch (error) {
          console.error(`[WriteNode] Database operation failed:`, (error as Error).message);
          console.error(`[WriteNode] Error details:`, {
            table: payload.table,
            mode: payload.mode,
            columns: effectiveColumns,
            rowCount: dataRows.length,
            firstRow: dataRows[0]
          });
          throw error;
        }
        
        // For UPDATE/DELETE operations, return summary result instead of input rows
        if (payload.mode === 'update' || payload.mode === 'delete') {
          console.log(`[WriteNode] Template execution returning summary for ${payload.mode}`);
          return createWriteSummaryResult(payload.table, payload.mode, 0, payload.staticWhere, payload.staticValues)
        }
        
        console.log(`[WriteNode] Execution completed, returning input`);
        return input
      }

      // Fallback to original logic for non-template cases
      console.log(`[WriteNode] Using fallback execution path (no template fields)`);
      // Validate and normalise columns present in actual rows
      const { dataRows, effectiveColumns, dynamicColumns } = prepareRows(inputRows, payload)
      console.log(`[WriteNode] Fallback - effectiveColumns: ${effectiveColumns.length}, dynamicColumns: ${dynamicColumns.length}, dataRows: ${dataRows.length}`);
      
      // Check for UPDATE-specific requirements
      if (payload.mode === 'update') {
        const hasStaticSets = Object.keys(payload.staticValues ?? {}).length > 0
        const hasDynamicSets = dynamicColumns.length > 0
        const hasWhere = (payload.whereColumns?.length ?? 0) > 0 || 
                         Object.keys(payload.staticWhere ?? {}).length > 0

        if (!hasStaticSets && !hasDynamicSets) {
          console.warn('[WriteNode] UPDATE has no SET values, skipping')
          return input
        }
        if (!hasWhere) {
          console.warn('[WriteNode] UPDATE has no WHERE clause, refusing to execute')
          return input
        }
        // Has values and WHERE - proceed even if dynamicColumns is empty
      } else if (!effectiveColumns.length && payload.mode !== 'delete' && !payload.staticValues) {
        // Only return early if there are no effective columns AND no static values
        console.log(`[WriteNode] No effective columns and no static values, returning early`);
        return input
      }

      const batchSize = payload.batchSize ?? 100
      console.log(`[WriteNode] Fallback executing ${payload.mode} with ${dataRows.length} rows`);

      let rowsAffected = 0; // Track actual rows affected for summary

      try {
        switch (payload.mode) {
          case 'insert':
            console.log(`[WriteNode] Fallback running INSERT on ${payload.table}`);
            await runInsert(backend, payload.table, dynamicColumns, dataRows, false, batchSize, payload.returning, payload.staticValues)
            break

          case 'insert_ignore':
            console.log(`[WriteNode] Fallback running INSERT IGNORE on ${payload.table}`);
            await runInsert(backend, payload.table, dynamicColumns, dataRows, true, batchSize, payload.returning, payload.staticValues)
            break

          case 'update':
            console.log(`[WriteNode] Fallback running UPDATE on ${payload.table}`);

            // Handle pure-static UPDATE (no dynamic columns, only staticValues)
            if (dynamicColumns.length === 0 && Object.keys(payload.staticValues ?? {}).length > 0) {
              // Build predicate from staticWhere if wherePredicate not set
              const predicate = payload.wherePredicate
                ?? buildWritePredicate(payload.staticWhere ?? {});

              if (!predicate && payload.mode === 'update') {
                // No WHERE clause on UPDATE — require explicit confirmation
                throw new Error(
                  'UPDATE without WHERE clause is not permitted. ' +
                  'Provide a filter to identify which rows to update.'
                );
              }

              // Build SET clause from both dynamic columns and static values
              const setClauses: string[] = [];
              const setParams: any[] = [];
              let paramIndex = 1;

              // 1. Dynamic columns — resolved from upstream row via columnAliases
              const dataRow = dataRows[0] ?? {};
              for (const col of payload.columns) {
                const val = dataRow[col];  // dataRow is the merged row from prepareRows
                if (val === undefined) {
                  console.log(`[WriteNode] Dropping dynamic column '${col}' - not resolvable`);
                  continue;
                }
                setClauses.push(`"${col}" = $${paramIndex}`);
                setParams.push(val);
                paramIndex++;
                console.log(`[WriteNode] Added dynamic column '${col}': ${val}`);
              }

              // 2. Static values — literal values from staticValues
              //    Skip columns already covered by dynamic columns above
              const dynamicCols = new Set(payload.columns);
              const whereKeys = new Set(Object.keys(payload.staticWhere ?? {}));
              
              for (const [col, val] of Object.entries(payload.staticValues ?? {})) {
                if (dynamicCols.has(col)) continue;  // already in SET via dynamic path
                if (whereKeys.has(col)) continue;   // skip WHERE keys
                
                const SQL_EXPRESSIONS = new Set(['NOW()', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_USER', 'gen_random_uuid()']);
                if (typeof val === 'string' && SQL_EXPRESSIONS.has(val.trim())) {
                  setClauses.push(`"${col}" = ${val.trim()}`);
                  // no param push — SQL expression embedded directly
                  console.log(`[WriteNode] Added static column '${col}' as SQL expression: ${val.trim()}`);
                } else {
                  setClauses.push(`"${col}" = $${paramIndex}`);
                  setParams.push(val);
                  paramIndex++;
                  console.log(`[WriteNode] Added static column '${col}': ${val}`);
                }
              }

              const setClause = setClauses.join(', ');

              // Build WHERE clause from predicate
              const whereResult = predicate
                ? predicateToSQL(predicate, setParams.length + 1)
                : null;

              const sql = whereResult
                ? `UPDATE "${payload.table}" SET ${setClause} WHERE ${whereResult.sql}`
                : `UPDATE "${payload.table}" SET ${setClause}`;

              const params = [...setParams, ...(whereResult?.params ?? [])];

              console.log(`[WriteNode] Bulk UPDATE SQL: ${sql}`);
              console.log(`[WriteNode] Bulk UPDATE params: ${JSON.stringify(params)}`);

              const result = await backend.rawQuery(sql, params);
              rowsAffected = result.rowCount ?? 0;
              console.log(`[WriteNode] ${result.rowCount} rows affected`);
            } else {
              // Standard UPDATE with dynamic columns
              await runUpdate(backend, payload.table, effectiveColumns, dataRows, payload.whereColumns!, payload.returning, payload)
            }
            break

          case 'upsert':
            console.log(`[WriteNode] Fallback running UPSERT on ${payload.table}`);
            await runUpsert(
              backend,
              payload.table,
              effectiveColumns,
              dataRows,
              payload.conflictColumns!,
              payload.updateColumns,
              payload.returning,
            )
            break

          case 'delete':
            console.log(`[WriteNode] Fallback running DELETE on ${payload.table}`);
            await runDelete(backend, payload.table, dataRows, payload.whereColumns!, payload.returning)
            break

          default:
            throw new Error(`WriteNode: unknown mode '${(payload as any).mode}'`)
        }
        console.log(`[WriteNode] Fallback execution completed successfully`);
      } catch (error) {
        console.error(`[WriteNode] Fallback execution failed:`, (error as Error).message);
        throw error;
      }

      // For UPDATE/DELETE operations, return summary result instead of input rows
      if (payload.mode === 'update' || payload.mode === 'delete') {
        console.log(`[WriteNode] Fallback returning summary for ${payload.mode}`);
        return createWriteSummaryResult(payload.table, payload.mode, rowsAffected, payload.staticWhere, payload.staticValues)
      }
      
      // Pass input through unchanged for INSERT operations - downstream nodes see what was written
      console.log(`[WriteNode] Fallback returning input`);
      return input
    },
  }
}

// ─── Row helpers ─────────────────────────────────────────────────────────────

function extractRows(input: DataValue | undefined | null): Row[] {
  if (input == null) return []          // null/undefined guard
  if (input.kind === 'void') return []
  if (input.kind === 'tabular')  return input.data.rows as Row[]
  if (input.kind === 'record')   return [input.data as Row]
  if (input.kind === 'collection') {
    return input.data.flatMap(item => extractRows(item))
  }
  return []
}

// Helper function to get column value with alias support
function getColumnValue(
  col: string,
  row: Record<string, any>,
  staticValues?: Record<string, any>,
  columnAliases?: Record<string, string>,
  isDynamicColumn: boolean = false
): any {
  // For dynamic columns, check upstream row data first (higher priority than staticValues)
  if (isDynamicColumn) {
    // Check direct column match in row
    if (col in row) return row[col]

    // Check alias match
    const aliasedField = columnAliases?.[col]
    if (aliasedField && aliasedField in row) return row[aliasedField]

    // Fall back to staticValues if no upstream data
    if (staticValues && col in staticValues) return staticValues[col]
  } else {
    // For static columns, staticValues takes priority
    if (staticValues && col in staticValues) return staticValues[col]

    // Check direct column match in row
    if (col in row) return row[col]

    // Check alias match
    const aliasedField = columnAliases?.[col]
    if (aliasedField && aliasedField in row) return row[aliasedField]
  }

  // Not found
  return undefined
}

// Helper function to resolve column value for Calcite path (matches getColumnValue logic but without staticValues)
function resolveColumnValue(
  col: string,
  row: Record<string, any>,
  columnAliases?: Record<string, string>
): any {
  // Direct match
  if (col in row) return row[col]
  // Alias match
  const aliasedField = columnAliases?.[col]
  if (aliasedField && aliasedField in row) return row[aliasedField]
  return null
}

// Helper function to create summary result for UPDATE/DELETE operations
function createWriteSummaryResult(
  table: string,
  operation: string,
  rowsAffected: number,
  staticWhere?: Record<string, any>,
  staticValues?: Record<string, any>
): DataValue {
  const resultRow = {
    table,
    operation,
    rows_affected: rowsAffected,
    where: JSON.stringify(staticWhere ?? {}),
    set: JSON.stringify(staticValues ?? {})
  }

  const schema = {
    columns: [
      { name: 'table', type: { kind: 'any' } as any, nullable: false },
      { name: 'operation', type: { kind: 'any' } as any, nullable: false },
      { name: 'rows_affected', type: { kind: 'any' } as any, nullable: false },
      { name: 'where', type: { kind: 'any' } as any, nullable: true },
      { name: 'set', type: { kind: 'any' } as any, nullable: true }
    ]
  }

  return {
    kind: 'tabular',
    data: {
      schema,
      rows: [resultRow]
    },
    schema
  }
}

function prepareRows(
  inputRows: Row[],
  payload: WritePayload,
): { dataRows: Row[]; effectiveColumns: string[]; dynamicColumns: string[] } {
  console.log(`[WriteNode] prepareRows called for table: ${payload.table}, mode: ${payload.mode}`);
  console.log(`[WriteNode] prepareRows - input rows: ${inputRows.length}, payload columns: ${payload.columns?.length || 0}`);
  
  if (payload.mode === 'delete') {
    console.log(`[WriteNode] prepareRows - delete mode, returning input rows`);
    return { dataRows: inputRows, effectiveColumns: [], dynamicColumns: [] }
  }

  const firstRow = inputRows[0]
  console.log(`[WriteNode] prepareRows - first row keys:`, Object.keys(firstRow));
  console.log(`[WriteNode] prepareRows - staticValues:`, payload.staticValues);
  console.log(`[WriteNode] prepareRows - columnAliases:`, payload.columnAliases);
  
  // Calculate effectiveColumns using the same resolution logic as row building
  const effectiveColumns = payload.columns.filter(col => {
    const value = getColumnValue(col, firstRow, payload.staticValues, payload.columnAliases, true)
    if (value !== undefined) return true

    console.warn(`[WriteNode] Dropping column '${col}' - not resolvable`)
    return false
  })
  
  console.log(`[WriteNode] prepareRows - effectiveColumns: ${effectiveColumns.length}: [${effectiveColumns.join(', ')}]`);

  // Helper to resolve special values like NOW()
  const resolveValue = (val: unknown): unknown => {
    if (val === 'NOW()' || val === 'now()') return new Date().toISOString()
    return val
  }

  // Merge static values into each row
  const dataRows = inputRows.map((row, index) => {
    const mergedRow: Row = {}

    // Resolve each column through the proper hierarchy
    for (const col of payload.columns) {
      const value = getColumnValue(col, row, payload.staticValues, payload.columnAliases, true)
      if (value !== undefined) {
        mergedRow[col] = resolveValue(value)
      }
    }
    
    if (index === 0) {
      console.log(`[WriteNode] Input rows sample:`, JSON.stringify(inputRows.slice(0, 2), null, 2))
      console.log(`[WriteNode] Payload whereColumns:`, JSON.stringify(payload.whereColumns, null, 2))
      console.log(`[WriteNode] prepareRows - sample merged row:`, mergedRow);
    }
    return mergedRow;
  })

  // Calculate dynamic columns (those not in staticValues AND have values in data rows)
  const staticKeys = new Set(Object.keys(payload.staticValues ?? {}))
  const dynamicColumns = payload.columns.filter(col => {
    if (staticKeys.has(col)) return false
    // Only include if at least one row has a value for this column
    return dataRows.some(row => row[col] !== undefined)
  })

  console.log(`[WriteNode] prepareRows - staticValues:`, payload.staticValues);
  console.log(`[WriteNode] prepareRows - staticKeys: [${Array.from(staticKeys).join(', ')}]`);
  console.log(`[WriteNode] prepareRows - original columns: [${payload.columns.join(', ')}]`);
  console.log(`[WriteNode] prepareRows - dynamicColumns: [${dynamicColumns.join(', ')}]`);
  console.log(`[WriteNode] prepareRows - returning ${dataRows.length} dataRows`);
  return { dataRows, effectiveColumns, dynamicColumns }
}

// ─── SQL helpers ─────────────────────────────────────────────────────────────

/** Quote a Postgres identifier to prevent reserved-word collisions. */
function qi(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}

/** Build a $1, $2, … placeholder string for N values starting at offset. */
function placeholders(count: number, offset = 0): string {
  return Array.from({ length: count }, (_, i) => `$${offset + i + 1}`).join(', ')
}

// ─── INSERT ──────────────────────────────────────────────────────────────────

async function runInsert(
  backend: StorageBackend,
  table: string,
  columns: string[],
  rows: Row[],
  ignoreConflict: boolean,
  batchSize: number,
  returning?: string[],
  staticValues?: Record<string, any>,
): Promise<void> {
  // Merge static values into the column list for the INSERT statement
  const allColumns = staticValues ? [...new Set([...columns, ...Object.keys(staticValues)])] : columns
  const colList = allColumns.map(qi).join(', ')
  const onConflict = ignoreConflict ? ' ON CONFLICT DO NOTHING' : ''
  const returningClause = returning?.length ? ` RETURNING ${returning.map(qi).join(', ')}` : ''

  console.log(`[WriteNode] runInsert - columns: [${columns.join(', ')}], staticValues:`, staticValues);
  console.log(`[WriteNode] runInsert - allColumns: [${allColumns.join(', ')}]`);

  // Process in batches to stay within Postgres parameter limits (~65k)
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize)

    const rowPlaceholders = batch.map((_, rowIdx) =>
      `(${placeholders(allColumns.length, rowIdx * allColumns.length)})`
    ).join(', ')

    const values = batch.flatMap(row => 
      allColumns.map(col => {
        // If it's a static value, use that; otherwise use the row value
        if (staticValues && col in staticValues) {
          return staticValues[col]
        }
        return row[col]
      })
    )

    const sql = `INSERT INTO ${qi(table)} (${colList}) VALUES ${rowPlaceholders}${onConflict}${returningClause}`
    await backend.rawQuery(sql, values)
  }
}

// ─── UPDATE ──────────────────────────────────────────────────────────────────

async function runUpdate(
  backend: StorageBackend,
  table: string,
  setColumns: string[],
  rows: Row[],
  whereColumns: string[],
  returning?: string[],
  payload?: WritePayload
): Promise<void> {
  // Validate whereColumns are in the rows
  const firstRow = rows[0]
  const missingWhere = whereColumns.filter(c => !(c in firstRow))
  if (missingWhere.length) {
    throw new Error(
      `WriteNode UPDATE [${table}]: whereColumns not in input rows: ${missingWhere.join(', ')}. ` +
      `Available: ${Object.keys(firstRow).join(', ')}`,
    )
  }

  // Exclude whereColumns from SET clause (they identify the row, not update it)
  const actualSetCols = setColumns.filter(c => !whereColumns.includes(c))
  if (!actualSetCols.length) {
    throw new Error(
      `WriteNode UPDATE [${table}]: all columns are whereColumns — nothing to SET. ` +
      `Add non-where columns to the columns list.`,
    )
  }

  const returningClause = returning?.length ? ` RETURNING ${returning.map(qi).join(', ')}` : ''

  // Build predicate from staticWhere if available
  const predicate = payload?.wherePredicate
    ?? buildWritePredicate(payload?.staticWhere ?? {});

  if (!predicate && payload?.mode === 'update' && whereColumns.length === 0) {
    throw new Error(
      `UPDATE on ${table} refused — no WHERE clause`
    );
  }

  // Execute one UPDATE per row (each row may match different WHERE values)
  for (const row of rows) {
    const params: unknown[] = []

    const setClause = actualSetCols.map(col => {
      params.push(row[col])
      return `${qi(col)} = $${params.length}`
    }).join(', ')

    // Build WHERE clause using predicateToSQL for static conditions,
    // or dynamic whereColumns for row-specific conditions
    let whereClause: string;
    if (predicate) {
      // Static WHERE conditions from predicateToSQL
      const setParamCount = actualSetCols.length;
      const whereResult = predicateToSQL(predicate, setParamCount + 1);
      whereClause = whereResult.sql;
      params.push(...whereResult.params);
    } else if (whereColumns.length > 0) {
      // Dynamic WHERE conditions from row values
      whereClause = whereColumns.map(col => {
        params.push(row[col])
        return `${qi(col)} = $${params.length}`
      }).join(' AND ');
    } else {
      throw new Error(
        `UPDATE on ${table} refused — no WHERE clause`
      );
    }

    const sql = `UPDATE ${qi(table)} SET ${setClause} WHERE ${whereClause}${returningClause}`
    await backend.rawQuery(sql, params)
  }
}

// ─── UPSERT ──────────────────────────────────────────────────────────────────

async function runUpsert(
  backend: StorageBackend,
  table: string,
  columns: string[],
  rows: Row[],
  conflictColumns: string[],
  updateColumns: string[] | undefined,
  returning?: string[],
): Promise<void> {
  // Columns to SET on conflict — defaults to non-conflict columns
  const setCols = updateColumns ?? columns.filter(c => !conflictColumns.includes(c))
  if (!setCols.length) {
    // Nothing to update on conflict — equivalent to insert_ignore
    await runInsert(backend, table, columns, rows, true, 100, returning, undefined)
    return
  }

  const colList = columns.map(qi).join(', ')
  const conflictList = conflictColumns.map(qi).join(', ')
  const returningClause = returning?.length ? ` RETURNING ${returning.map(qi).join(', ')}` : ''

  // Execute one UPSERT per row to keep param indexing simple
  for (const row of rows) {
    const values = columns.map(col => row[col])

    const rowPlaceholders = placeholders(columns.length)

    const updateClause = setCols.map(col => {
      const idx = columns.indexOf(col) + 1
      return `${qi(col)} = $${idx}`
    }).join(', ')

    const sql =
      `INSERT INTO ${qi(table)} (${colList}) VALUES (${rowPlaceholders}) ` +
      `ON CONFLICT (${conflictList}) DO UPDATE SET ${updateClause}` +
      returningClause

    await backend.rawQuery(sql, values)
  }
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

async function runDelete(
  backend: StorageBackend,
  table: string,
  rows: Row[],
  whereColumns: string[],
  returning?: string[],
): Promise<void> {
  if (!whereColumns.length) {
    throw new Error(
      `WriteNode DELETE [${table}]: whereColumns is empty. ` +
      `Refusing to delete all rows without a WHERE clause.`,
    )
  }

  const firstRow = rows[0]
  const missingWhere = whereColumns.filter(c => !(c in firstRow))
  if (missingWhere.length) {
    throw new Error(
      `WriteNode DELETE [${table}]: whereColumns not in input rows: ${missingWhere.join(', ')}. ` +
      `Available: ${Object.keys(firstRow).join(', ')}`,
    )
  }

  const returningClause = returning?.length ? ` RETURNING ${returning.map(qi).join(', ')}` : ''

  // One DELETE per row to target individual records
  for (const row of rows) {
    const params: unknown[] = []

    const whereClause = whereColumns.map(col => {
      const val = row[col]
      if (val === null || val === undefined) {
        // IS NULL instead of = NULL
        return `${qi(col)} IS NULL`
      }
      params.push(val)
      return `${qi(col)} = $${params.length}`
    }).join(' AND ')

    const sql = `DELETE FROM ${qi(table)} WHERE ${whereClause}${returningClause}`
    await backend.rawQuery(sql, params)
  }
}