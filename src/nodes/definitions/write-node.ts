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
  calciteClient?: CalciteClient
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
      
      // Normalise input to a flat row array
      const inputRows = extractRows(input)
      console.log(`[WriteNode] Extracted ${inputRows.length} rows from input`);
      
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
              // paramColumns are SET columns - get from staticValues or first row
              return inputRows[0]?.[col] ?? payload.staticValues?.[col] ?? null
            })
            const allParams = [...dynamicParams, ...compiled.staticParams]
            const result = await pool.query(compiled.sql, allParams)
            console.log(`[WriteNode] Calcite ${payload.mode} on ${payload.table}: ${result.rowCount} rows affected`)
            return input
          }
          
          // Per-row iteration (dynamic WHERE from input rows)
          for (const row of inputRows) {
            // Dynamic params from row fields - handle both SET columns and WHERE columns
            const dynamicParams = compiled.paramColumns.map(col => row[col] ?? null)
            // Static params (literals) appended after
            const allParams = [...dynamicParams, ...compiled.staticParams]
            
            console.log(`[WriteNode] Executing with params:`, allParams)
            const result = await pool.query(compiled.sql, allParams)
            rowsAffected += result.rowCount ?? 0
          }
          
          console.log(`[WriteNode] Calcite ${payload.mode} on ${payload.table}: ${rowsAffected} rows affected`)
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
              await runInsert(backend, payload.table, effectiveColumns, dataRows, false, batchSize, payload.returning)
              console.log(`[WriteNode] INSERT completed successfully`);
              break
            case 'insert_ignore':
              console.log(`[WriteNode] Running INSERT IGNORE on ${payload.table}`);
              await runInsert(backend, payload.table, effectiveColumns, dataRows, true, batchSize, payload.returning)
              console.log(`[WriteNode] INSERT IGNORE completed successfully`);
              break
            case 'update':
              console.log(`[WriteNode] Running UPDATE on ${payload.table}`);
              await runUpdate(backend, payload.table, effectiveColumns, dataRows, payload.whereColumns!, payload.returning)
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
        
        console.log(`[WriteNode] Execution completed, returning input`);
        return input
      }

      // Fallback to original logic for non-template cases
      console.log(`[WriteNode] Using fallback execution path (no template fields)`);
      // Validate and normalise columns present in actual rows
      const { dataRows, effectiveColumns } = prepareRows(inputRows, payload)
      console.log(`[WriteNode] Fallback - effectiveColumns: ${effectiveColumns.length}, dataRows: ${dataRows.length}`);
      
      if (!effectiveColumns.length && payload.mode !== 'delete') {
        console.log(`[WriteNode] No effective columns and not delete mode, returning early`);
        return input
      }

      const batchSize = payload.batchSize ?? 100
      console.log(`[WriteNode] Fallback executing ${payload.mode} with ${dataRows.length} rows`);

      try {
        switch (payload.mode) {
          case 'insert':
            console.log(`[WriteNode] Fallback running INSERT on ${payload.table}`);
            await runInsert(backend, payload.table, effectiveColumns, dataRows, false, batchSize, payload.returning)
            break

          case 'insert_ignore':
            console.log(`[WriteNode] Fallback running INSERT IGNORE on ${payload.table}`);
            await runInsert(backend, payload.table, effectiveColumns, dataRows, true, batchSize, payload.returning)
            break

          case 'update':
            console.log(`[WriteNode] Fallback running UPDATE on ${payload.table}`);
            await runUpdate(backend, payload.table, effectiveColumns, dataRows, payload.whereColumns!, payload.returning)
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

      // Pass input through unchanged - downstream nodes see what was written
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

function prepareRows(
  inputRows: Row[],
  payload: WritePayload,
): { dataRows: Row[]; effectiveColumns: string[] } {
  console.log(`[WriteNode] prepareRows called for table: ${payload.table}, mode: ${payload.mode}`);
  console.log(`[WriteNode] prepareRows - input rows: ${inputRows.length}, payload columns: ${payload.columns?.length || 0}`);
  
  if (payload.mode === 'delete') {
    console.log(`[WriteNode] prepareRows - delete mode, returning input rows`);
    return { dataRows: inputRows, effectiveColumns: [] }
  }

  const firstRow = inputRows[0]
  console.log(`[WriteNode] prepareRows - first row keys:`, Object.keys(firstRow));
  console.log(`[WriteNode] prepareRows - staticValues:`, payload.staticValues);
  
  // Columns from input rows
  const inputColumns = payload.columns.filter(c => c in firstRow)
  const missingCols = payload.columns.filter(c => !(c in firstRow) && !(c in (payload.staticValues ?? {})))
  console.log(`[WriteNode] prepareRows - inputColumns: ${inputColumns.length}, missingCols: ${missingCols.length}`);
  
  if (missingCols.length) {
    console.warn(
      `WriteNode [${payload.table}]: columns not in input rows or staticValues: ${missingCols.join(', ')}. ` +
      `Available in row: ${Object.keys(firstRow).join(', ')}`
    )
  }

  // Static columns
  const staticCols = Object.keys(payload.staticValues ?? {})
  console.log(`[WriteNode] prepareRows - staticCols: ${staticCols.length}`);
  
  // Effective columns = input columns + static columns (deduplicated)
  const effectiveColumns = [...new Set([...inputColumns, ...staticCols])]
  console.log(`[WriteNode] prepareRows - effectiveColumns: ${effectiveColumns.length}:`, effectiveColumns);

  if (effectiveColumns.length === 0) {
    console.error(`[WriteNode] prepareRows - no effective columns!`);
    throw new Error(
      `WriteNode [${payload.table}]: no columns to write. ` +
      `Specified: ${payload.columns.join(', ')}. ` +
      `Available in row: ${Object.keys(firstRow).join(', ')}`
    )
  }

  // Helper to resolve special values like NOW()
  const resolveValue = (val: unknown): unknown => {
    if (val === 'NOW()' || val === 'now()') return new Date().toISOString()
    return val
  }

  // Merge static values into each row
  const dataRows = inputRows.map((row, index) => {
    const mergedRow = {
      ...Object.fromEntries(inputColumns.map(col => [col, row[col] ?? null])),
      ...Object.fromEntries(
        Object.entries(payload.staticValues ?? {}).map(([k, v]) => [k, resolveValue(v)])
      )
    };
    if (index === 0) {
      console.log(`[WriteNode] Input rows sample:`, JSON.stringify(inputRows.slice(0, 2), null, 2))
      console.log(`[WriteNode] Payload whereColumns:`, JSON.stringify(payload.whereColumns, null, 2))
      console.log(`[WriteNode] prepareRows - sample merged row:`, mergedRow);
    }
    return mergedRow;
  })

  console.log(`[WriteNode] prepareRows - returning ${dataRows.length} dataRows`);
  return { dataRows, effectiveColumns }
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
): Promise<void> {
  const colList = columns.map(qi).join(', ')
  const onConflict = ignoreConflict ? ' ON CONFLICT DO NOTHING' : ''
  const returningClause = returning?.length ? ` RETURNING ${returning.map(qi).join(', ')}` : ''

  // Process in batches to stay within Postgres parameter limits (~65k)
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize)

    const rowPlaceholders = batch.map((_, rowIdx) =>
      `(${placeholders(columns.length, rowIdx * columns.length)})`
    ).join(', ')

    const values = batch.flatMap(row => columns.map(col => row[col]))

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

  // Execute one UPDATE per row (each row may match different WHERE values)
  for (const row of rows) {
    const params: unknown[] = []

    const setClause = actualSetCols.map(col => {
      params.push(row[col])
      return `${qi(col)} = $${params.length}`
    }).join(', ')

    const whereClause = whereColumns.map(col => {
      params.push(row[col])
      return `${qi(col)} = $${params.length}`
    }).join(' AND ')

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
    await runInsert(backend, table, columns, rows, true, 100, returning)
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