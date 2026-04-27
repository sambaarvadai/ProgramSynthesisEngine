import type { QueryIntent } from '../query-ast/query-intent.js'
import type { WritePayload } from '../../nodes/payloads.js'
import type { SchemaConfig } from '../schema/schema-config.js'
import { getAppConfig } from '../../config/app-config.js'

export type CalciteCompileResult = {
  sql: string
  paramColumns: string[]    // row field names -> $1, $2...
  staticParams: unknown[]   // literal values appended
  aggregated: boolean      // whether aggregation is present
  writePayload?: WritePayload
  dialect: string
  optimizations: string[]  // list of optimizations applied
}

export class CalciteClient {
  private baseUrl: string
  private config: ReturnType<typeof getAppConfig>

  constructor(baseUrl?: string) {
    this.config = getAppConfig();
    this.baseUrl = baseUrl || this.config.services.calciteUrl;
  }

  async isAvailable(): Promise<boolean> {
    try {
      const res = await fetch(`${this.baseUrl}/health`, { 
        signal: AbortSignal.timeout(this.config.services.calciteHealthTimeout) 
      })
      return res.ok
    } catch { return false }
  }

  async compileSelect(
    intent: QueryIntent,
    schema: SchemaConfig
  ): Promise<CalciteCompileResult> {
    console.log('[Calcite Client] Original intent table:', intent.table);
    console.log('[Calcite Client] Original intent joins:', intent.joins);
    console.log('[Calcite Client] Original intent filters:', intent.filters);
    console.log('[Calcite Client] Original intent columns:', intent.columns);
    console.log('[Calcite Client] Original intent orderBy:', intent.orderBy);
    console.log('[Calcite Client] Original intent groupBy:', intent.groupBy);
    console.log('[Calcite Client] Original intent having:', intent.having);
    
    const calciteSchema = schemaToCalcite(schema, intent);
    console.log('[Calcite Client] Calcite schema tables:', calciteSchema.map(t => t.name));
    
    const body = {
      schema: calciteSchema,
      table: intent.table, // Keep original case
      columns: intent.columns,
      joins: intent.joins?.map(j => ({
        table: j.table, // Keep original case
        kind: j.kind ?? 'INNER',
        onLeft: j.on?.left,
        onRight: j.on?.right
      })),
      filters: intent.filters?.map(f => ({
        field: f.field,
        table: f.table, // Keep original case
        operator: f.operator,
        value: f.value
      })),
      groupBy: intent.groupBy,
      aggregations: intent.aggregations,
      orderBy: intent.orderBy,
      limit: intent.limit,
      offset: intent.offset,
      dialect: 'POSTGRESQL'
    }
    
    return this.post('/compile/select', body)
  }

  async compileInsert(
    payload: WritePayload,
    schema: SchemaConfig
  ): Promise<CalciteCompileResult> {
    // Deduplicate columns: separate dynamic from static columns
    const staticKeys = new Set(Object.keys(payload.staticValues ?? {}))
    
    // Dynamic columns: from input rows only (not also in staticValues)
    const dynamicColumns = payload.columns.filter(col => !staticKeys.has(col))
    
    console.log(`[Calcite Client] Deduplicating columns:`);
    console.log(`  Original columns: [${payload.columns.join(', ')}]`);
    console.log(`  Static keys: [${Array.from(staticKeys).join(', ')}]`);
    console.log(`  Dynamic columns: [${dynamicColumns.join(', ')}]`);

    const body = {
      schema: schemaToCalcite(schema, null, payload.table),
      table: payload.table, // Keep original case
      columns: dynamicColumns, // Send only dynamic columns to Calcite
      staticValues: payload.staticValues, // Static values are added by CalciteCompiler
      mode: payload.mode,
      conflictColumns: payload.conflictColumns,
      updateColumns: payload.updateColumns,
      dialect: 'POSTGRESQL'
    }
    return this.post('/compile/insert', body)
  }

  async compileUpdate(
    payload: WritePayload,
    schema: SchemaConfig
  ): Promise<CalciteCompileResult> {
    const body = {
      schema: schemaToCalcite(schema, null, payload.table),
      table: payload.table, // Keep original case
      setColumns: payload.columns,
      staticSets: payload.staticValues,
      whereColumns: payload.whereColumns ?? [],
      whereFilters: payload.staticWhere ?? {},
      dialect: 'POSTGRESQL'
    }
    return this.post('/compile/update', body)
  }

  async compileDelete(
    payload: WritePayload,
    schema: SchemaConfig
  ): Promise<CalciteCompileResult> {
    const body = {
      schema: schemaToCalcite(schema, null, payload.table),
      table: payload.table, // Keep original case
      whereColumns: payload.whereColumns,
      dialect: 'POSTGRESQL'
    }
    return this.post('/compile/delete', body)
  }

  private async post(path: string, body: unknown): Promise<CalciteCompileResult> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(this.config.services.calciteRequestTimeout)
    })
    
    if (!res.ok) {
      const err = await res.json()
      throw new Error(`Calcite error: ${err.error}`)
    }
    
    const result = await res.json()
    return {
      ...result,
      optimizations: result.optimizations || []
    }
  }
}

function extractSubqueryTables(filters: any[]): string[] {
  const tables: string[] = []
  for (const f of filters ?? []) {
    if (typeof f.value === 'string' && f.value.trim().toLowerCase().startsWith('select')) {
      // Extract table names from simple subqueries: "SELECT x FROM table_name WHERE ..."
      const fromMatch = f.value.match(/\bFROM\s+(\w+)/i)
      const joinMatches = f.value.matchAll(/\bJOIN\s+(\w+)/gi)
      if (fromMatch) tables.push(fromMatch[1])
      for (const m of joinMatches) tables.push(m[1])
    }
  }
  return tables
}

function schemaToCalcite(
  schema: SchemaConfig,
  intent: QueryIntent | null,
  specificTable?: string
): any[] {
  // Pass every table in the schema - Calcite needs to see any table that 
  // could appear in a subquery, even if it's not in the main FROM/JOIN list
  console.log('[schemaToCalcite] Schema tables passed to Calcite:', Array.from(schema.tables.keys()));
  console.log('[schemaToCalcite] Note: including all schema tables (not just query tables) to support subqueries');
  
  // Safety net: extract tables from subquery filter values
  const subqueryTables = intent ? extractSubqueryTables(intent.filters || []) : []
  if (subqueryTables.length > 0) {
    console.log('[schemaToCalcite] Tables found in subqueries:', subqueryTables);
  }
  
  const result = []
  for (const [name, table] of Array.from(schema.tables.entries())) {
    result.push({
      name: name, // Keep original case
      columns: table.columns.map(col => ({
        name: col.name,
        type: engineTypeToCalcite(col.type),
        nullable: col.nullable
      })),
      primaryKey: table.primaryKey,
      foreignKeys: schema.foreignKeys
        .filter(fk => fk.fromTable === name)
        .map(fk => ({
          fromColumn: fk.fromColumn,
          toTable: fk.toTable, // Keep original case
          toColumn: fk.toColumn
        }))
    })
  }
  return result
}

function engineTypeToCalcite(type: any): string {
  switch (type.kind) {
    case 'number':   return 'NUMERIC'
    case 'string':   return 'VARCHAR'
    case 'boolean':  return 'BOOLEAN'
    case 'datetime': return 'TIMESTAMP'
    default:         return 'VARCHAR'
  }
}
