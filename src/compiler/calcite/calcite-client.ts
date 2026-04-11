import type { QueryIntent } from '../query-ast/query-intent.js'
import type { WritePayload } from '../../nodes/payloads.js'
import type { SchemaConfig } from '../schema/schema-config.js'

export type CalciteCompileResult = {
  sql: string
  paramColumns: string[]    // row field names -> $1, $2...
  staticParams: unknown[]   // literal values appended
  optimizations: string[]
  dialect: string
}

export class CalciteClient {
  private baseUrl: string

  constructor(baseUrl = 'http://localhost:8765') {
    this.baseUrl = baseUrl
  }

  async isAvailable(): Promise<boolean> {
    try {
      const res = await fetch(`${this.baseUrl}/health`, { signal: AbortSignal.timeout(1000) })
      return res.ok
    } catch { return false }
  }

  async compileSelect(
    intent: QueryIntent,
    schema: SchemaConfig
  ): Promise<CalciteCompileResult> {
    const body = {
      schema: schemaToCalcite(schema, intent),
      table: intent.table,
      columns: intent.columns,
      joins: intent.joins?.map(j => ({
        table: j.table,
        kind: j.kind ?? 'INNER',
        onLeft: j.on?.left,
        onRight: j.on?.right
      })),
      filters: intent.filters?.map(f => ({
        field: f.field,
        table: f.table,
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
    const body = {
      schema: schemaToCalcite(schema, null, payload.table),
      table: payload.table,
      columns: payload.columns,
      staticValues: payload.staticValues,
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
      table: payload.table,
      setColumns: payload.columns,
      staticSets: payload.staticValues,
      whereColumns: payload.whereColumns ?? [],
      staticWhere: payload.staticWhere ?? {},
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
      table: payload.table,
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
      signal: AbortSignal.timeout(5000)
    })
    
    if (!res.ok) {
      const err = await res.json()
      throw new Error(`Calcite error: ${err.error}`)
    }
    
    return res.json()
  }
}

function schemaToCalcite(
  schema: SchemaConfig,
  intent: QueryIntent | null,
  specificTable?: string
): any[] {
  // Include only tables relevant to this query
  const relevantTables = new Set<string>()
  
  if (specificTable) {
    relevantTables.add(specificTable)
  }
  
  if (intent) {
    relevantTables.add(intent.table)
    intent.joins?.forEach(j => relevantTables.add(j.table))
  }
  
  const result = []
  for (const [name, table] of schema.tables) {
    if (relevantTables.size === 0 || relevantTables.has(name)) {
      result.push({
        name,
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
            toTable: fk.toTable,
            toColumn: fk.toColumn
          }))
      })
    }
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
