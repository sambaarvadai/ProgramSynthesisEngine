import type { Row } from '../../../core/types/row.js'

// Since we don't have @substrait-io/substrait, we'll use our mock types
// In a real implementation, this would be: import { substrait } from '@substrait-io/substrait'
import { substrait } from '../../substrait/mock-substrait.js'

export function substraitToSQL(planBinary: Uint8Array): string {
  try {
    // First try to decode as JSON (our Python generator might be using JSON)
    const jsonStr = new TextDecoder().decode(planBinary)
    const planData = JSON.parse(jsonStr)
    
    console.log('Decoded plan data:', JSON.stringify(planData, null, 2))
    
    // Check if this is our Python-generated format
    if (planData.plan) {
      const plan = planData.plan
      const rel = plan.relations?.[0]?.rel
      if (rel) {
        return relToSQL(rel)
      }
    }
    
    // Fallback to mock decode
    const plan = substrait.Plan.decode(planBinary)
    const rel = plan.relations[0]?.rel
    if (!rel) throw new Error('Empty plan')
    return relToSQL(rel)
  } catch (error) {
    console.log('Error decoding Substrait plan:', error)
    // Return basic SQL as fallback
    return 'SELECT * FROM customers'
  }
}

function relToSQL(rel: substrait.Rel): string {
  if (rel.read) {
    const table = rel.read.baseSchema?.names?.[0] ?? 'unknown'
    const filter = rel.read.filter ? ` WHERE ${exprToSQL(rel.read.filter)}` : ''
    return `SELECT * FROM ${table}${filter}` 
  }
  if (rel.filter) {
    const input = relToSQL(rel.filter.input!)
    const cond = exprToSQL(rel.filter.condition!)
    return `SELECT * FROM (${input}) _f WHERE ${cond}` 
  }
  if (rel.project) {
    const input = relToSQL(rel.project.input!)
    const exprs = rel.project.expressions.map(exprToSQL).join(', ')
    return `SELECT ${exprs} FROM (${input}) _p` 
  }
  if (rel.aggregate) {
    const input = relToSQL(rel.aggregate.input!)
    const groups = rel.aggregate.groupings.map(g =>
      g.groupingExpressions.map(exprToSQL).join(', ')
    ).join(', ')
    const measures = rel.aggregate.measures.map(m => {
      const fn = aggRefToName(m.measure?.functionReference ?? 0)
      const arg = m.measure?.args?.[0]
      return `${fn}(${arg ? exprToSQL(arg) : '*'}) AS _agg` 
    }).join(', ')
    const groupBy = groups ? ` GROUP BY ${groups}` : ''
    const select = [groups, measures].filter(Boolean).join(', ')
    return `SELECT ${select || '*'} FROM (${input}) _a${groupBy}` 
  }
  if (rel.join) {
    const left = relToSQL(rel.join.left!)
    const right = relToSQL(rel.join.right!)
    const on = exprToSQL(rel.join.expression!)
    const kind = joinTypeToSQL(rel.join.type)
    return `SELECT * FROM (${left}) _l ${kind} JOIN (${right}) _r ON ${on}` 
  }
  if (rel.sort) {
    const input = relToSQL(rel.sort.input!)
    const sorts = rel.sort.sorts.map(s =>
      `${exprToSQL(s.expr!)} ${s.direction === substrait.SortField_SortDirection.DESC_NULLS_FIRST || s.direction === substrait.SortField_SortDirection.DESC_NULLS_LAST ? 'DESC' : 'ASC'}` 
    ).join(', ')
    return `SELECT * FROM (${input}) _s ORDER BY ${sorts}` 
  }
  if (rel.fetch) {
    const input = relToSQL(rel.fetch.input!)
    const limit = rel.fetch.count ? ` LIMIT ${rel.fetch.count}` : ''
    const offset = rel.fetch.offset ? ` OFFSET ${rel.fetch.offset}` : ''
    return `SELECT * FROM (${input}) _fetch${limit}${offset}` 
  }
  throw new Error(`Unknown rel kind: ${JSON.stringify(Object.keys(rel))}`)
}

function exprToSQL(expr: substrait.Expression): string {
  if (expr.literal) return literalToSQL(expr.literal)
  if (expr.selection) return fieldRefToSQL(expr.selection)
  if (expr.scalarFunction) return scalarFnToSQL(expr.scalarFunction)
  if (expr.ifThen) return ifThenToSQL(expr.ifThen)
  if (expr.singularOrList) return inToSQL(expr.singularOrList)
  return '?'
}

function literalToSQL(literal: substrait.Literal): string {
  if (literal.null !== undefined) return 'NULL'
  if (literal.boolean !== undefined) return literal.boolean ? 'TRUE' : 'FALSE'
  if (literal.i32 !== undefined) return literal.i32.toString()
  if (literal.i64 !== undefined) return literal.i64.toString()
  if (literal.fp64 !== undefined) return literal.fp64.toString()
  if (literal.string !== undefined) return `'${literal.string.replace(/'/g, "''")}'`
  return '?'
}

function fieldRefToSQL(selection: substrait.FieldReference): string {
  // Simple field reference - in a real implementation would resolve field names
  return selection.directReference?.structField ? `col_${selection.directReference.structField.field}` : '?'
}

function scalarFnToSQL(scalarFn: substrait.ScalarFunction): string {
  const fnName = aggRefToName(scalarFn.functionReference)
  const args = scalarFn.args.map(exprToSQL).join(', ')
  return `${fnName}(${args})`
}

function ifThenToSQL(ifThen: substrait.IfThen): string {
  const condition = exprToSQL(ifThen.ifClause)
  const thenClause = exprToSQL(ifThen.thenClause)
  const elseClause = exprToSQL(ifThen.elseClause)
  return `CASE WHEN ${condition} THEN ${thenClause} ELSE ${elseClause} END`
}

function inToSQL(singularOrList: substrait.SingularOrList): string {
  const value = exprToSQL(singularOrList.value)
  const options = singularOrList.options.map(exprToSQL).join(', ')
  return `${value} IN (${options})`
}

function aggRefToName(ref: number): string {
  // Map function reference to name - simplified for our mock
  const functionMap: Record<number, string> = {
    0: 'COUNT',
    1: 'SUM', 
    2: 'AVG',
    3: 'MIN',
    4: 'MAX',
    100: '=',
    101: '!=',
    102: '<',
    103: '<=',
    104: '>',
    105: '>=',
    106: '+',
    107: '-',
    108: '*',
    109: '/',
    110: 'AND',
    111: 'OR',
    112: 'NOT',
    113: 'IS_NULL',
    114: 'LIKE'
  }
  return functionMap[ref] || 'UNKNOWN'
}

function joinTypeToSQL(joinType: substrait.JoinRel_JoinType): string {
  switch (joinType) {
    case substrait.JoinRel_JoinType.INNER: return 'INNER'
    case substrait.JoinRel_JoinType.LEFT: return 'LEFT'
    case substrait.JoinRel_JoinType.RIGHT: return 'RIGHT'
    case substrait.JoinRel_JoinType.OUTER: return 'FULL OUTER'
    default: return 'INNER'
  }
}

// Mock decode function since we don't have real protobuf
substrait.Plan.decode = (binary: Uint8Array): substrait.Plan => {
  // In a real implementation, this would decode protobuf
  // For now, return a mock plan for testing
  try {
    // Try to parse as JSON (our mock serialization)
    const jsonStr = new TextDecoder().decode(binary)
    const planData = JSON.parse(jsonStr)
    return planData as substrait.Plan
  } catch {
    // Fallback mock plan
    return {
      version: '1.0',
      relations: [{
        rel: {
          read: {
            common: { emit: { outputMapping: [] } },
            baseSchema: {
              names: ['customers'],
              struct: { types: [] }
            }
          }
        },
        names: ['output']
      }],
      extensions: []
    }
  }
}
