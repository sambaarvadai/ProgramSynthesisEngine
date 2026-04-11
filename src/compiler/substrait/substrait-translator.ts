import { exec } from 'child_process'
import { promisify } from 'util'
import * as path from 'path'
import * as fs from 'fs'
import { fileURLToPath } from 'url'

const execAsync = promisify(exec)

// Fix __dirname for ES modules
const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

import type { QueryPlan, QueryDAGNode, QueryDAGNodeId } from '../query-ast/query-ast.js'
import type { ExprAST, AggFn } from '../../core/ast/expr-ast.js'
import type { RowSchema } from '../../core/types/schema.js'

export class SubstraitTranslator {
  private pythonScriptPath: string

  constructor() {
    // Path to the Python Substrait generator script
    this.pythonScriptPath = path.join(__dirname, 'substrait-generator.py')
  }

  async translate(plan: QueryPlan): Promise<Uint8Array> {
    try {
      // Convert QueryPlan to JSON string
      const planJson = this.queryPlanToJson(plan)
      
      // Create temporary file for the plan
      const tempFile = path.join(__dirname, 'temp_plan.json')
      fs.writeFileSync(tempFile, planJson)
      
      try {
        // Call Python script to generate Substrait protobuf
        const { stdout } = await execAsync(`python3 "${this.pythonScriptPath}" "${tempFile}"`)
        
        // Convert stdout buffer to Uint8Array
        const binaryData = Buffer.from(stdout)
        
        return binaryData
      } finally {
        // Clean up temporary file
        if (fs.existsSync(tempFile)) {
          fs.unlinkSync(tempFile)
        }
      }
    } catch (error) {
      throw new Error(`Failed to translate QueryPlan to Substrait: ${error}`)
    }
  }

  private queryPlanToJson(plan: QueryPlan): string {
    // Convert QueryPlan to JSON-serializable format
    const jsonPlan = {
      root: plan.root,
      nodes: this.nodesToJson(plan.nodes),
      optimizations: plan.optimizations
    }
    return JSON.stringify(jsonPlan, null, 2)
  }

  private nodesToJson(nodes: Map<QueryDAGNodeId, QueryDAGNode>): Record<string, any> {
    const jsonNodes: Record<string, any> = {}
    
    for (const [nodeId, node] of nodes.entries()) {
      jsonNodes[nodeId] = this.nodeToJson(node)
    }
    
    return jsonNodes
  }

  private nodeToJson(node: QueryDAGNode): any {
    const jsonNode: any = {
      kind: node.kind
    }
    
    switch (node.kind) {
      case 'Scan':
        jsonNode.payload = this.scanNodeToJson(node.payload)
        break
      case 'Filter':
        jsonNode.predicate = this.exprToJson(node.predicate)
        jsonNode.input = node.input
        break
      case 'Join':
        jsonNode.payload = this.joinNodeToJson(node.payload)
        jsonNode.left = node.left
        jsonNode.right = node.right
        break
      case 'Agg':
        jsonNode.keys = node.keys.map(key => this.exprToJson(key))
        jsonNode.aggregations = node.aggregations.map(agg => ({
          fn: agg.fn,
          expr: this.exprToJson(agg.expr),
          alias: agg.alias
        }))
        jsonNode.input = node.input
        break
      case 'Project':
        jsonNode.columns = node.columns.map(col => ({
          expr: this.exprToJson(col.expr),
          alias: col.alias
        }))
        jsonNode.input = node.input
        break
      case 'Sort':
        jsonNode.keys = node.keys.map(key => ({
          expr: this.exprToJson(key.expr),
          direction: key.direction,
          nulls: key.nulls
        }))
        jsonNode.input = node.input
        break
      case 'Limit':
        jsonNode.count = node.count
        jsonNode.offset = node.offset
        jsonNode.input = node.input
        break
    }
    
    return jsonNode
  }

  private scanNodeToJson(scanNode: any): any {
    return {
      table: scanNode.table,
      alias: scanNode.alias,
      schema: this.schemaToJson(scanNode.schema),
      predicate: scanNode.predicate ? this.exprToJson(scanNode.predicate) : undefined
    }
  }

  private joinNodeToJson(joinNode: any): any {
    return {
      kind: joinNode.kind,
      table: joinNode.table,
      alias: joinNode.alias,
      on: this.exprToJson(joinNode.on),
      schema: this.schemaToJson(joinNode.schema)
    }
  }

  private schemaToJson(schema: RowSchema): any {
    return {
      columns: schema.columns.map(col => ({
        name: col.name,
        type: col.type,
        nullable: col.nullable
      }))
    }
  }

  private exprToJson(expr: ExprAST): any {
    const jsonExpr: any = {
      kind: expr.kind
    }
    
    switch (expr.kind) {
      case 'Literal':
        jsonExpr.value = expr.value
        jsonExpr.type = expr.type
        break
      case 'FieldRef':
        jsonExpr.table = expr.table
        jsonExpr.field = expr.field
        break
      case 'VarRef':
        jsonExpr.name = expr.name
        break
      case 'BinaryOp':
        jsonExpr.op = expr.op
        jsonExpr.left = this.exprToJson(expr.left)
        jsonExpr.right = this.exprToJson(expr.right)
        break
      case 'UnaryOp':
        jsonExpr.op = expr.op
        jsonExpr.operand = this.exprToJson(expr.operand)
        break
      case 'FunctionCall':
        jsonExpr.name = expr.name
        jsonExpr.args = expr.args.map(arg => this.exprToJson(arg))
        break
      case 'Conditional':
        jsonExpr.condition = this.exprToJson(expr.condition)
        jsonExpr.then = this.exprToJson(expr.then)
        jsonExpr.else = this.exprToJson(expr.else)
        break
      case 'Cast':
        jsonExpr.expr = this.exprToJson(expr.expr)
        jsonExpr.to = expr.to
        break
      case 'IsNull':
        jsonExpr.expr = this.exprToJson(expr.expr)
        break
      case 'In':
        jsonExpr.expr = this.exprToJson(expr.expr)
        jsonExpr.values = expr.values.map(val => this.exprToJson(val))
        break
      case 'Between':
        jsonExpr.expr = this.exprToJson(expr.expr)
        jsonExpr.low = this.exprToJson(expr.low)
        jsonExpr.high = this.exprToJson(expr.high)
        break
      case 'WindowExpr':
        jsonExpr.fn = expr.fn
        jsonExpr.over = this.exprToJson(expr.over)
        jsonExpr.partition = expr.partition?.map(part => this.exprToJson(part))
        jsonExpr.orderBy = expr.orderBy?.map(order => this.exprToJson(order))
        break
      case 'SqlExpr':
        jsonExpr.sql = expr.sql
        break
      case 'Wildcard':
        // Wildcard has no additional properties
        break
    }
    
    return jsonExpr
  }

  // Legacy methods for backward compatibility - these are now handled by Python
  private dagNodeToRel(nodeId: QueryDAGNodeId, nodes: Map<QueryDAGNodeId, QueryDAGNode>): any {
    throw new Error('This method is now handled by the Python bridge')
  }

  private exprToSubstrait(expr: ExprAST): any {
    throw new Error('This method is now handled by the Python bridge')
  }

  private rowSchemaToSubstrait(schema: RowSchema): any {
    throw new Error('This method is now handled by the Python bridge')
  }

  private joinTypeToSubstrait(kind: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL'): any {
    throw new Error('This method is now handled by the Python bridge')
  }

  private sortDirectionToSubstrait(direction: 'ASC' | 'DESC', nulls: 'FIRST' | 'LAST'): any {
    throw new Error('This method is now handled by the Python bridge')
  }

  private aggFnRef(fn: AggFn): number {
    throw new Error('This method is now handled by the Python bridge')
  }

  private serializePlan(plan: any): Uint8Array {
    throw new Error('This method is now handled by the Python bridge')
  }
}
