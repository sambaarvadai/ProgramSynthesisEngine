import type { SchemaConfig } from '../compiler/schema/schema-config.js';
import type { StorageBackend } from '../core/storage/storage-backend.js';
import type { TempStore } from '../core/storage/temp-store.js';
import type { ExprEvaluator } from './expr-evaluator.js';
import type { QueryIntent } from '../compiler/query-ast/query-intent.js';
import type { QueryPlan } from '../compiler/query-ast/query-ast.js';
import type { ExecutionContext } from '../core/context/execution-context.js';
import type { Row } from '../core/types/row.js';
import type { RowSchema } from '../core/types/schema.js';
import type { TraceEvent, ExecutionTrace } from '../core/context/execution-trace.js';
import type { CalciteClient } from '../compiler/calcite/index.js';
import { QueryASTBuilder, type ValidationResult } from '../compiler/query-ast/query-ast-builder.js';
import { QueryPlanner } from '../compiler/query-ast/query-planner.js';
import { OperatorTreeBuilder } from '../compiler/query/operator-tree-builder.js';
import { collectAll } from './physical-operator.js';
import { traceEvent } from '../core/context/execution-trace.js';

export interface QueryExecutorConfig {
  schema: SchemaConfig;
  backend: StorageBackend;
  tempStore: TempStore;
  evaluator: ExprEvaluator;
  batchSize?: number;
  calciteClient?: CalciteClient;
}

export interface QueryResult {
  rows: Row[];
  schema: RowSchema;
  rowCount: number;
  trace: ExecutionTrace;
  optimizationsApplied: string[];
}

export class QueryValidationError extends Error {
  constructor(public errors: string[]) {
    super(`Query validation failed:\n${errors.map(e => `  - ${e}`).join('\n')}`);
    this.name = 'QueryValidationError';
  }
}

export class QueryExecutor {
  private astBuilder: QueryASTBuilder;
  private planner: QueryPlanner;
  private treeBuilder: OperatorTreeBuilder;
  private config: QueryExecutorConfig;

  constructor(config: QueryExecutorConfig) {
    this.config = config;
    this.astBuilder = new QueryASTBuilder(config.schema, config.evaluator);
    this.planner = new QueryPlanner(config.schema);
    this.treeBuilder = new OperatorTreeBuilder(
      config.backend,
      config.tempStore,
      config.evaluator,
      config.batchSize || 100
    );
  }

  private needsCalcite(intent: QueryIntent): boolean {
    const hasJoins = (intent.joins?.length ?? 0) > 0;
    const hasSubqueryFilter = intent.filters?.some(f =>
      (f.operator === 'NOT IN' || f.operator === 'IN') &&
      typeof f.value === 'string' &&
      f.value.trim().toLowerCase().startsWith('select')
    ) ?? false;
    const hasAggregation = (intent.groupBy?.length ?? 0) > 0 ||
                           intent.columns.some(c => c.agg);
    return hasJoins || hasSubqueryFilter || hasAggregation;
  }

  private buildRawSQL(intent: QueryIntent): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    let sql = 'SELECT ';
    
    // Build SELECT columns
    const columns = intent.columns.map(col => {
      let field: string;
      if (col.table) {
        field = `"${col.table}"."${col.field}"`;
      } else if (col.field === '*') {
        field = '*';
      } else {
        field = `"${col.field}"`;
      }
      
      if (col.agg) {
        return `${col.agg}(${field})${col.alias ? ` AS "${col.alias}"` : ''}`;
      } else {
        return col.alias ? `${field} AS "${col.alias}"` : field;
      }
    });
    sql += columns.join(', ');
    
    // Build FROM and JOINs
    sql += ` FROM "${intent.table}"`;
    
    if (intent.joins) {
      for (const join of intent.joins) {
        const joinKind = join.kind || 'INNER';
        const onLeft = join.on?.left?.includes('.') ? 
          `"${join.on.left.split('.')[0]}"."${join.on.left.split('.')[1]}"` : 
          `"${join.on?.left}"`;
        const onRight = join.on?.right?.includes('.') ? 
          `"${join.on.right.split('.')[0]}"."${join.on.right.split('.')[1]}"` : 
          `"${join.on?.right}"`;
        sql += ` ${joinKind} JOIN "${join.table}" ON ${onLeft} = ${onRight}`;
      }
    }
    
    // Build WHERE clause
    if (intent.filters && intent.filters.length > 0) {
      sql += ' WHERE ';
      const conditions = intent.filters.map(f => {
        const field = f.table ? `"${f.table}"."${f.field}"` : `"${f.field}"`;
        
        if (f.operator === 'NOT IN' && typeof f.value === 'string' && f.value.trim().toLowerCase().startsWith('select')) {
          return `${field} NOT IN (${f.value.trim()})`;
        } else if (f.operator === 'IN' && typeof f.value === 'string' && f.value.trim().toLowerCase().startsWith('select')) {
          return `${field} IN (${f.value.trim()})`;
        } else if (f.operator === 'IS NULL') {
          return `${field} IS NULL`;
        } else if (f.operator === 'IS NOT NULL') {
          return `${field} IS NOT NULL`;
        } else {
          const value = typeof f.value === 'string' ? `'${f.value}'` : f.value;
          return `${field} ${f.operator} ${value}`;
        }
      });
      sql += conditions.join(' AND ');
    }
    
    // Build GROUP BY
    if (intent.groupBy && intent.groupBy.length > 0) {
      sql += ' GROUP BY ' + intent.groupBy.map(g => `"${g}"`).join(', ');
    }
    
    // Build ORDER BY
    if (intent.orderBy && intent.orderBy.length > 0) {
      sql += ' ORDER BY ' + intent.orderBy.map(ob => {
        // Check if this is ordering by an alias from the SELECT columns
        const aliasColumn = intent.columns.find(col => col.alias === ob.field);
        if (aliasColumn) {
          // Order by alias directly (no quotes needed for aliases in ORDER BY)
          return `${ob.field} ${ob.direction || 'ASC'}`;
        } else {
          // Order by regular field
          const field = ob.table ? `"${ob.table}"."${ob.field}"` : `"${ob.field}"`;
          return `${field} ${ob.direction || 'ASC'}`;
        }
      }).join(', ');
    }
    
    // Build LIMIT and OFFSET
    if (intent.limit) sql += ` LIMIT ${intent.limit}`;
    if (intent.offset) sql += ` OFFSET ${intent.offset}`;
    
    return { sql, params };
  }

  async execute(intent: QueryIntent, ctx: ExecutionContext, additionalFields?: Map<string, { name: string; type: any }[]>): Promise<QueryResult> {
    
    if (this.needsCalcite(intent)) {
      // Try Calcite first
      if (this.config.calciteClient) {
        try {
          const compiled = await this.config.calciteClient.compileSelect(
            intent,
            this.config.schema
          );
          console.log('[QueryExecutor] Calcite SQL:', compiled.sql);
          
          const pool = (this.config.backend as any).pool;
          const result = await pool.query(compiled.sql, compiled.staticParams);
          
          const schema: RowSchema = {
            columns: result.fields.map((f: any) => ({
              name: f.name,
              type: { kind: 'any' } as any,
              nullable: true
            }))
          };
          
          traceEvent(ctx.trace, {
            nodeId: ctx.executionId,
            kind: 'complete',
            rowsOut: result.rows.length,
            meta: { optimizations: compiled.optimizations }
          });
          
          return {
            rows: result.rows,
            schema,
            rowCount: result.rows.length,
            trace: ctx.trace,
            optimizationsApplied: compiled.optimizations
          };
        } catch (err) {
          console.warn('[QueryExecutor] Calcite failed, using raw SQL fallback:', (err as Error).message);
        }
      }
      
      // Fallback: buildRawSQL -> Postgres directly (NOT physical operators)
      // Physical operators can't handle joins correctly
      const { sql, params } = this.buildRawSQL(intent);
      console.log('[QueryExecutor] Raw SQL fallback:', sql);
      const pool = (this.config.backend as any).pool;
      const result = await pool.query(sql, params);
      const schema = {
        columns: result.fields.map((f: any) => ({
          name: f.name,
          type: { kind: 'any' } as any,
          nullable: true
        }))
      };
      return {
        rows: result.rows,
        schema,
        rowCount: result.rows.length,
        trace: ctx.trace,
        optimizationsApplied: ['raw_sql_fallback']
      };
      // Never falls through to physical operators for join queries
    }
    
    // 1. Build AST with validation
    const { ast, validation } = this.astBuilder.build(intent, additionalFields);
    
    console.log(`[QueryExecutor] AST built, validation: ${validation.isValid}, errors:`, validation.errors);
    if (!validation.isValid) {
      throw new QueryValidationError(validation.errors);
    }

    // 2. Plan with optimizations
    const plan = this.planner.plan(ast);
    console.log(`[QueryExecutor] Plan created with ${plan.optimizations.length} optimizations`);

    // 3. Build operator tree
    const root = this.treeBuilder.build(plan);
    console.log(`[QueryExecutor] Operator tree built`);

    // 4. Execute
    const result = await collectAll(root, ctx, ctx.budget.maxRowsPerNode);
    console.log(`[QueryExecutor] Query executed, returned ${result.rows.length} rows`);

    // 5. Record to trace
    traceEvent(ctx.trace, {
      nodeId: ctx.executionId,
      kind: 'complete',
      rowsOut: result.rows.length,
      meta: { optimizations: plan.optimizations }
    });

    // 6. Return result
    return {
      rows: result.rows,
      schema: result.schema,
      rowCount: result.rows.length,
      trace: ctx.trace,
      optimizationsApplied: []
    };
  }

  async explain(intent: QueryIntent): Promise<string> {
    // Build AST and plan without executing
    const { ast, validation } = this.astBuilder.build(intent);
    
    if (!validation.isValid) {
      throw new QueryValidationError(validation.errors);
    }

    const plan = this.planner.plan(ast);

    // Generate human-readable plan description
    const lines: string[] = [];
    
    lines.push('Query Execution Plan');
    lines.push('=====================');
    lines.push('');
    
    // List optimizations applied
    if (plan.optimizations.length > 0) {
      lines.push('Optimizations Applied:');
      plan.optimizations.forEach(opt => {
        lines.push(`  - ${opt}`);
      });
      lines.push('');
    } else {
      lines.push('No optimizations applied');
      lines.push('');
    }

    // List DAG nodes in execution order
    lines.push('Execution Plan (DAG Nodes):');
    lines.push('');
    
    // Walk the tree from root to produce execution order
    const executionOrder = this.getExecutionOrder(plan.root, plan.nodes);
    
    executionOrder.forEach((nodeId, index) => {
      const node = plan.nodes.get(nodeId);
      if (!node) return;
      
      const step = index + 1;
      lines.push(`${step}. ${this.formatNode(node)}`);
      
      // Add node-specific details
      if (node.kind === 'Scan') {
        lines.push(`   Table: ${node.payload.table}${node.payload.alias ? ` (alias: ${node.payload.alias})` : ''}`);
      } else if (node.kind === 'Filter') {
        lines.push(`   Predicate: ${this.formatExpr(node.predicate)}`);
      } else if (node.kind === 'Join') {
        lines.push(`   Type: ${node.payload.kind}`);
        lines.push(`   Table: ${node.payload.table}`);
      } else if (node.kind === 'Agg') {
        if (node.keys.length > 0) {
          lines.push(`   GroupBy: ${node.keys.map(k => this.formatExpr(k)).join(', ')}`);
        }
        if (node.aggregations.length > 0) {
          lines.push(`   Aggregations: ${node.aggregations.map((a: any) => `${a.fn}(${a.alias})`).join(', ')}`);
        }
      } else if (node.kind === 'Sort') {
        lines.push(`   OrderBy: ${node.keys.map(k => `${this.formatExpr(k.expr)} ${k.direction}`).join(', ')}`);
      } else if (node.kind === 'Limit') {
        lines.push(`   Limit: ${node.count}${node.offset > 0 ? `, Offset: ${node.offset}` : ''}`);
      } else if (node.kind === 'Project') {
        lines.push(`   Columns: ${node.columns.map(c => c.alias).join(', ')}`);
      }
      
      lines.push('');
    });

    return lines.join('\n');
  }

  private getExecutionOrder(
    root: string,
    nodes: Map<string, any>
  ): string[] {
    const order: string[] = [];
    const visited = new Set<string>();
    
    const traverse = (nodeId: string) => {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);
      
      const node = nodes.get(nodeId);
      if (!node) return;
      
      // Traverse inputs first
      if ('input' in node) {
        traverse(node.input);
      }
      if ('left' in node) {
        traverse(node.left);
      }
      if ('right' in node) {
        traverse(node.right);
      }
      
      // Add current node
      order.push(nodeId);
    };
    
    traverse(root);
    return order;
  }

  private formatNode(node: any): string {
    switch (node.kind) {
      case 'Scan':
        return `Scan`;
      case 'Filter':
        return `Filter`;
      case 'Join':
        return `Join`;
      case 'Agg':
        return `Aggregation`;
      case 'Project':
        return `Projection`;
      case 'Sort':
        return `Sort`;
      case 'Limit':
        return `Limit`;
      default:
        return `Unknown (${node.kind})`;
    }
  }

  private formatExpr(expr: any): string {
    switch (expr.kind) {
      case 'Literal':
        return String(expr.value);
      case 'FieldRef':
        return expr.table ? `${expr.table}.${expr.field}` : expr.field;
      case 'VarRef':
        return `$${expr.name}`;
      case 'BinaryOp':
        return `${this.formatExpr(expr.left)} ${expr.op} ${this.formatExpr(expr.right)}`;
      case 'UnaryOp':
        return `${expr.op} ${this.formatExpr(expr.operand)}`;
      case 'FunctionCall':
        return `${expr.name}(${expr.args.map((a: any) => this.formatExpr(a)).join(', ')})`;
      case 'IsNull':
        return `${this.formatExpr(expr.expr)} IS NULL`;
      case 'In':
        return `${this.formatExpr(expr.expr)} IN (${expr.values.map((v: any) => this.formatExpr(v)).join(', ')})`;
      case 'Between':
        return `${this.formatExpr(expr.expr)} BETWEEN ${this.formatExpr(expr.low)} AND ${this.formatExpr(expr.high)}`;
      default:
        return `Expression(${expr.kind})`;
    }
  }
}
