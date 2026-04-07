import type { SchemaConfig } from '../schema/schema-config.js';
import { getTable, getRowSchema, findJoinPath, tableExists } from '../schema/schema-config.js';
import type { QueryIntent, QueryIntentColumn, QueryIntentFilter, QueryIntentJoin, QueryIntentOrderBy } from './query-intent.js';
import type { QueryAST, ScanNode, JoinNode, ProjectionNode, OrderByNode } from './query-ast.js';
import type { ExprAST, BinaryOperator } from '../../core/ast/expr-ast.js';
import type { AggFn } from '../../core/ast/expr-ast.js';
import type { Value } from '../../core/types/value.js';
import type { EngineType } from '../../core/types/engine-type.js';
import { ExprEvaluator } from '../../executors/expr-evaluator.js';

export interface ValidationResult {
  isValid: boolean;
  errors: string[];
}

export interface QueryASTBuildResult {
  ast: QueryAST;
  validation: ValidationResult;
}

export class QueryASTBuilder {
  constructor(
    private schema: SchemaConfig,
    private evaluator: ExprEvaluator
  ) {}

  build(intent: QueryIntent): QueryASTBuildResult {
    const validation = this.validateIntent(intent);
    
    if (!validation.isValid) {
      return {
        ast: {} as QueryAST, // Will never be used when validation fails
        validation
      };
    }

    const ast = this.buildAST(intent);
    
    return {
      ast,
      validation: { isValid: true, errors: [] }
    };
  }

  validateIntent(intent: QueryIntent): ValidationResult {
    const errors: string[] = [];

    // Validate primary table exists
    if (!tableExists(this.schema, intent.table)) {
      errors.push(`Primary table '${intent.table}' does not exist in schema`);
    }

    // Validate all column fields exist
    for (const column of intent.columns) {
      // Skip aggregated columns, '*' wildcard (used in COUNT(*), etc.), and computed expressions (expr)
      if (column.agg || column.field === '*' || column.expr) {
        continue;
      }

      const table = column.table || intent.table;
      if (!tableExists(this.schema, table)) {
        errors.push(`Table '${table}' referenced in column '${column.field}' does not exist`);
        continue;
      }

      const tableConfig = getTable(this.schema, table);
      const columnExists = tableConfig.columns.some(col => col.name === column.field);
      if (!columnExists) {
        errors.push(`Column '${column.field}' does not exist in table '${table}'`);
      }
    }

    // Validate filter fields exist
    if (intent.filters) {
      // Create a map of aliases to actual table names
      const aliasToTable = new Map<string, string>();
      aliasToTable.set(intent.table, intent.table);
      if (intent.joins) {
        for (const join of intent.joins) {
          aliasToTable.set(join.alias || join.table, join.table);
        }
      }

      for (const filter of intent.filters) {
        const table = filter.table || intent.table;
        
        // Resolve alias to actual table name
        const actualTable = aliasToTable.get(table) || table;
        
        if (!tableExists(this.schema, actualTable)) {
          errors.push(`Table '${table}' referenced in filter '${filter.field}' does not exist`);
          continue;
        }

        const tableConfig = getTable(this.schema, actualTable);
        const columnExists = tableConfig.columns.some(col => col.name === filter.field);
        if (!columnExists) {
          errors.push(`Filter column '${filter.field}' does not exist in table '${table}'`);
        }
      }
    }

    // Validate join tables exist
    if (intent.joins) {
      for (const join of intent.joins) {
        if (!tableExists(this.schema, join.table)) {
          errors.push(`Join table '${join.table}' does not exist in schema`);
        }
      }
    }

    // Validate groupBy fields exist
    if (intent.groupBy) {
      // Create a map of aliases to actual table names
      const aliasToTable = new Map<string, string>();
      aliasToTable.set(intent.table, intent.table);
      if (intent.joins) {
        for (const join of intent.joins) {
          aliasToTable.set(join.alias || join.table, join.table);
        }
      }

      const allTables = [intent.table, ...(intent.joins?.map(j => j.table) || [])];

      for (const field of intent.groupBy) {
        let fieldExists = false;
        let errorDetails = '';

        if (field.includes('.')) {
          // Parse as 'table.column'
          const [tableName, columnName] = field.split('.');
          
          // Resolve alias to actual table name
          const actualTable = aliasToTable.get(tableName) || tableName;
          
          if (!tableExists(this.schema, actualTable)) {
            errors.push(`GroupBy field '${field}' references non-existent table '${tableName}'`);
            continue;
          }

          const tableConfig = getTable(this.schema, actualTable);
          if (tableConfig.columns.some(col => col.name === columnName)) {
            fieldExists = true;
          } else {
            errorDetails = ` in table '${tableName}'`;
          }
        } else {
          // No table prefix - check all tables
          for (const tableName of allTables) {
            const tableConfig = getTable(this.schema, tableName);
            if (tableConfig.columns.some(col => col.name === field)) {
              fieldExists = true;
              break;
            }
          }

          if (!fieldExists) {
            errorDetails = ` in any table (${allTables.join(', ')})`;
          }
        }

        if (!fieldExists) {
          errors.push(`GroupBy field '${field}' does not exist${errorDetails}`);
        }
      }
    }

    // Validate having filters
    if (intent.having) {
      // Create a map of aliases to actual table names
      const aliasToTable = new Map<string, string>();
      aliasToTable.set(intent.table, intent.table);
      if (intent.joins) {
        for (const join of intent.joins) {
          aliasToTable.set(join.alias || join.table, join.table);
        }
      }

      for (const filter of intent.having) {
        const table = filter.table || intent.table;
        
        // Resolve alias to actual table name
        const actualTable = aliasToTable.get(table) || table;
        
        if (!tableExists(this.schema, actualTable)) {
          errors.push(`Table '${table}' referenced in having filter '${filter.field}' does not exist`);
          continue;
        }

        const tableConfig = getTable(this.schema, actualTable);
        const columnExists = tableConfig.columns.some(col => col.name === filter.field);
        if (!columnExists) {
          errors.push(`Having column '${filter.field}' does not exist in table '${table}'`);
        }
      }
    }

    // Validate aggregation rules
    if (intent.aggregations && intent.groupBy) {
      const nonAggregatedColumns = intent.columns.filter(col => !col.agg && !col.expr);
      
      // Collect all tables in the query (primary + joined)
      const allTables = [intent.table, ...(intent.joins?.map(j => j.table) || [])];

      for (const column of nonAggregatedColumns) {
        const field = column.table ? `${column.table}.${column.field}` : column.field;
        
        // Check if column is in groupBy
        const inGroupBy = intent.groupBy.includes(field) || intent.groupBy.includes(column.field);
        
        if (inGroupBy) {
          continue; // OK - column is in groupBy
        }

        // Check if column comes from a joined table
        // TODO: Full functional dependency check - column should only be allowed if
        // its table is functionally determined by the groupBy keys via foreign key.
        // For now, we allow any column from a joined table since the join key
        // constraint implies a functional dependency in most common cases.
        const columnTable = column.table || intent.table;
        const isFromJoinedTable = intent.joins?.some(j => j.table === columnTable);
        
        if (isFromJoinedTable) {
          // Allow columns from joined tables (join key implies functional dependency)
          continue;
        }

        // Column is from primary table but not in groupBy - error
        errors.push(`Non-aggregated column '${column.field}' must be included in GROUP BY`);
      }
    }

    // Validate limit and offset
    if (intent.limit !== undefined && intent.limit < 0) {
      errors.push('Limit must be non-negative');
    }
    if (intent.offset !== undefined && intent.offset < 0) {
      errors.push('Offset must be non-negative');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  private buildAST(intent: QueryIntent): QueryAST {
    const scanNode = this.buildScanNode(intent);
    
    // Separate filters by table
    const primaryTableFilters: QueryIntentFilter[] = [];
    const joinedTableFilters: Map<string, QueryIntentFilter[]> = new Map();
    
    if (intent.filters) {
      for (const filter of intent.filters) {
        const filterTable = filter.table || intent.table;
        
        // Check if this filter applies to a joined table
        const joinedTable = intent.joins?.find(j => {
          const joinAlias = j.alias || j.table;
          return filterTable === joinAlias || filterTable === j.table;
        });
        
        if (joinedTable) {
          // This filter applies to a joined table
          const joinKey = joinedTable.alias || joinedTable.table;
          if (!joinedTableFilters.has(joinKey)) {
            joinedTableFilters.set(joinKey, []);
          }
          joinedTableFilters.get(joinKey)!.push(filter);
        } else {
          // This filter applies to the primary table
          primaryTableFilters.push(filter);
        }
      }
    }
    
    const joinNodes = this.buildJoinNodes(intent, joinedTableFilters);
    const whereExpr = this.buildWhereExpr(primaryTableFilters);
    const projections = this.buildProjections(intent, scanNode, joinNodes);

    // Build table-to-alias mapping for field resolution
    const tableToAlias: Map<string, string> = new Map();
    tableToAlias.set(intent.table, scanNode.alias);
    if (intent.joins) {
      for (const join of intent.joins) {
        tableToAlias.set(join.table, join.alias || join.table);
      }
    }

    const orderBy = this.buildOrderBy(intent, tableToAlias);

    return {
      kind: 'Select',
      columns: projections,
      from: scanNode,
      joins: joinNodes,
      where: whereExpr,
      groupBy: intent.groupBy?.map(field => this.parseFieldReference(field, tableToAlias)),
      aggregations: intent.aggregations?.map(agg => ({
        fn: agg.fn,
        expr: this.parseFieldReference(agg.expr as string, tableToAlias),
        alias: agg.alias
      })),
      having: this.buildWhereExpr(intent.having || []),
      orderBy: orderBy,
      limit: intent.limit,
      offset: intent.offset
    };
  }

  private buildScanNode(intent: QueryIntent): ScanNode {
    const tableConfig = getTable(this.schema, intent.table);
    const schema = getRowSchema(this.schema, intent.table);
    
    return {
      table: intent.table,
      alias: tableConfig.alias || intent.table,
      schema
    };
  }

  private buildJoinNodes(intent: QueryIntent, joinedTableFilters: Map<string, QueryIntentFilter[]>): JoinNode[] {
    if (!intent.joins) {
      return [];
    }

    const joinNodes: JoinNode[] = [];

    for (const join of intent.joins) {
      let onExpr: ExprAST;
      const joinKey = join.alias || join.table;

      if (join.on) {
        // Use explicit join condition
        onExpr = {
          kind: 'BinaryOp',
          op: '=',
          left: this.parseFieldReference(join.on.left),
          right: this.parseFieldReference(join.on.right)
        };
      } else {
        // Derive join condition from foreign keys
        const joinPath = findJoinPath(this.schema, intent.table, join.table);
        if (!joinPath || joinPath.length === 0) {
          throw new Error(`No foreign key relationship found from '${intent.table}' to '${join.table}'`);
        }

        if (joinPath.length === 1) {
          // Direct join
          const fk = joinPath[0];
          onExpr = {
            kind: 'BinaryOp',
            op: '=',
            left: this.parseFieldReference(`${fk.fromTable}.${fk.fromColumn}`),
            right: this.parseFieldReference(`${fk.toTable}.${fk.toColumn}`)
          };
        } else {
          // Multi-hop join - create intermediate joins
          // For now, just use the first relationship in the path
          const firstFk = joinPath[0];
          onExpr = {
            kind: 'BinaryOp',
            op: '=',
            left: this.parseFieldReference(`${firstFk.fromTable}.${firstFk.fromColumn}`),
            right: this.parseFieldReference(`${firstFk.toTable}.${firstFk.toColumn}`)
          };

          // TODO: Create intermediate join nodes for multi-hop paths
        }
      }

      // Add filters for this joined table to the ON condition (for LEFT joins)
      const joinFilters = joinedTableFilters.get(joinKey) || [];
      if (joinFilters.length > 0) {
        for (const filter of joinFilters) {
          const filterExpr = this.buildFilterExpr(filter);
          // Combine with ON condition using AND
          onExpr = {
            kind: 'BinaryOp',
            op: 'AND',
            left: onExpr,
            right: filterExpr
          };
        }
      }

      const joinTableConfig = getTable(this.schema, join.table);
      const schema = getRowSchema(this.schema, join.table);

      joinNodes.push({
        kind: join.kind || 'INNER',
        table: join.table,
        alias: join.alias || join.table,
        on: onExpr,
        schema
      });
    }

    return joinNodes;
  }

  private buildWhereExpr(filters: QueryIntentFilter[]): ExprAST | undefined {
    if (filters.length === 0) {
      return undefined;
    }

    if (filters.length === 1) {
      return this.buildFilterExpr(filters[0]);
    }

    // Combine multiple filters with AND
    let result: ExprAST = this.buildFilterExpr(filters[0]);
    
    for (let i = 1; i < filters.length; i++) {
      result = {
        kind: 'BinaryOp',
        op: 'AND',
        left: result,
        right: this.buildFilterExpr(filters[i])
      };
    }

    return result;
  }

  private buildFilterExpr(filter: QueryIntentFilter): ExprAST {
    const fieldRef = { kind: 'FieldRef' as const, field: filter.field };

    switch (filter.operator) {
      case 'IS NULL':
        return {
          kind: 'IsNull',
          expr: fieldRef
        };

      case 'IS NOT NULL':
        return {
          kind: 'UnaryOp',
          op: 'NOT',
          operand: {
            kind: 'IsNull',
            expr: fieldRef
          }
        };

      case 'IN':
        if (!Array.isArray(filter.value)) {
          throw new Error('IN operator requires array value');
        }
        return {
          kind: 'In',
          expr: fieldRef,
          values: filter.value.map(val => ({ kind: 'Literal' as const, value: val, type: { kind: 'any' as const } }))
        };

      case 'BETWEEN':
        if (!Array.isArray(filter.value) || filter.value.length !== 2) {
          throw new Error('BETWEEN operator requires array with exactly 2 values');
        }
        return {
          kind: 'Between',
          expr: fieldRef,
          low: { kind: 'Literal', value: filter.value[0], type: { kind: 'any' as const } },
          high: { kind: 'Literal', value: filter.value[1], type: { kind: 'any' as const } }
        };

      default:
        // Binary operators: =, !=, <, >, <=, >=, LIKE
        if (filter.value === undefined && !filter.valueRef && !filter.expr) {
          throw new Error(`Filter '${filter.field}' must have either value, valueRef, or expr`);
        }

        const value = filter.expr
          ? { kind: 'SqlExpr' as const, sql: filter.expr }
          : filter.valueRef
            ? { kind: 'VarRef' as const, name: filter.valueRef }
            : { kind: 'Literal' as const, value: filter.value!, type: { kind: 'any' as const } };

        return {
          kind: 'BinaryOp',
          op: filter.operator as BinaryOperator,
          left: fieldRef,
          right: value
        };
    }
  }

  private buildProjections(intent: QueryIntent, scanNode: ScanNode, joinNodes: JoinNode[]): ProjectionNode[] {
    return intent.columns
      .filter(column => !column.agg && !column.expr) // Skip aggregated columns and computed expressions - they're handled by aggregations array
      .map(column => {
      let expr: ExprAST;

      // Simple field reference
      expr = this.parseFieldReference(column.field);

      return {
        expr,
        alias: column.alias || column.field
      };
    });
  }

  private buildOrderBy(intent: QueryIntent, tableToAlias?: Map<string, string>): OrderByNode[] {
    if (!intent.orderBy) {
      return [];
    }

    return intent.orderBy.map(order => ({
      expr: this.parseFieldReference(order.field, tableToAlias),
      direction: order.direction || 'ASC',
      nulls: order.nulls || 'LAST'
    }));
  }

  private parseFieldReference(field: string, tableToAlias?: Map<string, string>): ExprAST {
    // Handle SQL * wildcard
    if (field === '*') {
      return { kind: 'Wildcard' };
    }

    // If field contains SQL functions or parentheses, try to extract the field reference
    // e.g., "COALESCE(SUM(o.total), 0)" -> extract "o.total"
    if (field.includes('(') || field.toUpperCase().includes('COALESCE') || field.toUpperCase().includes('SUM') || field.toUpperCase().includes('AVG')) {
      // Try to extract field from pattern like SUM(table.field) or table.field
      const match = field.match(/([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)/);
      if (match) {
        const tableName = match[1];
        const fieldName = match[2];
        // Map table name to alias if provided
        const alias = tableToAlias?.get(tableName) || tableName;
        return {
          kind: 'FieldRef',
          field: fieldName,
          table: alias
        };
      }
      // Try simple field name
      const simpleMatch = field.match(/\b([a-zA-Z_][a-zA-Z0-9_]*)\b/);
      if (simpleMatch) {
        return {
          kind: 'FieldRef',
          field: simpleMatch[1]
        };
      }
    }

    // Handle qualified field references (table.field)
    if (field.includes('.')) {
      const parts = field.split('.');
      if (parts.length === 2) {
        const tableName = parts[0];
        const fieldName = parts[1];
        // Map table name to alias if provided
        const alias = tableToAlias?.get(tableName) || tableName;
        return {
          kind: 'FieldRef',
          field: fieldName,
          table: alias
        };
      }
    }

    // Simple field reference
    return {
      kind: 'FieldRef',
      field
    };
  }

  private parseSimpleExpression(expr: string): ExprAST {
    // Very simple expression parsing for common cases
    // Support: "field op literal" or "field op field"
    
    // This is a placeholder - in a real implementation you'd use a proper parser
    // For now, just return as field reference
    return this.parseFieldReference(expr);
  }
}
