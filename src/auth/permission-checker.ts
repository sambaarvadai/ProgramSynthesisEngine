import { grantStore } from './grant-store.js';
import { findJoinPath } from '../compiler/schema/schema-config.js';
import type { SchemaConfig } from '../compiler/schema/schema-config.js';
import type { PipelineIntent, PipelineStepIntent } from '../compiler/pipeline/pipeline-intent.js';
import type { QueryPayload, WritePayload, TransformPayload, LLMPayload } from '../nodes/payloads.js';
import type { QueryIntent } from '../compiler/query-ast/query-intent.js';
import type { ExprAST } from '../core/ast/expr-ast.js';

export type MissingColumnResult =
  | { complete: true }
  | {
      complete: false;
      missing: Array<{
        column: string;
        nullable: boolean;
        description: string;
      }>;
    };

export interface PermissionCheckResult {
  allowed: boolean;
  denied?: string[];
  message?: string;
}

export interface GrantedSchemaResult {
  ok: boolean;
  schema?: SchemaConfig;
  message?: string;
}

export interface JoinCompletenessResult {
  ok: boolean;
  missingTables?: string[];
  message?: string;
}

export interface PostIntentValidationResult {
  ok: boolean;
  gate: 'post-intent';
  violations?: Array<{ step: string; table: string; column?: string }>;
  message?: string;
}

export class PermissionChecker {
  /**
   * Scans the raw NL string for any table names or column names from schema
   * that appear as whole words (case-insensitive) and checks access permissions.
   */
  checkExplicitMentions(input: string, userId: string, schema: any): PermissionCheckResult {
    try {
      const denied: string[] = [];
      const inputLower = input.toLowerCase();
      const foundMentions: string[] = [];
      
      // Handle both SchemaConfig (tables) and BuiltSchema (parsed.tables)
      const tables = schema.tables || schema.parsed?.tables;
      if (!tables) {
        return { allowed: true, denied: [] };
      }
      
      // Check table mentions - improved regex for multi-word table names
      for (const tableName of tables.keys()) {
        if (!tableName) continue;
        // Use word boundaries and escape special characters in table names
        const escapedTableName = tableName.toLowerCase().replace(/[-[\]{}()*+?.,\\^$|#\s]/g, '\\$&');
        const tablePattern = new RegExp(`\\b${escapedTableName}\\b`, 'i');
        
        if (tablePattern.test(inputLower)) {
          foundMentions.push(tableName);
          
          if (!grantStore.checkTableAccess(userId, tableName, 'read')) {
            denied.push(tableName); 
          }
        }
      }
      
      // Check column mentions - more restrictive to avoid false matches
      const skippedColumns: string[] = [];
      for (const [tableName, tableConfig] of tables) {
        if (!tableConfig?.columns) continue;
        for (const column of tableConfig.columns) {
          if (!column?.name) continue;
          // Skip common words that could appear in normal text
          const commonWords = ['id', 'name', 'email', 'created', 'updated', 'status', 'type'];
          if (commonWords.includes(column.name.toLowerCase())) {
            skippedColumns.push(`${tableName}.${column.name}`);
            continue;
          }

          const escapedColumnName = column.name.toLowerCase().replace(/[-[\]{}()*+?.,\\^$|#\s]/g, '\\$&');
          const columnPattern = new RegExp(`\\b${escapedColumnName}\\b`, 'i');

          if (columnPattern.test(inputLower)) {
            foundMentions.push(`${tableName}.${column.name}`);

            if (!grantStore.checkColumnAccess(userId, tableName, column.name, 'read')) {
              denied.push(`${tableName}.${column.name}`);
            }
          }
        }
      }

      // Single summary line for skipped common word columns
      if (skippedColumns.length > 0) {
        console.debug(
          `[DEBUG] Skipped ${skippedColumns.length} common word columns ` +
          `(${skippedColumns.slice(0, 5).join(', ')}` +
          `${skippedColumns.length > 5 ? ', ...' : ''})`
        );
      }
      
      if (denied.length > 0) {
        return {
          allowed: false,
          denied
        };
      }
      
      return { allowed: true, denied: [] };
    } catch (e) {
      console.error('[PermissionChecker] Error in checkExplicitMentions:', e);
      return { allowed: true, denied: [] };
    }
  }
  
  /**
   * Gets the granted schema for a user or returns an error if no tables are accessible.
   */
  getGrantedSchemaOrError(userId: string, fullSchema: SchemaConfig): GrantedSchemaResult {
    const grantedSchema = grantStore.getGrantedSchema(userId, fullSchema);
    
    if (grantedSchema.tables.size === 0) {
      // Get list of tables the user has access to for the error message
      const userTableGrants = this.getUserAccessibleTables(userId);
      const accessibleTablesList = userTableGrants.length > 0 
        ? `[${userTableGrants.join(', ')}]` 
        : 'none';
      
      return {
        ok: false,
        message: `Your current access doesn't include any tables relevant to this query. You have access to: ${accessibleTablesList}. To request more, say: request access to [table]`
      };
    }
    
    return {
      ok: true,
      schema: grantedSchema
    };
  }
  
  /**
   * Helper method to get list of tables a user has access to.
   */
  private getUserAccessibleTables(userId: string): string[] {
    // This would need to be implemented in GrantStore, but for now we'll return empty
    // In a real implementation, you'd query the table_grants table
    return [];
  }

  /**
   * Checks if all bridge tables needed for joins between selected tables are accessible.
   */
  checkJoinCompleteness(
    selectedTables: string[],
    grantedSchema: SchemaConfig,
    fullSchema: SchemaConfig
  ): JoinCompletenessResult {
    const missingTables: string[] = [];
    
    // Check every pair of selected tables for join paths
    for (let i = 0; i < selectedTables.length; i++) {
      for (let j = i + 1; j < selectedTables.length; j++) {
        const tableA = selectedTables[i];
        const tableB = selectedTables[j];
        
        try {
          // Find join path in full schema
          const joinPath = findJoinPath(fullSchema, tableA, tableB);
          
          // Extract all bridge tables from the join path
          const bridgeTables = new Set<string>();
          for (const fk of joinPath) {
            // Add both fromTable and toTable from each foreign key in the path
            bridgeTables.add(fk.fromTable);
            bridgeTables.add(fk.toTable);
          }
          
          // Remove the source and destination tables - we only care about bridge tables
          bridgeTables.delete(tableA);
          bridgeTables.delete(tableB);
          
          // Check if all bridge tables are in granted schema
          for (const bridgeTable of bridgeTables) {
            if (!grantedSchema.tables.has(bridgeTable)) {
              missingTables.push(bridgeTable);
            }
          }
        } catch (error) {
          // No join path exists between these tables, which is fine
          // We only care about cases where a join path exists but bridge tables are missing
          continue;
        }
      }
    }
    
    if (missingTables.length > 0) {
      // Find specific table pairs that need the missing bridge tables
      const tablePairsNeedingBridge: string[] = [];
      
      for (let i = 0; i < selectedTables.length; i++) {
        for (let j = i + 1; j < selectedTables.length; j++) {
          const tableA = selectedTables[i];
          const tableB = selectedTables[j];
          
          try {
            const joinPath = findJoinPath(fullSchema, tableA, tableB);
            const bridgeTables = new Set<string>();
            
            for (const fk of joinPath) {
              bridgeTables.add(fk.fromTable);
              bridgeTables.add(fk.toTable);
            }
            
            bridgeTables.delete(tableA);
            bridgeTables.delete(tableB);
            
            // Check if this pair needs any of the missing bridge tables
            const needsMissingBridge = Array.from(bridgeTables).some(
              bridge => missingTables.includes(bridge)
            );
            
            if (needsMissingBridge) {
              tablePairsNeedingBridge.push(`${tableA} and ${tableB}`);
              break; // Only add each pair once
            }
          } catch (error) {
            continue;
          }
        }
      }
      
      const uniqueMissingTables = [...new Set(missingTables)];
      const message = `To join ${tablePairsNeedingBridge.join(', ')}, access to ${uniqueMissingTables.join(', ')} is also needed. Request access with: request access to ${uniqueMissingTables.join(', ')}`;
      
      return {
        ok: false,
        missingTables: uniqueMissingTables,
        message
      };
    }
    
    return { ok: true };
  }

  /**
   * Validates permissions on a generated PipelineIntent as a safety net against LLM hallucinations.
   */
  validatePipelineIntent(
    intent: PipelineIntent,
    userId: string,
    grantedSchema: SchemaConfig
  ): PostIntentValidationResult {
    const violations: Array<{ step: string; table: string; column?: string }> = [];

    for (const step of intent.steps) {
      switch (step.kind) {
        case 'query': {
          // This will be checked after enrichNodes when QueryPayload is populated
          // For now, we can't validate query steps since they don't have payloads yet
          break;
        }

        case 'write': {
          // Check if user has write access to the target table
          // Note: WritePayload will be populated during enrichNodes, so we check step.config for now
          const config = step.config as any;
          if (config?.table) {
            if (!grantStore.checkTableAccess(userId, config.table, 'write')) {
              violations.push({
                step: step.id,
                table: config.table
              });
            }
          }
          break;
        }

        case 'transform': {
          // Check FieldRef table references in transform operations
          // Note: TransformPayload will be populated during enrichNodes
          const config = step.config as any;
          if (config?.operations) {
            this.extractTableReferencesFromTransformOps(config.operations, violations, step.id, userId);
          }
          break;
        }

        case 'llm': {
          // Check FieldRef table references in LLM prompts
          // Note: LLMPayload will be populated during enrichNodes
          const config = step.config as any;
          if (config?.systemPrompt || config?.userPrompt) {
            this.extractTableReferencesFromLLMPayload(config, violations, step.id, userId);
          }
          break;
        }

        // Other step types (conditional, loop, merge, parallel, etc.) don't typically
        // reference tables directly, so we skip them for now
      }
    }

    if (violations.length > 0) {
      const tableList = [...new Set(violations.map(v => v.table))].join(', ');
      return {
        ok: false,
        gate: 'post-intent',
        violations,
        message: `The generated plan references tables/columns you don't have access to: ${tableList}. This may mean the query needs a table you haven't been granted. Request access with: request access to ${tableList}`
      };
    }

    return { ok: true, gate: 'post-intent' };
  }

  /**
   * Validates permissions on an enriched PipelineGraph (after enrichNodes).
   * This is the main validation method that checks actual payloads.
   */
  validateEnrichedPipeline(
    intent: PipelineIntent,
    userId: string,
    grantedSchema: SchemaConfig,
    graph: any // PipelineGraph - using any to avoid circular import
  ): PostIntentValidationResult {
    const violations: Array<{ step: string; table: string; column?: string }> = [];

    for (const step of intent.steps) {
      const node = graph.nodes.get(step.id);
      if (!node) continue;

      switch (node.kind) {
        case 'query': {
          const payload = node.payload as QueryPayload;
          const queryIntent = payload.intent;

          // Check primary table
          if (!grantedSchema.tables.has(queryIntent.table)) {
            violations.push({
              step: step.id,
              table: queryIntent.table
            });
          }

          // Check join tables
          if (queryIntent.joins) {
            for (const join of queryIntent.joins) {
              if (!grantedSchema.tables.has(join.table)) {
                violations.push({
                  step: step.id,
                  table: join.table
                });
              }
            }
          }

          // Check column-level access
          this.checkQueryIntentColumns(queryIntent, violations, step.id, userId);

          break;
        }

        case 'write': {
          const payload = node.payload as WritePayload;
          if (!grantStore.checkTableAccess(userId, payload.table, 'write')) {
            violations.push({
              step: step.id,
              table: payload.table
            });
          }

          // Check column-level access for write columns
          for (const column of payload.columns) {
            if (!grantStore.checkColumnAccess(userId, payload.table, column, 'write')) {
              violations.push({
                step: step.id,
                table: payload.table,
                column
              });
            }
          }

          break;
        }

        case 'transform': {
          const payload = node.payload as TransformPayload;
          this.extractTableReferencesFromTransformOps(payload.operations, violations, step.id, userId);
          break;
        }

        case 'llm': {
          const payload = node.payload as LLMPayload;
          this.extractTableReferencesFromLLMPayload(payload, violations, step.id, userId);
          break;
        }
      }
    }

    if (violations.length > 0) {
      const tableList = [...new Set(violations.map(v => v.table))].join(', ');
      return {
        ok: false,
        gate: 'post-intent',
        violations,
        message: `The generated plan references tables/columns you don't have access to: ${tableList}. This may mean the query needs a table you haven't been granted. Request access with: request access to ${tableList}`
      };
    }

    return { ok: true, gate: 'post-intent' };
  }

  /**
   * Extract table references from TransformPayload operations and check permissions.
   */
  private extractTableReferencesFromTransformOps(
    operations: any[], // TransformOp[]
    violations: Array<{ step: string; table: string; column?: string }>,
    stepId: string,
    userId: string
  ): void {
    for (const op of operations) {
      // Recursively extract table references from ExprAST nodes
      this.extractTableReferencesFromExprAST(op.expr, violations, stepId, userId);
      this.extractTableReferencesFromExprAST(op.predicate, violations, stepId, userId);
      
      // For sort operations
      if (op.keys) {
        for (const key of op.keys) {
          this.extractTableReferencesFromExprAST(key.expr, violations, stepId, userId);
        }
      }
    }
  }

  /**
   * Extract table references from LLMPayload and check permissions.
   */
  private extractTableReferencesFromLLMPayload(
    payload: any, // LLMPayload
    violations: Array<{ step: string; table: string; column?: string }>,
    stepId: string,
    userId: string
  ): void {
    // Check system prompt and user prompt for FieldRef expressions
    if (payload.systemPrompt) {
      for (const part of payload.systemPrompt.parts) {
        if (part.kind === 'expr') {
          this.extractTableReferencesFromExprAST(part.expr, violations, stepId, userId);
        }
      }
    }

    if (payload.userPrompt) {
      for (const part of payload.userPrompt.parts) {
        if (part.kind === 'expr') {
          this.extractTableReferencesFromExprAST(part.expr, violations, stepId, userId);
        }
      }
    }
  }

  /**
   * Recursively extract table references from ExprAST and check permissions.
   */
  private extractTableReferencesFromExprAST(
    expr: ExprAST | undefined,
    violations: Array<{ step: string; table: string; column?: string }>,
    stepId: string,
    userId: string
  ): void {
    if (!expr) return;

    switch (expr.kind) {
      case 'FieldRef': {
        if (expr.table) {
          // Check table access
          if (!grantStore.checkTableAccess(userId, expr.table, 'read')) {
            violations.push({
              step: stepId,
              table: expr.table
            });
          } else {
            // Check column access
            if (!grantStore.checkColumnAccess(userId, expr.table, expr.field, 'read')) {
              violations.push({
                step: stepId,
                table: expr.table,
                column: expr.field
              });
            }
          }
        }
        break;
      }

      case 'BinaryOp': {
        this.extractTableReferencesFromExprAST(expr.left, violations, stepId, userId);
        this.extractTableReferencesFromExprAST(expr.right, violations, stepId, userId);
        break;
      }

      case 'UnaryOp': {
        this.extractTableReferencesFromExprAST(expr.operand, violations, stepId, userId);
        break;
      }

      case 'FunctionCall': {
        for (const arg of expr.args) {
          this.extractTableReferencesFromExprAST(arg, violations, stepId, userId);
        }
        break;
      }

      case 'Conditional': {
        this.extractTableReferencesFromExprAST(expr.condition, violations, stepId, userId);
        this.extractTableReferencesFromExprAST(expr.then, violations, stepId, userId);
        this.extractTableReferencesFromExprAST(expr.else, violations, stepId, userId);
        break;
      }

      case 'Cast': {
        this.extractTableReferencesFromExprAST(expr.expr, violations, stepId, userId);
        break;
      }

      case 'IsNull': {
        this.extractTableReferencesFromExprAST(expr.expr, violations, stepId, userId);
        break;
      }

      case 'In': {
        this.extractTableReferencesFromExprAST(expr.expr, violations, stepId, userId);
        for (const value of expr.values) {
          this.extractTableReferencesFromExprAST(value, violations, stepId, userId);
        }
        break;
      }

      case 'Between': {
        this.extractTableReferencesFromExprAST(expr.expr, violations, stepId, userId);
        this.extractTableReferencesFromExprAST(expr.low, violations, stepId, userId);
        this.extractTableReferencesFromExprAST(expr.high, violations, stepId, userId);
        break;
      }

      case 'WindowExpr': {
        this.extractTableReferencesFromExprAST(expr.over, violations, stepId, userId);
        if (expr.partition) {
          for (const part of expr.partition) {
            this.extractTableReferencesFromExprAST(part, violations, stepId, userId);
          }
        }
        if (expr.orderBy) {
          for (const order of expr.orderBy) {
            this.extractTableReferencesFromExprAST(order, violations, stepId, userId);
          }
        }
        break;
      }

      // Literal, VarRef, SqlExpr, Wildcard don't contain table references
      case 'Literal':
      case 'VarRef':
      case 'SqlExpr':
      case 'Wildcard':
        break;
    }
  }

  /**
   * Check column-level access for QueryIntent columns, filters, etc.
   */
  private checkQueryIntentColumns(
    queryIntent: QueryIntent,
    violations: Array<{ step: string; table: string; column?: string }>,
    stepId: string,
    userId: string
  ): void {
    // Check columns
    for (const column of queryIntent.columns) {
      const table = column.table || queryIntent.table;
      if (!grantStore.checkColumnAccess(userId, table, column.field, 'read')) {
        violations.push({
          step: stepId,
          table,
          column: column.field
        });
      }
    }

    // Check filters
    if (queryIntent.filters) {
      for (const filter of queryIntent.filters) {
        const table = filter.table || queryIntent.table;
        if (!grantStore.checkColumnAccess(userId, table, filter.field, 'read')) {
          violations.push({
            step: stepId,
            table,
            column: filter.field
          });
        }
      }
    }

    // Check having clauses
    if (queryIntent.having) {
      for (const having of queryIntent.having) {
        const table = having.table || queryIntent.table;
        if (!grantStore.checkColumnAccess(userId, table, having.field, 'read')) {
          violations.push({
            step: stepId,
            table,
            column: having.field
          });
        }
      }
    }

    // Check order by
    if (queryIntent.orderBy) {
      for (const orderBy of queryIntent.orderBy) {
        const table = orderBy.table || queryIntent.table;
        if (!grantStore.checkColumnAccess(userId, table, orderBy.field, 'read')) {
          violations.push({
            step: stepId,
            table,
            column: orderBy.field
          });
        }
      }
    }
  }

  /**
   * Gets the required columns for a table based on schema configuration.
   * A column is "required" if: !nullable && !hasDefault && !isGenerated
   */
  getRequiredColumns(tableName: string, schema: SchemaConfig): string[] {
    const tableConfig = schema.tables.get(tableName);
    if (!tableConfig) {
      throw new Error(`Table ${tableName} not found in schema`);
    }

    return tableConfig.columns
      .filter(column => {
        // A column is required if it's not nullable and doesn't have a default value and isn't generated
        const isRequired = !column.nullable && 
                           !(column.hasDefault === true) && 
                           !(column.isGenerated === true);
        return isRequired;
      })
      .map(column => column.name);
  }

  /**
   * Checks for missing columns in a write operation.
   */
  getMissingColumns(
    tableName: string,
    schema: SchemaConfig,
    writePayload: WritePayload,
    upstreamColumns: string[]
  ): MissingColumnResult {
    const tableConfig = schema.tables.get(tableName);
    if (!tableConfig) {
      throw new Error(`Table ${tableName} not found in schema`);
    }

    // Required column check only applies to INSERT and UPSERT modes.
    // For UPDATE: only the SET columns need to be present.
    // For DELETE: only WHERE columns matter.
    const isInsertMode = ['insert', 'insert_ignore', 'upsert'].includes(writePayload.mode)
    
    if (!isInsertMode) {
      // For UPDATE specifically, validate that at least one SET column is provided
      if (writePayload.mode === 'update') {
        const hasSetColumns = (writePayload.columns?.length ?? 0) > 0 || 
                              Object.keys(writePayload.staticValues ?? {}).length > 0
        if (!hasSetColumns) {
          return {
            complete: false,
            missing: [{
              column: '(SET clause)',
              nullable: false,
              description: 'UPDATE requires at least one column to set'
            }]
          }
        }
      }
      return { complete: true }  // UPDATE/DELETE never fails required-column check
    }

    // Get required columns for the table (INSERT/UPSERT only)
    const requiredColumns = this.getRequiredColumns(tableName, schema);

    // Build set of available columns from:
    // 1. writePayload.columns (dynamic columns from input)
    // 2. writePayload.staticValues keys with non-empty values (static values)
    // 3. upstreamColumns (columns available from upstream QueryNode)
    const availableColumns = new Set([
      ...(writePayload.columns || []),
      ...(writePayload.staticValues ? Object.keys(writePayload.staticValues).filter(key => {
        const value = writePayload.staticValues![key];
        // Consider empty strings as missing for required fields
        return value !== '' && value !== null && value !== undefined;
      }) : []),
      ...upstreamColumns
    ]);

    // Find missing required columns
    const missingRequired = requiredColumns.filter(col => !availableColumns.has(col));

    // If no missing required columns, we're complete
    if (missingRequired.length === 0) {
      return { complete: true };
    }

    // Build missing column information
    const missing = missingRequired.map(columnName => {
      const columnConfig = tableConfig.columns.find(col => col.name === columnName);
      return {
        column: columnName,
        nullable: columnConfig?.nullable ?? false,
        description: columnConfig?.description ?? `${columnName} column`
      };
    });

    return {
      complete: false,
      missing
    };
  }
}
