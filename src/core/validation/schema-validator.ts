/**
 * Schema Validation System - Pre-flight validation for pipeline operations
 */

import type { SchemaConfig } from '../../compiler/schema/schema-config.js';
import type { PipelineGraph, PipelineNode } from '../graph/index.js';
import type { WritePayload } from '../../nodes/payloads.js';
import { crmSchema } from '../../schema/crm-schema.js';

// Define types locally to avoid import issues
interface TableSchema {
  columns: Record<string, ColumnSchema>;
  primaryKey?: string[];
  foreignKeys?: any[];
}

interface ColumnSchema {
  type: string;
  nullable?: boolean;
  hasDefault?: boolean;
  primaryKey?: boolean;
  default?: any;
}

export interface SchemaValidationError {
  nodeId: string;
  operation: string;
  table: string;
  column?: string;
  error: string;
  severity: 'error' | 'warning';
  suggestion: string;
}

export interface SchemaValidationResult {
  isValid: boolean;
  errors: SchemaValidationError[];
  warnings: SchemaValidationError[];
  summary: string;
}

export interface ColumnRequirement {
  name: string;
  required: boolean;
  type?: string;
  hasDefault?: boolean;
}

export interface TableRequirement {
  tableName: string;
  requiredColumns: ColumnRequirement[];
  optionalColumns: ColumnRequirement[];
}

export class SchemaValidator {
  constructor(
    private schemaConfig: SchemaConfig
  ) {}

  /**
   * Validate entire pipeline against live database schema
   */
  async validatePipeline(graph: PipelineGraph): Promise<SchemaValidationResult> {
    const errors: SchemaValidationError[] = [];
    const warnings: SchemaValidationError[] = [];

    // Get live database schema
    const liveSchema = await this.getLiveSchema();
    
    // Validate each node in the pipeline
    const INTERNAL_NODE_KINDS = new Set(['input', 'output', '_input', '_output']);
    
    for (const [nodeId, node] of graph.nodes) {
      // Skip internal scheduler nodes
      if (INTERNAL_NODE_KINDS.has(node.kind) || INTERNAL_NODE_KINDS.has(nodeId)) {
        continue;
      }
      
      const nodeValidation = await this.validateNode(nodeId, node, liveSchema);
      errors.push(...nodeValidation.errors);
      warnings.push(...nodeValidation.warnings);
    }

    const isValid = errors.length === 0;
    const summary = this.generateSummary(errors, warnings);

    return {
      isValid,
      errors,
      warnings,
      summary
    };
  }

  /**
   * Validate a single node against the live schema
   */
  private async validateNode(
    nodeId: string, 
    node: PipelineNode, 
    liveSchema: Map<string, TableSchema>
  ): Promise<{ errors: SchemaValidationError[], warnings: SchemaValidationError[] }> {
    const errors: SchemaValidationError[] = [];
    const warnings: SchemaValidationError[] = [];

    switch (node.kind) {
      case 'write':
        const writeValidation = await this.validateWriteNode(nodeId, node, liveSchema);
        errors.push(...writeValidation.errors);
        warnings.push(...writeValidation.warnings);
        break;
      
      case 'query':
        const queryValidation = await this.validateQueryNode(nodeId, node, liveSchema);
        errors.push(...queryValidation.errors);
        warnings.push(...queryValidation.warnings);
        break;
      
      case 'llm':
        // LLM nodes typically don't directly access database schema
        break;
      
      default:
        warnings.push({
          nodeId,
          operation: node.kind,
          table: 'unknown',
          error: `Unknown node type: ${node.kind}`,
          severity: 'warning',
          suggestion: 'Verify node type is supported'
        });
    }

    return { errors, warnings };
  }

  /**
   * Validate write node against live schema
   */
  private async validateWriteNode(
    nodeId: string,
    node: PipelineNode,
    liveSchema: Map<string, TableSchema>
  ): Promise<{ errors: SchemaValidationError[], warnings: SchemaValidationError[] }> {
    const errors: SchemaValidationError[] = [];
    const warnings: SchemaValidationError[] = [];
    const payload = node.payload as WritePayload;

    // Check if table exists
    if (!liveSchema.has(payload.table)) {
      errors.push({
        nodeId,
        operation: 'write',
        table: payload.table,
        error: `Table '${payload.table}' does not exist in database`,
        severity: 'error',
        suggestion: `Check table name or create table '${payload.table}'`
      });
      return { errors, warnings };
    }

    const tableSchema = liveSchema.get(payload.table)!;
    const tableColumns = new Map(Object.entries(tableSchema.columns));

    // Validate target columns
    console.log(`[Schema Validator] Validating write node ${nodeId} for table ${payload.table}`);
    console.log(`[Schema Validator] Payload columns:`, payload.columns);
    console.log(`[Schema Validator] Table columns available:`, Array.from(tableColumns.keys()));
    
    for (const column of payload.columns || []) {
      if (!tableColumns.has(column)) {
        console.log(`[Schema Validator] Column '${column}' not found in table '${payload.table}'`);
        errors.push({
          nodeId,
          operation: 'write',
          table: payload.table,
          column,
          error: `Column '${column}' does not exist in table '${payload.table}'`,
          severity: 'error',
          suggestion: `Check column name or add column '${column}' to table`
        });
      }
    }

    // Validate static values columns too
    for (const column of Object.keys(payload.staticValues || {})) {
      if (!tableColumns.has(column)) {
        errors.push({
          nodeId,
          operation: 'write',
          table: payload.table,
          column,
          error: `Column '${column}' does not exist in table '${payload.table}'`,
          severity: 'error',
          suggestion: `Check column name or add column '${column}' to table`
        });
      }
    }

    // Check for missing required columns only for INSERT and UPSERT modes
    const isInsertMode = ['insert', 'insert_ignore', 'upsert'].includes(payload.mode);
    
    if (isInsertMode) {
      const missingRequired = this.findMissingRequiredColumns(payload, tableSchema, payload.table);
      for (const missing of missingRequired) {
        errors.push({
          nodeId,
          operation: 'write',
          table: payload.table,
          column: missing,
          error: `Required column '${missing}' is missing from write operation`,
          severity: 'error',
          suggestion: `Add '${missing}' to columns or provide a default value`
        });
      }
    }
    // For UPDATE/DELETE: don't check required columns - only SET columns need to be present

    // Validate static values for column types
    const tableColumnNames = new Set(tableColumns.keys());
    for (const [column, value] of Object.entries(payload.staticValues || {})) {
      if (tableColumns.has(column)) {
        // Skip type checking for values that look like field references
        if (typeof value === 'string' && tableColumnNames.has(value)) {
          // Value looks like a field reference, not a literal - skip type check
          console.warn(`[Schema Validator] Skipping type check for '${column}': ` +
            `value '${value}' looks like a field reference`)
          continue
        }
        
        const columnSchema = tableColumns.get(column)!;
        const typeValidation = this.validateColumnType(column, value, columnSchema, payload.table);
        if (!typeValidation.isValid) {
          errors.push({
            nodeId,
            operation: 'write',
            table: payload.table,
            column,
            error: typeValidation.error,
            severity: 'error',
            suggestion: typeValidation.suggestion
          });
        }
      }
    }

    // Validate mode-specific requirements
    const modeValidation = this.validateWriteMode(payload, tableSchema);
    errors.push(...modeValidation.errors);
    warnings.push(...modeValidation.warnings);

    // Additional UPDATE-specific validation
    if (payload.mode === 'update') {
      const tableColumnNames = new Set(Object.keys(tableSchema.columns));
      
      // For UPDATE, the columns being SET must exist in the table
      // (payload.columns = dynamic SET columns, payload.staticValues keys = static SET values)
      const allSetColumns = [
        ...payload.columns,
        ...Object.keys(payload.staticValues ?? {})
      ];
      
      for (const col of allSetColumns) {
        if (!tableColumnNames.has(col)) {
          errors.push({
            nodeId,
            operation: 'write',
            table: payload.table,
            column: col,
            error: `SET column '${col}' does not exist in table '${payload.table}'`,
            severity: 'error',
            suggestion: `Remove '${col}' from the update or check the column name`
          });
        }
      }
      
      // Also validate staticWhere columns exist
      for (const col of Object.keys(payload.staticWhere ?? {})) {
        if (!tableColumnNames.has(col)) {
          errors.push({
            nodeId,
            operation: 'write',
            table: payload.table,
            column: col,
            error: `WHERE column '${col}' does not exist in table '${payload.table}'`,
            severity: 'error',
            suggestion: `Check the column name in staticWhere`
          });
        }
      }
    }

    return { errors, warnings };
  }

  /**
   * Validate query node against live schema
   */
  private async validateQueryNode(
    nodeId: string,
    node: PipelineNode,
    liveSchema: Map<string, TableSchema>
  ): Promise<{ errors: SchemaValidationError[], warnings: SchemaValidationError[] }> {
    const errors: SchemaValidationError[] = [];
    const warnings: SchemaValidationError[] = [];

    // For query nodes, we'll validate the SQL against the schema
    // This is a simplified validation - in practice, you might want to parse the SQL
    const description = (node.payload as any)?.description || '';
    
    // Extract table names from description (simplified regex approach)
    const tableMatches = description.match(/\b([a-z_][a-z0-9_]*)\b/gi) || [];
    
    for (const tableName of tableMatches) {
      if (!liveSchema.has(tableName) && this.looksLikeTableName(tableName)) {
        errors.push({
          nodeId,
          operation: 'query',
          table: tableName,
          error: `Table '${tableName}' referenced in query does not exist`,
          severity: 'error',
          suggestion: `Check table name or ensure '${tableName}' exists in database`
        });
      }
    }

    return { errors, warnings };
  }

  /**
   * Get live database schema
   */
  public async getLiveSchema(): Promise<Map<string, TableSchema>> {
    try {
      // This would typically query the database information schema
      // For now, we'll use the configured schema as a fallback
      // In a real implementation, you'd query: 
      // SELECT table_name, column_name, data_type, is_nullable, column_default 
      // FROM information_schema.columns WHERE table_schema = 'public'
      
      const liveSchema = new Map<string, TableSchema>();
      
      // Use the configured schema as live schema (in production, query actual DB)
      for (const [tableName, tableConfig] of this.schemaConfig.tables) {
        // Convert TableConfig to TableSchema format
        const tableSchema: TableSchema = {
          columns: {}
        };
        
        // Convert columns array to record
        for (const column of tableConfig.columns) {
          tableSchema.columns[column.name] = {
            type: column.type.kind === 'string' ? 'varchar' : 
                  column.type.kind === 'number' ? 'integer' : 
                  column.type.kind === 'boolean' ? 'boolean' : 'text',
            nullable: column.nullable,
            hasDefault: column.hasDefault || false,
            primaryKey: column.primaryKey || false
          };
        }
        
        liveSchema.set(tableName, tableSchema);
      }
      
      return liveSchema;
    } catch (error) {
      throw new Error(`Failed to get live schema: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Find missing required columns for write operation
   */
  private findMissingRequiredColumns(payload: WritePayload, tableSchema: TableSchema, tableName: string): string[] {
    const missing: string[] = [];
    const providedColumns = new Set([
      ...(payload.columns || []),
      ...(Object.keys(payload.staticValues || {})),
      ...(payload.staticWhere ? Object.keys(payload.staticWhere) : []),
      ...(payload.whereColumns || [])
    ]);

    for (const [columnName, columnSchema] of Object.entries(tableSchema.columns)) {
      // Use DDLParser's accurate default info from crmSchema.parsed.tables
      // (not from live schema which may lack defaults)
      const colDef = (crmSchema as any).parsed.tables
        .get(tableName)?.columns.get(columnName);
      
      // Check if column is required (not nullable and no default)
      // Skip columns that have defaults or are primary keys (typically auto-generated)
      const hasDefault = columnSchema.hasDefault || 
                        columnSchema.primaryKey ||
                        (columnSchema as any).defaultRaw !== null ||
                        (colDef?.defaultRaw !== null);
      
      if (columnSchema.nullable === false && !hasDefault) {
        if (!providedColumns.has(columnName)) {
          missing.push(columnName);
        }
      }
    }

    return missing;
  }

  /**
   * Validate column type compatibility
   */
  private validateColumnType(
    columnName: string,
    value: any,
    columnSchema: ColumnSchema,
    tableName: string
  ): { isValid: boolean; error: string; suggestion: string } {
    // Basic type validation - could be enhanced with more sophisticated checks
    if (value === null || value === undefined) {
      if (columnSchema.nullable === false) {
        return {
          isValid: false,
          error: `Column '${columnName}' cannot be null`,
          suggestion: `Provide a non-null value or make column nullable`
        };
      }
      return { isValid: true, error: '', suggestion: '' };
    }

    // Use DDLParser's accurate type info from crmSchema.parsed.tables
    // (not from engine SchemaConfig which has TEXT fallback)
    const colDef = (crmSchema as any).parsed.tables
      .get(tableName)?.columns.get(columnName);
    const sqlType = colDef?.type?.toUpperCase() ?? 'TEXT';
    
    const expectedJsType = this.sqlTypeToJsType(sqlType);
    const actualJsType = typeof value;
    
    if (actualJsType !== expectedJsType) {
      return {
        isValid: false,
        error: `Column '${columnName}' expects ${expectedJsType}, got ${actualJsType}`,
        suggestion: `Convert value to ${expectedJsType}`
      };
    }

    return { isValid: true, error: '', suggestion: '' };
  }

  /**
   * Map SQL types to JavaScript types
   */
  private sqlTypeToJsType(sqlType: string): string {
    const t = sqlType.toUpperCase();
    if (t === 'INT' || t === 'INTEGER' || t === 'SERIAL' || 
        t === 'BIGINT' || t === 'SMALLINT' || t === 'INT2' || t === 'INT4' || t === 'INT8' ||
        t.startsWith('NUMERIC') || t === 'REAL' || t === 'DOUBLE PRECISION' || t === 'FLOAT') {
      return 'number';
    }
    if (t === 'BOOLEAN' || t === 'BOOL') return 'boolean';
    if (t === 'JSONB' || t === 'JSON') return 'object';
    return 'string';  // TEXT, VARCHAR, CHAR, TIMESTAMPTZ, DATE, INET, etc.
  }

  /**
   * Validate write mode specific requirements
   */
  private validateWriteMode(
    payload: WritePayload,
    tableSchema: TableSchema
  ): { errors: SchemaValidationError[], warnings: SchemaValidationError[] } {
    const errors: SchemaValidationError[] = [];
    const warnings: SchemaValidationError[] = [];

    switch (payload.mode) {
      case 'insert_ignore':
        // Check if there are unique constraints that might cause conflicts
        if (payload.columns && payload.columns.length === 0) {
          warnings.push({
            nodeId: '',
            operation: 'write',
            table: payload.table,
            error: 'INSERT_IGNORE mode with no columns specified',
            severity: 'warning',
            suggestion: 'Specify columns to avoid unexpected behavior'
          });
        }
        break;
      
      case 'upsert':
        if (!payload.conflictColumns || payload.conflictColumns.length === 0) {
          errors.push({
            nodeId: '',
            operation: 'write',
            table: payload.table,
            error: 'UPSERT mode requires conflict columns',
            severity: 'error',
            suggestion: 'Specify conflictColumns for UPSERT operation'
          });
        }
        break;
      
      case 'update':
        const hasDynamicWhere = payload.whereColumns && payload.whereColumns.length > 0
        const hasStaticWhere = payload.staticWhere && Object.keys(payload.staticWhere).length > 0
        
        if (!hasDynamicWhere && !hasStaticWhere) {
          errors.push({
            nodeId: '',
            operation: 'write',
            table: payload.table,
            error: 'UPDATE mode requires where conditions',
            severity: 'error',
            suggestion: 'Specify whereColumns for dynamic WHERE, or staticWhere for literal conditions (e.g. staticWhere: { id: 107 })'
          });
        }
        break;
    }

    return { errors, warnings };
  }

  /**
   * Check if a string looks like a table name
   */
  private looksLikeTableName(name: string): boolean {
    // Simple heuristic - could be enhanced
    return /^[a-z_][a-z0-9_]*$/i.test(name) && 
           !['and', 'or', 'not', 'in', 'like', 'between', 'is', 'null'].includes(name.toLowerCase());
  }

  /**
   * Generate validation summary
   */
  private generateSummary(errors: SchemaValidationError[], warnings: SchemaValidationError[]): string {
    if (errors.length === 0 && warnings.length === 0) {
      return 'Schema validation passed successfully';
    }

    const parts: string[] = [];
    
    if (errors.length > 0) {
      parts.push(`${errors.length} error${errors.length > 1 ? 's' : ''} found`);
    }
    
    if (warnings.length > 0) {
      parts.push(`${warnings.length} warning${warnings.length > 1 ? 's' : ''}`);
    }

    // Extract the most common operation mode from errors for context
    let modeContext = '';
    if (errors.length > 0) {
      const modes = errors.map(e => e.operation).filter((op, i, arr) => arr.indexOf(op) === i);
      if (modes.length === 1) {
        modeContext = ` (${modes[0]})`;
      }
    }

    return `Schema validation${modeContext}: ${parts.join(', ')}`;
  }

  /**
   * Format validation result for display
   */
  formatForDisplay(result: SchemaValidationResult): string {
    let output = '';
    
    output += `\n=== Schema Validation Results ===\n`;
    output += `Status: ${result.isValid ? 'PASSED' : 'FAILED'}\n`;
    output += `Summary: ${result.summary}\n`;

    if (result.errors.length > 0) {
      output += `\nErrors:\n`;
      result.errors.forEach((error, i) => {
        output += `  ${i + 1}. [${error.nodeId}] ${error.error}\n`;
        output += `     Table: ${error.table}${error.column ? `, Column: ${error.column}` : ''}\n`;
        output += `     Suggestion: ${error.suggestion}\n`;
      });
    }

    if (result.warnings.length > 0) {
      output += `\nWarnings:\n`;
      result.warnings.forEach((warning, i) => {
        output += `  ${i + 1}. [${warning.nodeId}] ${warning.error}\n`;
        output += `     Suggestion: ${warning.suggestion}\n`;
      });
    }

    return output;
  }
}
