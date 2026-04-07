// Scan operator for reading data from storage backends

import { BasePhysicalOperator } from '../physical-operator.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import type { RowBatch, Row } from '../../core/types/row.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { ExprAST } from '../../core/ast/expr-ast.js';
import type { StorageBackend } from '../../core/storage/storage-backend.js';
import type { Value } from '../../core/types/value.js';

interface ScanOptions {
  table: string;
  alias: string;
  schema: RowSchema;
  predicate?: ExprAST;
  columns?: string[];
  batchSize: number;
  backend: StorageBackend;
}

export class ScanOperator extends BasePhysicalOperator {
  readonly kind = 'Scan';
  
  private generator: AsyncIterator<RowBatch> | null = null;

  constructor(private opts: ScanOptions) {
    super();
  }

  protected async doOpen(ctx: ExecutionContext): Promise<void> {
    // Validate table exists in schema (not a live DB check)
    if (!this.opts.schema.columns || this.opts.schema.columns.length === 0) {
      throw new Error(`Schema for table '${this.opts.table}' is empty`);
    }

    // Initialize generator from backend.scan
    this.generator = this.opts.backend.scan({
      table: this.opts.table,
      predicate: this.opts.predicate,
      columns: this.opts.columns,
      batchSize: this.opts.batchSize,
    })[Symbol.asyncIterator]();

    // Record trace event
    ctx.trace.events.push({
      nodeId: this.kind,
      kind: 'start',
      timestamp: Date.now(),
      meta: { 
        table: this.opts.table,
        alias: this.opts.alias,
        hasPredicate: !!this.opts.predicate,
        columns: this.opts.columns || ['*']
      }
    });
  }

  protected async doNextBatch(size: number): Promise<RowBatch> {
    if (!this.generator) {
      throw new Error('ScanOperator not opened');
    }

    const result = await this.generator.next();
    
    if (result.done) {
      // Return empty RowBatch when exhausted
      return {
        rows: [],
        schema: this.opts.schema
      };
    }

    const batch = result.value;
    
    // Alias all row fields: if alias is 'o', field 'total' becomes 'o.total'
    // AND keep unaliased name too for convenience
    const aliasedRows = batch.rows.map(row => {
      const aliasedRow: Row = {};
      
      // Add aliased fields
      for (const [fieldName, value] of Object.entries(row)) {
        const aliasedFieldName = `${this.opts.alias}.${fieldName}`;
        aliasedRow[aliasedFieldName] = value as Value;
      }
      
      // Keep original unaliased fields too
      Object.assign(aliasedRow, row);
      
      return aliasedRow;
    });

    return {
      rows: aliasedRows,
      schema: batch.schema
    };
  }

  protected async doClose(): Promise<void> {
    if (this.generator) {
      await this.generator.return?.();
      this.generator = null;
    }
  }
}
