// Join operator for combining data from two sources

import { BasePhysicalOperator } from '../physical-operator.js';
import { collectAll } from '../physical-operator.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import type { RowBatch, Row } from '../../core/types/row.js';
import type { Value } from '../../core/types/value.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { ExprAST } from '../../core/ast/expr-ast.js';
import type { PhysicalOperator } from '../physical-operator.js';
import type { ExprEvaluator } from '../expr-evaluator.js';
import type { TempStore } from '../../core/storage/temp-store.js';

export type JoinKind = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';

export interface JoinOptions {
  left: PhysicalOperator;
  right: PhysicalOperator;
  on: ExprAST;
  kind: JoinKind;
  evaluator: ExprEvaluator;
  tempStore: TempStore;
  batchSize: number;
  buildSide?: 'left' | 'right';
}

export class JoinOperator extends BasePhysicalOperator {
  readonly kind = 'Join';
  
  private hashTable: Map<string, Row[]> = new Map();
  private rightGenerator: AsyncIterator<RowBatch> | null = null;
  private pendingRows: Row[] = [];
  private ctx: ExecutionContext | null = null;
  private buildSchema: RowSchema = { columns: [] };
  private probeSchema: RowSchema = { columns: [] };
  private spilledToTemp = false;
  private tempKey = '';
  private matchedBuildKeys: Set<string> = new Set();
  private hasProcessedUnmatched = false;

  constructor(private opts: JoinOptions) {
    super();
  }

  protected async doOpen(ctx: ExecutionContext): Promise<void> {
    // Store ctx
    this.ctx = ctx;
    
    const buildSide = this.opts.buildSide || 'left';
    const buildOp = buildSide === 'left' ? this.opts.left : this.opts.right;
    const probeOp = buildSide === 'left' ? this.opts.right : this.opts.left;
    
    // Build phase: collectAll from build side (collectAll will open/close the operator)
    const buildBatch = await collectAll(buildOp, ctx, Math.min(this.opts.batchSize, ctx.budget.maxRowsPerNode));
    
    // Open the probe operator (not the build operator since collectAll handled it)
    await probeOp.open(ctx);
    
    // Store schemas
    this.buildSchema = buildBatch.schema;
    this.probeSchema = { columns: [] }; // Will be populated when we get first probe batch
    
    // Build hash table keyed by join key fields extracted from condition
    await this.buildHashTable(buildBatch.rows, this.opts.on);
    
    // Initialize probe generator
    this.rightGenerator = this.createProbeGenerator(probeOp, this.opts.batchSize);
  }

  protected async doNextBatch(size: number): Promise<RowBatch> {
    if (!this.ctx) {
      throw new Error('JoinOperator not opened');
    }

    const outputRows: Row[] = [];
    
    // Process probe batches until we have enough output rows
    while (outputRows.length < size && this.rightGenerator) {
      const result = await this.rightGenerator.next();
      
      if (result.done) {
        this.rightGenerator = null;
        break;
      }
      
      const probeBatch = result.value;
      
            
      // Capture probe schema on first batch
      if (this.probeSchema.columns.length === 0) {
        this.probeSchema = probeBatch.schema;
      }

      // Process each probe row
      for (const probeRow of probeBatch.rows) {
        const joinKey = this.extractEquiJoinKey(probeRow, this.opts.on, false);
        
        if (this.spilledToTemp) {
          // Read from temp storage
          const buildBatches = await this.opts.tempStore.read(this.tempKey, size);
          for await (const buildBatch of buildBatches) {
            for (const buildRow of buildBatch.rows) {
              const buildJoinKey = this.extractEquiJoinKey(buildRow, this.opts.on, true);
              if (this.shouldJoin(joinKey, buildJoinKey, this.opts.on)) {
                const outputRow = this.mergeRows(buildRow, probeRow, this.buildSchema, this.probeSchema);
                outputRows.push(outputRow);
                this.matchedBuildKeys.add(buildJoinKey);
              }
            }
          }
        } else {
          // Read from hash table
          const buildRows = this.hashTable.get(joinKey);
          
          if (buildRows) {
            this.matchedBuildKeys.add(joinKey);
            for (const buildRow of buildRows) {
              const outputRow = this.mergeRows(buildRow, probeRow, this.buildSchema, this.probeSchema);
              // Evaluate non-equi join conditions if any
              try {
                const passes = this.opts.evaluator.evaluate(this.opts.on, this.ctx!.scope, outputRow);
                if (passes === true) {
                  outputRows.push(outputRow);
                }
              } catch (e) {
                console.error(`Error evaluating join condition: ${e}`);
              }
            }
          }
        }
        
        if (outputRows.length >= size) {
          break;
        }
      }
    }
    
    // Add any pending rows
    while (outputRows.length < size && this.pendingRows.length > 0) {
      outputRows.push(this.pendingRows.shift()!);
    }
    
    // Handle RIGHT join unmatched rows at the end
    if (this.rightGenerator === null && (this.opts.kind === 'RIGHT' || this.opts.kind === 'FULL')) {
      const leftIsBuild = (this.opts.buildSide || 'left') === 'left';
      if (!leftIsBuild) {
        // Add unmatched build rows
        for (const [key, buildRows] of Array.from(this.hashTable.entries())) {
          for (const buildRow of buildRows) {
            const outputRow = this.mergeRows(buildRow, null, this.buildSchema, this.probeSchema);
            outputRows.push(outputRow);
          }
        }
        this.hashTable.clear();
      }
    }

    // Handle LEFT join unmatched rows at the end (only once)
    if (this.rightGenerator === null && this.opts.kind === 'LEFT' && !this.hasProcessedUnmatched) {
      const leftIsBuild = (this.opts.buildSide || 'left') === 'left';
      if (leftIsBuild) {
        // Add unmatched build rows (left side rows that didn't match any probe rows)
        for (const [key, buildRows] of Array.from(this.hashTable.entries())) {
          if (!this.matchedBuildKeys.has(key)) {
            for (const buildRow of buildRows) {
              const outputRow = this.mergeRows(buildRow, null, this.buildSchema, this.probeSchema);
              outputRows.push(outputRow);
            }
          }
        }
        this.hasProcessedUnmatched = true;
      }
    }

    return {
      rows: outputRows,
      schema: this.mergeSchemas(this.buildSchema, this.probeSchema)
    };
  }

  protected async doClose(): Promise<void> {
    if (this.rightGenerator) {
      this.rightGenerator = null;
    }
    
    if (this.spilledToTemp && this.tempKey) {
      await this.opts.tempStore.delete(this.tempKey);
    }
    
    this.hashTable.clear();
    this.pendingRows.length = 0;
    this.matchedBuildKeys.clear();
    this.hasProcessedUnmatched = false;
  }

  private async buildHashTable(rows: Row[], condition: ExprAST): Promise<void> {
    const isEquiJoin = condition.kind === 'BinaryOp' && 
                      ['=', '=='].includes((condition as any).op);
    
    if (rows.length > this.ctx!.budget.maxRowsPerNode / 2) {
      // Spill to temp storage for large datasets
      this.spilledToTemp = true;
      this.tempKey = `join_build_${Date.now()}`;
      
      const batch = { rows, schema: this.buildSchema };
      await this.opts.tempStore.write(this.tempKey, batch);
      
      // Clear hash table to save memory
      this.hashTable.clear();
    } else {
      // Build in memory
      for (const row of rows) {
        const joinKey = isEquiJoin 
          ? this.extractEquiJoinKey(row, condition, true)
          : '_all'; // Degenerate case for complex conditions
        
        if (!this.hashTable.has(joinKey)) {
          this.hashTable.set(joinKey, []);
        }
        this.hashTable.get(joinKey)!.push(row);
      }
    }
  }

  private createProbeGenerator(probeOp: PhysicalOperator, batchSize: number): AsyncIterator<RowBatch> {
    return {
      async next(): Promise<IteratorResult<RowBatch>> {
        try {
          const batch = await probeOp.nextBatch(batchSize);
          if (batch.rows.length === 0) {
            return { done: true, value: undefined };
          }
          return { done: false, value: batch };
        } catch (error) {
          throw error;
        }
      }
    };
  }

  private extractEquiJoinKey(row: Row, onExpr: ExprAST, isBuild: boolean): string {
    // Extract the equi-join key from the ON expression
    // For now, handle simple equality: left.field = right.field
    if (onExpr.kind === 'BinaryOp' && onExpr.op === '=') {
      const leftKey = this.getFieldValue(onExpr.left, row);
      const rightKey = this.getFieldValue(onExpr.right, row);
      return isBuild ? String(leftKey) : String(rightKey);
    }
    
    // If ON has AND conditions, extract the first equality
    if (onExpr.kind === 'BinaryOp' && onExpr.op === 'AND') {
      return this.extractEquiJoinKey(row, onExpr.left, isBuild);
    }
    
    return JSON.stringify(row);
  }

  private getFieldValue(expr: ExprAST, row: Row): Value {
    if (expr.kind === 'FieldRef') {
      return row[expr.field];
    }
    if (expr.kind === 'Literal') {
      return expr.value;
    }
    throw new Error('Unsupported expression for join key extraction');
  }

  private shouldJoin(probeKey: string, buildKey: string, condition: ExprAST): boolean {
    // For equi-joins: compare keys
    if (condition.kind === 'BinaryOp' && condition.op === '=') {
      return probeKey === buildKey;
    }
    // For complex conditions: always join (simplified)
    return true;
  }

  private mergeRows(left: Row | null, right: Row | null, leftSchema: RowSchema, rightSchema: RowSchema): Row {
    const merged: Row = {};
    
    if (left) {
      for (const [key, value] of Object.entries(left)) {
        merged[`left.${key}`] = value;
        merged[key] = value; // Also keep un-prefixed for convenience
      }
    }
    
    if (right) {
      for (const [key, value] of Object.entries(right)) {
        merged[`right.${key}`] = value;
        merged[key] = value; // Also keep un-prefixed for convenience
      }
    } else {
      // Set null for all right fields when right is null
      for (const col of rightSchema.columns || []) {
        merged[`right.${col.name}`] = null;
      }
    }
    
    return merged;
  }

  private mergeSchemas(leftSchema: RowSchema, rightSchema: RowSchema): RowSchema {
    const leftColumns = leftSchema?.columns || [];
    const rightColumns = rightSchema?.columns || [];
    
    const mergedColumns = [
      ...leftColumns.map(col => ({ ...col, name: `left.${col.name}` })),
      ...rightColumns.map(col => ({ ...col, name: `right.${col.name}` })),
      // Also keep original column names
      ...leftColumns,
      ...rightColumns
    ];
    
    return { columns: mergedColumns };
  }
}
