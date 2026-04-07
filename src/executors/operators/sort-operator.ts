// Sort operator for ordering rows by specified keys

import { BasePhysicalOperator } from '../physical-operator.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import type { RowBatch, Row } from '../../core/types/row.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { ExprAST } from '../../core/ast/expr-ast.js';
import type { PhysicalOperator } from '../physical-operator.js';
import type { ExprEvaluator } from '../expr-evaluator.js';
import type { TempStore } from '../../core/storage/temp-store.js';
import type { Value } from '../../core/types/value.js';
import { collectAll } from '../physical-operator.js';

export type SortDirection = 'ASC' | 'DESC';
export type NullsOrder = 'FIRST' | 'LAST';

export interface SortKey {
  expr: ExprAST;
  direction: SortDirection;
  nulls: NullsOrder;
}

interface SortOptions {
  input: PhysicalOperator;
  keys: SortKey[];
  evaluator: ExprEvaluator;
  tempStore: TempStore;
  memoryLimitRows: number; // spill to tempStore beyond this
}

export class SortOperator extends BasePhysicalOperator {
  readonly kind = 'Sort';
  
  private sortedBatches: RowBatch[] = [];
  private batchIndex = 0;
  private built = false;
  private ctx: ExecutionContext | null = null;

  constructor(private opts: SortOptions) {
    super();
  }

  protected async doOpen(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    // Don't open input here - collectAll will handle it in doNextBatch
  }

  protected async doNextBatch(size: number): Promise<RowBatch> {
    if (!this.ctx) {
      throw new Error('SortOperator not opened');
    }

    // If not built: collect all input rows and sort
    if (!this.built) {
      const allInput = await collectAll(this.opts.input, this.ctx, this.ctx.budget.maxRowsPerNode);
      
      if (allInput.rows.length <= this.opts.memoryLimitRows) {
        // Sort in memory
        const sortedRows = this.sortInMemory(allInput.rows);
        this.chunkSortedRows(sortedRows, size);
      } else {
        // External merge sort for large datasets
        await this.externalMergeSort(allInput.rows, size);
      }
      
      this.built = true;
    }
    
    // Return sortedBatches[batchIndex++]
    if (this.batchIndex >= this.sortedBatches.length) {
      return {
        rows: [],
        schema: { columns: [] }
      };
    }
    
    return this.sortedBatches[this.batchIndex++];
  }

  protected async doClose(): Promise<void> {
    await this.opts.input.close();
    // tempStore cleanup handled by externalMergeSort
  }

  private sortInMemory(rows: Row[]): Row[] {
    return rows.sort((a, b) => this.compareRows(a, b, this.opts.keys));
  }

  private async externalMergeSort(rows: Row[], batchSize: number): Promise<void> {
    const chunkSize = this.opts.memoryLimitRows;
    const chunks: { key: string; rows: Row[] }[] = [];
    
    // Write chunks to tempStore
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const tempKey = `sort_chunk_${i}`;
      await this.opts.tempStore.write(tempKey, {
        rows: chunk,
        schema: { columns: [] } // Schema not critical for temp storage
      });
      chunks.push({ key: tempKey, rows: chunk });
    }
    
    // Sort each chunk in memory
    for (const chunk of chunks) {
      chunk.rows.sort((a: Row, b: Row) => this.compareRows(a, b, this.opts.keys));
    }
    
    // Merge sorted chunks (k-way merge)
    const sortedRows: Row[] = [];
    const remainingChunks = [...chunks];
    
    while (remainingChunks.length > 0) {
      // Find smallest element from each chunk
      const smallestElements = remainingChunks.map(chunk => 
        chunk.rows.length > 0 ? chunk.rows[0] : null
      );
      
      // Find the overall smallest element
      let smallestIndex = -1;
      let smallestElement: Row | null = null;
      
      for (let i = 0; i < smallestElements.length; i++) {
        const element = smallestElements[i];
        if (element !== null) {
          if (smallestElement === null || this.compareRows(element, smallestElement, this.opts.keys) < 0) {
            smallestElement = element;
            smallestIndex = i;
          }
        }
      }
      
      if (smallestIndex === -1) {
        // All remaining chunks are empty
        break;
      }
      
      // Add smallest element to result
      sortedRows.push(smallestElement!);
      
      // Remove from the chunk that had the smallest element
      remainingChunks[smallestIndex].rows.shift();
      
      // Remove empty chunks
      for (let i = remainingChunks.length - 1; i >= 0; i--) {
        if (remainingChunks[i].rows.length === 0) {
          remainingChunks.splice(i, 1);
        }
      }
    }
    
    // Clean up temp store
    for (const chunk of chunks) {
      await this.opts.tempStore.delete(chunk.key);
    }
    
    this.chunkSortedRows(sortedRows, batchSize);
  }

  private chunkSortedRows(sortedRows: Row[], chunkSize: number): void {
    const outputSchema = this.inferOutputSchema(sortedRows[0]);
    
    for (let i = 0; i < sortedRows.length; i += chunkSize) {
      const chunk = sortedRows.slice(i, i + chunkSize);
      this.sortedBatches.push({
        rows: chunk,
        schema: outputSchema
      });
    }
  }

  private compareRows(a: Row, b: Row, keys: SortKey[]): number {
    // Lexicographic comparison across sort keys
    for (const key of keys) {
      try {
        const aValue = this.opts.evaluator.evaluate(key.expr, this.ctx!.scope, a);
        const bValue = this.opts.evaluator.evaluate(key.expr, this.ctx!.scope, b);
        
        // Handle nulls: NULLS FIRST → null < everything, NULLS LAST → null > everything
        let comparison = this.compareValues(aValue, bValue, key.direction);
        
        // Apply nulls ordering
        if (aValue === null && bValue !== null) {
          comparison = key.nulls === 'FIRST' ? -1 : 1;
        } else if (aValue !== null && bValue === null) {
          comparison = key.nulls === 'FIRST' ? 1 : -1;
        }
        
        if (comparison !== 0) {
          return comparison;
        }
      } catch (error) {
        console.error(`Error evaluating sort key: ${error}`);
        return 0;
      }
    }
    
    return 0;
  }

  private compareValues(a: Value, b: Value, direction: SortDirection): number {
    if (a === null && b === null) return 0;
    if (a === null) return -1;
    if (b === null) return 1;
    
    // Direction: ASC normal, DESC reversed
    const comparison = this.basicCompare(a, b);
    return direction === 'ASC' ? comparison : -comparison;
  }

  private basicCompare(a: Value, b: Value): number {
    // Both null
    if (a === null && b === null) return 0;
    if (a === null) return -1;
    if (b === null) return 1;

    // Both numbers — numeric comparison
    if (typeof a === 'number' && typeof b === 'number') {
      return a < b ? -1 : a > b ? 1 : 0;
    }

    // Both booleans
    if (typeof a === 'boolean' && typeof b === 'boolean') {
      return (a ? 1 : 0) - (b ? 1 : 0);
    }

    // Numeric strings — parse and compare numerically
    if (typeof a === 'string' && typeof b === 'string') {
      const numA = parseFloat(a);
      const numB = parseFloat(b);
      if (!isNaN(numA) && !isNaN(numB)) {
        return numA < numB ? -1 : numA > numB ? 1 : 0;
      }
      // Pure string comparison
      return a < b ? -1 : a > b ? 1 : 0;
    }

    // Mixed — convert to string
    return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
  }

  private inferOutputSchema(sampleRow: Row): RowSchema {
    if (!sampleRow) {
      return { columns: [] };
    }

    const columns = Object.entries(sampleRow).map(([name, value]) => ({
      name,
      type: this.inferTypeFromValue(value),
      nullable: value === null
    }));

    return { columns };
  }

  private inferTypeFromValue(value: Value): any {
    if (value === null) return { kind: 'null' };
    if (typeof value === 'string') return { kind: 'string' };
    if (typeof value === 'number') return { kind: 'number' };
    if (typeof value === 'boolean') return { kind: 'boolean' };
    if (Array.isArray(value)) return { kind: 'array', item: { kind: 'any' } };
    if (typeof value === 'object') return { kind: 'record', fields: {} };
    return { kind: 'any' };
  }
}
