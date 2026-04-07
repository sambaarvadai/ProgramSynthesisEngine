// Aggregation operator for GROUP BY and aggregate functions

import { BasePhysicalOperator } from '../physical-operator.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import type { RowBatch, Row } from '../../core/types/row.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { ExprAST } from '../../core/ast/expr-ast.js';
import type { PhysicalOperator } from '../physical-operator.js';
import type { ExprEvaluator } from '../expr-evaluator.js';
import type { TempStore } from '../../core/storage/temp-store.js';
import type { AggFn } from '../../core/ast/expr-ast.js';
import type { Value } from '../../core/types/value.js';
import { collectAll } from '../physical-operator.js';

interface AggregationSpec {
  fn: AggFn;
  expr: ExprAST;
  alias: string;
}

interface AggOptions {
  input: PhysicalOperator;
  groupBy: ExprAST[];
  aggregations: AggregationSpec[];
  evaluator: ExprEvaluator;
  tempStore: TempStore;
  batchSize: number;
}

type SumAvgAccumulator = {
  fn: 'SUM' | 'AVG';
  sum: number;
  count: number;
};

type CountAccumulator = {
  fn: 'COUNT';
  count: number;
};

type CountDistinctAccumulator = {
  fn: 'COUNT_DISTINCT';
  seen: Set<string>;
};

type MinAccumulator = {
  fn: 'MIN';
  min: Value;
};

type MaxAccumulator = {
  fn: 'MAX';
  max: Value;
};

type AggAccumulator = SumAvgAccumulator | CountAccumulator | CountDistinctAccumulator | MinAccumulator | MaxAccumulator;

type GroupAccumulator = {
  key: Record<string, Value>; // group-by field values
  accumulators: AggAccumulator[];
};

export class AggOperator extends BasePhysicalOperator {
  readonly kind = 'Agg';
  
  private groups: Map<string, GroupAccumulator> = new Map();
  private resultBatches: RowBatch[] = [];
  private resultIndex = 0;
  private built = false;
  private ctx: ExecutionContext | null = null;

  constructor(private opts: AggOptions) {
    super();
  }

  protected async doOpen(ctx: ExecutionContext): Promise<void> {
    // Store ctx
    this.ctx = ctx;
    // Don't open input here - collectAll will handle it in doNextBatch
    
    // Do NOT consume input yet (lazy build)
    this.built = false;
  }

  protected async doNextBatch(size: number): Promise<RowBatch> {
    if (!this.ctx) {
      throw new Error('AggOperator not opened');
    }

    // If not built: consume ALL input (aggregation requires full scan)
    if (!this.built) {
      const allInput = await collectAll(this.opts.input, this.ctx, this.ctx.budget.maxRowsPerNode);
      
      // Build groups map
      for (const row of allInput.rows) {
        const groupKey = this.groupKey(row);
        
        if (!this.groups.has(groupKey)) {
          this.groups.set(groupKey, {
            key: this.parseGroupKey(groupKey),
            accumulators: this.opts.aggregations.map(agg => this.createAccumulator(agg.fn))
          });
        }
        
        const group = this.groups.get(groupKey)!;
        for (let i = 0; i < this.opts.aggregations.length; i++) {
          const agg = this.opts.aggregations[i];
          
          // Special handling for COUNT(*) - Wildcard means count all rows
          if (agg.fn === 'COUNT' && agg.expr.kind === 'Wildcard') {
            this.updateAccumulator(group.accumulators[i], 1); // Count each row
          } else {
            const value = this.opts.evaluator.evaluate(agg.expr, this.ctx.scope, row);
            this.updateAccumulator(group.accumulators[i], value);
          }
        }
      }
      
      // Finalize accumulators → result rows
      const resultRows: Row[] = [];
      for (const group of Array.from(this.groups.values())) {
        const resultRow: Row = {};
        
        // Add group-by fields
        Object.assign(resultRow, group.key);
        
        // Add aggregated fields
        for (let i = 0; i < this.opts.aggregations.length; i++) {
          const agg = this.opts.aggregations[i];
          const finalized = this.finalizeAccumulator(group.accumulators[i]);
          resultRow[agg.alias] = finalized;
        }
        
        resultRows.push(resultRow);
      }
      
      // Chunk result rows into resultBatches by size
      const outputSchema = this.inferOutputSchema(resultRows[0]);
      for (let i = 0; i < resultRows.length; i += this.opts.batchSize) {
        const chunk = resultRows.slice(i, i + this.opts.batchSize);
        this.resultBatches.push({
          rows: chunk,
          schema: outputSchema
        });
      }
      
      this.built = true;
    }
    
    // Return resultBatches[resultIndex++]
    if (this.resultIndex >= this.resultBatches.length) {
      return {
        rows: [],
        schema: { columns: [] }
      };
    }
    
    return this.resultBatches[this.resultIndex++];
  }

  protected async doClose(): Promise<void> {
    // Close input operator
    await this.opts.input.close();
    
    // Clear groups map
    this.groups.clear();
  }

  private groupKey(row: Row): string {
    // Evaluate each groupBy expr, JSON.stringify result
    const keyParts: string[] = [];
    
    for (const expr of this.opts.groupBy) {
      try {
        const value = this.opts.evaluator.evaluate(expr, this.ctx!.scope, row);
        keyParts.push(JSON.stringify(value));
      } catch (error) {
        console.error(`Error evaluating group key: ${error}`);
        console.error(`Expression:`, expr);
        console.error(`Row:`, row);
        keyParts.push('null');
      }
    }
    
    return JSON.stringify(keyParts);
  }

  private parseGroupKey(groupKey: string): Row {
    // Parse the group key back into a row object with field names
    const keyParts = JSON.parse(groupKey);
    const result: Row = {};
    
    for (let i = 0; i < this.opts.groupBy.length; i++) {
      const expr = this.opts.groupBy[i];
      if (expr.kind === 'FieldRef') {
        // keyParts[i] is already JSON.stringify'd, so we need to parse it again
        result[expr.field] = JSON.parse(keyParts[i]);
      }
    }
    
    return result;
  }

  private createAccumulator(fn: AggFn): AggAccumulator {
    switch (fn) {
      case 'SUM':
      case 'AVG':
        return { fn, sum: 0, count: 0 };
      case 'COUNT':
        return { fn, count: 0 };
      case 'COUNT_DISTINCT':
        return { fn, seen: new Set() };
      case 'MIN':
        return { fn, min: null as Value };
      case 'MAX':
        return { fn, max: null as Value };
      default:
        throw new Error(`Unsupported aggregate function: ${fn}`);
    }
  }

  private updateAccumulator(acc: AggAccumulator, value: Value): void {
    if (!acc) return;
    
    switch (acc.fn) {
      case 'SUM':
      case 'AVG':
        if (typeof value === 'number') {
          (acc as SumAvgAccumulator).sum += value;
          (acc as SumAvgAccumulator).count++;
        }
        break;
      case 'COUNT':
        (acc as CountAccumulator).count++;
        break;
      case 'COUNT_DISTINCT':
        if (value !== null) {
          (acc as CountDistinctAccumulator).seen.add(JSON.stringify(value));
        }
        break;
      case 'MIN':
        if (value !== null && typeof value === 'number') {
          const currentMin = (acc as MinAccumulator).min;
          if (currentMin === null || (typeof currentMin === 'number' && value < currentMin)) {
            (acc as MinAccumulator).min = value;
          }
        }
        break;
      case 'MAX':
        if (value !== null && typeof value === 'number') {
          const currentMax = (acc as MaxAccumulator).max;
          if (currentMax === null || (typeof currentMax === 'number' && value > currentMax)) {
            (acc as MaxAccumulator).max = value;
          }
        }
        break;
    }
  }

  private finalizeAccumulator(acc: AggAccumulator): Value {
    switch (acc.fn) {
      case 'SUM':
        return (acc as SumAvgAccumulator).sum;
      case 'AVG':
        const sumAvg = acc as SumAvgAccumulator;
        return sumAvg.count > 0 ? sumAvg.sum / sumAvg.count : 0;
      case 'COUNT':
        return (acc as CountAccumulator).count;
      case 'COUNT_DISTINCT':
        return (acc as CountDistinctAccumulator).seen.size;
      case 'MIN':
        return (acc as MinAccumulator).min;
      case 'MAX':
        return (acc as MaxAccumulator).max;
      default:
        return null;
    }
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
