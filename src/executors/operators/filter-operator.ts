// Filter operator for applying predicates to row batches

import { BasePhysicalOperator } from '../physical-operator.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import type { RowBatch, Row } from '../../core/types/row.js';
import type { ExprAST } from '../../core/ast/expr-ast.js';
import type { PhysicalOperator } from '../physical-operator.js';
import type { ExprEvaluator } from '../expr-evaluator.js';

interface FilterOptions {
  input: PhysicalOperator;
  predicate: ExprAST;
  evaluator: ExprEvaluator;
}

export class FilterOperator extends BasePhysicalOperator {
  readonly kind = 'Filter';
  
  private ctx: ExecutionContext | null = null;
  private currentBatch: Row[] = [];
  private inputExhausted = false;
  private cachedSchema: any = null;

  constructor(private opts: FilterOptions) {
    super();
  }

  protected async doOpen(ctx: ExecutionContext): Promise<void> {
    // Open input operator
    await this.opts.input.open(ctx);
    
    // Store ctx for use in doNextBatch
    this.ctx = ctx;
    
    // Reset state
    this.currentBatch = [];
    this.inputExhausted = false;
    this.cachedSchema = null;
  }

  protected async doNextBatch(size: number): Promise<RowBatch> {
    if (!this.ctx) {
      throw new Error('FilterOperator not opened');
    }

    // Keep pulling batches until we accumulate `size` passing rows
    // or input is exhausted
    while (this.currentBatch.length < size && !this.inputExhausted) {
      const inputBatch = await this.opts.input.nextBatch(size);
      
      if (inputBatch.rows.length === 0) {
        this.inputExhausted = true;
        break;
      }

      // Cache schema from first batch
      if (!this.cachedSchema) {
        this.cachedSchema = inputBatch.schema;
      }

      // Evaluate predicate for each row
      for (const row of inputBatch.rows) {
        try {
          const result = this.opts.evaluator.evaluate(
            this.opts.predicate, 
            this.ctx.scope, 
            row
          );
          
          // Keep row if result is true
          if (result === true) {
            this.currentBatch.push(row);
            
            // Stop as soon as we have `size` passing rows
            if (this.currentBatch.length >= size) {
              break;
            }
          }
        } catch (error) {
          // Log evaluation error but continue processing
          console.error(`Error evaluating filter predicate: ${error}`);
        }
      }
      
      // Important: do NOT pull more rows than necessary
      // stop as soon as we have `size` passing rows
      if (this.currentBatch.length >= size) {
        break;
      }
    }

    // Return accumulated passing rows
    const outputRows = this.currentBatch.splice(0, size);
    
    // Preserve input schema on output RowBatch
    const schema = this.cachedSchema || { columns: [] };

    return {
      rows: outputRows,
      schema
    };
  }

  protected async doClose(): Promise<void> {
    // Close input operator
    await this.opts.input.close();
    
    // Clean up state
    this.ctx = null;
    this.currentBatch = [];
    this.inputExhausted = false;
    this.cachedSchema = null;
  }
}
