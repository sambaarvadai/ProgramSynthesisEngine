// Limit operator for restricting output rows with offset and limit

import { BasePhysicalOperator } from '../physical-operator.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import type { RowBatch, Row } from '../../core/types/row.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { PhysicalOperator } from '../physical-operator.js';

interface LimitOptions {
  input: PhysicalOperator;
  limit: number;
  offset: number;
}

export class LimitOperator extends BasePhysicalOperator {
  readonly kind = 'Limit';
  
  private rowsSeen = 0;
  private rowsEmitted = 0;
  private ctx: ExecutionContext | null = null;
  private inputSchema: RowSchema | undefined;

  constructor(private opts: LimitOptions) {
    super();
  }

  protected async doOpen(ctx: ExecutionContext): Promise<void> {
    await this.opts.input.open(ctx);
    this.ctx = ctx;
    // Get schema from input operator without consuming data
    // We'll capture it when we get the first batch
  }

  protected async doNextBatch(size: number): Promise<RowBatch> {
    if (!this.ctx) {
      throw new Error('LimitOperator not opened');
    }

    // Emit rows until rowsEmitted >= limit
    const outputRows: Row[] = [];
    while (this.rowsEmitted < this.opts.limit) {
      // Check if input is exhausted before pulling batch
      if (this.opts.input.state !== 'open') {
        break;
      }
      const inputBatch = await this.opts.input.nextBatch(size);
      
      if (inputBatch.rows.length === 0) {
        break; // Input exhausted
      }
      
      // Capture schema on first batch
      if (this.inputSchema === undefined) {
        this.inputSchema = inputBatch.schema;
      }
      
      // Skip rows until we've passed the offset
      let startIdx = 0;
      if (this.rowsSeen < this.opts.offset) {
        startIdx = Math.min(this.opts.offset - this.rowsSeen, inputBatch.rows.length);
        this.rowsSeen += startIdx;
      }
      
      // Take remaining rows up to limit
      const remaining = this.opts.limit - this.rowsEmitted;
      const toTake = Math.min(inputBatch.rows.length - startIdx, remaining);
      
      if (toTake > 0) {
        outputRows.push(...inputBatch.rows.slice(startIdx, startIdx + toTake));
        this.rowsEmitted += toTake;
        this.rowsSeen += toTake;
      }
      
      if (this.rowsEmitted >= this.opts.limit) {
        break;
      }
    }

    return {
      rows: outputRows,
      schema: this.inputSchema || { columns: [] }
    };
  }

  protected async doClose(): Promise<void> {
    await this.opts.input.close();
  }
}
