// Core batch iterator protocol for all physical operators

import type { ExecutionContext } from '../core/context/execution-context.js';
import type { RowBatch } from '../core/types/row.js';
import type { ExecutionBudget } from '../core/context/execution-budget.js';
import { traceEvent } from '../core/context/execution-trace.js';

export type PhysicalOperatorState = 'idle' | 'open' | 'exhausted' | 'closed' | 'error';

export interface PhysicalOperator {
  readonly kind: string;
  readonly state: PhysicalOperatorState;

  open(ctx: ExecutionContext): Promise<void>;
  nextBatch(size: number): Promise<RowBatch>;
  close(): Promise<void>;
}

export class BudgetExceededError extends Error {
  public readonly budget: ExecutionBudget;
  public readonly exceeded: string;

  constructor(budget: ExecutionBudget, exceeded: string) {
    super(`Budget exceeded: ${exceeded}`);
    this.name = 'BudgetExceededError';
    this.budget = budget;
    this.exceeded = exceeded;
  }
}

export abstract class BasePhysicalOperator implements PhysicalOperator {
  public abstract readonly kind: string;
  private _state: PhysicalOperatorState = 'idle';
  private _ctx: ExecutionContext | null = null;

  get state(): PhysicalOperatorState {
    return this._state;
  }

  protected setState(newState: PhysicalOperatorState): void {
    this._state = newState;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    if (this._state !== 'idle') {
      throw new Error(`Cannot open operator in state '${this._state}'`);
    }

    this._ctx = ctx;

    try {
      await this.doOpen(ctx);
      this.setState('open');
      traceEvent(ctx.trace, {
        nodeId: this.kind,
        kind: 'start',
        rowsIn: 0,
        rowsOut: 0,
      });
    } catch (error) {
      this.setState('error');
      if (this._ctx) {
        traceEvent(this._ctx.trace, {
          nodeId: this.kind,
          kind: 'error',
          error: error instanceof Error ? error.message : String(error),
          rowsIn: 0,
          rowsOut: 0,
        });
      }
      throw error;
    }
  }

  async nextBatch(size: number): Promise<RowBatch> {
    if (this._state !== 'open') {
      throw new Error(`Cannot get next batch from operator in state '${this._state}'`);
    }

    try {
      const batch = await this.doNextBatch(size);
      
      if (batch.rows.length === 0) {
        this.setState('exhausted');
        if (this._ctx) {
          traceEvent(this._ctx.trace, {
            nodeId: this.kind,
            kind: 'complete',
            rowsOut: 0,
          });
        }
      } else {
        if (this._ctx) {
          traceEvent(this._ctx.trace, {
            nodeId: this.kind,
            kind: 'batch',
            rowsOut: batch.rows.length,
          });
        }
      }

      return batch;
    } catch (error) {
      this.setState('error');
      if (this._ctx) {
        traceEvent(this._ctx.trace, {
          nodeId: this.kind,
          kind: 'error',
          error: error instanceof Error ? error.message : String(error),
          rowsIn: 0,
          rowsOut: 0,
        });
      }
      throw error;
    }
  }

  async close(): Promise<void> {
    if (this._state === 'closed') {
      return; // idempotent
    }

    try {
      await this.doClose();
      this.setState('closed');
      if (this._ctx) {
        traceEvent(this._ctx.trace, {
          nodeId: this.kind,
          kind: 'complete',
        });
      }
    } catch (error) {
      this.setState('error');
      if (this._ctx) {
        traceEvent(this._ctx.trace, {
          nodeId: this.kind,
          kind: 'error',
          error: error instanceof Error ? error.message : String(error),
          rowsIn: 0,
          rowsOut: 0,
        });
      }
      throw error;
    } finally {
      this._ctx = null;
    }
  }

  protected abstract doOpen(ctx: ExecutionContext): Promise<void>;
  protected abstract doNextBatch(size: number): Promise<RowBatch>;
  protected abstract doClose(): Promise<void>;
}

export async function collectAll(
  op: PhysicalOperator,
  ctx: ExecutionContext,
  batchSize = 100
): Promise<RowBatch> {
  await op.open(ctx);

  const allRows: any[] = [];
  let totalRows = 0;
  let lastSchema: any = undefined;

  try {
    while (op.state === 'open') {
      const batch = await op.nextBatch(batchSize);
      
      if (batch.rows.length === 0) {
        break;
      }

      totalRows += batch.rows.length;
      if (batch.schema) {
        lastSchema = batch.schema;
      }

      // Check budget limits
      if (totalRows > ctx.budget.maxRowsPerNode) {
        throw new BudgetExceededError(ctx.budget, `maxRowsPerNode exceeded: ${totalRows} > ${ctx.budget.maxRowsPerNode}`);
      }

      allRows.push(...batch.rows);
    }

    return {
      rows: allRows,
      schema: lastSchema || { columns: [] },
    };
  } finally {
    await op.close();
  }
}
