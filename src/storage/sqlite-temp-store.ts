// SQLite temporary storage implementation

import type { TempStore } from '../core/storage/temp-store.js';
import type { RowBatch, Row } from '../core/types/row.js';
import type { RowSchema } from '../core/types/schema.js';
import type { PhysicalOperatorResult } from '../core/storage/storage-backend.js';
import type { Value } from '../core/types/value.js';

export class SQLiteTempStore implements TempStore {
  private data: Map<string, RowBatch[]> = new Map();

  constructor(dbPath: string = ':memory:') {
    // In-memory implementation for testing
  }

  private ensureTable(key: string, schema: RowSchema): void {
    if (!this.data.has(key)) {
      this.data.set(key, []);
    }
  }

  async write(key: string, batch: RowBatch): Promise<void> {
    this.ensureTable(key, batch.schema);
    this.data.get(key)!.push(batch);
  }

  async append(key: string, batch: RowBatch): Promise<void> {
    this.ensureTable(key, batch.schema);
    this.data.get(key)!.push(batch);
  }

  async *read(key: string, batchSize: number): AsyncIterable<RowBatch> {
    const batches = this.data.get(key);
    
    if (!batches) {
      throw new Error(`Table for key '${key}' does not exist`);
    }

    for (const batch of batches) {
      yield batch;
    }
  }

  async delete(key: string): Promise<void> {
    this.data.delete(key);
  }

  async clear(): Promise<void> {
    this.data.clear();
  }

  async exists(key: string): Promise<boolean> {
    return Promise.resolve(this.data.has(key));
  }

  private inferSchemaFromRow(row: Row): RowSchema {
    const columns = Object.entries(row).map(([name, value]) => ({
      name,
      type: this.inferTypeFromValue(value),
      nullable: value === null,
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

  // Cleanup method for graceful shutdown
  close(): void {
    // No-op for in-memory implementation
  }
}
