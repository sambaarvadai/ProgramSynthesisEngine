import type { StorageBackend } from '../../../core/storage/storage-backend.js';
import type { RowBatch, Row } from '../../../core/types/row.js';
import type { RowSchema } from '../../../core/types/schema.js';
import type { ExprAST } from '../../../core/ast/expr-ast.js';

export class MockStorageBackend implements StorageBackend {
  private data: Record<string, Row[]>;

  constructor(initialData: Record<string, Row[]>) {
    this.data = { ...initialData };
  }

  async connect(): Promise<void> {
    // Mock implementation
  }

  async disconnect(): Promise<void> {
    // Mock implementation
  }

  async transaction<T>(fn: () => Promise<T>): Promise<T> {
    return await fn();
  }

  async *scan(opts: {
    table: string;
    predicate?: ExprAST;
    columns?: string[];
    batchSize: number;
  }): AsyncGenerator<RowBatch> {
    const rows = this.data[opts.table] || [];
    const schema = this.inferSchema(rows[0]);
    
    for (let i = 0; i < rows.length; i += opts.batchSize) {
      const batch = rows.slice(i, i + opts.batchSize);
      yield { rows: batch, schema };
    }
  }

  async insert(table: string, rows: Row[]): Promise<void> {
    if (!this.data[table]) {
      this.data[table] = [];
    }
    this.data[table].push(...rows);
  }

  async createTemp(schema: RowSchema): Promise<string> {
    const tempId = `temp_${Date.now()}`;
    this.data[tempId] = [];
    return tempId;
  }

  async dropTemp(table: string): Promise<void> {
    delete this.data[table];
  }

  async tableExists(table: string): Promise<boolean> {
    return table in this.data;
  }

  async getSchema(table: string): Promise<RowSchema> {
    const rows = this.data[table] || [];
    return this.inferSchema(rows[0]);
  }

  getTableData(table: string): Row[] {
    return this.data[table] || [];
  }

  private inferSchema(sampleRow: Row | undefined): RowSchema {
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

  private inferTypeFromValue(value: any): any {
    if (value === null) return { kind: 'null' };
    if (typeof value === 'string') return { kind: 'string' };
    if (typeof value === 'number') return { kind: 'number' };
    if (typeof value === 'boolean') return { kind: 'boolean' };
    if (Array.isArray(value)) return { kind: 'array', item: { kind: 'any' } };
    if (typeof value === 'object') return { kind: 'record', fields: {} };
    return { kind: 'any' };
  }
}
