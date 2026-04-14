import type { StorageBackend } from '../../../core/storage/storage-backend.js';
import type { RowBatch, Row } from '../../../core/types/row.js';
import type { RowSchema } from '../../../core/types/schema.js';
import type { ExprAST } from '../../../core/ast/expr-ast.js';
import type { Scope } from '../../../core/scope/scope.js';
import { ExprEvaluator } from '../../expr-evaluator.js';
import { FunctionRegistry } from '../../../core/registry/function-registry.js';

export class MockStorageBackend implements StorageBackend {
  private data: Record<string, Row[]>;
  private exprEvaluator: ExprEvaluator;

  constructor(initialData: Record<string, Row[]>) {
    this.data = { ...initialData };
    this.exprEvaluator = new ExprEvaluator(new FunctionRegistry());
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
    let rows = this.data[opts.table] || [];
    const schema = this.inferSchema(rows[0]);
    
    // Apply predicate filtering if provided
    if (opts.predicate) {
      const mockScope: Scope = {
        id: 'mock',
        kind: 'global',
        bindings: new Map(),
        parent: null
      };
      rows = rows.filter(row => {
        try {
          const result = this.exprEvaluator.evaluate(opts.predicate!, mockScope, row);
          return Boolean(result);
        } catch (error) {
          console.error('Error evaluating predicate in MockStorageBackend:', error);
          return false;
        }
      });
    }
    
    // Apply column projection if specified
    if (opts.columns && !opts.columns.includes('*')) {
      rows = rows.map(row => {
        const projectedRow: Row = {};
        for (const col of opts.columns!) {
          if (col in row) {
            projectedRow[col] = row[col];
          }
        }
        return projectedRow;
      });
    }
    
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

  async rawQuery(sql: string, params?: any[]): Promise<{ rows: any[]; rowCount: number }> {
    // Mock implementation - for testing purposes only
    console.log('MockStorageBackend.rawQuery called with:', { sql, params });
    
    // Handle specific test queries
    if (sql.includes('COUNT') && sql.includes('GROUP BY') && sql.includes('customer_id')) {
      // Handle "customers with most orders" query
      const orders = this.data['orders'] || [];
      const customerGroups = new Map<number, number>();
      
      // Count orders per customer
      orders.forEach(order => {
        const customerId = order.customer_id as number;
        if (customerId != null) {
          customerGroups.set(customerId, (customerGroups.get(customerId) || 0) + 1);
        }
      });
      
      // Convert to array and sort by count descending
      const results = Array.from(customerGroups.entries())
        .map(([customer_id, order_count]) => ({ customer_id, order_count }))
        .sort((a, b) => b.order_count - a.order_count)
        .slice(0, 5); // Limit 5
      
      return { rows: results, rowCount: results.length };
    }
    
    // Return empty result by default
    return { rows: [], rowCount: 0 };
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
