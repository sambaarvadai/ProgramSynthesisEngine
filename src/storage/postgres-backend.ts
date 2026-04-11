// PostgreSQL storage backend implementation

import pg from 'pg';
import type { Pool, PoolClient } from 'pg';
import type { StorageBackend, PhysicalOperatorResult } from '../core/storage/storage-backend.js';
import type { RowBatch, Row } from '../core/types/row.js';
import type { RowSchema } from '../core/types/schema.js';
import type { Value } from '../core/types/value.js';
import type { EngineType } from '../core/types/engine-type.js';
import type { ExprAST } from '../core/ast/expr-ast.js';

interface PoolConfig {
  max?: number;
  idleTimeoutMs?: number;
}

export class PostgresBackend implements StorageBackend {
  private pool: Pool;
  private connectionString: string;

  constructor(connectionString: string, poolConfig: PoolConfig = {}) {
    this.connectionString = connectionString;
    this.pool = new pg.Pool({
      connectionString,
      max: poolConfig.max || 10,
      idleTimeoutMillis: poolConfig.idleTimeoutMs || 30000,
    });

    // Parse numeric types as JavaScript numbers
    pg.types.setTypeParser(1700, (val) => parseFloat(val));  // NUMERIC/DECIMAL
    pg.types.setTypeParser(701, (val) => parseFloat(val));   // FLOAT8
    pg.types.setTypeParser(20, (val) => parseInt(val, 10));  // INT8/BIGINT
    // INT4 (OID 23) is already parsed as number by default
  }

  async connect(): Promise<void> {
    try {
      const client = await this.pool.connect();
      await client.query('SELECT 1');
      client.release();
      console.log(`PostgreSQL connected: pool max=${this.pool.options.max}`);
    } catch (error) {
      throw new Error(`Failed to connect to PostgreSQL: ${error}`);
    }
  }

  async disconnect(): Promise<void> {
    await this.pool.end();
    console.log('PostgreSQL disconnected');
  }

  async transaction<T>(fn: () => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn();
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  scan(opts: {
    table: string;
    predicate?: ExprAST;
    columns?: string[];
    batchSize: number;
  }): PhysicalOperatorResult {
    return this.scanWithCursor(opts);
  }

  private async *scanWithCursor(opts: {
    table: string;
    predicate?: ExprAST;
    columns?: string[];
    batchSize: number;
  }): AsyncGenerator<RowBatch> {
    const client = await this.pool.connect();
    const cursorName = `pee_cursor_${Date.now()}_${Math.random().toString(36).slice(2)}`;

    try {
      await client.query('BEGIN');

      const columns = opts.columns?.join(', ') || '*';
      let whereClause = '';
      let params: Value[] = [];

      if (opts.predicate && this.isSimplePredicate(opts.predicate)) {
        const sql = this.predicateToSQL(opts.predicate);
        whereClause = `WHERE ${sql.clause}`;
        params = sql.params;
      }

      const query = `
        DECLARE ${cursorName} CURSOR FOR
        SELECT ${columns} FROM ${opts.table} ${whereClause}
      `;

      await client.query(query, params);

      while (true) {
        const fetchQuery = `FETCH ${opts.batchSize} FROM ${cursorName}`;
        const result = await client.query(fetchQuery);

        if (result.rows.length === 0) {
          break;
        }

        const schema = await this.inferSchemaFromResult(result);
        const rows = result.rows.map((row: any) => this.convertRowFromPostgres(row));

        yield { rows, schema };

        if (result.rows.length < opts.batchSize) {
          break;
        }
      }

      await client.query(`CLOSE ${cursorName}`);
      await client.query('COMMIT');
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        // Ignore rollback errors
      }
      throw error;
    } finally {
      client.release();
    }
  }

  async insert(table: string, rows: Row[]): Promise<void> {
    if (rows.length === 0) return;

    const schema = await this.getSchema(table);
    const columns = schema.columns.map(col => col.name);
    const maxBatchSize = 1000;

    for (let i = 0; i < rows.length; i += maxBatchSize) {
      const batch = rows.slice(i, i + maxBatchSize);
      await this.insertBatch(table, columns, batch);
    }
  }

  private async insertBatch(table: string, columns: string[], rows: Row[]): Promise<void> {
    const client = await this.pool.connect();
    try {
      const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
      const valuePlaceholders = rows.map((_, rowIndex) => {
        const start = rowIndex * columns.length + 1;
        const values = columns.map((_, colIndex) => `$${start + colIndex}`);
        return `(${values.join(', ')})`;
      }).join(', ');

      const query = `
        INSERT INTO ${table} (${columns.join(', ')})
        VALUES ${valuePlaceholders}
      `;

      const flatValues = rows.flatMap(row => columns.map(col => row[col]));
      await client.query(query, flatValues);
    } finally {
      client.release();
    }
  }

  async createTemp(schema: RowSchema): Promise<string> {
    const tableName = `_pee_temp_${crypto.randomUUID().replace(/-/g, '')}`;
    const columns = schema.columns.map(col => {
      const pgType = this.engineTypeToPostgres(col.type);
      const nullable = col.nullable ? '' : ' NOT NULL';
      return `${col.name} ${pgType}${nullable}`;
    }).join(', ');

    const query = `CREATE TEMP TABLE ${tableName} (${columns})`;
    const client = await this.pool.connect();
    try {
      await client.query(query);
      return tableName;
    } finally {
      client.release();
    }
  }

  async dropTemp(table: string): Promise<void> {
    if (!table.startsWith('_pee_temp_')) {
      throw new Error(`Invalid temp table name: ${table}`);
    }

    const query = `DROP TABLE IF EXISTS ${table}`;
    const client = await this.pool.connect();
    try {
      await client.query(query);
    } finally {
      client.release();
    }
  }

  async tableExists(table: string): Promise<boolean> {
    const query = `
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = $1
      )
    `;
    const client = await this.pool.connect();
    try {
      const result = await client.query(query, [table]);
      return result.rows[0].exists;
    } finally {
      client.release();
    }
  }

  async getSchema(table: string): Promise<RowSchema> {
    const query = `
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = $1
      ORDER BY ordinal_position
    `;
    const client = await this.pool.connect();
    try {
      const result = await client.query(query, [table]);
      const columns = result.rows.map((row: any) => ({
        name: row.column_name,
        type: this.postgresTypeToEngine(0, row.data_type),
        nullable: row.is_nullable === 'YES',
      }));
      return { columns };
    } finally {
      client.release();
    }
  }

  private async inferSchemaFromResult(result: any): Promise<RowSchema> {
    const columns = result.fields.map((field: any) => ({
      name: field.name,
      type: this.postgresTypeToEngine(field.dataTypeID, field.dataTypeID === 23 ? 'integer' : 'text'),
      nullable: true, // Assume nullable for query results
    }));
    return { columns };
  }

  private convertRowFromPostgres(row: any): Row {
    const converted: Row = {};
    for (const [key, value] of Object.entries(row)) {
      converted[key] = this.convertValueFromPostgres(value);
    }
    return converted;
  }

  private convertValueFromPostgres(value: any): Value {
    if (value === null || value === undefined) return null;
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return value;
    }
    if (value instanceof Date) {
      return value.toISOString();
    }
    return value;
  }

  private engineTypeToPostgres(type: EngineType): string {
    switch (type.kind) {
      case 'string': return 'TEXT';
      case 'number': return 'NUMERIC';
      case 'boolean': return 'BOOLEAN';
      case 'datetime': return 'TIMESTAMPTZ';
      case 'json': return 'JSONB';
      case 'array': return 'JSONB';
      case 'record': return 'JSONB';
      case 'any': return 'JSONB';
      default: return 'JSONB';
    }
  }

  private postgresTypeToEngine(dataTypeID: number, typeName: string): EngineType {
    // OID-based mapping for precision
    switch (dataTypeID) {
      case 1700:
        return { kind: 'number' }; // NUMERIC/DECIMAL
      case 701:
        return { kind: 'number' }; // FLOAT8
      case 700:
        return { kind: 'number' }; // FLOAT4
      case 20:
        return { kind: 'number' }; // INT8/BIGINT
      case 23:
        return { kind: 'number' }; // INT4
      case 21:
        return { kind: 'number' }; // INT2/SMALLINT
    }

    // Fallback to typeName-based mapping
    switch (typeName.toLowerCase()) {
      case 'text':
      case 'varchar':
      case 'char':
        return { kind: 'string' };
      case 'numeric':
      case 'decimal':
      case 'integer':
      case 'int':
      case 'bigint':
      case 'smallint':
      case 'real':
      case 'double precision':
        return { kind: 'number' };
      case 'boolean':
        return { kind: 'boolean' };
      case 'timestamp':
      case 'timestamptz':
        return { kind: 'datetime' };
      case 'json':
      case 'jsonb':
        return { kind: 'json' };
      default:
        return { kind: 'any' };
    }
  }

  private isSimplePredicate(pred: ExprAST): boolean {
    if (pred.kind === 'BinaryOp') {
      // Handle all comparison operators with FieldRef and Literal
      if (['=', '!=', '<', '>', '<=', '>=', 'LIKE'].includes(pred.op) &&
          pred.left.kind === 'FieldRef' && pred.right.kind === 'Literal') {
        return true;
      }
      // Handle SqlExpr - if right side is SqlExpr, it's safe to use directly
      if (pred.left.kind === 'FieldRef' && pred.right.kind === 'SqlExpr') {
        return true;
      }
    }
    // Handle IS NULL and IS NOT NULL
    if (pred.kind === 'IsNull' && pred.expr.kind === 'FieldRef') {
      return true;
    }
    if (pred.kind === 'UnaryOp' && pred.op === 'NOT' && 
        pred.operand.kind === 'IsNull' && pred.operand.expr.kind === 'FieldRef') {
      return true;
    }
    // Handle IN operator
    if (pred.kind === 'In' && pred.expr.kind === 'FieldRef') {
      return true;
    }
    return false;
  }

  private predicateToSQL(pred: ExprAST): { clause: string; params: Value[] } {
    // Handle all binary comparison operators with FieldRef and Literal
    if (pred.kind === 'BinaryOp' && 
        ['=', '!=', '<', '>', '<=', '>='].includes(pred.op) &&
        pred.left.kind === 'FieldRef' && pred.right.kind === 'Literal') {
      return {
        clause: `${pred.left.field} ${pred.op} $1`,
        params: [pred.right.value],
      };
    }

    // Handle LIKE operator
    if (pred.kind === 'BinaryOp' && pred.op === 'LIKE' &&
        pred.left.kind === 'FieldRef' && pred.right.kind === 'Literal') {
      return {
        clause: `${pred.left.field} LIKE $1`,
        params: [pred.right.value],
      };
    }

    // Handle IN operator using the In expression kind
    if (pred.kind === 'In' && pred.expr.kind === 'FieldRef') {
      const values = pred.values.map(v => {
        if (v.kind === 'Literal') {
          return v.value;
        }
        throw new Error('IN operator only supports literal values');
      });
      const placeholders = values.map((_: any, i: number) => `$${i + 1}`).join(', ');
      return {
        clause: `${pred.expr.field} IN (${placeholders})`,
        params: values,
      };
    }

    // Handle IS NULL and IS NOT NULL
    if (pred.kind === 'IsNull' && pred.expr.kind === 'FieldRef') {
      return {
        clause: `${pred.expr.field} IS NULL`,
        params: [],
      };
    }

    if (pred.kind === 'UnaryOp' && pred.op === 'NOT' && 
        pred.operand.kind === 'IsNull' && pred.operand.expr.kind === 'FieldRef') {
      return {
        clause: `${pred.operand.expr.field} IS NOT NULL`,
        params: [],
      };
    }

    // Handle SqlExpr - output raw SQL directly
    if (pred.kind === 'BinaryOp' && pred.left.kind === 'FieldRef' && pred.right.kind === 'SqlExpr') {
      return {
        clause: `${pred.left.field} ${pred.op} ${(pred.right as any).sql}`,
        params: [],
      };
    }

    throw new Error('Predicate is not simple enough for SQL translation');
  }
}
