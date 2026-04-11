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
        const { clause, params: sqlParams } = this.predicateToSQL(opts.predicate, []);
        whereClause = `WHERE ${clause}`;
        params = sqlParams;
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
    try {
      this.predicateToSQL(pred, []);
      return true;
    } catch {
      return false; // Only throws for VarRef, WindowExpr, unknown functions
    }
  }

  private predicateToSQL(
    pred: ExprAST,
    params: Value[] = []
  ): { clause: string; params: Value[] } {
    const addParam = (value: Value): string => {
      params.push(value);
      return `$${params.length}`;
    };

    switch (pred.kind) {
      case 'Literal': {
        if (pred.value === null) {
          return { clause: 'NULL', params };
        }
        if (typeof pred.value === 'string') {
          return { clause: addParam(pred.value), params };
        }
        if (typeof pred.value === 'number') {
          return { clause: `${addParam(pred.value)}::numeric`, params };
        }
        if (typeof pred.value === 'boolean') {
          return { clause: `${addParam(pred.value)}::boolean`, params };
        }
        return { clause: addParam(pred.value), params };
      }

      case 'FieldRef': {
        const field = pred.table ? `"${pred.table}"."${pred.field}"` : `"${pred.field}"`;
        return { clause: field, params };
      }

      case 'VarRef': {
        throw new Error('VarRef expressions cannot be translated to SQL (pipeline variables)');
      }

      case 'BinaryOp': {
        const left = this.predicateToSQL(pred.left, params);
        const right = this.predicateToSQL(pred.right, params);

        // Arithmetic operators
        if (['+', '-', '*', '/', '%'].includes(pred.op)) {
          return { clause: `(${left.clause} ${pred.op} ${right.clause})`, params };
        }

        // Comparison operators
        if (['=', '!=', '<', '>', '<=', '>='].includes(pred.op)) {
          // Special case for NULL comparisons
          if (pred.right.kind === 'Literal' && pred.right.value === null) {
            if (pred.op === '=') {
              return { clause: `(${left.clause} IS NULL)`, params };
            }
            if (pred.op === '!=') {
              return { clause: `(${left.clause} IS NOT NULL)`, params };
            }
          }
          return { clause: `(${left.clause} ${pred.op} ${right.clause})`, params };
        }

        // Logical operators
        if (pred.op === 'AND') {
          return { clause: `(${left.clause} AND ${right.clause})`, params };
        }
        if (pred.op === 'OR') {
          return { clause: `(${left.clause} OR ${right.clause})`, params };
        }

        // String operators
        if (pred.op === 'LIKE') {
          return { clause: `(${left.clause} LIKE ${right.clause})`, params };
        }
        if (pred.op === 'CONCAT') {
          return { clause: `(${left.clause} || ${right.clause})`, params };
        }

        throw new Error(`Unsupported binary operator: ${pred.op}`);
      }

      case 'UnaryOp': {
        const operand = this.predicateToSQL(pred.operand, params);
        if (pred.op === 'NOT') {
          return { clause: `NOT (${operand.clause})`, params };
        }
        if (pred.op === 'NEG') {
          return { clause: `-${operand.clause}`, params };
        }
        throw new Error(`Unsupported unary operator: ${pred.op}`);
      }

      case 'FunctionCall': {
        const args = pred.args.map(arg => this.predicateToSQL(arg, params).clause);
        
        // Map common function names to SQL equivalents
        const sqlFunction = pred.name.toUpperCase();
        const supportedFunctions = [
          'ABS', 'FLOOR', 'CEIL', 'ROUND', 'UPPER', 'LOWER', 'TRIM', 
          'LENGTH', 'COALESCE', 'NOW', 'DATE_TRUNC'
        ];
        
        if (!supportedFunctions.includes(sqlFunction)) {
          throw new Error(`Unsupported function: ${pred.name}`);
        }
        
        return { clause: `${sqlFunction}(${args.join(', ')})`, params };
      }

      case 'Conditional': {
        const condition = this.predicateToSQL(pred.condition, params);
        const thenClause = this.predicateToSQL(pred.then, params);
        const elseClause = this.predicateToSQL(pred.else, params);
        return {
          clause: `(CASE WHEN ${condition.clause} THEN ${thenClause.clause} ELSE ${elseClause.clause} END)`,
          params
        };
      }

      case 'IsNull': {
        const operand = this.predicateToSQL(pred.expr, params);
        return { clause: `(${operand.clause} IS NULL)`, params };
      }

      case 'In': {
        const expr = this.predicateToSQL(pred.expr, params);
        const values = pred.values.map(v => {
          if (v.kind === 'Literal') {
            const value = v.value;
            if (typeof value === 'number') {
              return `${addParam(value)}::numeric`;
            }
            return addParam(value);
          }
          throw new Error('IN operator only supports literal values');
        });
        return { clause: `(${expr.clause} IN (${values.join(', ')}))`, params };
      }

      case 'Between': {
        const expr = this.predicateToSQL(pred.expr, params);
        const low = this.predicateToSQL(pred.low, params);
        const high = this.predicateToSQL(pred.high, params);
        return { clause: `(${expr.clause} BETWEEN ${low.clause} AND ${high.clause})`, params };
      }

      case 'Cast': {
        const expr = this.predicateToSQL(pred.expr, params);
        const pgType = this.engineTypeToPostgres(pred.to);
        return { clause: `(${expr.clause}::${pgType})`, params };
      }

      case 'WindowExpr': {
        throw new Error('Window functions cannot be pushed down to SQL (need ORDER context)');
      }

      case 'SqlExpr': {
        return { clause: (pred as any).sql, params };
      }

      case 'Wildcard': {
        throw new Error('Wildcard expressions are not valid in predicates');
      }

      default: {
        throw new Error(`Unsupported expression kind: ${(pred as any).kind}`);
      }
    }
  }

  private engineTypeToPostgres(engineType: any): string {
    switch (engineType.kind) {
      case 'string': return 'TEXT';
      case 'number': return 'NUMERIC';
      case 'boolean': return 'BOOLEAN';
      case 'datetime': return 'TIMESTAMPTZ';
      case 'json': return 'JSONB';
      default: return 'TEXT';
    }
  }
}
