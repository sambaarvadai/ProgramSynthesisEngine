import { Pool } from 'pg';
import { createHash } from 'crypto';
import { readFileSync } from 'fs';
import { SemanticCache } from './SemanticCache.js';
import { dataSourceRegistry } from '../storage/DataSourceRegistry.js';

export class SchemaStateManager {
  constructor(private pool: Pool) {}

  computeDDLHash(ddlContent: string): string {
    return createHash('sha256').update(ddlContent).digest('hex');
  }

  async getStoredHash(datasource: string): Promise<string | null> {
    try {
      const result = await this.pool.query(
        `SELECT ddl_hash FROM pee_schema_state
         WHERE datasource = $1
         ORDER BY recorded_at DESC LIMIT 1`,
        [datasource]
      );
      return result.rows[0]?.ddl_hash ?? null;
    } catch {
      return null;
    }
  }

  async saveHash(
    datasource: string,
    hash: string,
    tableCount: number,
    columnCount: number
  ): Promise<void> {
    await this.pool.query(
      `INSERT INTO pee_schema_state
         (datasource, ddl_hash, table_count, column_count)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (datasource) 
         DO UPDATE SET 
           ddl_hash = EXCLUDED.ddl_hash,
           table_count = EXCLUDED.table_count,
           column_count = EXCLUDED.column_count,
           recorded_at = NOW()`,
      [datasource, hash, tableCount, columnCount]
    );
  }

  async checkAndHandleSchemaChange(
    cache: SemanticCache
  ): Promise<boolean> {

    let schemaChanged = false;

    // Check each datasource independently
    for (const source of dataSourceRegistry.all()) {
      if (!source.schema || !source.ddlPath) continue;  // skip non-postgres sources

      const ddlContent = readFileSync(source.ddlPath, 'utf-8');
      const hash = this.computeDDLHash(ddlContent);
      const storedHash = await this.getStoredHash(source.name);

      // Count total columns across all tables
      const columnCount = [...source.schema.parsed.tables.values()].reduce(
        (sum: number, t: any) => sum + t.columns.size, 0
      );

      if (storedHash === null) {
        await this.saveHash(source.name, hash, source.schema.parsed.tables.size, columnCount);
        console.log(`[SchemaState] Initial DDL hash recorded for ${source.name}`);
        continue;
      }

      if (storedHash === hash) {
        console.log(`[SchemaState] Schema unchanged for ${source.name}`);
        continue;
      }

      schemaChanged = true;
      await cache.invalidateAll(`DDL schema change detected for ${source.name}`);
      await this.saveHash(source.name, hash, source.schema.parsed.tables.size, columnCount);
    }

    if (!schemaChanged) {
      console.log('[SchemaState] All schemas unchanged — cache valid');
    }

    return schemaChanged;
  }
}
