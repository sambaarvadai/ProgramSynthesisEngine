import { Pool } from 'pg';
import { createHash } from 'crypto';
import { readFileSync } from 'fs';
import { SemanticCache } from './SemanticCache.js';

export class SchemaStateManager {
  constructor(private pool: Pool) {}

  computeDDLHash(ddlPath: string): string {
    const ddl = readFileSync(ddlPath, 'utf-8');
    return createHash('sha256').update(ddl).digest('hex');
  }

  async getStoredHash(): Promise<string | null> {
    try {
      const result = await this.pool.query(
        `SELECT ddl_hash FROM pee_schema_state
           ORDER BY recorded_at DESC LIMIT 1`
      );
      return result.rows[0]?.ddl_hash ?? null;
    } catch {
      return null;
    }
  }

  async saveHash(
    hash: string,
    tableCount: number,
    columnCount: number
  ): Promise<void> {
    await this.pool.query(
      `INSERT INTO pee_schema_state
         (ddl_hash, table_count, column_count)
         VALUES ($1, $2, $3)`,
      [hash, tableCount, columnCount]
    );
  }

  async checkAndHandleSchemaChange(
    ddlPath: string,
    cache: SemanticCache,
    tableCount: number,
    columnCount: number
  ): Promise<boolean> {

    const currentHash = this.computeDDLHash(ddlPath);
    const storedHash = await this.getStoredHash();

    if (storedHash === null) {
      // First run — save hash, nothing to invalidate
      await this.saveHash(currentHash, tableCount, columnCount);
      console.log('[SchemaState] Initial DDL hash recorded');
      return false;
    }

    if (storedHash === currentHash) {
      console.log('[SchemaState] Schema unchanged — cache valid');
      return false;
    }

    // Schema changed — invalidate all cache entries
    console.warn(
      '[SchemaState] DDL hash changed — schema was modified. ' +
      'Invalidating all semantic cache entries.'
    );

    await cache.invalidateAll('DDL schema change detected');
    await this.saveHash(currentHash, tableCount, columnCount);

    console.log('[SchemaState] New DDL hash recorded');
    return true;  // schema changed
  }
}
