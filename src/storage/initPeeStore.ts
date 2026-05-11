import { getPeeStorePool } from './PeeStoreBackend.js';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

export async function initPeeStore(): Promise<void> {
  try {
    const pool = getPeeStorePool();
    const __dirname = dirname(fileURLToPath(import.meta.url));
    const schemaSql = readFileSync(
      join(__dirname, 'pee-store-schema.sql'), 
      'utf-8'
    );
    await pool.query(schemaSql);
    console.log('[PeeStore] Schema initialized');
  } catch (e) {
    const error = e as any;
    // If the error is about the datasource column not existing, add it
    if (error.code === '42703' && error.column === 'datasource') {
      console.log('[PeeStore] Adding datasource column to pee_schema_state');
      try {
        const pool = getPeeStorePool();
        await pool.query(`
          ALTER TABLE pee_schema_state 
          ADD COLUMN IF NOT EXISTS datasource TEXT NOT NULL DEFAULT 'default';
        `);
        await pool.query(`
          CREATE UNIQUE INDEX IF NOT EXISTS idx_pee_schema_state_datasource
          ON pee_schema_state(datasource);
        `);
        console.log('[PeeStore] Datasource column added successfully');
      } catch (migrationError) {
        console.warn('[PeeStore] Failed to add datasource column:', migrationError);
      }
    } else {
      console.warn('[PeeStore] Schema init failed:', e);
    }
  }
}
