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
    console.warn('[PeeStore] Schema init failed:', e);
  }
}
