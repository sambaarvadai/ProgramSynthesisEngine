import * as dotenv from 'dotenv';
dotenv.config();
import { getPeeStorePool } from '../PeeStoreBackend.js';

async function migrate() {
  const pool = getPeeStorePool();
  
  try {
    const result = await pool.query(
      `DELETE FROM pee_semantic_cache`
    );
    console.log(`✅ Deleted ${result.rowCount} cache entries from pee_semantic_cache`);
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

migrate();
