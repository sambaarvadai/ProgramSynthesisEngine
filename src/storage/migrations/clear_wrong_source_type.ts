import * as dotenv from 'dotenv';
dotenv.config();
import { getPeeStorePool } from '../PeeStoreBackend.js';

async function migrate() {
  const pool = getPeeStorePool();
  
  try {
    // Delete entries with wrong source_type (postgres instead of crm)
    const result = await pool.query(
      `DELETE FROM pee_semantic_cache WHERE source_type = 'postgres'`
    );
    console.log(`✅ Deleted ${result.rowCount} cache entries with wrong source_type`);
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

migrate();
