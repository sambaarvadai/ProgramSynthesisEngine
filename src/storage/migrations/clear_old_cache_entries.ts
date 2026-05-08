import * as dotenv from 'dotenv';
dotenv.config();
import { getPeeStorePool } from '../PeeStoreBackend.js';

async function migrate() {
  const pool = getPeeStorePool();
  
  try {
    // Delete old cache entries that don't have plan_json
    const result = await pool.query(
      `DELETE FROM pee_semantic_cache WHERE plan_json IS NULL`
    );
    console.log(`✅ Deleted ${result.rowCount} old cache entries without plan_json`);
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

migrate();
