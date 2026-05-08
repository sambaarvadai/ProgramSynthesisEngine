import * as dotenv from 'dotenv';
dotenv.config();
import { getPeeStorePool } from '../PeeStoreBackend.js';

async function migrate() {
  const pool = getPeeStorePool();
  
  try {
    await pool.query(
      `ALTER TABLE pee_semantic_cache ADD COLUMN IF NOT EXISTS plan_json JSONB`
    );
    console.log('✅ Migration completed: Added plan_json column to pee_semantic_cache');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

migrate();
