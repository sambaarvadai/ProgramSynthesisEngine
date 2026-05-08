import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function resetPeeStore() {
  const pool = new Pool({
    connectionString: process.env.PEE_STORE_DATABASE_URL
  });

  try {
    console.log('Dropping tables...');
    await pool.query('DROP TABLE IF EXISTS pee_pipeline_nodes CASCADE');
    await pool.query('DROP TABLE IF EXISTS pee_pipelines CASCADE');
    await pool.query('DROP TABLE IF EXISTS pee_semantic_cache CASCADE');
    await pool.query('DROP TABLE IF EXISTS pee_schema_state CASCADE');
    console.log('Tables dropped successfully');
  } catch (e) {
    console.error('Error dropping tables:', e);
  } finally {
    await pool.end();
  }
}

resetPeeStore().catch(console.error);
