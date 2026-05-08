import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function resetCrmDb() {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL
  });

  try {
    console.log('Dropping existing tables...');
    const tables = ['customers', 'order_items', 'orders', 'products', 'support_tickets'];
    for (const table of tables) {
      await pool.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
      console.log(`  Dropped ${table}`);
    }
    console.log('Tables dropped successfully');
  } catch (e) {
    console.error('Error dropping tables:', e);
  } finally {
    await pool.end();
  }
}

resetCrmDb().catch(console.error);
