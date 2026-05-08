import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function checkTables() {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL
  });

  try {
    const result = await pool.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      ORDER BY table_name
    `);
    console.log('Tables in pee_dev:');
    result.rows.forEach(row => console.log(`  - ${row.table_name}`));
  } catch (e) {
    console.error('Error:', e);
  } finally {
    await pool.end();
  }
}

checkTables().catch(console.error);
