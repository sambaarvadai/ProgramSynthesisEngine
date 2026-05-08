import { Pool } from 'pg';
import { readFileSync } from 'fs';
import * as dotenv from 'dotenv';
dotenv.config();

async function initCrmSchema() {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL
  });

  try {
    console.log('Reading crm_postgres.sql...');
    const sql = readFileSync('./crm_postgres.sql', 'utf-8');
    console.log('Executing schema initialization...');
    await pool.query(sql);
    console.log('CRM schema initialized successfully');
  } catch (e) {
    console.error('Error initializing CRM schema:', e);
  } finally {
    await pool.end();
  }
}

initCrmSchema().catch(console.error);
