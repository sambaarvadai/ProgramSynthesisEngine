import { Pool } from 'pg';
import { readFileSync } from 'fs';
import * as dotenv from 'dotenv';
dotenv.config();

async function seedCrmData() {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL
  });

  try {
    console.log('Reading crm_seed.sql...');
    const sql = readFileSync('./crm_seed.sql', 'utf-8');
    console.log('Executing seed data...');
    await pool.query(sql);
    console.log('CRM seed data populated successfully');
  } catch (e) {
    console.error('Error seeding CRM data:', e);
  } finally {
    await pool.end();
  }
}

seedCrmData().catch(console.error);
