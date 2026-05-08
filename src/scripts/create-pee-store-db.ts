import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function createPeeStoreDb() {
  // Connect to postgres database (default database)
  const pool = new Pool({
    connectionString: process.env.PEE_STORE_DATABASE_URL?.replace('/pee_store', '/postgres')
  });

  try {
    console.log('Creating pee_store database...');
    await pool.query('CREATE DATABASE pee_store');
    console.log('pee_store database created successfully');
  } catch (e: any) {
    if (e.code === '42P04') {
      console.log('pee_store database already exists');
    } else {
      console.error('Error creating pee_store database:', e);
    }
  } finally {
    await pool.end();
  }
}

createPeeStoreDb().catch(console.error);
