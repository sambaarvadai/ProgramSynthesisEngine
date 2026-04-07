import { PostgresBackend } from '../storage/postgres-backend.js';
import { crmSchema } from '../config/index.js';
import * as dotenv from 'dotenv';
dotenv.config();

async function verify() {
  console.log('Verifying database setup...\n');

  const backend = new PostgresBackend(process.env.DATABASE_URL!);
  await backend.connect();

  for (const tableName of crmSchema.tables.keys()) {
    const exists = await backend.tableExists(tableName);
    if (!exists) {
      console.log(`❌ Table '${tableName}' does not exist`);
      continue;
    }

    // Count rows
    const result = await (backend as any).pool.query(
      `SELECT COUNT(*) as count FROM ${tableName}`,
    );
    const count = result.rows[0].count;
    console.log(`✔ ${tableName}: ${count} rows`);
  }

  await backend.disconnect();
  console.log('\nDatabase verified.');
}

verify().catch(err => {
  console.error('Verification failed:', err.message);
  process.exit(1);
});
