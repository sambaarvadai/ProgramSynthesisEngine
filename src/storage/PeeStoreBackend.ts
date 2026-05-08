import { Pool } from 'pg';

let storePool: Pool | null = null;

export function getPeeStorePool(): Pool {
  if (!storePool) {
    const url = process.env.PEE_STORE_DATABASE_URL;
    if (!url) {
      throw new Error(
        'PEE_STORE_DATABASE_URL not set — pipeline persistence disabled'
      );
    }
    storePool = new Pool({ connectionString: url, max: 5 });
  }
  return storePool;
}

export async function connectPeeStore(): Promise<boolean> {
  try {
    const pool = getPeeStorePool();
    await pool.query('SELECT 1');
    console.log('[PeeStore] Connected to pee_store database');
    return true;
  } catch (e) {
    console.warn(
      '[PeeStore] Could not connect to pee_store — ' +
      'pipeline persistence disabled. Error:', e
    );
    return false;
  }
}
