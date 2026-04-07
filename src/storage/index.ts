// Storage module exports

export type { PhysicalOperatorResult, StorageBackend } from '../core/storage/storage-backend.js';
export type { TempStore } from '../core/storage/temp-store.js';
export { PostgresBackend } from './postgres-backend.js';
export { SQLiteTempStore } from './sqlite-temp-store.js';
