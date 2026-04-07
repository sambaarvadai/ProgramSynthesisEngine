// Defines temporary storage implementations and management

import type { RowBatch } from '../types/row.js';
import type { PhysicalOperatorResult } from './storage-backend.js';

export interface TempStore {
  write(key: string, batch: RowBatch): Promise<void>;
  read(key: string, batchSize: number): PhysicalOperatorResult;
  append(key: string, batch: RowBatch): Promise<void>;
  delete(key: string): Promise<void>;
  exists(key: string): Promise<boolean>;
  clear(): Promise<void>; // called at end of execution
}
