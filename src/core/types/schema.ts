// Defines schema definitions and schema validation

import type { EngineType } from './engine-type.js';

export type RowSchema = {
  columns: Array<{
    name: string;
    type: EngineType;
    nullable: boolean;
  }>;
};
