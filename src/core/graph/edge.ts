// Defines graph edge structures and edge operations

import type { EngineType } from '../types/engine-type.js';

export type EdgeId = string;

export type EdgeKind = 'data' | 'control';

export type ControlCondition = 'true' | 'false' | 'always' | 'error';

export interface PipelineEdge {
  id: EdgeId;
  from: string; // NodeId
  to: string;   // NodeId
  kind: EdgeKind;
  // data edge fields
  outputKey?: string;
  inputKey?: string;
  schema?: EngineType;
  // control edge fields
  condition?: ControlCondition;
}
