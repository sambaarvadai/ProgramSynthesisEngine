// Defines graph edge structures and edge operations

import type { DataType } from '../types/data-value.js';

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
  dataType?: DataType;        // replaces schema?: EngineType
  // control edge fields
  condition?: ControlCondition;
}
