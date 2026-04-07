// Defines execution tracing and trace management

import type { Value } from '../types/value.js';

export type TraceEventKind = 'start' | 'complete' | 'error' | 'skip' | 'batch';

export interface TraceEvent {
  nodeId: string; // NodeId
  kind: TraceEventKind;
  timestamp: number;
  durationMs?: number;
  rowsIn?: number;
  rowsOut?: number;
  batchCount?: number;
  error?: string;
  meta?: Record<string, Value>;
}

export interface ExecutionTrace {
  events: TraceEvent[];
}

export function traceEvent(
  trace: ExecutionTrace,
  event: Omit<TraceEvent, 'timestamp'>
): void {
  const fullEvent: TraceEvent = {
    ...event,
    timestamp: Date.now(),
  };
  trace.events.push(fullEvent);
}

export function traceFilter(trace: ExecutionTrace, nodeId: string): TraceEvent[] {
  return trace.events.filter(event => event.nodeId === nodeId);
}

export function traceSummary(
  trace: ExecutionTrace
): Record<string, { totalMs: number; rowsOut: number; status: string }> {
  const summary: Record<string, { totalMs: number; rowsOut: number; status: string }> = {};
  
  for (const event of trace.events) {
    if (!summary[event.nodeId]) {
      summary[event.nodeId] = {
        totalMs: 0,
        rowsOut: 0,
        status: 'unknown',
      };
    }
    
    const nodeSummary = summary[event.nodeId];
    
    if (event.durationMs) {
      nodeSummary.totalMs += event.durationMs;
    }
    
    if (event.rowsOut !== undefined) {
      nodeSummary.rowsOut = event.rowsOut;
    }
    
    // Update status based on event kind
    if (event.kind === 'error') {
      nodeSummary.status = 'error';
    } else if (event.kind === 'complete' && nodeSummary.status !== 'error') {
      nodeSummary.status = 'complete';
    } else if (event.kind === 'skip' && nodeSummary.status === 'unknown') {
      nodeSummary.status = 'skip';
    }
  }
  
  return summary;
}
