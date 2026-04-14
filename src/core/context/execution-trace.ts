// Defines execution tracing and trace management

import type { Value } from '../types/value.js';
import { BaseError, ErrorUtils } from '../errors/index.js';

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
  errorDetails?: BaseError; // Enhanced error tracking
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

// Enhanced error tracking functions
export function traceError(
  trace: ExecutionTrace,
  nodeId: string,
  error: BaseError | Error,
  context?: Record<string, Value>
): void {
  const baseError = error instanceof BaseError ? error : ErrorUtils.wrapError(error, {
    nodeId,
    ...context
  });

  traceEvent(trace, {
    nodeId,
    kind: 'error',
    error: error.message,
    errorDetails: baseError,
    meta: context
  });
}

export function extractErrorsFromTrace(trace: ExecutionTrace): BaseError[] {
  const errors: BaseError[] = [];
  
  for (const event of trace.events) {
    if (event.kind === 'error' && event.errorDetails) {
      errors.push(event.errorDetails);
    }
  }
  
  return errors;
}

export function getErrorSummary(trace: ExecutionTrace): {
  totalErrors: number;
  errorsByNode: Record<string, BaseError[]>;
  errorsByCategory: Record<string, number>;
  errorsBySeverity: Record<string, number>;
} {
  const errors = extractErrorsFromTrace(trace);
  const errorsByNode: Record<string, BaseError[]> = {};
  const errorsByCategory: Record<string, number> = {};
  const errorsBySeverity: Record<string, number> = {};

  for (const error of errors) {
    // Group by node
    const nodeId = error.metadata.context.nodeId || 'unknown';
    if (!errorsByNode[nodeId]) {
      errorsByNode[nodeId] = [];
    }
    errorsByNode[nodeId].push(error);

    // Group by category
    const category = error.metadata.category;
    errorsByCategory[category] = (errorsByCategory[category] || 0) + 1;

    // Group by severity
    const severity = error.metadata.severity;
    errorsBySeverity[severity] = (errorsBySeverity[severity] || 0) + 1;
  }

  return {
    totalErrors: errors.length,
    errorsByNode,
    errorsByCategory,
    errorsBySeverity
  };
}
