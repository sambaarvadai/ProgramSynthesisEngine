// Error Capture and Aggregation System

import { BaseError, ErrorCategory, ErrorSeverity, ErrorLevel, ErrorContext, ErrorMetadata } from './error-taxonomy.js';
import type { ExecutionTrace, TraceEvent } from '../context/execution-trace.js';

export interface ErrorAggregation {
  category: ErrorCategory;
  severity: ErrorSeverity;
  level: ErrorLevel;
  count: number;
  firstOccurrence: number;
  lastOccurrence: number;
  affectedNodes: string[];
  affectedPipelines: string[];
  errorCodes: string[];
  sampleErrors: BaseError[];
  recoverySuccessRate?: number;
}

export interface ErrorSummary {
  totalErrors: number;
  criticalErrors: number;
  errorRate: number;
  categories: Record<ErrorCategory, ErrorAggregation>;
  severity: Record<ErrorSeverity, number>;
  levels: Record<ErrorLevel, number>;
  topErrors: Array<{
    errorCode: string;
    count: number;
    description: string;
  }>;
  trends: {
    increasing: string[];
    decreasing: string[];
    stable: string[];
  };
  recommendations: string[];
}

export interface ErrorPattern {
  pattern: string;
  frequency: number;
  confidence: number;
  description: string;
  suggestedAction: string;
}

export class ErrorCapture {
  private errors: BaseError[] = [];
  private errorHistory: Map<string, BaseError[]> = new Map();
  private aggregationCache: Map<string, ErrorAggregation> = new Map();

  constructor(private maxHistorySize: number = 1000) {}

  public captureError(error: BaseError | Error, context?: ErrorContext): BaseError {
    let baseError: BaseError;
    
    if (error instanceof BaseError) {
      baseError = error;
    } else {
      // Convert regular Error to BaseError with minimal metadata
      baseError = new BaseError(
        error.message,
        'GENERIC_ERROR',
        ErrorCategory.SYSTEM,
        ErrorSeverity.ERROR,
        ErrorLevel.NODE,
        context || {}
      );
    }

    // Add context if provided
    if (context) {
      baseError.withContext(context);
    }

    // Store error
    this.errors.push(baseError);
    
    // Maintain history size
    if (this.errors.length > this.maxHistorySize) {
      this.errors.shift();
    }

    // Update error history by correlation ID
    const correlationId = baseError.metadata.correlationId;
    if (correlationId) {
      if (!this.errorHistory.has(correlationId)) {
        this.errorHistory.set(correlationId, []);
      }
      this.errorHistory.get(correlationId)!.push(baseError);
    }

    // Invalidate aggregation cache
    this.aggregationCache.clear();

    return baseError;
  }

  public getErrors(filter?: {
    category?: ErrorCategory;
    severity?: ErrorSeverity;
    level?: ErrorLevel;
    nodeId?: string;
    pipelineId?: string;
    timeRange?: { start: number; end: number };
  }): BaseError[] {
    return this.errors.filter(error => {
      if (filter?.category && error.metadata.category !== filter.category) return false;
      if (filter?.severity && error.metadata.severity !== filter.severity) return false;
      if (filter?.level && error.metadata.level !== filter.level) return false;
      if (filter?.nodeId && error.metadata.context.nodeId !== filter.nodeId) return false;
      if (filter?.pipelineId && error.metadata.context.pipelineId !== filter.pipelineId) return false;
      if (filter?.timeRange) {
        const timestamp = error.metadata.timestamp;
        if (timestamp < filter.timeRange.start || timestamp > filter.timeRange.end) return false;
      }
      return true;
    });
  }

  public aggregateErrors(): Record<string, ErrorAggregation> {
    if (this.aggregationCache.size > 0) {
      return Object.fromEntries(this.aggregationCache);
    }

    const aggregations = new Map<string, ErrorAggregation>();

    for (const error of this.errors) {
      const key = `${error.metadata.category}:${error.metadata.severity}:${error.metadata.level}`;
      
      if (!aggregations.has(key)) {
        aggregations.set(key, {
          category: error.metadata.category,
          severity: error.metadata.severity,
          level: error.metadata.level,
          count: 0,
          firstOccurrence: error.metadata.timestamp,
          lastOccurrence: error.metadata.timestamp,
          affectedNodes: [],
          affectedPipelines: [],
          errorCodes: [],
          sampleErrors: []
        });
      }

      const agg = aggregations.get(key)!;
      agg.count++;
      agg.lastOccurrence = Math.max(agg.lastOccurrence, error.metadata.timestamp);
      
      if (error.metadata.context.nodeId && !agg.affectedNodes.includes(error.metadata.context.nodeId!)) {
        agg.affectedNodes.push(error.metadata.context.nodeId);
      }
      
      if (error.metadata.context.pipelineId && !agg.affectedPipelines.includes(error.metadata.context.pipelineId!)) {
        agg.affectedPipelines.push(error.metadata.context.pipelineId);
      }
      
      if (!agg.errorCodes.includes(error.metadata.errorCode)) {
        agg.errorCodes.push(error.metadata.errorCode);
      }
      
      if (agg.sampleErrors.length < 3) {
        agg.sampleErrors.push(error);
      }
    }

    this.aggregationCache = aggregations;
    return Object.fromEntries(aggregations);
  }

  public generateSummary(): ErrorSummary {
    const aggregations = this.aggregateErrors();
    const categories = {} as Record<ErrorCategory, ErrorAggregation>;
    const severity = {} as Record<ErrorSeverity, number>;
    const levels = {} as Record<ErrorLevel, number>;
    
    let criticalErrors = 0;

    // Initialize counters
    Object.values(ErrorCategory).forEach(cat => categories[cat] = {} as ErrorAggregation);
    Object.values(ErrorSeverity).forEach(sev => severity[sev] = 0);
    Object.values(ErrorLevel).forEach(lvl => levels[lvl] = 0);

    // Populate from aggregations
    for (const agg of Object.values(aggregations)) {
      categories[agg.category] = agg;
      severity[agg.severity] += agg.count;
      levels[agg.level] += agg.count;
      
      if (agg.severity === ErrorSeverity.CRITICAL) {
        criticalErrors += agg.count;
      }
    }

    // Calculate top errors
    const errorCounts = new Map<string, number>();
    for (const error of this.errors) {
      const count = errorCounts.get(error.metadata.errorCode) || 0;
      errorCounts.set(error.metadata.errorCode, count + 1);
    }

    const topErrors = Array.from(errorCounts.entries())
      .sort(([, a], [, b]) => b - a)
      .slice(0, 10)
      .map(([errorCode, count]) => ({
        errorCode,
        count,
        description: this.getErrorDescription(errorCode)
      }));

    // Generate recommendations
    const recommendations = this.generateRecommendations(aggregations);

    return {
      totalErrors: this.errors.length,
      criticalErrors,
      errorRate: this.calculateErrorRate(),
      categories,
      severity,
      levels,
      topErrors,
      trends: this.calculateTrends(),
      recommendations
    };
  }

  public detectPatterns(): ErrorPattern[] {
    const patterns: ErrorPattern[] = [];
    
    // Pattern: Repeated errors in same node
    const nodeErrors = new Map<string, BaseError[]>();
    for (const error of this.errors) {
      const nodeId = error.metadata.context.nodeId;
      if (nodeId) {
        if (!nodeErrors.has(nodeId)) nodeErrors.set(nodeId, []);
        nodeErrors.get(nodeId)!.push(error);
      }
    }

    for (const [nodeId, errors] of nodeErrors) {
      if (errors.length >= 3) {
        patterns.push({
          pattern: `repeated_errors_in_node`,
          frequency: errors.length,
          confidence: Math.min(errors.length / 5, 1),
          description: `Node ${nodeId} is failing repeatedly`,
          suggestedAction: 'Review node configuration and input data'
        });
      }
    }

    // Pattern: Escalating error severity
    const recentErrors = this.errors.slice(-20);
    if (recentErrors.length >= 10) {
      const criticalCount = recentErrors.filter(e => e.metadata.severity === ErrorSeverity.CRITICAL).length;
      if (criticalCount >= 3) {
        patterns.push({
          pattern: 'escalating_severity',
          frequency: criticalCount,
          confidence: criticalCount / recentErrors.length,
          description: 'Error severity is escalating in recent operations',
          suggestedAction: 'Pause operations and investigate system health'
        });
      }
    }

    return patterns;
  }

  public integrateWithTrace(trace: ExecutionTrace): void {
    for (const event of trace.events) {
      if (event.kind === 'error' && event.error) {
        this.captureError(new BaseError(
          event.error,
          'TRACE_ERROR',
          ErrorCategory.EXECUTION,
          ErrorSeverity.ERROR,
          ErrorLevel.NODE,
          {
            nodeId: event.nodeId,
            operation: 'trace_event'
          }
        ));
      }
    }
  }

  private getErrorDescription(errorCode: string): string {
    // Map common error codes to descriptions
    const descriptions: Record<string, string> = {
      'VAL001': 'Validation failed',
      'EXE001': 'Query execution failed',
      'SYS001': 'Memory limit exceeded',
      'DAT001': 'Data not found',
      'NET001': 'Connection failed',
      'RES001': 'Budget exceeded'
    };
    return descriptions[errorCode] || errorCode;
  }

  private calculateErrorRate(): number {
    // Simple error rate calculation (errors per minute in recent history)
    const recentTime = Date.now() - (5 * 60 * 1000); // Last 5 minutes
    const recentErrors = this.errors.filter(e => e.metadata.timestamp > recentTime);
    return recentErrors.length / 5; // errors per minute
  }

  private calculateTrends(): { increasing: string[]; decreasing: string[]; stable: string[] } {
    // Simple trend analysis based on recent vs older error rates
    const midPoint = Date.now() - (10 * 60 * 1000); // 10 minutes ago
    const recentErrors = this.errors.filter(e => e.metadata.timestamp > midPoint);
    const olderErrors = this.errors.filter(e => e.metadata.timestamp <= midPoint);

    const recentRate = recentErrors.length / 5; // per minute
    const olderRate = olderErrors.length / 5;   // per minute

    const threshold = 0.2; // 20% change threshold

    return {
      increasing: recentRate > olderRate * (1 + threshold) ? ['overall_error_rate'] : [],
      decreasing: recentRate < olderRate * (1 - threshold) ? ['overall_error_rate'] : [],
      stable: Math.abs(recentRate - olderRate) <= olderRate * threshold ? ['overall_error_rate'] : []
    };
  }

  private generateRecommendations(aggregations: Record<string, ErrorAggregation>): string[] {
    const recommendations: string[] = [];

    for (const agg of Object.values(aggregations)) {
      if (agg.severity === ErrorSeverity.CRITICAL && agg.count >= 2) {
        recommendations.push(`Address critical ${agg.category} errors affecting ${agg.affectedNodes.length} nodes`);
      }

      if (agg.count >= 5) {
        recommendations.push(`Investigate recurring ${agg.category} errors (${agg.count} occurrences)`);
      }
    }

    if (recommendations.length === 0) {
      recommendations.push('Error rates are within acceptable ranges');
    }

    return recommendations;
  }
}
