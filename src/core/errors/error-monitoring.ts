// Error Monitoring and Analytics System

import { ErrorCapture, ErrorSummary, ErrorPattern } from './error-capture.js';
import { BaseError, ErrorCategory, ErrorSeverity, ErrorLevel } from './error-taxonomy.js';
import { ErrorFramer, Audience } from './error-response.js';
import type { ExecutionTrace } from '../context/execution-trace.js';

export interface ErrorMetrics {
  totalErrors: number;
  errorsByCategory: Record<ErrorCategory, number>;
  errorsBySeverity: Record<ErrorSeverity, number>;
  errorsByLevel: Record<ErrorLevel, number>;
  errorRate: number; // errors per minute
  criticalErrorRate: number; // critical errors per minute
  recoverySuccessRate: number; // percentage of successful recoveries
  averageResolutionTime: number; // average time to resolve errors (ms)
}

export interface ErrorAlert {
  id: string;
  type: 'threshold' | 'pattern' | 'critical' | 'escalation' | 'burst';
  severity: ErrorSeverity;
  message: string;
  description: string;
  affectedComponents: string[];
  suggestedActions: string[];
  timestamp: number;
  resolved: boolean;
  resolvedAt?: number;
}

export interface ErrorReport {
  period: {
    start: number;
    end: number;
    duration: number;
  };
  metrics: ErrorMetrics;
  summary: ErrorSummary;
  patterns: ErrorPattern[];
  alerts: ErrorAlert[];
  recommendations: string[];
  trends: {
    errorRate: 'increasing' | 'decreasing' | 'stable';
    severity: 'improving' | 'degrading' | 'stable';
    categories: Record<ErrorCategory, 'increasing' | 'decreasing' | 'stable'>;
  };
}

export class ErrorMonitoring {
  private errorCapture: ErrorCapture;
  private alerts: Map<string, ErrorAlert> = new Map();
  private thresholds: Map<string, { value: number; severity: ErrorSeverity }> = new Map();
  private alertHandlers: Array<(alert: ErrorAlert) => void> = [];
  private metricsHistory: Array<{ timestamp: number; metrics: ErrorMetrics }> = [];

  constructor(private config: {
    alertThresholds?: Record<string, { value: number; severity: ErrorSeverity }>;
    maxHistorySize?: number;
    enableAutoRecovery?: boolean;
  } = {}) {
    this.errorCapture = new ErrorCapture(config.maxHistorySize || 1000);
    
    // Set default thresholds
    this.thresholds = new Map([
      ['critical_error_rate', { value: 0.1, severity: ErrorSeverity.CRITICAL }],
      ['total_error_rate', { value: 1.0, severity: ErrorSeverity.ERROR }],
      ['error_burst', { value: 5, severity: ErrorSeverity.WARNING }],
      ['pattern_detection', { value: 3, severity: ErrorSeverity.WARNING }]
    ]);

    // Override with custom thresholds
    if (config.alertThresholds) {
      Object.entries(config.alertThresholds).forEach(([key, threshold]) => {
        this.thresholds.set(key, threshold);
      });
    }
  }

  public captureError(error: BaseError | Error, context?: any): BaseError {
    const baseError = this.errorCapture.captureError(error, context);
    
    // Check for alerts
    this.checkAlerts(baseError);
    
    // Update metrics
    this.updateMetrics();
    
    return baseError;
  }

  public integrateWithTrace(trace: ExecutionTrace): void {
    this.errorCapture.integrateWithTrace(trace);
    this.updateMetrics();
  }

  public generateReport(timeRange?: { start: number; end: number }): ErrorReport {
    const now = Date.now();
    const defaultRange = {
      start: now - (60 * 60 * 1000), // Last hour
      end: now
    };
    const range = timeRange || defaultRange;
    
    const errors = this.errorCapture.getErrors({
      timeRange: range
    });
    
    const summary = this.errorCapture.generateSummary();
    const patterns = this.errorCapture.detectPatterns();
    const metrics = this.calculateMetrics(errors, range);
    const trends = this.calculateTrends(metrics);
    
    const activeAlerts = Array.from(this.alerts.values())
      .filter(alert => !alert.resolved && alert.timestamp >= range.start);
    
    const recommendations = this.generateRecommendations(summary, patterns, metrics);
    
    return {
      period: {
        ...range,
        duration: range.end - range.start
      },
      metrics,
      summary,
      patterns,
      alerts: activeAlerts,
      recommendations,
      trends
    };
  }

  public formatReportForUser(report: ErrorReport): string {
    let message = '';
    
    // Header
    message += 'Error Monitoring Report\n';
    message += '======================\n\n';
    
    // Period
    message += `Period: ${new Date(report.period.start).toLocaleString()} - ${new Date(report.period.end).toLocaleString()}\n`;
    message += `Duration: ${Math.round(report.period.duration / 60000)} minutes\n\n`;
    
    // Key metrics
    message += 'Key Metrics:\n';
    message += `  Total Errors: ${report.metrics.totalErrors}\n`;
    message += `  Error Rate: ${report.metrics.errorRate.toFixed(2)}/min\n`;
    message += `  Critical Errors: ${report.metrics.errorsBySeverity[ErrorSeverity.CRITICAL] || 0}\n`;
    message += `  Recovery Success Rate: ${(report.metrics.recoverySuccessRate * 100).toFixed(1)}%\n\n`;
    
    // Alerts
    if (report.alerts.length > 0) {
      message += 'Active Alerts:\n';
      report.alerts.forEach(alert => {
        message += `  ${alert.type.toUpperCase()}: ${alert.message}\n`;
      });
      message += '\n';
    }
    
    // Patterns
    if (report.patterns.length > 0) {
      message += 'Detected Patterns:\n';
      report.patterns.forEach(pattern => {
        message += `  - ${pattern.description}\n`;
      });
      message += '\n';
    }
    
    // Recommendations
    if (report.recommendations.length > 0) {
      message += 'Recommendations:\n';
      report.recommendations.forEach(rec => {
        message += `  ${rec}\n`;
      });
      message += '\n';
    }
    
    // Trends
    message += 'Trends:\n';
    message += `  Error Rate: ${report.trends.errorRate}\n`;
    message += `  Severity: ${report.trends.severity}\n\n`;
    
    return message;
  }

  public formatReportForDeveloper(report: ErrorReport): string {
    let message = '';
    
    message += 'Error Monitoring Report (Technical)\n';
    message += '===================================\n\n';
    
    // Detailed metrics
    message += 'Metrics:\n';
    message += JSON.stringify(report.metrics, null, 2) + '\n\n';
    
    // Error breakdown
    message += 'Error Breakdown:\n';
    Object.entries(report.summary.categories).forEach(([category, agg]) => {
      if (agg.count > 0) {
        message += `  ${category}: ${agg.count} errors\n`;
        message += `    Affected nodes: ${agg.affectedNodes.join(', ')}\n`;
        message += `    Error codes: ${agg.errorCodes.join(', ')}\n`;
      }
    });
    message += '\n';
    
    // Detailed patterns
    message += 'Error Patterns:\n';
    report.patterns.forEach(pattern => {
      message += `  Pattern: ${pattern.pattern}\n`;
      message += `  Frequency: ${pattern.frequency}\n`;
      message += `  Confidence: ${Math.round(pattern.confidence * 100)}%\n`;
      message += `  Suggested Action: ${pattern.suggestedAction}\n\n`;
    });
    
    // Alert details
    if (report.alerts.length > 0) {
      message += 'Alert Details:\n';
      report.alerts.forEach(alert => {
        message += `  ${alert.id}: ${alert.type} (${alert.severity})\n`;
        message += `    Message: ${alert.message}\n`;
        message += `    Affected: ${alert.affectedComponents.join(', ')}\n`;
        message += `    Suggested: ${alert.suggestedActions.join(', ')}\n\n`;
      });
    }
    
    return message;
  }

  public addAlertHandler(handler: (alert: ErrorAlert) => void): void {
    this.alertHandlers.push(handler);
  }

  public resolveAlert(alertId: string): void {
    const alert = this.alerts.get(alertId);
    if (alert) {
      alert.resolved = true;
      alert.resolvedAt = Date.now();
    }
  }

  public getActiveAlerts(): ErrorAlert[] {
    return Array.from(this.alerts.values()).filter(alert => !alert.resolved);
  }

  private checkAlerts(error: BaseError): void {
    // Critical error alert
    if (error.metadata.severity === ErrorSeverity.CRITICAL) {
      this.createAlert({
        type: 'critical',
        severity: ErrorSeverity.CRITICAL,
        message: `Critical error detected: ${error.metadata.errorCode}`,
        description: error.message,
        affectedComponents: [error.metadata.context.nodeId || 'unknown'],
        suggestedActions: error.recoverySuggestions.map(s => s.description)
      });
    }
    
    // Error burst detection
    const recentErrors = this.errorCapture.getErrors({
      timeRange: {
        start: Date.now() - (5 * 60 * 1000), // Last 5 minutes
        end: Date.now()
      }
    });
    
    if (recentErrors.length >= 5) {
      this.createAlert({
        type: 'burst',
        severity: ErrorSeverity.WARNING,
        message: `Error burst detected: ${recentErrors.length} errors in 5 minutes`,
        description: 'High frequency of errors detected in short time period',
        affectedComponents: [...new Set(recentErrors.map(e => e.metadata.context.nodeId).filter((id): id is string => Boolean(id)))],
        suggestedActions: ['Investigate system health', 'Check resource availability', 'Consider throttling operations']
      });
    }
    
    // Pattern-based alerts
    const patterns = this.errorCapture.detectPatterns();
    patterns.forEach(pattern => {
      if (pattern.confidence > 0.8) {
        this.createAlert({
          type: 'pattern',
          severity: ErrorSeverity.WARNING,
          message: `High-confidence error pattern: ${pattern.pattern}`,
          description: pattern.description,
          affectedComponents: ['system'],
          suggestedActions: [pattern.suggestedAction]
        });
      }
    });
  }

  private createAlert(alertData: Omit<ErrorAlert, 'id' | 'timestamp' | 'resolved'>): void {
    const alert: ErrorAlert = {
      id: crypto.randomUUID(),
      timestamp: Date.now(),
      resolved: false,
      ...alertData
    };
    
    this.alerts.set(alert.id, alert);
    
    // Notify handlers
    this.alertHandlers.forEach(handler => {
      try {
        handler(alert);
      } catch (error) {
        console.error('Error in alert handler:', error);
      }
    });
  }

  private updateMetrics(): void {
    const errors = this.errorCapture.getErrors();
    const metrics = this.calculateMetrics(errors, {
      start: Date.now() - (60 * 60 * 1000), // Last hour
      end: Date.now()
    });
    
    this.metricsHistory.push({
      timestamp: Date.now(),
      metrics
    });
    
    // Keep history size manageable
    if (this.metricsHistory.length > 100) {
      this.metricsHistory.shift();
    }
  }

  private calculateMetrics(errors: BaseError[], timeRange: { start: number; end: number }): ErrorMetrics {
    const recentErrors = errors.filter(e => 
      e.metadata.timestamp >= timeRange.start && e.metadata.timestamp <= timeRange.end
    );
    
    const errorsByCategory = {} as Record<ErrorCategory, number>;
    const errorsBySeverity = {} as Record<ErrorSeverity, number>;
    const errorsByLevel = {} as Record<ErrorLevel, number>;
    
    // Initialize counters
    Object.values(ErrorCategory).forEach(cat => errorsByCategory[cat] = 0);
    Object.values(ErrorSeverity).forEach(sev => errorsBySeverity[sev] = 0);
    Object.values(ErrorLevel).forEach(lvl => errorsByLevel[lvl] = 0);
    
    // Count errors
    recentErrors.forEach(error => {
      errorsByCategory[error.metadata.category]++;
      errorsBySeverity[error.metadata.severity]++;
      errorsByLevel[error.metadata.level]++;
    });
    
    const durationMinutes = (timeRange.end - timeRange.start) / (60 * 1000);
    const errorRate = recentErrors.length / durationMinutes;
    const criticalErrorRate = (errorsBySeverity[ErrorSeverity.CRITICAL] || 0) / durationMinutes;
    
    // Calculate recovery success rate
    const recoverableErrors = recentErrors.filter(e => 
      e.recoverySuggestions.some(s => s.type === 'immediate' || s.type === 'automated')
    );
    const recoverySuccessRate = recoverableErrors.length > 0 ? 0.7 : 0; // Simplified calculation
    
    return {
      totalErrors: recentErrors.length,
      errorsByCategory,
      errorsBySeverity,
      errorsByLevel,
      errorRate,
      criticalErrorRate,
      recoverySuccessRate,
      averageResolutionTime: 5000 // Placeholder
    };
  }

  private calculateTrends(currentMetrics: ErrorMetrics): ErrorReport['trends'] {
    if (this.metricsHistory.length < 2) {
      return {
        errorRate: 'stable',
        severity: 'stable',
        categories: {} as Record<ErrorCategory, 'increasing' | 'decreasing' | 'stable'>
      };
    }
    
    const previousMetrics = this.metricsHistory[this.metricsHistory.length - 2].metrics;
    
    const errorRateTrend = currentMetrics.errorRate > previousMetrics.errorRate * 1.2 ? 'increasing' :
                          currentMetrics.errorRate < previousMetrics.errorRate * 0.8 ? 'decreasing' : 'stable';
    
    const severityTrend = currentMetrics.criticalErrorRate > previousMetrics.criticalErrorRate * 1.2 ? 'degrading' :
                         currentMetrics.criticalErrorRate < previousMetrics.criticalErrorRate * 0.8 ? 'improving' : 'stable';
    
    const categoryTrends = {} as Record<ErrorCategory, 'increasing' | 'decreasing' | 'stable'>;
    Object.values(ErrorCategory).forEach(category => {
      const current = currentMetrics.errorsByCategory[category];
      const previous = previousMetrics.errorsByCategory[category];
      categoryTrends[category] = current > previous * 1.2 ? 'increasing' :
                                 current < previous * 0.8 ? 'decreasing' : 'stable';
    });
    
    return {
      errorRate: errorRateTrend,
      severity: severityTrend,
      categories: categoryTrends
    };
  }

  private generateRecommendations(summary: ErrorSummary, patterns: ErrorPattern[], metrics: ErrorMetrics): string[] {
    const recommendations: string[] = [];
    
    // High error rate
    if (metrics.errorRate > 2.0) {
      recommendations.push('Error rate is high. Consider investigating system health and resource availability.');
    }
    
    // Critical errors
    if (metrics.criticalErrorRate > 0.1) {
      recommendations.push('Critical errors detected. Immediate attention required.');
    }
    
    // Pattern-based recommendations
    patterns.forEach(pattern => {
      if (pattern.confidence > 0.7) {
        recommendations.push(pattern.suggestedAction);
      }
    });
    
    // Recovery rate
    if (metrics.recoverySuccessRate < 0.5) {
      recommendations.push('Low recovery success rate. Review error handling and recovery strategies.');
    }
    
    return recommendations;
  }
}
