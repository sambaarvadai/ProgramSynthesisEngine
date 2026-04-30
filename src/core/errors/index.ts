// Error Handling System - Main Exports

// Core taxonomy and types
export {
  ErrorCategory,
  ErrorSeverity,
  ErrorLevel,
  BaseError,
  ERROR_CODES
} from './error-taxonomy.js';

// Specific error classes
export {
  ValidationError,
  SchemaMismatchError,
  ExecutionError,
  QueryExecutionError,
  SystemError,
  DatabaseConnectionError,
  DataError,
  DataNotFoundError,
  ResourceError,
  BudgetExceededError
} from './specific-errors.js';

// Import all types for use in ErrorUtils
import {
  ErrorCategory,
  ErrorSeverity,
  ErrorLevel,
  ErrorMetadata,
  RecoverySuggestion,
  BaseError,
  ERROR_CODES
} from './error-taxonomy.js';

import {
  ValidationError,
  ExecutionError,
  BudgetExceededError
} from './specific-errors.js';

// Utility functions for common error handling scenarios
export class ErrorUtils {
  /**
   * Create a validation error with common recovery suggestions
   */
  static createValidationError(message: string, context?: any, validationErrors?: string[]): ValidationError {
    return new ValidationError(message, ERROR_CODES.VALIDATION_FAILED, context, validationErrors);
  }

  /**
   * Create an execution error with retry suggestions
   */
  static createExecutionError(message: string, nodeId: string, context?: any): ExecutionError {
    return new ExecutionError(message, ERROR_CODES.NODE_EXECUTION_FAILED, nodeId, context);
  }

  /**
   * Create a resource error with budget information
   */
  static createBudgetExceededError(budgetType: string, currentUsage: number, limit: number, context?: any): BudgetExceededError {
    return new BudgetExceededError(budgetType, currentUsage, limit, context);
  }

  /**
   * Wrap a regular Error into a BaseError with minimal metadata
   */
  static wrapError(error: Error, context?: any): BaseError {
    if (error instanceof BaseError) {
      return error;
    }

    return new BaseError(
      error.message,
      'WRAPPED_ERROR',
      ErrorCategory.SYSTEM,
      ErrorSeverity.ERROR,
      ErrorLevel.NODE,
      context
    );
  }

  /**
   * Determine if an error is recoverable
   */
  static isRecoverable(error: BaseError): boolean {
    return error.recoverySuggestions.some((s: RecoverySuggestion) => s.type === 'immediate' || s.type === 'automated');
  }

  /**
   * Get the best recovery suggestion for an error
   */
  static getBestRecoverySuggestion(error: BaseError): RecoverySuggestion | null {
    const suggestions = error.recoverySuggestions
      .filter((s: RecoverySuggestion) => s.estimatedSuccess && s.estimatedSuccess > 0.5)
      .sort((a: RecoverySuggestion, b: RecoverySuggestion) => (b.estimatedSuccess || 0) - (a.estimatedSuccess || 0));
    
    return suggestions.length > 0 ? suggestions[0] : null;
  }

  /**
   * Check if an error should be escalated
   */
  static shouldEscalate(error: BaseError): boolean {
    return error.metadata.severity === ErrorSeverity.CRITICAL ||
           error.recoverySuggestions.some((s: RecoverySuggestion) => s.type === 'escalation') ||
           (error.metadata.tags?.includes('escalation_required') ?? false);
  }

  /**
   * Create a correlation context for error tracking
   */
  static createCorrelationContext(executionId: string, pipelineId?: string, nodeId?: string): any {
    return {
      executionId,
      pipelineId,
      nodeId,
      timestamp: Date.now()
    };
  }

  /**
   * Format error for logging with structured metadata
   */
  static formatForLogging(error: BaseError): string {
    return JSON.stringify({
      timestamp: error.metadata.timestamp,
      correlationId: error.metadata.correlationId,
      errorCode: error.metadata.errorCode,
      category: error.metadata.category,
      severity: error.metadata.severity,
      level: error.metadata.level,
      message: error.message,
      nodeId: error.metadata.context.nodeId,
      pipelineId: error.metadata.context.pipelineId,
      stackTrace: error.stack
    });
  }

  /**
   * Extract error metrics for monitoring
   */
  static extractMetrics(error: BaseError): Record<string, number | string> {
    return {
      error_count: 1,
      error_category: error.metadata.category,
      error_severity: error.metadata.severity,
      error_level: error.metadata.level,
      error_code: error.metadata.errorCode,
      timestamp: error.metadata.timestamp,
      node_id: error.metadata.context.nodeId || 'unknown',
      pipeline_id: error.metadata.context.pipelineId || 'unknown'
    };
  }
}
