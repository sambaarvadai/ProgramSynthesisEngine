// Specific Error Classes with Recovery Suggestions

import { BaseError, ErrorCategory, ErrorSeverity, ErrorLevel, RecoverySuggestion, ERROR_CODES } from './error-taxonomy.js';

// Validation Errors
export class ValidationError extends BaseError {
  constructor(
    message: string,
    errorCode: string = ERROR_CODES.VALIDATION_FAILED,
    context: any = {},
    validationErrors?: string[]
  ) {
    const recoverySuggestions: RecoverySuggestion[] = [
      {
        type: 'manual',
        action: 'review_input_data',
        description: 'Review and correct the input data according to the validation requirements',
        priority: 'high',
        estimatedSuccess: 0.9
      },
      {
        type: 'automated',
        action: 'auto_correct_data',
        description: 'Attempt to automatically correct common data format issues',
        priority: 'medium',
        estimatedSuccess: 0.6,
        automated: true,
        parameters: { validationErrors }
      }
    ];

    super(message, errorCode, ErrorCategory.VALIDATION, ErrorSeverity.ERROR, ErrorLevel.NODE, context, recoverySuggestions);
    
    if (validationErrors) {
      this.withTag('validation_errors');
      this.metadata.context.validationErrors = validationErrors;
    }
  }

  protected generateUserMessage(): string {
    return `The input data doesn't match the required format. Please check your data and try again.`;
  }
}

export class SchemaMismatchError extends ValidationError {
  constructor(
    expectedSchema: string,
    actualSchema: string,
    context: any = {}
  ) {
    const message = `Schema mismatch: expected ${expectedSchema}, got ${actualSchema}`;
    super(message, ERROR_CODES.SCHEMA_MISMATCH, context, [message]);
    
    this.metadata.context.expectedSchema = expectedSchema;
    this.metadata.context.actualSchema = actualSchema;
  }

  protected generateUserMessage(): string {
    return `The data structure doesn't match what was expected. Please ensure your data has the correct format.`;
  }
}

// Execution Errors
export class ExecutionError extends BaseError {
  constructor(
    message: string,
    errorCode: string = ERROR_CODES.NODE_EXECUTION_FAILED,
    nodeId: string,
    context: any = {},
    severity: ErrorSeverity = ErrorSeverity.ERROR
  ) {
    const recoverySuggestions: RecoverySuggestion[] = [
      {
        type: 'immediate',
        action: 'retry_execution',
        description: 'Retry the operation with the same parameters',
        priority: 'high',
        estimatedSuccess: 0.7,
        automated: true
      },
      {
        type: 'manual',
        action: 'check_input_data',
        description: 'Verify the input data is correct and complete',
        priority: 'medium',
        estimatedSuccess: 0.8
      }
    ];

    super(message, errorCode, ErrorCategory.EXECUTION, severity, ErrorLevel.NODE, 
          { ...context, nodeId }, recoverySuggestions);
  }

  protected generateUserMessage(): string {
    return `There was a problem executing this operation. Please try again or check your input data.`;
  }
}

export class QueryExecutionError extends ExecutionError {
  constructor(
    query: string,
    originalError: Error,
    context: any = {}
  ) {
    super(`Query execution failed: ${originalError.message}`, ERROR_CODES.QUERY_EXECUTION_FAILED, 
          context.nodeId || 'unknown', context);
    
    this.metadata.context.query = query;
    this.metadata.context.originalError = originalError.message;
    this.withTag('database_error');
    
    // Add query-specific recovery suggestions
    this.recoverySuggestions.push({
      type: 'manual',
      action: 'optimize_query',
      description: 'The query may be too complex. Consider simplifying it or adding indexes.',
      priority: 'medium',
      estimatedSuccess: 0.6
    });
  }

  protected generateUserMessage(): string {
    return `The database query couldn't be executed. This might be due to invalid data or a complex query.`;
  }
}

// System Errors
export class SystemError extends BaseError {
  constructor(
    message: string,
    errorCode: string,
    context: any = {},
    severity: ErrorSeverity = ErrorSeverity.CRITICAL
  ) {
    const recoverySuggestions: RecoverySuggestion[] = [
      {
        type: 'escalation',
        action: 'contact_support',
        description: 'This appears to be a system issue. Please contact technical support.',
        priority: 'high',
        estimatedSuccess: 0.8
      },
      {
        type: 'manual',
        action: 'check_system_status',
        description: 'Check system status and resource availability',
        priority: 'medium',
        estimatedSuccess: 0.5
      }
    ];

    super(message, errorCode, ErrorCategory.SYSTEM, severity, ErrorLevel.SYSTEM, context, recoverySuggestions);
  }

  protected generateUserMessage(): string {
    return `A system error occurred. Our technical team has been notified. Please try again later.`;
  }
}

export class DatabaseConnectionError extends SystemError {
  constructor(
    connectionDetails: string,
    originalError: Error,
    context: any = {}
  ) {
    super(`Database connection failed: ${originalError.message}`, ERROR_CODES.DATABASE_CONNECTION_FAILED, 
          context, ErrorSeverity.CRITICAL);
    
    this.metadata.context.connectionDetails = connectionDetails;
    this.metadata.context.originalError = originalError.message;
    this.withTag('database_connection');
  }

  protected generateUserMessage(): string {
    return `Unable to connect to the database. Please check your connection and try again.`;
  }
}

// Data Errors
export class DataError extends BaseError {
  constructor(
    message: string,
    errorCode: string,
    context: any = {},
    severity: ErrorSeverity = ErrorSeverity.ERROR
  ) {
    const recoverySuggestions: RecoverySuggestion[] = [
      {
        type: 'manual',
        action: 'verify_data_integrity',
        description: 'Check the data for consistency and completeness',
        priority: 'high',
        estimatedSuccess: 0.8
      }
    ];

    super(message, errorCode, ErrorCategory.DATA, severity, ErrorLevel.NODE, context, recoverySuggestions);
  }

  protected generateUserMessage(): string {
    return `There's an issue with the data. Please verify the information and try again.`;
  }
}

export class DataNotFoundError extends DataError {
  constructor(
    dataType: string,
    identifier: string,
    context: any = {}
  ) {
    super(`Data not found: ${dataType} with identifier ${identifier}`, ERROR_CODES.DATA_NOT_FOUND, context);
    
    this.metadata.context.dataType = dataType;
    this.metadata.context.identifier = identifier;
    this.withTag('not_found');
  }

  protected generateUserMessage(): string {
    return `The requested data couldn't be found. Please check the identifier and try again.`;
  }
}

// Resource Errors
export class ResourceError extends BaseError {
  constructor(
    message: string,
    errorCode: string,
    resourceType: string,
    context: any = {}
  ) {
    const recoverySuggestions: RecoverySuggestion[] = [
      {
        type: 'manual',
        action: 'reduce_resource_usage',
        description: 'Try reducing the amount of data or complexity of the operation',
        priority: 'high',
        estimatedSuccess: 0.7
      },
      {
        type: 'automated',
        action: 'optimize_resource_usage',
        description: 'Automatically optimize resource usage',
        priority: 'medium',
        estimatedSuccess: 0.5,
        automated: true
      }
    ];

    super(message, errorCode, ErrorCategory.RESOURCE, ErrorSeverity.ERROR, ErrorLevel.PIPELINE, context, recoverySuggestions);
    
    this.metadata.context.resourceType = resourceType;
    this.withTag('resource_limit');
  }

  protected generateUserMessage(): string {
    return `The operation exceeded available resources. Try reducing the scope or complexity.`;
  }
}

export class BudgetExceededError extends ResourceError {
  constructor(
    budgetType: string,
    currentUsage: number,
    limit: number,
    context: any = {}
  ) {
    super(`Budget exceeded: ${budgetType} usage ${currentUsage} exceeds limit ${limit}`, 
          ERROR_CODES.BUDGET_EXCEEDED, budgetType, context);
    
    this.metadata.context.currentUsage = currentUsage;
    this.metadata.context.limit = limit;
    this.withMetrics({ [budgetType]: currentUsage });
  }

  protected generateUserMessage(): string {
    return `The operation exceeded the allowed budget. Please reduce the scope or increase the budget limits.`;
  }
}
