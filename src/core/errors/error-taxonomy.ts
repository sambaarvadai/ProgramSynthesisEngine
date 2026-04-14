// Enhanced Error Taxonomy and Classification System

export enum ErrorCategory {
  VALIDATION = 'validation',
  EXECUTION = 'execution', 
  SYSTEM = 'system',
  DATA = 'data',
  NETWORK = 'network',
  CONFIGURATION = 'configuration',
  RESOURCE = 'resource',
  SECURITY = 'security'
}

export enum ErrorSeverity {
  CRITICAL = 'critical',
  ERROR = 'error',
  WARNING = 'warning',
  INFO = 'info'
}

export enum ErrorLevel {
  NODE = 'node',
  PIPELINE = 'pipeline', 
  SYSTEM = 'system'
}

export interface ErrorContext {
  nodeId?: string;
  pipelineId?: string;
  stepId?: string;
  executionId?: string;
  sessionId?: string;
  component?: string;
  operation?: string;
  inputMetadata?: Record<string, any>;
  environment?: Record<string, any>;
  // Dynamic properties for specific error types
  [key: string]: any;
}

export interface ErrorMetadata {
  timestamp: number;
  category: ErrorCategory;
  severity: ErrorSeverity;
  level: ErrorLevel;
  errorCode: string;
  context: ErrorContext;
  stackTrace?: string;
  cause?: BaseError;
  affectedEntities?: string[];
  recoveryAttempts?: number;
  userId?: string;
  correlationId?: string;
  tags?: string[];
  metrics?: {
    executionTime?: number;
    memoryUsage?: number;
    rowsProcessed?: number;
    retryCount?: number;
  };
}

export interface RecoverySuggestion {
  type: 'immediate' | 'manual' | 'automated' | 'escalation';
  action: string;
  description: string;
  priority: 'high' | 'medium' | 'low';
  estimatedSuccess?: number;
  automated?: boolean;
  parameters?: Record<string, any>;
}

export class BaseError extends Error {
  public readonly metadata: ErrorMetadata;
  public readonly recoverySuggestions: RecoverySuggestion[];
  public readonly userMessage: string;
  public readonly technicalDetails: string;

  constructor(
    message: string,
    errorCode: string,
    category: ErrorCategory,
    severity: ErrorSeverity = ErrorSeverity.ERROR,
    level: ErrorLevel = ErrorLevel.NODE,
    context: ErrorContext = {},
    recoverySuggestions: RecoverySuggestion[] = []
  ) {
    super(message);
    this.name = this.constructor.name;
    
    this.metadata = {
      timestamp: Date.now(),
      category,
      severity,
      level,
      errorCode,
      context,
      correlationId: crypto.randomUUID(),
      tags: []
    };

    this.recoverySuggestions = recoverySuggestions;
    this.userMessage = this.generateUserMessage();
    this.technicalDetails = this.generateTechnicalDetails();
  }

  protected generateUserMessage(): string {
    return this.message; // Override in subclasses for user-friendly messages
  }

  protected generateTechnicalDetails(): string {
    return `${this.metadata.category}:${this.metadata.errorCode} - ${this.message}`;
  }

  public withContext(additionalContext: Partial<ErrorContext>): this {
    Object.assign(this.metadata.context, additionalContext);
    return this;
  }

  public withCause(cause: BaseError): this {
    this.metadata.cause = cause;
    return this;
  }

  public withTag(tag: string): this {
    if (!this.metadata.tags) this.metadata.tags = [];
    this.metadata.tags.push(tag);
    return this;
  }

  public withMetrics(metrics: Partial<ErrorMetadata['metrics']>): this {
    this.metadata.metrics = { ...this.metadata.metrics, ...metrics };
    return this;
  }

  public toJSON() {
    return {
      name: this.name,
      message: this.message,
      userMessage: this.userMessage,
      technicalDetails: this.technicalDetails,
      metadata: this.metadata,
      recoverySuggestions: this.recoverySuggestions,
      stack: this.stack
    };
  }
}

// Error Code Registry
export const ERROR_CODES = {
  // Validation Errors
  VALIDATION_FAILED: 'VAL001',
  SCHEMA_MISMATCH: 'VAL002', 
  MISSING_REQUIRED_FIELD: 'VAL003',
  INVALID_DATA_TYPE: 'VAL004',
  
  // Execution Errors
  QUERY_EXECUTION_FAILED: 'EXE001',
  NODE_EXECUTION_FAILED: 'EXE002',
  PIPELINE_EXECUTION_FAILED: 'EXE003',
  TIMEOUT_EXCEEDED: 'EXE004',
  
  // System Errors
  MEMORY_LIMIT_EXCEEDED: 'SYS001',
  DATABASE_CONNECTION_FAILED: 'SYS002',
  FILE_SYSTEM_ERROR: 'SYS003',
  CONFIGURATION_ERROR: 'SYS004',
  
  // Data Errors
  DATA_NOT_FOUND: 'DAT001',
  DATA_CORRUPTION: 'DAT002',
  FOREIGN_KEY_VIOLATION: 'DAT003',
  DUPLICATE_KEY: 'DAT004',
  
  // Network Errors
  CONNECTION_FAILED: 'NET001',
  TIMEOUT: 'NET002',
  RATE_LIMIT_EXCEEDED: 'NET003',
  SERVICE_UNAVAILABLE: 'NET004',
  
  // Resource Errors
  BUDGET_EXCEEDED: 'RES001',
  QUOTA_EXCEEDED: 'RES002',
  RESOURCE_NOT_AVAILABLE: 'RES003'
} as const;

export type ErrorCode = typeof ERROR_CODES[keyof typeof ERROR_CODES];
