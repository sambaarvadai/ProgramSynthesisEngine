/**
 * Application configuration management
 * Centralizes all hardcoded values with environment variable support
 */

export interface AppConfig {
  // Database configuration
  database: {
    apiRegistryPath: string;
    authDbPath: string;
    crmDbPath: string;
  };
  
  // External service URLs
  services: {
    calciteUrl: string;
    calciteHealthTimeout: number;
    calciteRequestTimeout: number;
  };
  
  // LLM configuration
  llm: {
    maxTokens: {
      tablePreSelector: number;
      queryIntentGenerator: number;
      pipelineIntentGenerator: number;
      pipelineCompiler: number;
      sessionManager: number;
    };
  };
  
  // Execution limits and budgets
  execution: {
    maxRowsPerNode: number;
    maxMemoryMB: number;
    timeoutMs: number;
    maxLLMCalls: number;
    maxIterations: number;
    maxBatchSize: number;
    minTimeoutMs: number;
  };
  
  // HTTP configuration
  http: {
    defaultMaxRetries: number;
    defaultBackoffMs: number;
    maxBatchSize: number;
  };
  
  // Database connection pools
  databasePools: {
    postgres: {
      maxConnections: number;
      idleTimeoutMs: number;
    },
  };
  
  // API selection limits
  apiSelection: {
    maxTables: number;
    maxEndpoints: number;
  };
  
  // Schema processing
  schema: {
    maxSummaryLength: number;
    memoryLimitRows: number;
  };
  
  // Authentication
  auth: {
    sessionTimeoutMs: number;
    maxLoginAttempts: number;
    passwordMinLength: number;
  };
}
  


/**
 * Get application configuration from environment variables or defaults
 */
export function getAppConfig(): AppConfig {
  const config = {
    database: {
      apiRegistryPath: process.env.API_REGISTRY_DB || './data/pipelines.db',
      authDbPath: process.env.AUTH_DB || './data/pipelines.db',
      crmDbPath: process.env.CRM_DB || 'postgresql://postgres:pee_user@pee_postgres:5432/crm_full',
    },
    
    services: {
      calciteUrl: process.env.CALCITE_URL || 'http://localhost:8765',
      calciteHealthTimeout: parseInt(process.env.CALCITE_HEALTH_TIMEOUT || '1000'),
      calciteRequestTimeout: parseInt(process.env.CALCITE_REQUEST_TIMEOUT || '5000'),
    },
    
    llm: {
      maxTokens: {
        tablePreSelector: parseInt(process.env.LLM_MAX_TOKENS_TABLE_PRE_SELECTOR || '4096'),
        queryIntentGenerator: parseInt(process.env.LLM_MAX_TOKENS_QUERY_INTENT || '4096'),
        pipelineIntentGenerator: parseInt(process.env.LLM_MAX_TOKENS_PIPELINE_INTENT || '1000'),
        pipelineCompiler: parseInt(process.env.LLM_MAX_TOKENS_PIPELINE_COMPILER || '1000'),
        sessionManager: parseInt(process.env.LLM_MAX_TOKENS_SESSION_MANAGER || '1000'),
      },
    },
    
    execution: {
      maxRowsPerNode: parseInt(process.env.MAX_ROWS_PER_NODE || '10000'),
      maxMemoryMB: parseInt(process.env.MAX_MEMORY_MB || '512'),
      timeoutMs: parseInt(process.env.TIMEOUT_MS || '300000'),
      maxLLMCalls: parseInt(process.env.MAX_LLM_CALLS || '20'),
      maxIterations: parseInt(process.env.MAX_ITERATIONS || '1000'),
      maxBatchSize: parseInt(process.env.MAX_BATCH_SIZE || '100'),
      minTimeoutMs: parseInt(process.env.MIN_TIMEOUT_MS || '30000'),
    },
    
    http: {
      defaultMaxRetries: parseInt(process.env.HTTP_MAX_RETRIES || '2'),
      defaultBackoffMs: parseInt(process.env.HTTP_BACKOFF_MS || '1000'),
      maxBatchSize: parseInt(process.env.HTTP_MAX_BATCH_SIZE || '1000'),
    },
    
    databasePools: {
      postgres: {
        maxConnections: parseInt(process.env.POSTGRES_MAX_CONNECTIONS || '10'),
        idleTimeoutMs: parseInt(process.env.POSTGRES_IDLE_TIMEOUT_MS || '30000'),
      },
    },
    
    apiSelection: {
      maxTables: parseInt(process.env.MAX_TABLES || '5'),
      maxEndpoints: parseInt(process.env.MAX_ENDPOINTS || '5'),
    },
    
    schema: {
      maxSummaryLength: parseInt(process.env.MAX_SCHEMA_SUMMARY_LENGTH || '8000'),
      memoryLimitRows: parseInt(process.env.MEMORY_LIMIT_ROWS || '10000'),
    },
    
    auth: {
      sessionTimeoutMs: parseInt(process.env.AUTH_SESSION_TIMEOUT_MS || '1800000'),
      maxLoginAttempts: parseInt(process.env.AUTH_MAX_LOGIN_ATTEMPTS || '3'),
      passwordMinLength: parseInt(process.env.AUTH_PASSWORD_MIN_LENGTH || '8'),
    },
  };
  
  return config;
}


/**
 * Default configuration values (used when environment variables are not set)
 */
export const DEFAULT_APP_CONFIG: AppConfig = getAppConfig();
