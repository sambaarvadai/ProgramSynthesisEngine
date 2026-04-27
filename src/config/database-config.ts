/**
 * Database configuration management
 * Provides centralized database path configuration with environment variable support
 */

export interface DatabaseConfig {
  apiRegistryPath: string;
  authDbPath: string;
  crmPostgresUrl?: string;
}

/**
 * Get database configuration from environment variables or defaults
 */
export function getDatabaseConfig(): DatabaseConfig {
  const dataDir = process.env.DATA_DIR || './data';
  
  return {
    apiRegistryPath: process.env.API_REGISTRY_DB || `${dataDir}/pipelines.db`,
    authDbPath: process.env.AUTH_DB || `${dataDir}/pipelines.db`,
    crmPostgresUrl: process.env.CRM_POSTGRES_URL || 'postgresql://pee_user:pee_password@localhost:5432/pee_dev'
  };
}

/**
 * Default database paths (used when environment variables are not set)
 */
export const DEFAULT_DB_CONFIG: DatabaseConfig = {
  apiRegistryPath: './data/pipelines.db',
  authDbPath: './data/pipelines.db'
};
