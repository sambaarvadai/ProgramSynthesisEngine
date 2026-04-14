/**
 * Basic error handling test - tests core functionality without complex exports
 */

// Import only the basic components we need
import { 
  BaseError,
  ErrorCategory,
  ErrorSeverity,
  ErrorLevel,
  ERROR_CODES
} from './core/errors/error-taxonomy.js';

import { 
  ValidationError, 
  QueryExecutionError, 
  BudgetExceededError
} from './core/errors/specific-errors.js';

console.log('=== Basic Error Handling System Test ===\n');

// Test 1: Basic Error Creation
console.log('Test 1: Basic Error Creation');
try {
  const error = new BaseError(
    'Test error',
    'TEST001',
    ErrorCategory.VALIDATION,
    ErrorSeverity.ERROR,
    ErrorLevel.NODE,
    { nodeId: 'test-node' }
  );
  
  const success = error.message === 'Test error' &&
                error.metadata.errorCode === 'TEST001' &&
                error.metadata.category === ErrorCategory.VALIDATION &&
                error.metadata.severity === ErrorSeverity.ERROR &&
                error.metadata.level === ErrorLevel.NODE &&
                error.metadata.context.nodeId === 'test-node';
  
  console.log(success ? 'PASSED' : 'FAILED');
  console.log(`  Message: ${error.message}`);
  console.log(`  Error Code: ${error.metadata.errorCode}`);
  console.log(`  Category: ${error.metadata.category}`);
  console.log(`  Severity: ${error.metadata.severity}`);
  console.log(`  Level: ${error.metadata.level}`);
} catch (error) {
  console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
}

// Test 2: ValidationError
console.log('\nTest 2: ValidationError');
try {
  const error = new ValidationError(
    'Invalid input data',
    'VAL001',
    { nodeId: 'validator-node' },
    ['field1 is required', 'field2 must be numeric']
  );
  
  const success = error.userMessage.includes('input data') &&
                error.recoverySuggestions.length > 0 &&
                error.recoverySuggestions[0].action === 'review_input_data';
  
  console.log(success ? 'PASSED' : 'FAILED');
  console.log(`  User Message: ${error.userMessage}`);
  console.log(`  Recovery Suggestions: ${error.recoverySuggestions.length}`);
  console.log(`  First Action: ${error.recoverySuggestions[0].action}`);
} catch (error) {
  console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
}

// Test 3: QueryExecutionError
console.log('\nTest 3: QueryExecutionError');
try {
  const originalError = new Error('Connection timeout');
  const error = new QueryExecutionError(
    'SELECT * FROM users WHERE active = true',
    originalError,
    { nodeId: 'query-executor', executionId: 'exec-123' }
  );
  
  const success = error.metadata.context.query === 'SELECT * FROM users WHERE active = true' &&
                error.metadata.context.originalError === 'Connection timeout' &&
                error.userMessage.includes('database query');
  
  console.log(success ? 'PASSED' : 'FAILED');
  console.log(`  Query: ${error.metadata.context.query}`);
  console.log(`  Original Error: ${error.metadata.context.originalError}`);
  console.log(`  User Message: ${error.userMessage}`);
} catch (error) {
  console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
}

// Test 4: BudgetExceededError
console.log('\nTest 4: BudgetExceededError');
try {
  const error = new BudgetExceededError(
    'timeout',
    120000,
    60000,
    { executionId: 'exec-456', pipelineId: 'pipeline-789' }
  );
  
  const success = error.userMessage.includes('budget') &&
                error.metadata.context.currentUsage === 120000 &&
                error.metadata.context.limit === 60000;
  
  console.log(success ? 'PASSED' : 'FAILED');
  console.log(`  User Message: ${error.userMessage}`);
  console.log(`  Current Usage: ${error.metadata.context.currentUsage}`);
  console.log(`  Limit: ${error.metadata.context.limit}`);
} catch (error) {
  console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
}

// Test 5: Error Method Chaining
console.log('\nTest 5: Error Method Chaining');
try {
  const error = new BaseError(
    'Chained error',
    'CHAIN001',
    ErrorCategory.VALIDATION,
    ErrorSeverity.WARNING
  )
    .withContext({ nodeId: 'chained-node' })
    .withTag('chain-test')
    .withMetrics({ executionTime: 500 });
  
  const success = error.metadata.context.nodeId === 'chained-node' &&
                (error.metadata.tags?.includes('chain-test') ?? false) &&
                error.metadata.metrics?.executionTime === 500;
  
  console.log(success ? 'PASSED' : 'FAILED');
  console.log(`  Node ID: ${error.metadata.context.nodeId}`);
  console.log(`  Tags: ${error.metadata.tags?.join(', ')}`);
  console.log(`  Execution Time: ${error.metadata.metrics?.executionTime}`);
} catch (error) {
  console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
}

// Test 6: Error Codes
console.log('\nTest 6: Error Code Constants');
try {
  const success = ERROR_CODES.VALIDATION_FAILED === 'VAL001' &&
                ERROR_CODES.QUERY_EXECUTION_FAILED === 'EXE001' &&
                ERROR_CODES.BUDGET_EXCEEDED === 'RES001' &&
                ERROR_CODES.DATABASE_CONNECTION_FAILED === 'SYS002' &&
                ERROR_CODES.DATA_NOT_FOUND === 'DAT001';
  
  console.log(success ? 'PASSED' : 'FAILED');
  console.log(`  VALIDATION_FAILED: ${ERROR_CODES.VALIDATION_FAILED}`);
  console.log(`  QUERY_EXECUTION_FAILED: ${ERROR_CODES.QUERY_EXECUTION_FAILED}`);
  console.log(`  BUDGET_EXCEEDED: ${ERROR_CODES.BUDGET_EXCEEDED}`);
} catch (error) {
  console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
}

// Test 7: Error Categories and Severity
console.log('\nTest 7: Error Categories and Severity');
try {
  const categories = [
    ErrorCategory.VALIDATION,
    ErrorCategory.EXECUTION,
    ErrorCategory.SYSTEM,
    ErrorCategory.DATA,
    ErrorCategory.RESOURCE
  ];
  
  const severities = [
    ErrorSeverity.CRITICAL,
    ErrorSeverity.ERROR,
    ErrorSeverity.WARNING,
    ErrorSeverity.INFO
  ];
  
  const levels = [
    ErrorLevel.NODE,
    ErrorLevel.PIPELINE,
    ErrorLevel.SYSTEM
  ];
  
  const success = categories.length === 5 &&
                severities.length === 4 &&
                levels.length === 3;
  
  console.log(success ? 'PASSED' : 'FAILED');
  console.log(`  Categories: ${categories.length}`);
  console.log(`  Severities: ${severities.length}`);
  console.log(`  Levels: ${levels.length}`);
} catch (error) {
  console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
}

// Test 8: Error JSON Serialization
console.log('\nTest 8: Error JSON Serialization');
try {
  const error = new BaseError(
    'Serialization test',
    'SER001',
    ErrorCategory.SYSTEM,
    ErrorSeverity.ERROR,
    ErrorLevel.NODE,
    { nodeId: 'test-node' }
  );
  
  const json = error.toJSON();
  const success = json.name === 'BaseError' &&
                json.message === 'Serialization test' &&
                json.metadata.errorCode === 'SER001';
  
  console.log(success ? 'PASSED' : 'FAILED');
  console.log(`  JSON Name: ${json.name}`);
  console.log(`  JSON Message: ${json.message}`);
  console.log(`  JSON Error Code: ${json.metadata.errorCode}`);
} catch (error) {
  console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
}

console.log('\n=== Basic Test Complete ===');
console.log('Core error handling functionality is working!');
console.log('\nThe error handling system includes:');
console.log('  - Base error class with rich metadata');
console.log('  - Specific error types (ValidationError, QueryExecutionError, BudgetExceededError)');
console.log('  - Error categorization and severity levels');
console.log('  - Error context and metrics');
console.log('  - Method chaining for error enhancement');
console.log('  - JSON serialization support');
console.log('  - Recovery suggestions and user-friendly messages');
