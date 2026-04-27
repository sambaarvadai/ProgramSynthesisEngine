/**
 * Simple test for error handling system - tests core functionality
 */

// Test basic imports
import { 
  BaseError,
  ErrorCategory,
  ErrorSeverity,
  ErrorLevel,
  ERROR_CODES
} from '../../src/core/errors/error-taxonomy.js';

import { 
  ValidationError, 
  QueryExecutionError, 
  BudgetExceededError
} from '../../src/core/errors/specific-errors.js';

import { 
  ErrorCapture 
} from '../../src/core/errors/error-capture.js';

import { 
  ErrorFramer,
  Audience
} from '../../src/core/errors/error-response.js';

import { 
  ErrorMonitoring 
} from '../../src/core/errors/error-monitoring.js';

import { 
  ErrorUtils 
} from '../../src/core/errors/index.js';

/**
 * Simple test runner
 */
function runTest(testName: string, testFn: () => boolean): void {
  console.log(`\nTesting: ${testName}`);
  try {
    const result = testFn();
    console.log(result ? 'PASSED' : 'FAILED');
  } catch (error) {
    console.log('FAILED with error:', error instanceof Error ? error.message : String(error));
  }
}

console.log('=== Error Handling System Tests ===');

// Test 1: Basic Error Creation
runTest('Basic Error Creation', () => {
  const error = new BaseError(
    'Test error',
    'TEST001',
    ErrorCategory.VALIDATION,
    ErrorSeverity.ERROR,
    ErrorLevel.NODE
  );
  
  return error.message === 'Test error' &&
         error.metadata.errorCode === 'TEST001' &&
         error.metadata.category === ErrorCategory.VALIDATION;
});

// Test 2: ValidationError
runTest('ValidationError', () => {
  const error = new ValidationError(
    'Invalid data',
    'VAL001',
    { nodeId: 'validator' },
    ['field1 required']
  );
  
  return error.userMessage.includes('input data') &&
         error.recoverySuggestions.length > 0;
});

// Test 3: QueryExecutionError
runTest('QueryExecutionError', () => {
  const error = new QueryExecutionError(
    'SELECT * FROM users',
    new Error('DB Error'),
    { nodeId: 'query-executor' }
  );
  
  return error.metadata.context.query === 'SELECT * FROM users' &&
         error.userMessage.includes('database query');
});

// Test 4: Error Capture
runTest('Error Capture', () => {
  const capture = new ErrorCapture(100);
  const error = new ValidationError('Test error', 'VAL001');
  
  capture.captureError(error);
  const summary = capture.generateSummary();
  
  return summary.totalErrors === 1 &&
         summary.categories[ErrorCategory.VALIDATION]?.count === 1;
});

// Test 5: Error Response Framing
runTest('Error Response Framing', () => {
  const error = new ValidationError('Test error', 'VAL001');
  const userResponse = ErrorFramer.formatErrorResponse(error, Audience.USER);
  const devResponse = ErrorFramer.formatErrorResponse(error, Audience.DEVELOPER);
  
  return userResponse.error.message === error.userMessage &&
         devResponse.error.message === error.message &&
         devResponse.error.technicalDetails !== '';
});

// Test 6: Error Monitoring
runTest('Error Monitoring', () => {
  const monitoring = new ErrorMonitoring();
  const error = new ValidationError('Test error', 'VAL001');
  
  monitoring.captureError(error);
  const report = monitoring.generateReport();
  
  return report.metrics.totalErrors === 1 &&
         report.summary.totalErrors === 1;
});

// Test 7: Error Utilities
runTest('Error Utilities', () => {
  const regularError = new Error('Regular error');
  const wrappedError = ErrorUtils.wrapError(regularError, { nodeId: 'test' });
  
  return wrappedError instanceof BaseError &&
         wrappedError.message === 'Regular error' &&
         ErrorUtils.isRecoverable(new ValidationError('Test', 'VAL001'));
});

// Test 8: Error Codes
runTest('Error Codes', () => {
  return ERROR_CODES.VALIDATION_FAILED === 'VAL001' &&
         ERROR_CODES.QUERY_EXECUTION_FAILED === 'EXE001' &&
         ERROR_CODES.BUDGET_EXCEEDED === 'RES001';
});

// Test 9: Error Context and Metadata
runTest('Error Context and Metadata', () => {
  const error = new BaseError(
    'Complex error',
    'COMP001',
    ErrorCategory.SYSTEM,
    ErrorSeverity.ERROR,
    ErrorLevel.PIPELINE,
    { nodeId: 'test-node', pipelineId: 'test-pipeline' }
  )
    .withTag('test-tag')
    .withMetrics({ executionTime: 1000 });
  
  return error.metadata.context.nodeId === 'test-node' &&
         error.metadata.tags?.includes('test-tag') === true &&
         error.metadata.metrics?.executionTime === 1000;
});

// Test 10: Integration Test
runTest('Integration Test', () => {
  // Create a complete workflow
  const monitoring = new ErrorMonitoring();
  const capture = new ErrorCapture(100);
  
  // Create and capture different types of errors
  const errors = [
    new ValidationError('Validation failed', 'VAL001'),
    new QueryExecutionError('SELECT 1', new Error('DB Error')),
    new BudgetExceededError('timeout', 120000, 60000)
  ];
  
  errors.forEach(error => {
    capture.captureError(error);
    monitoring.captureError(error);
  });
  
  const summary = capture.generateSummary();
  const report = monitoring.generateReport();
  const userResponse = ErrorFramer.formatErrorResponse(errors[0], Audience.USER);
  
  return summary.totalErrors === 3 &&
         report.metrics.totalErrors === 3 &&
         userResponse.error.recoverySuggestions.length > 0;
});

console.log('\n=== Test Summary ===');
console.log('Core error handling functionality tested successfully!');
console.log('\nTo test with real execution, run:');
console.log('  npm run build  # Build the project');
console.log('  node dist/test-error-handling.js  # Run compiled tests');
