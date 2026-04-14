#!/usr/bin/env node

/**
 * Practical test runner for the error handling system
 * This file can be executed directly to test the error handling implementation
 */

import { demonstrateErrorHandling, demonstratePipelineErrorIntegration } from './src/core/errors/demo.js';

console.log('Starting Error Handling System Tests...\n');

// Test 1: Basic Error Handling
console.log('='.repeat(60));
console.log('TEST 1: Basic Error Handling Functionality');
console.log('='.repeat(60));

try {
  const result1 = demonstrateErrorHandling();
  console.log('Test 1 PASSED: Basic error handling works correctly');
  console.log(`  - Captured ${result1.summary.totalErrors} errors`);
  console.log(`  - Generated ${result1.patterns.length} patterns`);
  console.log(`  - Created ${result1.alerts.length} alerts`);
} catch (error) {
  console.log('Test 1 FAILED:', error.message);
  console.log('Stack:', error.stack);
}

// Test 2: Pipeline Integration
console.log('\n' + '='.repeat(60));
console.log('TEST 2: Pipeline Error Integration');
console.log('='.repeat(60));

try {
  const result2 = demonstratePipelineErrorIntegration();
  console.log('Test 2 PASSED: Pipeline integration works correctly');
  console.log(`  - Total errors: ${result2.metrics.totalErrors}`);
  console.log(`  - Error rate: ${result2.metrics.errorRate.toFixed(2)}/min`);
} catch (error) {
  console.log('Test 2 FAILED:', error.message);
  console.log('Stack:', error.stack);
}

// Test 3: Error Classification
console.log('\n' + '='.repeat(60));
console.log('TEST 3: Error Classification and Categorization');
console.log('='.repeat(60));

try {
  // Import the error handling classes
  const {
    ValidationError,
    QueryExecutionError,
    BudgetExceededError,
    SystemError,
    ErrorCategory,
    ErrorSeverity,
    ErrorLevel
  } = await import('./src/core/errors/index.js');

  // Create different error types
  const validationError = new ValidationError('Test validation error', 'VAL001');
  const queryError = new QueryExecutionError('SELECT 1', new Error('DB Error'));
  const budgetError = new BudgetExceededError('timeout', 120000, 60000);
  const systemError = new SystemError('System failure', 'SYS001', ErrorSeverity.CRITICAL);

  // Test classification
  const classifications = [
    { error: validationError, expectedCategory: ErrorCategory.VALIDATION },
    { error: queryError, expectedCategory: ErrorCategory.EXECUTION },
    { error: budgetError, expectedCategory: ErrorCategory.RESOURCE },
    { error: systemError, expectedCategory: ErrorCategory.SYSTEM }
  ];

  let passed = 0;
  classifications.forEach(({ error, expectedCategory }) => {
    if (error.metadata.category === expectedCategory) {
      passed++;
    } else {
      console.log(`  FAILED: Expected ${expectedCategory}, got ${error.metadata.category}`);
    }
  });

  if (passed === classifications.length) {
    console.log('Test 3 PASSED: All errors classified correctly');
  } else {
    console.log(`Test 3 FAILED: ${passed}/${classifications.length} classifications correct`);
  }

} catch (error) {
  console.log('Test 3 FAILED:', error.message);
  console.log('Stack:', error.stack);
}

// Test 4: Error Response Formatting
console.log('\n' + '='.repeat(60));
console.log('TEST 4: Error Response Formatting');
console.log('='.repeat(60));

try {
  const {
    ErrorFramer,
    Audience,
    ValidationError
  } = await import('./src/core/errors/index.js');

  const error = new ValidationError('Test error', 'VAL001');
  
  // Test user formatting
  const userResponse = ErrorFramer.formatErrorResponse(error, Audience.USER);
  const userDisplay = ErrorFramer.formatForDisplay(error, Audience.USER);
  
  // Test developer formatting
  const devResponse = ErrorFramer.formatErrorResponse(error, Audience.DEVELOPER);
  const devDisplay = ErrorFramer.formatForDisplay(error, Audience.DEVELOPER);

  // Validate formatting
  const checks = [
    userResponse.error.message !== error.message, // Should have user-friendly message
    userResponse.error.recoverySuggestions.length > 0, // Should have recovery suggestions
    devResponse.error.technicalDetails !== '', // Should have technical details
    userDisplay.actions.length > 0, // Should have actions for user
    devDisplay.details?.technical !== undefined // Should have technical details for dev
  ];

  const passed = checks.filter(Boolean).length;
  if (passed === checks.length) {
    console.log('Test 4 PASSED: Error response formatting works correctly');
  } else {
    console.log(`Test 4 FAILED: ${passed}/${checks.length} formatting checks passed`);
  }

} catch (error) {
  console.log('Test 4 FAILED:', error.message);
  console.log('Stack:', error.stack);
}

// Test 5: Error Monitoring and Alerting
console.log('\n' + '='.repeat(60));
console.log('TEST 5: Error Monitoring and Alerting');
console.log('='.repeat(60));

try {
  const {
    ErrorMonitoring,
    ValidationError,
    BudgetExceededError,
    ErrorSeverity
  } = await import('./src/core/errors/index.js');

  const monitoring = new ErrorMonitoring({
    alertThresholds: {
      critical_error_rate: { value: 0.1, severity: ErrorSeverity.CRITICAL },
      total_error_rate: { value: 1.0, severity: ErrorSeverity.ERROR }
    }
  });

  // Add some errors to trigger monitoring
  monitoring.captureError(new ValidationError('Test error 1', 'VAL001'));
  monitoring.captureError(new ValidationError('Test error 2', 'VAL002'));
  
  // Add a critical error to trigger alerts
  monitoring.captureError(new BudgetExceededError('timeout', 120000, 60000));

  const report = monitoring.generateReport();
  const alerts = monitoring.getActiveAlerts();

  const checks = [
    report.metrics.totalErrors >= 3,
    report.alerts.length > 0,
    alerts.some(alert => alert.type === 'critical'),
    report.recommendations.length > 0
  ];

  const passed = checks.filter(Boolean).length;
  if (passed === checks.length) {
    console.log('Test 5 PASSED: Error monitoring and alerting works correctly');
    console.log(`  - Total errors: ${report.metrics.totalErrors}`);
    console.log(`  - Active alerts: ${alerts.length}`);
    console.log(`  - Critical alerts: ${alerts.filter(a => a.type === 'critical').length}`);
  } else {
    console.log(`Test 5 FAILED: ${passed}/${checks.length} monitoring checks passed`);
  }

} catch (error) {
  console.log('Test 5 FAILED:', error.message);
  console.log('Stack:', error.stack);
}

// Test 6: Error Utilities
console.log('\n' + '='.repeat(60));
console.log('TEST 6: Error Utilities');
console.log('='.repeat(60));

try {
  const {
    ErrorUtils,
    ValidationError,
    BaseError,
    ErrorCategory,
    ErrorSeverity
  } = await import('./src/core/errors/index.js');

  // Test error wrapping
  const regularError = new Error('Regular error');
  const wrappedError = ErrorUtils.wrapError(regularError, { nodeId: 'test-node' });

  // Test utility functions
  const utils = [
    ErrorUtils.isRecoverable(new ValidationError('Test', 'VAL001')),
    !ErrorUtils.shouldEscalate(new ValidationError('Test', 'VAL001')),
    ErrorUtils.shouldEscalate(new BaseError('Critical', 'CRIT001', ErrorCategory.SYSTEM, ErrorSeverity.CRITICAL)),
    wrappedError.metadata.context.nodeId === 'test-node'
  ];

  const passed = utils.filter(Boolean).length;
  if (passed === utils.length) {
    console.log('Test 6 PASSED: Error utilities work correctly');
  } else {
    console.log(`Test 6 FAILED: ${passed}/${utils.length} utility checks passed`);
  }

} catch (error) {
  console.log('Test 6 FAILED:', error.message);
  console.log('Stack:', error.stack);
}

// Summary
console.log('\n' + '='.repeat(60));
console.log('TEST SUMMARY');
console.log('='.repeat(60));
console.log('Error handling system tests completed.');
console.log('Run the demo file directly for more detailed testing:');
console.log('  node src/core/errors/demo.js');
console.log('\nTo test with specific components, import the classes:');
console.log('  import { ValidationError, ErrorMonitoring } from "./src/core/errors/index.js";');
