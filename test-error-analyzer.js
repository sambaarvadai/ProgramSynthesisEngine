#!/usr/bin/env node

/**
 * Test script for the error analyzer
 */

import { ErrorAnalyzer } from './src/core/llm/error-analyzer.js';

async function testErrorAnalyzer() {
  console.log('Testing Error Analyzer...\n');
  
  // Initialize error analyzer
  const analyzer = new ErrorAnalyzer({
    anthropicApiKey: process.env.ANTHROPIC_API_KEY
  });

  // Test the exact error from the CLI
  const testError = new Error('null value in column "order_id" of relation "email_log" violates not-null constraint');
  
  const context = {
    query: 'INSERT into email_log a new record with customer_id and email from Globex Inc, subject \'Campaign\', status \'sent\', sent_at = now()',
    pipeline: 'Insert email log for Globex Inc',
    node: 'insert_email_log',
    operation: 'database_insert',
    stackTrace: 'error: null value in column "order_id" of relation "email_log" violates not-null constraint\n    at PostgresBackend.rawQuery',
    additionalInfo: 'WriteNode failed during INSERT operation with missing order_id field'
  };

  try {
    console.log('Analyzing error with AI...');
    const analysis = await analyzer.analyzeError(testError, context);
    
    console.log('\n=== AI Error Analysis ===');
    console.log(analyzer.formatForDisplay(analysis));
    
    console.log('\n=== Raw Analysis JSON ===');
    console.log(JSON.stringify(analysis, null, 2));
    
  } catch (error) {
    console.error('Error analysis failed:', error instanceof Error ? error.message : String(error));
    
    // Test fallback analysis
    console.log('\nTesting fallback analysis...');
    const fallbackAnalysis = analyzer.createFallbackAnalysis(testError, context);
    console.log(analyzer.formatForDisplay(fallbackAnalysis));
  }
}

// Run the test
testErrorAnalyzer().catch(console.error);
