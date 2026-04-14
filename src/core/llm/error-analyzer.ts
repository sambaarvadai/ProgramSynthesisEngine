/**
 * Error Analyzer - Sends errors to LLM for detailed analysis and next steps
 */

import Anthropic from '@anthropic-ai/sdk';
import { MODELS } from '../../config/models.js';

export interface ErrorAnalysis {
  errorType: string;
  rootCause: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  impact: string;
  nextSteps: string[];
  prevention: string[];
  codeFixes: string[];
}

export interface ErrorAnalyzerConfig {
  anthropicApiKey: string;
  model?: string;
}

export class ErrorAnalyzer {
  private client: Anthropic;
  private model: string;

  constructor(config: ErrorAnalyzerConfig) {
    this.client = new Anthropic({ apiKey: config.anthropicApiKey });
    this.model = config.model || MODELS.QUERY_INTENT_GENERATOR; // Use Sonnet for complex analysis
  }

  /**
   * Analyze an error using LLM
   */
  async analyzeError(error: Error, context?: {
    query?: string;
    pipeline?: string;
    node?: string;
    operation?: string;
    stackTrace?: string;
    additionalInfo?: string;
  }): Promise<ErrorAnalysis> {
    const prompt = this.buildAnalysisPrompt(error, context);
    
    try {
      const response = await this.client.messages.create({
        model: this.model,
        max_tokens: 2000,
        messages: [{ role: 'user', content: prompt }],
        system: this.getSystemPrompt()
      });

      const content = response.content[0];
      if (content.type === 'text') {
        return this.parseAnalysisResponse(content.text);
      }
      
      throw new Error('Unexpected response format from LLM');
    } catch (llmError) {
      // Fallback to basic analysis if LLM fails
      return this.createFallbackAnalysis(error, context);
    }
  }

  private buildAnalysisPrompt(error: Error, context?: any): string {
    const errorInfo = {
      message: error.message,
      name: error.name,
      stack: error.stack,
      context: context || {}
    };

    return `Please analyze this database/pipeline execution error and provide detailed guidance:

ERROR DETAILS:
${JSON.stringify(errorInfo, null, 2)}

CONTEXT:
This error occurred in a Program Synthesis Engine that:
- Executes natural language queries against a database
- Uses pipeline-based data processing
- Has nodes for queries, writes, and transformations
- Uses PostgreSQL as the database backend

Please provide a JSON response with the following structure:
{
  "errorType": "categorize the error type",
  "rootCause": "explain what caused this error",
  "severity": "low|medium|high|critical",
  "impact": "explain the impact on the system/user",
  "nextSteps": ["step 1", "step 2", "step 3"],
  "prevention": ["prevention tip 1", "prevention tip 2"],
  "codeFixes": ["specific code/SQL fix 1", "specific code/SQL fix 2"]
}

Focus on:
1. Database constraint issues
2. Pipeline execution problems
3. Data flow problems
4. Schema mismatches
5. Provide actionable next steps

Return ONLY valid JSON, no markdown, no explanation.`;
  }

  private getSystemPrompt(): string {
    return `You are an expert database and pipeline systems analyst. Analyze errors in a Program Synthesis Engine context. Provide clear, actionable guidance for developers and users. Focus on database constraints, data flow, and pipeline execution issues.`;
  }

  private parseAnalysisResponse(response: string): ErrorAnalysis {
    try {
      // Clean up the response
      const clean = response
        .replace(/^```json\s*/i, '')
        .replace(/^```\s*/i, '')
        .replace(/^[^{]*({.*})[^}]*$/, '$1')
        .trim();

      const parsed = JSON.parse(clean);
      
      // Validate and ensure required fields
      return {
        errorType: parsed.errorType || 'Unknown',
        rootCause: parsed.rootCause || 'Unable to determine',
        severity: parsed.severity || 'medium',
        impact: parsed.impact || 'Error occurred during execution',
        nextSteps: Array.isArray(parsed.nextSteps) ? parsed.nextSteps : ['Contact support'],
        prevention: Array.isArray(parsed.prevention) ? parsed.prevention : ['Review error handling'],
        codeFixes: Array.isArray(parsed.codeFixes) ? parsed.codeFixes : ['Check error logs']
      };
    } catch (parseError) {
      throw new Error(`Failed to parse LLM response: ${parseError instanceof Error ? parseError.message : String(parseError)}`);
    }
  }

  private createFallbackAnalysis(error: Error, context?: any): ErrorAnalysis {
    const message = error.message.toLowerCase();
    
    // Basic pattern matching for common database errors
    if (message.includes('null value') && message.includes('violates not-null constraint')) {
      return {
        errorType: 'Database Constraint Violation',
        rootCause: 'Required column is missing or null',
        severity: 'high',
        impact: 'Database operation failed, data not inserted',
        nextSteps: [
          'Check which column requires a non-null value',
          'Ensure all required fields are provided',
          'Review the data mapping in the pipeline'
        ],
        prevention: [
          'Validate required fields before database operations',
          'Add default values for optional columns',
          'Review schema constraints'
        ],
        codeFixes: [
          'Add the missing required column value',
          'Use COALESCE or DEFAULT values in SQL',
          'Update the pipeline to include all required fields'
        ]
      };
    }

    if (message.includes('duplicate key') || message.includes('unique constraint')) {
      return {
        errorType: 'Duplicate Key Error',
        rootCause: 'Attempting to insert duplicate data',
        severity: 'medium',
        impact: 'Data insertion failed due to uniqueness constraint',
        nextSteps: [
          'Check for existing records',
          'Use UPSERT operation instead of INSERT',
          'Review data deduplication logic'
        ],
        prevention: [
          'Check for duplicates before insertion',
          'Use appropriate conflict resolution',
          'Implement data validation'
        ],
        codeFixes: [
          'Use INSERT ... ON CONFLICT UPDATE',
          'Add WHERE NOT EXISTS clause',
          'Implement proper error handling'
        ]
      };
    }

    // Generic fallback
    return {
      errorType: 'Unknown Error',
      rootCause: 'Unable to determine from error message',
      severity: 'medium',
      impact: 'Operation failed, system may be unstable',
      nextSteps: [
        'Review error logs for more details',
        'Check database connection and schema',
        'Verify input data format'
      ],
      prevention: [
        'Add better error handling',
        'Implement input validation',
        'Add logging for debugging'
      ],
      codeFixes: [
        'Add try-catch blocks around database operations',
        'Validate inputs before processing',
        'Add detailed logging'
      ]
    };
  }

  /**
   * Format analysis for user display
   */
  formatForDisplay(analysis: ErrorAnalysis): string {
    let output = '';
    
    output += `\n=== Error Analysis ===\n`;
    output += `Type: ${analysis.errorType}\n`;
    output += `Severity: ${analysis.severity.toUpperCase()}\n`;
    output += `Root Cause: ${analysis.rootCause}\n`;
    output += `Impact: ${analysis.impact}\n`;
    
    if (analysis.nextSteps.length > 0) {
      output += `\nNext Steps:\n`;
      analysis.nextSteps.forEach((step, i) => {
        output += `  ${i + 1}. ${step}\n`;
      });
    }
    
    if (analysis.codeFixes.length > 0) {
      output += `\nCode Fixes:\n`;
      analysis.codeFixes.forEach((fix, i) => {
        output += `  ${i + 1}. ${fix}\n`;
      });
    }
    
    if (analysis.prevention.length > 0) {
      output += `\nPrevention:\n`;
      analysis.prevention.forEach((tip, i) => {
        output += `  ${i + 1}. ${tip}\n`;
      });
    }
    
    return output;
  }
}
