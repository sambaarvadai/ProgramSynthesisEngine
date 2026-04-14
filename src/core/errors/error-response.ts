// Error Response Framing System for User-Friendly Error Messages

import { BaseError, ErrorCategory, ErrorSeverity, ErrorLevel, RecoverySuggestion } from './error-taxonomy.js';
import { ErrorSummary, ErrorPattern } from './error-capture.js';

export enum Audience {
  DEVELOPER = 'developer',
  USER = 'user',
  SYSTEM = 'system',
  SUPPORT = 'support'
}

export interface ErrorResponse {
  success: false;
  error: {
    code: string;
    message: string;
    userMessage: string;
    technicalDetails: string;
    category: ErrorCategory;
    severity: ErrorSeverity;
    level: ErrorLevel;
    timestamp: number;
    correlationId?: string;
    context?: Record<string, any>;
    recoverySuggestions: RecoverySuggestion[];
    affectedComponents?: string[];
    nextSteps?: string[];
  };
  metadata?: {
    executionId?: string;
    pipelineId?: string;
    sessionId?: string;
    requestId?: string;
  };
}

export interface FormattedErrorDisplay {
  title: string;
  message: string;
  severity: ErrorSeverity;
  icon: string;
  color: string;
  actions: Array<{
    label: string;
    action: string;
    type: 'primary' | 'secondary' | 'warning';
    automated?: boolean;
  }>;
  details?: {
    technical: string;
    context: Record<string, any>;
    stackTrace?: string;
  };
  relatedErrors?: string[];
}

export class ErrorFramer {
  private static readonly AUDIENCE_CONFIGS = {
    [Audience.DEVELOPER]: {
      showTechnicalDetails: true,
      showStackTrace: true,
      showContext: true,
      showRecoverySuggestions: true,
      simplifyMessages: false,
      includeNextSteps: true
    },
    [Audience.USER]: {
      showTechnicalDetails: false,
      showStackTrace: false,
      showContext: false,
      showRecoverySuggestions: true,
      simplifyMessages: true,
      includeNextSteps: true
    },
    [Audience.SYSTEM]: {
      showTechnicalDetails: true,
      showStackTrace: true,
      showContext: true,
      showRecoverySuggestions: false,
      simplifyMessages: false,
      includeNextSteps: false
    },
    [Audience.SUPPORT]: {
      showTechnicalDetails: true,
      showStackTrace: true,
      showContext: true,
      showRecoverySuggestions: true,
      simplifyMessages: false,
      includeNextSteps: true
    }
  };

  private static readonly SEVERITY_CONFIGS = {
    [ErrorSeverity.CRITICAL]: {
      icon: 'Critical Error',
      color: '#DC2626',
      urgency: 'immediate'
    },
    [ErrorSeverity.ERROR]: {
      icon: 'Error',
      color: '#EF4444',
      urgency: 'high'
    },
    [ErrorSeverity.WARNING]: {
      icon: 'Warning',
      color: '#F59E0B',
      urgency: 'medium'
    },
    [ErrorSeverity.INFO]: {
      icon: 'Info',
      color: '#3B82F6',
      urgency: 'low'
    }
  };

  public static formatErrorResponse(error: BaseError, audience: Audience = Audience.USER): ErrorResponse {
    const config = this.AUDIENCE_CONFIGS[audience];
    const severityConfig = this.SEVERITY_CONFIGS[error.metadata.severity];

    return {
      success: false,
      error: {
        code: error.metadata.errorCode,
        message: config.simplifyMessages ? error.userMessage : error.message,
        userMessage: error.userMessage,
        technicalDetails: config.showTechnicalDetails ? error.technicalDetails : '',
        category: error.metadata.category,
        severity: error.metadata.severity,
        level: error.metadata.level,
        timestamp: error.metadata.timestamp,
        correlationId: error.metadata.correlationId,
        context: config.showContext ? error.metadata.context : undefined,
        recoverySuggestions: config.showRecoverySuggestions ? error.recoverySuggestions : [],
        affectedComponents: this.getAffectedComponents(error),
        nextSteps: config.includeNextSteps ? this.generateNextSteps(error, audience) : undefined
      }
    };
  }

  public static formatForDisplay(error: BaseError, audience: Audience = Audience.USER): FormattedErrorDisplay {
    const severityConfig = this.SEVERITY_CONFIGS[error.metadata.severity];
    const config = this.AUDIENCE_CONFIGS[audience];

    return {
      title: this.generateTitle(error, audience),
      message: config.simplifyMessages ? error.userMessage : error.message,
      severity: error.metadata.severity,
      icon: severityConfig.icon,
      color: severityConfig.color,
      actions: this.formatActions(error.recoverySuggestions, audience),
      details: config.showTechnicalDetails ? {
        technical: error.technicalDetails,
        context: error.metadata.context,
        stackTrace: config.showStackTrace ? error.stack : undefined
      } : undefined,
      relatedErrors: this.findRelatedErrors(error)
    };
  }

  public static formatErrorSummary(summary: ErrorSummary, audience: Audience = Audience.DEVELOPER): string {
    const config = this.AUDIENCE_CONFIGS[audience];
    
    if (audience === Audience.USER) {
      return this.formatUserSummary(summary);
    }

    return this.formatTechnicalSummary(summary);
  }

  public static generateRecoveryMessage(error: BaseError): string {
    if (error.recoverySuggestions.length === 0) {
      return 'No specific recovery suggestions available.';
    }

    const primarySuggestion = error.recoverySuggestions.find(s => s.priority === 'high') || error.recoverySuggestions[0];
    
    let message = '';
    
    switch (primarySuggestion.type) {
      case 'immediate':
        message = `You can try ${primarySuggestion.action} immediately.`;
        break;
      case 'manual':
        message = `Please ${primarySuggestion.action}.`;
        break;
      case 'automated':
        message = `The system can ${primarySuggestion.action} automatically.`;
        break;
      case 'escalation':
        message = `This issue requires ${primarySuggestion.action}.`;
        break;
    }

    message += ` ${primarySuggestion.description}`;
    
    if (primarySuggestion.estimatedSuccess) {
      message += ` (Success rate: ${Math.round(primarySuggestion.estimatedSuccess * 100)}%)`;
    }

    return message;
  }

  public static formatPatternAnalysis(patterns: ErrorPattern[], audience: Audience = Audience.DEVELOPER): string {
    if (patterns.length === 0) {
      return 'No significant error patterns detected.';
    }

    const config = this.AUDIENCE_CONFIGS[audience];
    let message = '';

    if (audience === Audience.USER) {
      message = 'We\'ve noticed some patterns that might help:\n\n';
      patterns.forEach(pattern => {
        message += `**${pattern.description}**\n`;
        message += `${pattern.suggestedAction}\n\n`;
      });
    } else {
      message = 'Error Pattern Analysis:\n\n';
      patterns.forEach(pattern => {
        message += `Pattern: ${pattern.pattern}\n`;
        message += `Frequency: ${pattern.frequency}\n`;
        message += `Confidence: ${Math.round(pattern.confidence * 100)}%\n`;
        message += `Description: ${pattern.description}\n`;
        message += `Suggested Action: ${pattern.suggestedAction}\n\n`;
      });
    }

    return message;
  }

  private static generateTitle(error: BaseError, audience: Audience): string {
    const severityConfig = this.SEVERITY_CONFIGS[error.metadata.severity];
    
    if (audience === Audience.USER) {
      return `${severityConfig.icon}: ${error.userMessage}`;
    }

    return `${error.metadata.category.toUpperCase()} - ${error.metadata.errorCode}`;
  }

  private static formatActions(suggestions: RecoverySuggestion[], audience: Audience): Array<{
    label: string;
    action: string;
    type: 'primary' | 'secondary' | 'warning';
    automated?: boolean;
  }> {
    return suggestions.map(suggestion => ({
      label: this.formatActionLabel(suggestion, audience),
      action: suggestion.action,
      type: suggestion.priority === 'high' ? 'primary' : 
            suggestion.priority === 'medium' ? 'secondary' : 'warning',
      automated: suggestion.automated
    }));
  }

  private static formatActionLabel(suggestion: RecoverySuggestion, audience: Audience): string {
    if (audience === Audience.USER) {
      return suggestion.description;
    }

    return `${suggestion.action} (${suggestion.type})`;
  }

  private static getAffectedComponents(error: BaseError): string[] {
    const components: string[] = [];
    
    if (error.metadata.context.nodeId) {
      components.push(`Node: ${error.metadata.context.nodeId}`);
    }
    
    if (error.metadata.context.pipelineId) {
      components.push(`Pipeline: ${error.metadata.context.pipelineId}`);
    }
    
    if (error.metadata.context.component) {
      components.push(`Component: ${error.metadata.context.component}`);
    }

    return components;
  }

  private static generateNextSteps(error: BaseError, audience: Audience): string[] {
    const steps: string[] = [];
    const config = this.AUDIENCE_CONFIGS[audience];

    if (config.showRecoverySuggestions && error.recoverySuggestions.length > 0) {
      error.recoverySuggestions
        .filter(s => s.priority === 'high')
        .forEach(s => steps.push(s.description));
    }

    if (audience === Audience.USER) {
      steps.push('If the problem persists, please contact support.');
    } else if (audience === Audience.DEVELOPER) {
      steps.push('Check the error logs for more details.');
      steps.push('Review the system status and resource availability.');
    }

    return steps;
  }

  private static findRelatedErrors(error: BaseError): string[] {
    // This would integrate with the error capture system to find related errors
    // For now, return empty array
    return [];
  }

  private static formatUserSummary(summary: ErrorSummary): string {
    let message = '';
    
    if (summary.criticalErrors > 0) {
      message += `We encountered ${summary.criticalErrors} critical issue${summary.criticalErrors > 1 ? 's' : ''} that need immediate attention.\n\n`;
    } else if (summary.totalErrors > 0) {
      message += `We encountered ${summary.totalErrors} issue${summary.totalErrors > 1 ? 's' : ''} during processing.\n\n`;
    } else {
      message += 'Everything is running smoothly.\n\n';
    }

    if (summary.recommendations.length > 0) {
      message += 'Recommendations:\n';
      summary.recommendations.forEach(rec => {
        message += `  ${rec}\n`;
      });
    }

    return message;
  }

  private static formatTechnicalSummary(summary: ErrorSummary): string {
    let message = 'Error Summary Report\n';
    message += '===================\n\n';
    
    message += `Total Errors: ${summary.totalErrors}\n`;
    message += `Critical Errors: ${summary.criticalErrors}\n`;
    message += `Error Rate: ${summary.errorRate.toFixed(2)}/min\n\n`;

    message += 'By Category:\n';
    Object.entries(summary.categories).forEach(([category, agg]) => {
      if (agg.count > 0) {
        message += `  ${category}: ${agg.count} errors\n`;
      }
    });

    message += '\nBy Severity:\n';
    Object.entries(summary.severity).forEach(([severity, count]) => {
      if (count > 0) {
        message += `  ${severity}: ${count}\n`;
      }
    });

    if (summary.topErrors.length > 0) {
      message += '\nTop Errors:\n';
      summary.topErrors.forEach((error, index) => {
        message += `  ${index + 1}. ${error.errorCode}: ${error.count} occurrences\n`;
      });
    }

    if (summary.recommendations.length > 0) {
      message += '\nRecommendations:\n';
      summary.recommendations.forEach(rec => {
        message += `  - ${rec}\n`;
      });
    }

    return message;
  }
}
