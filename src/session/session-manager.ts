import { randomUUID } from 'node:crypto';
import type { PipelineIntent } from '../compiler/pipeline/pipeline-intent.js';
import type { PlanResult } from '../pipeline-engine.js';
import Anthropic from '@anthropic-ai/sdk';
import { MODELS } from '../config/models.js';
import { getAppConfig } from '../config/app-config.js';

export interface SessionTurn {
  id: string;
  timestamp: Date;
  userInput: string;
  intent: PipelineIntent;
  plan: PlanResult;
  isConversational: boolean;
}

export interface Session {
  id: string;
  userId: string;
  startTime: Date;
  turns: SessionTurn[];
}

export class SessionManager {
  private session: Session;
  private anthropicClient: Anthropic;

  constructor(anthropicApiKey: string, userId: string) {
    this.session = {
      id: randomUUID(),
      userId,
      startTime: new Date(),
      turns: [],
    };
    this.anthropicClient = new Anthropic({ apiKey: anthropicApiKey });
  }

  getSessionId(): string {
    return this.session.id;
  }

  getUserId(): string {
    return this.session.userId;
  }

  addTurn(
    userInput: string,
    intent: PipelineIntent,
    plan: PlanResult,
    isConversational: boolean,
  ): void {
    const turn: SessionTurn = {
      id: randomUUID(),
      timestamp: new Date(),
      userInput,
      intent,
      plan,
      isConversational,
    };

    this.session.turns.push(turn);

    // Check if we need to summarize (30 turns limit)
    if (this.session.turns.length > 30) {
      this.summarizeHistory();
    }
  }

  getHistory(): string {
    if (this.session.turns.length === 0) {
      return '';
    }

    const historyLines: string[] = [];
    
    for (const turn of this.session.turns) {
      historyLines.push(`User: ${turn.userInput}`);
      
      if (turn.isConversational) {
        historyLines.push(`Assistant: ${turn.intent.description}`);
      } else {
        historyLines.push(`Assistant: ${turn.intent.description}`);
        if (turn.intent.steps.length > 0) {
          const steps = turn.intent.steps
            .map((step, i) => `  ${i + 1}. [${step.kind}] ${step.id}: ${step.description}`)
            .join('\n');
          historyLines.push(`Steps:\n${steps}`);
        }
      }
      historyLines.push(''); // Empty line between turns
    }

    return historyLines.join('\n').trim();
  }

  private async summarizeHistory(): Promise<void> {
    if (this.session.turns.length <= 15) {
      return; // Not enough turns to summarize
    }

    const turnsToSummarize = this.session.turns.slice(0, 15);
    const turnsToKeep = this.session.turns.slice(15);

    // Create summary of first 15 turns
    const historyToSummarize = turnsToSummarize
      .map(turn => `User: ${turn.userInput}\nAssistant: ${turn.intent.description}`)
      .join('\n\n');

    try {
      const response = await this.anthropicClient.messages.create({
        model: MODELS.PIPELINE_INTENT_GENERATOR,
        max_tokens: getAppConfig().llm.maxTokens.sessionManager,
        messages: [
          {
            role: 'user',
            content: `Please summarize this conversation history in a concise paragraph that captures the main topics and context:

${historyToSummarize}

Focus on the key topics discussed and any important context that would be useful for future interactions.`
          }
        ],
      });

      const summary = response.content[0].type === 'text' 
        ? response.content[0].text 
        : 'Conversation summary unavailable';

      // Create a summary turn
      const summaryTurn: SessionTurn = {
        id: randomUUID(),
        timestamp: new Date(),
        userInput: '[Previous conversation summarized]',
        intent: {
          description: summary,
          steps: [],
          params: {},
        },
        plan: {
          intent: {
            description: summary,
            steps: [],
            params: {},
          },
          graph: turnsToSummarize[0]?.plan.graph || { nodes: new Map(), edges: new Map() },
          compilationErrors: [],
          intentRaw: summary,
        },
        isConversational: true,
      };

      // Rebuild session with summary and remaining turns
      this.session.turns = [summaryTurn, ...turnsToKeep];
    } catch (error) {
      console.error('Failed to summarize conversation history:', error);
      // If summarization fails, just remove the oldest turns
      this.session.turns = turnsToKeep;
    }
  }

  getSessionInfo(): { id: string; userId: string; startTime: Date; turnCount: number } {
    return {
      id: this.session.id,
      userId: this.session.userId,
      startTime: this.session.startTime,
      turnCount: this.session.turns.length,
    };
  }
}
