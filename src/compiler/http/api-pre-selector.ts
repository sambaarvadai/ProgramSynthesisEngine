import Anthropic from '@anthropic-ai/sdk';
import { MODELS } from '../../config/models.js';
import type { ApiEndpointRow } from '../../config/api-registry-store.js';

export interface ApiPreSelectorConfig {
  anthropicApiKey: string;
  maxEndpoints?: number; // default 5
  model?: string; // default from MODELS.TABLE_PRE_SELECTOR
}

export interface ApiPreSelectionResult {
  selectedEndpoints: ApiEndpointRow[];
  reasoning: string;
}

export class ApiPreSelector {
  private client: Anthropic;
  private config: ApiPreSelectorConfig;

  constructor(config: ApiPreSelectorConfig) {
    this.config = {
      maxEndpoints: config.maxEndpoints || 5,
      model: config.model || MODELS.TABLE_PRE_SELECTOR,
      anthropicApiKey: config.anthropicApiKey,
    };
    
    this.client = new Anthropic({
      apiKey: this.config.anthropicApiKey,
    });
  }

  async select(
    description: string,
    availableEndpoints: ApiEndpointRow[]
  ): Promise<ApiPreSelectionResult> {
    // Get summary list from registry store
    const summaryList = availableEndpoints
      .map(endpoint => {
        const accepts = endpoint.requestFields.map(f => f.name).join(',');
        const returns = endpoint.responseFields.map(f => f.name).join(',');
        return `${endpoint.id} | ${endpoint.method} ${endpoint.baseUrl} | accepts: ${accepts || 'none'} | returns: ${returns || 'none'}`;
      })
      .join('\n');

    const userPrompt = `Task: ${description}
   
Available APIs:
${summaryList}

Return the endpoint IDs needed for this task. Return an empty array if no APIs are needed (e.g. for pure database queries).
Only include endpoints that are genuinely needed - don't include speculative ones.`;

    try {
      const response = await this.client.messages.create({
        model: this.config.model!,
        max_tokens: 200,
        temperature: 0,
        system: "You are an API selector. Given a task description and a list of available API endpoints, return the IDs of endpoints that are relevant to the task. Return only raw JSON: { 'selectedIds': ['id1', 'id2'], 'reasoning': '...' }",
        messages: [{ role: 'user', content: userPrompt }],
      });

      const content = response.content[0];
      if (content.type !== 'text') {
        return { selectedEndpoints: [], reasoning: 'Unexpected response type from LLM' };
      }

      // Clean and parse response
      const cleanText = content.text.trim()
        .replace(/```json\n?/g, '')
        .replace(/```\n?/g, '');

      const result = JSON.parse(cleanText);
      
      if (!result.selectedIds || !Array.isArray(result.selectedIds)) {
        return { selectedEndpoints: [], reasoning: 'Invalid response format' };
      }

      // Filter endpoints by selected IDs
      const selectedEndpoints = availableEndpoints.filter(endpoint => 
        result.selectedIds.includes(endpoint.id)
      );

      return {
        selectedEndpoints,
        reasoning: result.reasoning || 'No reasoning provided'
      };

    } catch (error) {
      console.error('ApiPreSelector error:', error);
      return { selectedEndpoints: [], reasoning: 'parse failed' };
    }
  }
}
