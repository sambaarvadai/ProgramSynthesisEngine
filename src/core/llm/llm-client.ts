// Reusable LLM client abstraction for easy provider switching

export interface LLMMessage {
  role: 'user' | 'assistant' | 'system';
  content: string;
}

export interface LLMConfig {
  apiKey: string;
  model?: string;
  baseUrl?: string;
  maxTokens?: number;
  temperature?: number;
}

export interface LLMResponse {
  content: string;
  usage?: {
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
  };
}

export abstract class BaseLLMClient {
  protected config: LLMConfig;

  constructor(config: LLMConfig) {
    this.config = config;
  }

  abstract chat(messages: LLMMessage[], options?: {
    maxTokens?: number;
    temperature?: number;
  }): Promise<LLMResponse>;
}

// Anthropic Claude implementation
export class AnthropicClient extends BaseLLMClient {
  private defaultModel = 'claude-3-sonnet-20240229';
  private defaultMaxTokens = 4096;

  async chat(messages: LLMMessage[], options?: {
    maxTokens?: number;
    temperature?: number;
  }): Promise<LLMResponse> {
    const systemMessage = messages.find(m => m.role === 'system');
    const userMessages = messages.filter(m => m.role !== 'system');

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.config.apiKey,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true'
      },
      body: JSON.stringify({
        model: this.config.model || this.defaultModel,
        max_tokens: options?.maxTokens || this.config.maxTokens || this.defaultMaxTokens,
        temperature: options?.temperature ?? this.config.temperature,
        system: systemMessage?.content,
        messages: userMessages.map(({ role, content }) => ({ role, content }))
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Anthropic API error: ${response.status} ${errorText}`);
    }

    const data = await response.json();
    const content = data.content[0].text;
    
    console.log('[LLM Client Response]', content);

    return {
      content,
      usage: {
        inputTokens: data.usage?.input_tokens,
        outputTokens: data.usage?.output_tokens,
        totalTokens: data.usage?.total_tokens
      }
    };
  }
}

// OpenAI implementation (ready for future use)
export class OpenAIClient extends BaseLLMClient {
  private defaultModel = 'gpt-4';
  private defaultMaxTokens = 4096;

  async chat(messages: LLMMessage[], options?: {
    maxTokens?: number;
    temperature?: number;
  }): Promise<LLMResponse> {
    const response = await fetch(`${this.config.baseUrl || 'https://api.openai.com'}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.config.apiKey}`
      },
      body: JSON.stringify({
        model: this.config.model || this.defaultModel,
        max_tokens: options?.maxTokens || this.config.maxTokens || this.defaultMaxTokens,
        temperature: options?.temperature ?? this.config.temperature,
        messages: messages.map(({ role, content }) => ({ role, content }))
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`OpenAI API error: ${response.status} ${errorText}`);
    }

    const data = await response.json();
    const content = data.choices[0].message.content;

    return {
      content,
      usage: {
        inputTokens: data.usage?.prompt_tokens,
        outputTokens: data.usage?.completion_tokens,
        totalTokens: data.usage?.total_tokens
      }
    };
  }
}

// Factory function for easy client creation
export function createLLMClient(provider: 'anthropic' | 'openai', config: LLMConfig): BaseLLMClient {
  switch (provider) {
    case 'anthropic':
      return new AnthropicClient(config);
    case 'openai':
      return new OpenAIClient(config);
    default:
      throw new Error(`Unsupported LLM provider: ${provider}`);
  }
}

// Convenience function for simple calls
export async function callLLM(
  provider: 'anthropic' | 'openai',
  config: LLMConfig,
  messages: LLMMessage[],
  options?: {
    maxTokens?: number;
    temperature?: number;
  }
): Promise<string> {
  const client = createLLMClient(provider, config);
  const response = await client.chat(messages, options);
  return response.content;
}
