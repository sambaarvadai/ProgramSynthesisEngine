// LLM Configuration for easy provider switching

export interface LLMProviderConfig {
  provider: 'anthropic' | 'openai';
  apiKey: string;
  model?: string;
  baseUrl?: string;
  maxTokens?: number;
  temperature?: number;
}

export const DEFAULT_LLM_CONFIGS = {
  anthropic: {
    provider: 'anthropic' as const,
    model: 'claude-3-sonnet-20240229',
    maxTokens: 4096,
    temperature: 0
  },
  openai: {
    provider: 'openai' as const,
    model: 'gpt-4',
    maxTokens: 4096,
    temperature: 0
  }
};

export function createLLMConfig(
  provider: 'anthropic' | 'openai',
  apiKey: string,
  overrides?: Partial<LLMProviderConfig>
): LLMProviderConfig {
  const baseConfig = DEFAULT_LLM_CONFIGS[provider];
  return {
    ...baseConfig,
    provider,
    apiKey,
    ...overrides
  };
}

// Environment-based configuration
export function getLLMConfigFromEnv(
  provider?: 'anthropic' | 'openai'
): LLMProviderConfig {
  const envProvider = provider || (process.env.LLM_PROVIDER as 'anthropic' | 'openai') || 'anthropic';
  
  if (envProvider === 'anthropic') {
    return createLLMConfig('anthropic', process.env.ANTHROPIC_API_KEY!);
  } else {
    return createLLMConfig('openai', process.env.OPENAI_API_KEY!);
  }
}
