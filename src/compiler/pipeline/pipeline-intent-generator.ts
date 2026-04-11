import Anthropic from '@anthropic-ai/sdk';
import type {
  PipelineIntent,
  PipelineStepIntent,
} from './pipeline-intent.js';
import type { SchemaConfig } from '../schema/schema-config.js';
import { MODELS } from '../../config/models.js';

export type PipelineIntentGeneratorConfig = {
  anthropicApiKey: string;
  model?: string;
  schema?: SchemaConfig;
};

export class PipelineIntentGenerator {
  private client: Anthropic;

  constructor(private config: PipelineIntentGeneratorConfig) {
    this.client = new Anthropic({ apiKey: config.anthropicApiKey });
  }

  async generate(
    naturalLanguageDescription: string,
    context?: {
      availableParams?: Record<string, string>;
      exampleData?: Record<string, unknown>;
      sessionHistory?: string;
    },
  ): Promise<{ intent: PipelineIntent; raw: string }> {
    const systemPrompt = this.buildSystemPrompt();
    const userPrompt = this.buildUserPrompt(naturalLanguageDescription, context);

    const response = await this.client.messages.create({
      model: this.config.model ?? MODELS.PIPELINE_INTENT_GENERATOR,
      max_tokens: 2000,
      messages: [{ role: 'user', content: userPrompt }],
      system: systemPrompt,
    });

    const raw =
      response.content[0].type === 'text'
        ? response.content[0].text
        : '';
    console.log('[Pipeline Intent Generator LLM Output]', raw);

    const clean = raw
      .replace(/```json\n?/g, '')
      .replace(/```\n?/g, '')
      .trim();

    const intent = JSON.parse(clean) as PipelineIntent;
    this.fillDefaults(intent);

    // Sanitize LLM-generated budget values
    if (intent.budget) {
      // Never let LLM set timeout below 30s
      if (intent.budget.timeoutMs && intent.budget.timeoutMs < 30000) {
        delete intent.budget.timeoutMs; // let config/defaults handle it
      }
      // Never let LLM set maxLLMCalls to 0 or negative
      if (intent.budget.maxLLMCalls !== undefined && intent.budget.maxLLMCalls <= 0) {
        delete intent.budget.maxLLMCalls;
      }
    }

    return { intent, raw };
  }

  async refine(
    intent: PipelineIntent,
    feedback: string,
  ): Promise<{ intent: PipelineIntent; raw: string }> {
    const systemPrompt = this.buildSystemPrompt();

    const userPrompt = `Current pipeline intent:
${JSON.stringify(intent, null, 2)}

User feedback:
${feedback}

Please revise the pipeline intent based on the feedback. Return ONLY valid JSON, no markdown, no explanation.`;

    const response = await this.client.messages.create({
      model: this.config.model ?? MODELS.PIPELINE_INTENT_GENERATOR,
      max_tokens: 2000,
      messages: [{ role: 'user', content: userPrompt }],
      system: systemPrompt,
    });

    const raw =
      response.content[0].type === 'text'
        ? response.content[0].text
        : '';
    console.log('[Pipeline Intent Refine LLM Output]', raw);

    const clean = raw
      .replace(/```json\n?/g, '')
      .replace(/```\n?/g, '')
      .trim();

    const revisedIntent = JSON.parse(clean) as PipelineIntent;
    this.fillDefaults(revisedIntent);

    // Sanitize LLM-generated budget values
    if (revisedIntent.budget) {
      // Never let LLM set timeout below 30s
      if (revisedIntent.budget.timeoutMs && revisedIntent.budget.timeoutMs < 30000) {
        delete revisedIntent.budget.timeoutMs;
      }
      // Never let LLM set maxLLMCalls to 0 or negative
      if (revisedIntent.budget.maxLLMCalls !== undefined && revisedIntent.budget.maxLLMCalls <= 0) {
        delete revisedIntent.budget.maxLLMCalls;
      }
    }

    return { intent: revisedIntent, raw };
  }

  private buildSystemPrompt(): string {
    const schemaInfo = this.config.schema
      ? this.buildSchemaSummary()
      : '';

    return `You are a pipeline architect for ProgramExecutionEngine.
Convert natural language workflow descriptions into a structured PipelineIntent JSON.

Available node kinds and when to use them:
- query: fetch data from database tables
- transform: filter, map, sort, rename fields on a RowSet
- llm: use AI to analyze, classify, summarize, or generate text for rows
- http: call an external HTTP API with request body built from row data
          use for: sending emails via API, calling webhooks, fetching external data
          outputFields: fields extracted from the API response
- write: write rows back to the database (INSERT/UPDATE/UPSERT)
           use for: logging actions taken, updating records, persisting results
- conditional: branch based on a condition (requires trueBranch, falseBranch, mergeStep)
- loop: iterate over a dataset (requires loopBody, loopMode, loopOver)
- merge: combine outputs from multiple branches (requires mergeFrom)
- parallel: run multiple steps concurrently (requires parallelBranches)

CRITICAL RULES — follow exactly:

1. NEVER split a query + aggregation into separate steps.
   If the user wants grouped counts, sums, or averages:
   → use ONE query step with description that includes the aggregation.
   → WRONG: step 1 fetches raw data, step 2 groups and counts
   → RIGHT: step 1 fetches data grouped by X with COUNT(*)

2. A query step can do everything SQL can:
   filtering, joining, grouping, aggregating, sorting, limiting.
   Express it all in the query description.

3. Use transform steps ONLY for:
   - Renaming fields
   - Adding computed fields not expressible in SQL
   - Post-processing already-fetched data

4. Use loop + llm steps for AI enrichment per row.

5. Use conditional steps only for pipeline-level branching,
   not for row-level classification.
   For 'mark rows as X or Y based on condition':
   → use ONE transform step with addField using a conditional expression
   → NOT query → conditional → two transforms → merge

Example mappings:
  'count orders by status' → ONE query step: 'SELECT status, COUNT(*) FROM orders GROUP BY status'
  'revenue by region' → ONE query step with JOIN + GROUP BY + SUM
  'mark orders as high/low value' → ONE transform step with addField using conditional expr
  'for each customer generate AI summary' → query + loop + llm
  'send email to customer' → http step calling email API (Resend/SendGrid)
  'log that we sent the email' → write step inserting into audit table
  'update order status' → write step updating orders table
  'call webhook when order completes' → http step POSTing to webhook URL

SIMPLICITY RULES - always prefer the simpler plan:

1. Row-level classification/tagging -> ONE transform step with addField
   using a Conditional expression. Never use conditional->branch->merge
   for operations that apply to every row.
   
   WRONG for 'mark rows as X or Y':
     conditional -> transform(markX) -> transform(markY) -> merge
   
   RIGHT for 'mark rows as X or Y':
     transform with addField: { kind: 'Conditional', condition: ..., then: 'X', else: 'Y' }

2. Splitting then immediately merging -> pointless. If you split data
   into branches only to merge them back, use a single transform instead.
   
   WRONG: transform(addField) -> transform(filterA) -> transform(filterB) -> merge
   RIGHT: transform(addField) - the merge adds nothing

3. Use conditional branching ONLY when the two branches do genuinely
   different things that cannot be expressed as a single row operation.
   
   IMPORTANT: ConditionalNode routes the ENTIRE dataset based on one
   condition evaluated against the first row. It is for pipeline-level
   routing, not row-level filtering.

   For per-row conditional logic (different treatment per row):
   -> use LoopNode to iterate over rows
   -> use ConditionalNode INSIDE the loop body for per-row branching
   -> OR use TransformNode with Conditional ExprAST for simple cases

   ConditionalNode is appropriate when:
   - 'if the total count > 1000, send an alert' (scalar condition)
   - 'if any critical tickets exist, escalate the whole batch' (batch condition)
   
   ConditionalNode is NOT appropriate when:
   - 'for each row, if X do A else do B' -> use loop+conditional or transform

   Example of VALID conditional (pipeline-level):
     true branch: call LLM to analyze entire dataset
     false branch: add a null analysis field
   Example of INVALID conditional (row-level):
     true branch: filter to urgent tickets  
     false branch: filter to normal tickets
     (these should be a single addField with conditional expression or loop+conditional)

4. For workflows that send notifications and log them:
   query (fetch data) -> loop -> http (send notification) -> write (log it)
   This is the standard pattern. Don't overcomplicate it.

5. For workflows that log or notify, always filter out already-processed rows.
   For email_log: add a filter WHERE orders.id NOT IN (SELECT order_id FROM email_log)
   This makes the workflow idempotent - safe to re-run.

6. Minimum steps principle: if the same result can be achieved in
   N steps or N+1 steps, always choose N steps.

Additional rules:
- Every step must have a unique snake_case id
- dependsOn must reference valid step ids
- Every conditional must have a corresponding merge step
- Every loop must list its body steps in loopBody
- Keep steps focused and single-purpose
- Prefer simple linear pipelines unless branching/looping is clearly needed
- Return ONLY valid JSON, no markdown, no explanation
- Do not set budget values in the response. Omit the budget field entirely.

PipelineIntent schema:
{
  "description": "string - overall pipeline description",
  "steps": [
    {
      "id": "string - unique snake_case identifier",
      "kind": "query | transform | llm | http | conditional | loop | merge | parallel",
      "description": "string - natural language description of this step",
      "dependsOn": ["string - step ids this step depends on (data flow)"],
      // Conditional-specific
      "condition": "string - natural language predicate description",
      "trueBranch": "string - step id for true branch",
      "falseBranch": "string - step id for false branch",
      "mergeStep": "string - step id of the merge that follows",
      // Loop-specific
      "loopMode": "forEach | while",
      "loopOver": "string - description of what to iterate",
      "loopBody": ["string - step ids in the loop body"],
      "maxIterations": "number - default 100",
      // Merge-specific
      "mergeFrom": ["string - step ids to merge"],
      "mergeStrategy": "union | join | first",
      // Parallel-specific
      "parallelBranches": ["string - step ids to run in parallel"],
      "maxConcurrency": "number - default 3",
      // LLM-specific
      "outputFields": ["string - field names the LLM should produce"],
      // General config hints
      "config": "Record<string, unknown>"
    }
  ],
  "params": {
    "paramName": "string - description"
  },
  "budget": {
    "maxLLMCalls": "number",
    "maxIterations": "number",
    "timeoutMs": "number"
  }
}

${schemaInfo}`;
  }

  private buildSchemaSummary(): string {
    if (!this.config.schema) {
      return '';
    }

    const tables: string[] = [];
    for (const [name, table] of this.config.schema.tables) {
      const columns = table.columns
        .map(c => `  - ${c.name}: ${c.type.kind}${c.nullable ? ' (nullable)' : ''}${c.description ? ` - ${c.description}` : ''}`)
        .join('\n');
      tables.push(`- ${name}\n${columns}`);
    }

    return `Available database schema:
${tables.join('\n\n')}`;
  }

  private buildUserPrompt(
    naturalLanguageDescription: string,
    context?: {
      availableParams?: Record<string, string>;
      exampleData?: Record<string, unknown>;
      sessionHistory?: string;
    },
  ): string {
    let prompt = '';

    if (context?.sessionHistory) {
      prompt += `Session history:\n${context.sessionHistory}\n\n`;
    }

    if (this.config.schema) {
      prompt += `${this.buildSchemaSummary()}\n\n`;
    }

    if (context?.availableParams) {
      const params = Object.entries(context.availableParams)
        .map(([name, desc]) => `- ${name}: ${desc}`)
        .join('\n');
      prompt += `Available params:\n${params}\n\n`;
    }

    if (context?.exampleData) {
      prompt += `Example data:\n${JSON.stringify(context.exampleData, null, 2)}\n\n`;
    }

    prompt += `Workflow to build:\n${naturalLanguageDescription}`;

    return prompt;
  }

  private fillDefaults(intent: PipelineIntent): void {
    for (const step of intent.steps) {
      if (step.kind === 'loop' && step.maxIterations === undefined) {
        step.maxIterations = 100;
      }
      if (step.kind === 'merge' && step.mergeStrategy === undefined) {
        step.mergeStrategy = 'union';
      }
      if (step.kind === 'parallel' && step.maxConcurrency === undefined) {
        step.maxConcurrency = 3;
      }
    }
  }
}
