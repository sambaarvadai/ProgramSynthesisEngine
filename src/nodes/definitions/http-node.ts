import type { NodeDefinition } from '../../core/registry/node-registry.js'
import type { HttpPayload } from '../payloads.js'
import type { RowSet, Row } from '../../core/types/value.js'
import type { DataValue } from '../../core/types/data-value.js'
import { validationOk, validationFail } from '../../core/types/validation.js'
import { ExprEvaluator } from '../../executors/expr-evaluator.js'
import { apiRegistryStore } from '../../config/api-registry-store.js';
import { flattenLeaves, applyAliases, extractArrayRoot } from '../../utils/response-flattener.js';

// Concurrent executor with rate limiting
async function executeWithConcurrency<T>(
  items: T[],
  fn: (item: T) => Promise<Row[]>,
  concurrency: number,
  rateLimitPerSecond?: number
): Promise<Row[]> {
  const results: Row[] = []
  const queue = [...items]
  const minIntervalMs = rateLimitPerSecond ? 1000 / rateLimitPerSecond : 0
  let lastCallTime = 0
  
  async function worker() {
    while (queue.length > 0) {
      const item = queue.shift()
      if (!item) break
      
      // Rate limiting: enforce minimum interval between calls
      if (minIntervalMs > 0) {
        const now = Date.now()
        const elapsed = now - lastCallTime
        if (elapsed < minIntervalMs) {
          await new Promise(r => setTimeout(r, minIntervalMs - elapsed))
        }
        lastCallTime = Date.now()
      }
      
      const rows = await fn(item)
      results.push(...rows)
    }
  }
  
  // Launch N workers concurrently
  const workers = Array.from({ length: concurrency }, () => worker())
  await Promise.all(workers)
  return results
}

// Chunk helper for batch mode
function chunk<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size))
  }
  return chunks
}

// Extract single row fetch logic
async function fetchSingleRow(
  row: Row,
  payload: HttpPayload,
  endpoint: any,
  evaluator: ExprEvaluator,
  ctx: any
): Promise<Row[]> {
  // Build URL from template
  const url = buildTemplateString(payload.url, evaluator, ctx, row)

  // Build headers
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...Object.fromEntries(
      Object.entries(payload.headers ?? {}).map(([k, v]) => [
        k,
        buildTemplateString(v, evaluator, ctx, row)
      ])
    )
  }

  // Build auth header
  if (payload.auth) {
    if (payload.auth.kind === 'bearer') {
      const token = evaluator.evaluate(payload.auth.token, ctx.scope, row)
      headers['Authorization'] = `Bearer ${token}` 
    } else if (payload.auth.kind === 'apiKey') {
      const key = evaluator.evaluate(payload.auth.value, ctx.scope, row)
      headers[payload.auth.header] = String(key)
    }
  }

  // Build body using endpoint schema
  let body: string | undefined
  if (payload.method !== 'GET') {
    const bodyFields = endpoint?.requestFields.map((f: any) => f.name) || payload.bodyFields || []
    if (bodyFields.length > 0) {
      // Build body from endpoint schema fields
      const filtered: Record<string, any> = {}
      for (const f of bodyFields) {
        if (f in row) filtered[f] = row[f]
      }
      body = JSON.stringify(filtered)
    } else if (payload.body) {
      // Fall back to expression evaluation
      const bodyValue = evaluator.evaluate(payload.body, ctx.scope, row)
      body = typeof bodyValue === 'string' ? bodyValue : JSON.stringify(bodyValue)
    } else {
      // No body config at all - send full row as fallback
      body = JSON.stringify(row)
    }
  }

  // URL validation before fetch
  if (!url.startsWith('http')) {
    throw new Error(`HttpNode: invalid URL resolved: "${url}"`)
  }

  console.log(`[HttpNode] Single request - URL: ${url}`)
  console.log(`[HttpNode] Single request - Method: ${payload.method}`)
  console.log(`[HttpNode] Single request - Headers:`, headers)
  console.log(`[HttpNode] Single request - Body:`, body)

  // Execute with retry
  const maxRetries = payload.retryPolicy?.maxRetries ?? 0
  const backoffMs = payload.retryPolicy?.backoffMs ?? 1000
  let response: any
  let lastError: Error | undefined

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      console.log(`[HttpNode] Single request - Attempt ${attempt + 1}/${maxRetries + 1}`)
      const res = await fetch(url, {
        method: payload.method,
        headers,
        body: payload.method !== 'GET' ? body : undefined
      })

      console.log(`[HttpNode] Single request - Response status: ${res.status}`)
      if (!res.ok) {
        const errorText = await res.text()
        console.log(`[HttpNode] Single request - Error response:`, errorText)
        throw new Error(`HTTP ${res.status}: ${errorText}`)
      }

      response = await res.json().catch(() => ({ status: res.status }))
      console.log(`[HttpNode] Single request - Response body:`, JSON.stringify(response, null, 2))
      break
    } catch (err) {
      lastError = err as Error
      if (attempt < maxRetries) {
        await new Promise(r => setTimeout(r, backoffMs * Math.pow(2, attempt)))
      }
    }
  }

  if (lastError && !response) {
    // Fail-soft: return error row instead of throwing
    return [{
      ...row,
      http_status: 'error',
      http_error: lastError?.message ?? 'Unknown error'
    }]
  }

  // Build endpoint metadata for processResponse
  const endpointMeta = endpoint ? {
    responseMode: endpoint.responseMode,
    responseRoot: endpoint.responseRoot,
    responseFieldMeta: endpoint.responseFields.map((f: any) => ({ 
      name: f.name, 
      apiFieldName: f.apiFieldName || f.name, 
      jsonPath: undefined 
    }))
  } : null
  
  // Process response using processResponse function
  return processResponse(response, row, payload, endpointMeta)
}

function processResponse(
  response: any,
  row: Row,
  payload: HttpPayload,
  endpointMeta: { 
    responseMode: 'object' | 'array',
    responseRoot?: string,
    responseFieldMeta: Array<{ name: string; apiFieldName?: string; jsonPath?: string }>
  } | null
): Row[] {
  const mode = endpointMeta?.responseMode ?? 'object'
  const aliases = endpointMeta?.responseFieldMeta ?? []
  const outputFields = payload.outputFields ?? []
  
  if (mode === 'array') {
    // Extract the array, expand to multiple rows
    const items = extractArrayRoot(response, endpointMeta?.responseRoot)
    return items.map(item => {
      const flat = flattenLeaves(item)
      const aliased = applyAliases(flat, aliases)
      // Filter to outputFields if specified
      const filtered = outputFields.length > 0
        ? Object.fromEntries(outputFields.map(f => [f, aliased[f] ?? null]))
        : aliased
      return { ...row, http_status: 'success', ...filtered }
    })
  } else {
    // Object mode: flatten and alias the single response
    const flat = flattenLeaves(response)
    const aliased = applyAliases(flat, aliases)
    const filtered = outputFields.length > 0
      ? Object.fromEntries(outputFields.map(f => [f, aliased[f] ?? null]))
      : aliased
    return [{ ...row, http_status: 'success', ...filtered }]
  }
}

export function createHttpNodeDefinition(
  evaluator: ExprEvaluator
): NodeDefinition<HttpPayload, DataValue, DataValue> {
  return {
    kind: 'http',
    displayName: 'HTTP Request',
    icon: '??',
    color: '#10B981',
    inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'tabular' }, required: true }],
    outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'tabular' }, required: true }],

    validate(payload: unknown) {
      const p = payload as HttpPayload
      if (!p?.url) return validationFail([{ code: 'MISSING_URL', message: 'HttpNode requires url' }])
      if (!p?.method) return validationFail([{ code: 'MISSING_METHOD', message: 'HttpNode requires method' }])
      return validationOk()
    },

    inferOutputType(payload, inputType) {
      return { kind: 'tabular' }
    },

    async execute(payload: HttpPayload, input: DataValue, ctx): Promise<DataValue> {
      // Convert DataValue to RowSet for processing
      const inputRowSet = input.kind === 'tabular' ? input.data : 
                         input.kind === 'record' ? { schema: input.schema, rows: [input.data] } :
                         { schema: { columns: [] }, rows: [] }
      
      if (!inputRowSet?.rows?.length) return { kind: 'tabular', data: { schema: { columns: [] }, rows: [] }, schema: { columns: [] } }

      // Fetch endpoint metadata from database if endpointId is available
      let endpoint = null
      if (payload.endpointId) {
        endpoint = apiRegistryStore.getById(payload.endpointId)
      }

      // Check for batch mode
      if (payload.batchMode) {
        // Chunked batch mode with optional rate limiting
        const chunks = payload.chunkSize 
          ? chunk(inputRowSet.rows, payload.chunkSize) 
          : [inputRowSet.rows]
        
        const allOutputRows: Row[] = []
        const minIntervalMs = payload.rateLimitPerSecond ? 1000 / payload.rateLimitPerSecond : 0
        let lastCallTime = 0
        
        for (const chunkRows of chunks) {
          // Rate limiting between chunks
          if (minIntervalMs > 0 && allOutputRows.length > 0) {
            const now = Date.now()
            const elapsed = now - lastCallTime
            if (elapsed < minIntervalMs) {
              await new Promise(r => setTimeout(r, minIntervalMs - elapsed))
            }
            lastCallTime = Date.now()
          }
          
          // Send chunk as a single array request
          const url = buildTemplateString(payload.url, evaluator, ctx, chunkRows[0])
          const headers: Record<string, string> = {
            'Content-Type': 'application/json',
            ...Object.fromEntries(
              Object.entries(payload.headers ?? {}).map(([k, v]) => [
                k, buildTemplateString(v, evaluator, ctx, chunkRows[0])
              ])
            )
          }
          
          // Build batch body: array of filtered rows using endpoint schema
          const batchBody = chunkRows.map(row => {
            const bodyFields = endpoint?.requestFields.map(f => f.name) || payload.bodyFields || []
            if (bodyFields.length > 0) {
              const filtered: Record<string, any> = {}
              for (const f of bodyFields) {
                if (f in row) filtered[f] = row[f]
              }
              return filtered
            }
            return row
          })
          
          // URL validation before fetch
          if (!url.startsWith('http')) {
            throw new Error(`HttpNode: invalid URL resolved: "${url}"`)
          }

          console.log(`[HttpNode] Batch mode - URL: ${url}`)
          console.log(`[HttpNode] Batch mode - Method: ${payload.method}`)
          console.log(`[HttpNode] Batch mode - Chunk size: ${chunkRows.length}`)
          console.log(`[HttpNode] Batch mode - Headers:`, headers)
          console.log(`[HttpNode] Batch mode - Body:`, JSON.stringify(batchBody, null, 2))

          const res = await fetch(url, {
            method: payload.method,
            headers,
            body: JSON.stringify(batchBody)
          })
          console.log(`[HttpNode] Batch mode - Response status: ${res.status}`)
          if (!res.ok) {
            const errorText = await res.text()
            console.log(`[HttpNode] Batch mode - Error response:`, errorText)
            throw new Error(`HTTP ${res.status}: ${errorText}`)
          }
          const responseArray = await res.json()
          console.log(`[HttpNode] Batch mode - Response body:`, JSON.stringify(responseArray, null, 2))
          
          // Build endpoint metadata for processResponse
          const endpointMeta = endpoint ? {
            responseMode: endpoint.responseMode,
            responseRoot: endpoint.responseRoot,
            responseFieldMeta: endpoint.responseFields.map(f => ({ 
              name: f.name, 
              apiFieldName: f.apiFieldName || f.name, 
              jsonPath: undefined 
            }))
          } : null
          
          // Process response for each input row using processResponse
          for (const row of chunkRows) {
            const responseItem = Array.isArray(responseArray) ? responseArray[chunkRows.indexOf(row)] : responseArray
            const processedRows = processResponse(responseItem, row, payload, endpointMeta)
            allOutputRows.push(...processedRows)
          }
        }
        
        const allKeys = new Set<string>()
        allOutputRows.forEach(r => Object.keys(r).forEach(k => allKeys.add(k)))
        const schema = {
          columns: Array.from(allKeys).map(name => ({
            name, type: { kind: 'any' } as any, nullable: true
          }))
        }
        return { kind: 'tabular', data: { schema, rows: allOutputRows }, schema }
      }

      // Per-row execution with concurrency
      const concurrency = payload.concurrency ?? 1
      const startMs = Date.now()
      console.log(`[HttpNode] Starting ${inputRowSet.rows.length} requests with concurrency: ${concurrency}`)
      
      const outputRows = await executeWithConcurrency(
        inputRowSet.rows,
        async (row) => {
          return await fetchSingleRow(row, payload, endpoint, evaluator, ctx)
        },
        concurrency,
        payload.rateLimitPerSecond
      )
      
      // Count successes and errors for summary
      const successCount = outputRows.filter(r => r.http_status === 'success').length
      const errorCount = outputRows.filter(r => r.http_status === 'error').length
      console.log(`[HttpNode] Completed ${inputRowSet.rows.length} requests (concurrency: ${concurrency}, duration: ${Date.now() - startMs}ms, success: ${successCount}, errors: ${errorCount})`)

      // Only throw if ALL rows failed (no successes at all)
      if (outputRows.every(r => r.http_status === 'error')) {
        throw new Error('All HTTP requests failed')
      }

      // Infer output schema
      const allKeys = new Set<string>()
      outputRows.forEach(r => Object.keys(r).forEach(k => allKeys.add(k)))
      const schema = {
        columns: Array.from(allKeys).map(name => ({
          name,
          type: { kind: 'any' } as any,
          nullable: true
        }))
      }

      return { kind: 'tabular', data: { schema, rows: outputRows }, schema }
    }
  }
}

// Helper: build string from TemplateString
function buildTemplateString(
  template: import('../payloads.js').TemplateString,
  evaluator: ExprEvaluator,
  ctx: any,
  row: Row
): string {
  return template.parts.map(part => {
    if (part.kind === 'literal') return part.text
    return String(evaluator.evaluate(part.expr, ctx.scope, row) ?? '')
  }).join('')
}
