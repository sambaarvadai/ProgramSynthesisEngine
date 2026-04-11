import type { NodeDefinition } from '../../core/registry/node-registry.js'
import type { HttpPayload } from '../payloads.js'
import type { RowSet, Row } from '../../core/types/value.js'
import type { DataValue } from '../../core/types/data-value.js'
import { validationOk, validationFail } from '../../core/types/validation.js'
import { ExprEvaluator } from '../../executors/expr-evaluator.js'

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

      const outputRows: Row[] = []

      for (const row of inputRowSet.rows) {
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

        // Build body
        let body: string | undefined
        if (payload.body) {
          const bodyValue = evaluator.evaluate(payload.body, ctx.scope, row)
          body = typeof bodyValue === 'string' ? bodyValue : JSON.stringify(bodyValue)
        }

        // Execute with retry
        const maxRetries = payload.retryPolicy?.maxRetries ?? 0
        const backoffMs = payload.retryPolicy?.backoffMs ?? 1000
        let lastError: Error | undefined
        let response: any

        for (let attempt = 0; attempt <= maxRetries; attempt++) {
          try {
            const res = await fetch(url, {
              method: payload.method,
              headers,
              body: payload.method !== 'GET' ? body : undefined
            })

            if (!res.ok) {
              throw new Error(`HTTP ${res.status}: ${await res.text()}`)
            }

            response = await res.json().catch(() => ({ status: res.status }))
            break
          } catch (err) {
            lastError = err as Error
            if (attempt < maxRetries) {
              await new Promise(r => setTimeout(r, backoffMs * Math.pow(2, attempt)))
            }
          }
        }

        if (lastError && !response) throw lastError

        // Merge response into row
        const responseFields = typeof response === 'object' && response !== null
          ? response
          : { http_response: response }

        outputRows.push({
          ...row,
          http_status: 'success',
          ...responseFields
        })
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
