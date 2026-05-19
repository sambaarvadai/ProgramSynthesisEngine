import { Router, type Request, type Response } from 'express'
import { sseSetup, sseWrite, sseDone } from '../sse.js'
import crypto from 'crypto'
import type { PlanRequest, ExecuteRequest, PlanStep, PlanResponse } from '../types.js'
import { storePlan, getPlan, updatePlanOptionalFields } from '../types.js'
import type { BootstrappedServices } from '../../bootstrap.js'

let services: BootstrappedServices | null = null

export function setServices(s: BootstrappedServices | null): void {
  services = s
}

// Handle plan generation as JSON response (not SSE)
async function handlePlanJson(req: Request, res: Response) {
  console.log('[API] Handling plan as JSON response')
  
  if (!services) {
    res.status(500).json({
      error: {
        code: 'SERVICES_NOT_INITIALIZED',
        message: 'Services not initialized',
      },
    })
    return
  }

  const { message } = req.body as PlanRequest
  if (!message) {
    res.status(400).json({
      error: {
        code: 'INVALID_REQUEST',
        message: 'message is required',
      },
    })
    return
  }

  try {
    // Check semantic cache first
    let planResult
    let isCacheHit = false
    if (services.semanticCache) {
      const cacheHit = await services.semanticCache.lookup(message)
      if (cacheHit) {
        console.log('[API] Cache HIT for plan generation')
        // Re-enrich the cached plan for the current session
        // This ensures session-scoped values and FK aliases are resolved for the current user/workspace
        try {
          const userId = services.sessionManager.getUserId()
          const schema = services.pipelineEngine['config'].schema
          if (!schema) {
            throw new Error('Schema not configured')
          }
          // Re-enrich nodes to resolve session-scoped values and FK aliases
          // enrichNodes mutates the graph in place and returns a fieldMap
          await services.pipelineEngine['enrichNodes'](
            cacheHit.plan.graph,
            cacheHit.intent,
            schema,
            userId,
            services.pipelineEngine['config'].multiSchema
          )
          planResult = cacheHit.plan
          isCacheHit = true
        } catch (e) {
          console.warn('[API] Cache re-enrichment failed, falling through to full plan generation:', e)
          // Fall through to full plan generation if re-enrichment fails
          planResult = await services.pipelineEngine.plan(message, {
            sessionHistory: services.sessionManager.getHistory(),
          })
        }
      } else {
        console.log('[API] Cache MISS, generating new plan')
        // Plan the pipeline
        planResult = await services.pipelineEngine.plan(message, {
          sessionHistory: services.sessionManager.getHistory(),
        })
      }
    } else {
      console.log('[API] Semantic cache not available, generating new plan')
      // Plan the pipeline
      planResult = await services.pipelineEngine.plan(message, {
        sessionHistory: services.sessionManager.getHistory(),
      })
    }
    console.log('[API] Plan result received:', planResult)

    // Convert to API response format
    const planId = crypto.randomUUID()
    const steps: PlanStep[] = planResult.intent.steps.map(step => {
      const stepConfig = step.config as any
      return {
        id: step.id,
        kind: step.kind,
        description: step.description,
        datasource: stepConfig?.datasource || 'default',
        dependsOn: [],
        optionalFields: stepConfig?.optionalFields || [],
        resolvedFields: stepConfig?.resolvedFields || [],
      }
    })

    const planResponse: PlanResponse = {
      planId,
      description: planResult.intent.description,
      steps,
      estimatedLLMCalls: 0,
      compilationErrors: planResult.compilationErrors.map(err => ({
        code: err.code,
        message: err.message,
        stepId: err.stepId,
        missingColumns: err.missingColumns?.map(col => {
          // Handle different shapes of missingColumns
          if ('column' in col && 'nullable' in col) {
            return {
              column: col.column,
              nullable: col.nullable ?? true,
              description: col.description || `Required field for ${err.stepId}`
            }
          } else if ('column' in col) {
            // WRITE_FIELD_UNRESOLVABLE format
            return {
              column: col.column,
              nullable: true,
              description: (col as any).suggestion || `Required field for ${err.stepId}`
            }
          } else if ('name' in col) {
            // Alternative format with 'name' instead of 'column'
            return {
              column: col.name,
              nullable: (col as any).required !== true,
              description: `Required field for ${err.stepId}`
            }
          }
          // Fallback
          return {
            column: String(col),
            nullable: true,
            description: `Required field for ${err.stepId}`
          }
        }) ?? []
      })),
      params: planResult.intent.params,
    }

    // Store the plan for execution
    storePlan(planId, planResult.graph, planResult)

    // Store in semantic cache if available and plan was newly generated (not from cache)
    if (services.semanticCache && !isCacheHit) {
      try {
        const sourcesTouched = planResult.intent.steps
          .map(step => (step.config as any)?.datasource || 'default')
          .filter((v, i, a) => a.indexOf(v) === i) // unique
        await services.semanticCache.store(
          message,
          planResult.intent,
          planResult,
          sourcesTouched,
          planId
        )
        console.log('[API] Plan stored in semantic cache')
      } catch (e) {
        console.warn('[API] Failed to store plan in semantic cache:', e)
      }
    }

    console.log('[API] Returning plan as JSON')
    res.json(planResponse)
  } catch (error) {
    console.error('[API] Plan Error:', error)
    res.status(500).json({
      error: {
        code: 'PLAN_ERROR',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
    })
  }
}

// Handle execution as JSON response (not SSE)
async function handleExecuteJson(req: Request, res: Response) {
  console.log('[API] Handling execution as JSON response')
  
  if (!services) {
    res.status(500).json({
      error: {
        code: 'SERVICES_NOT_INITIALIZED',
        message: 'Services not initialized',
      },
    })
    return
  }

  const { planId, optionalValues, params } = req.body as ExecuteRequest
  if (!planId) {
    res.status(400).json({
      error: {
        code: 'INVALID_REQUEST',
        message: 'planId is required',
      },
    })
    return
  }

  const stored = getPlan(planId)
  if (!stored) {
    res.status(404).json({
      error: {
        code: 'PLAN_NOT_FOUND',
        message: 'Plan not found',
      },
    })
    return
  }

  try {
    console.log('[API] Starting pipeline execution')
    const startTime = Date.now()

    const result = await services.pipelineEngine.execute(stored.plan, params || {})

    const durationMs = Date.now() - startTime
    console.log('[API] Pipeline execution complete, duration:', durationMs)

    // Extract the output from the result
    const outputs = result.execution.outputs as Map<string, any>
    console.log('[API] Outputs:', outputs)
    
    const exitNodeOutput = outputs?.get('_output')
    console.log('[API] Exit node output:', exitNodeOutput)
    
    const rows = exitNodeOutput?.data?.rows || []
    const schema = exitNodeOutput?.schema?.columns || exitNodeOutput?.data?.schema || []
    
    console.log('[API] Extracted rows:', rows.length, 'schema:', schema.length)

    // Store execution result in semantic cache if available and execution was successful
    if (services.semanticCache && result.execution.status === 'success') {
      try {
        // Get the original message from the stored plan
        const originalMessage = stored.plan.intent?.description || ''
        if (originalMessage) {
          const sourcesTouched = stored.plan.intent?.steps
            ?.map((step: any) => (step.config as any)?.datasource || 'default')
            .filter((v: string, i: number, a: string[]) => a.indexOf(v) === i) || []
          await services.semanticCache.store(
            originalMessage,
            stored.plan.intent,
            stored.plan,
            sourcesTouched,
            result.plan.graph.id
          )
          console.log('[API] Execution result stored in semantic cache')
        }
      } catch (e) {
        console.warn('[API] Failed to store execution result in semantic cache:', e)
      }
    }

    // Generate execution description
    let description = ''
    const steps = stored.plan.intent?.steps || []
    const writeSteps = steps.filter((s: any) => s.kind === 'write')
    const querySteps = steps.filter((s: any) => s.kind === 'query')
    
    if (writeSteps.length > 0) {
      const writeStep = writeSteps[0]
      const table = (writeStep.config as any)?.table || 'table'
      const mode = (writeStep.config as any)?.mode || 'insert'
      const rowCount = rows.length || 0
      if (mode === 'insert' || mode === 'insert_ignore') {
        description = `Inserted ${rowCount} row${rowCount !== 1 ? 's' : ''} into ${table}`
      } else if (mode === 'update') {
        description = `Updated ${rowCount} row${rowCount !== 1 ? 's' : ''} in ${table}`
      } else if (mode === 'delete') {
        description = `Deleted ${rowCount} row${rowCount !== 1 ? 's' : ''} from ${table}`
      } else {
        description = `Executed ${mode} on ${table}`
      }
    } else if (querySteps.length > 0) {
      const queryStep = querySteps[0]
      const table = (queryStep.config as any)?.table || 'table'
      const rowCount = rows.length || 0
      description = `Fetched ${rowCount} row${rowCount !== 1 ? 's' : ''} from ${table}`
    } else {
      description = `Execution completed in ${durationMs}ms`
    }

    res.json({
      pipelineId: result.plan.graph.id,
      durationMs,
      status: result.execution.status,
      description,
      output: {
        rows,
        schema,
      },
    })
  } catch (error) {
    console.error('[API] Execute Error:', error)
    res.status(500).json({
      error: {
        code: 'EXECUTE_ERROR',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
    })
  }
}

export function executeRoutes(): Router {
  const router = Router()

  // POST /api/execute/:conversationId/plan - Plan with SSE streaming
  router.post('/:conversationId/plan', (req: Request, res: Response) => {
    console.log('[API] POST /api/execute/:conversationId/plan called')
    console.log('[API] conversationId:', req.params.conversationId)
    console.log('[API] body:', req.body)
    
    // Check if client wants JSON response instead of SSE
    const acceptJson = req.headers.accept === 'application/json'
    
    if (acceptJson) {
      // Return JSON response instead of SSE
      handlePlanJson(req, res)
      return
    }

    if (!services) {
      console.log('[API] ERROR: Services not initialized')
      res.status(500).json({
        error: {
          code: 'SERVICES_NOT_INITIALIZED',
          message: 'Services not initialized',
        },
      })
      return
    }

    const { message } = req.body as PlanRequest
    if (!message) {
      console.log('[API] ERROR: message is required')
      res.status(400).json({
        error: {
          code: 'INVALID_REQUEST',
          message: 'message is required',
        },
      })
      return
    }

    console.log('[API] Message received:', message)
    sseSetup(res)

    // Handle client disconnect
    req.on('close', () => {
      console.log('[API] Client disconnected')
      res.end()
    })

    // Execute planning with SSE events
    ;(async () => {
      try {
        console.log('[API] Starting plan generation...')
        const { pipelineEngine, sessionManager, semanticCache } = services!

        // Check semantic cache first
        let planResult
        let isCacheHit = false
        if (semanticCache) {
          const cacheHit = await semanticCache.lookup(message)
          if (cacheHit) {
            console.log('[API] Cache HIT for plan generation (SSE)')
            // Re-enrich the cached plan for the current session
            try {
              const userId = sessionManager.getUserId()
              const schema = pipelineEngine['config'].schema
              if (!schema) {
                throw new Error('Schema not configured')
              }
              // Re-enrich nodes to resolve session-scoped values and FK aliases
              await pipelineEngine['enrichNodes'](
                cacheHit.plan.graph,
                cacheHit.intent,
                schema,
                userId,
                pipelineEngine['config'].multiSchema
              )
              planResult = cacheHit.plan
              isCacheHit = true
            } catch (e) {
              console.warn('[API] Cache re-enrichment failed (SSE), falling through to full plan generation:', e)
              // Fall through to full plan generation if re-enrichment fails
              planResult = null
            }
          } else {
            console.log('[API] Cache MISS, generating new plan (SSE)')
          }
        }

        if (!planResult) {
          // Stream thinking events
          console.log('[API] Sending thinking events')
          sseWrite(res, 'thinking', { stage: 'pre_selecting_tables' })
          sseWrite(res, 'thinking', { stage: 'generating_intent' })

          // Plan the pipeline
          console.log('[API] Calling pipelineEngine.plan()')
          planResult = await pipelineEngine.plan(message, {
            sessionHistory: sessionManager.getHistory(),
          })
        }
        console.log('[API] Plan result received:', planResult)

        // Convert to API response format
        console.log('[API] Converting plan to response format')
        const planId = crypto.randomUUID()
        const steps: PlanStep[] = planResult.intent.steps.map(step => {
          const stepConfig = step.config as any
          return {
            id: step.id,
            kind: step.kind,
            description: step.description,
            datasource: stepConfig?.datasource || 'default',
            dependsOn: [],
            optionalFields: stepConfig?.optionalFields || [],
            resolvedFields: stepConfig?.resolvedFields || [],
          }
        })

        const planResponse: PlanResponse = {
          planId,
          description: planResult.intent.description,
          steps,
          estimatedLLMCalls: 0,
          compilationErrors: planResult.compilationErrors.map(err => ({
            code: err.code,
            message: err.message,
            stepId: err.stepId,
            missingColumns: err.missingColumns?.map(col => {
              // Handle different shapes of missingColumns
              if ('column' in col && 'nullable' in col) {
                return {
                  column: col.column,
                  nullable: col.nullable ?? true,
                  description: col.description || `Required field for ${err.stepId}`
                }
              } else if ('column' in col) {
                // WRITE_FIELD_UNRESOLVABLE format
                return {
                  column: col.column,
                  nullable: true,
                  description: (col as any).suggestion || `Required field for ${err.stepId}`
                }
              } else if ('name' in col) {
                // Alternative format with 'name' instead of 'column'
                return {
                  column: col.name,
                  nullable: (col as any).required !== true,
                  description: `Required field for ${err.stepId}`
                }
              }
              // Fallback
              return {
                column: String(col),
                nullable: true,
                description: `Required field for ${err.stepId}`
              }
            }) ?? []
          })),
          params: planResult.intent.params,
        }

        console.log('[API] Plan response:', planResponse)

        // Store the plan for execution
        storePlan(planId, planResult.graph, planResult)

        // Store in semantic cache if available and plan was newly generated (not from cache)
        if (semanticCache && !isCacheHit) {
          try {
            const sourcesTouched = planResult.intent.steps
              .map(step => (step.config as any)?.datasource || 'default')
              .filter((v, i, a) => a.indexOf(v) === i) // unique
            await semanticCache.store(
              message,
              planResult.intent,
              planResult,
              sourcesTouched,
              planId
            )
            console.log('[API] Plan stored in semantic cache (SSE)')
          } catch (e) {
            console.warn('[API] Failed to store plan in semantic cache (SSE):', e)
          }
        }

        console.log('[API] Sending plan event')
        sseWrite(res, 'plan', planResponse)
        // Small delay to ensure the plan event is sent before closing
        await new Promise(resolve => setTimeout(resolve, 100))
        sseDone(res)
        console.log('[API] Plan generation complete')
      } catch (error) {
        console.error('[API] Plan Error:', error)
        sseWrite(res, 'error', {
          code: 'PLAN_ERROR',
          message: error instanceof Error ? error.message : 'Unknown error',
        })
        sseDone(res)
      }
    })()
  })

  // POST /api/execute/:conversationId/execute - Execute with SSE streaming
  router.post('/:conversationId/execute', (req: Request, res: Response) => {
    console.log('[API] POST /api/execute/:conversationId/execute called')
    console.log('[API] conversationId:', req.params.conversationId)
    console.log('[API] body:', req.body)
    
    // Check if client wants JSON response instead of SSE
    const acceptJson = req.headers.accept === 'application/json'
    
    if (acceptJson) {
      // Return JSON response instead of SSE
      handleExecuteJson(req, res)
      return
    }

    if (!services) {
      console.log('[API] ERROR: Services not initialized')
      res.status(500).json({
        error: {
          code: 'SERVICES_NOT_INITIALIZED',
          message: 'Services not initialized',
        },
      })
      return
    }

    const { planId, optionalValues, params } = req.body as ExecuteRequest
    if (!planId) {
      console.log('[API] ERROR: planId is required')
      res.status(400).json({
        error: {
          code: 'INVALID_REQUEST',
          message: 'planId is required',
        },
      })
      return
    }

    console.log('[API] Executing plan:', planId)
    if (optionalValues) {
      console.log('[API] Optional values:', optionalValues)
      // Apply optional field values if provided
      for (const [stepId, values] of Object.entries(optionalValues)) {
        updatePlanOptionalFields(planId, stepId, values)
      }
    }

    const stored = getPlan(planId)
    if (!stored) {
      console.log('[API] ERROR: Plan not found')
      res.status(404).json({
        error: {
          code: 'PLAN_NOT_FOUND',
          message: 'Plan not found or expired',
        },
      })
      return
    }

    console.log('[API] Plan found, starting execution')
    sseSetup(res)

    // Handle client disconnect
    req.on('close', () => {
      console.log('[API] Client disconnected during execution')
      res.end()
    })

    // Execute with SSE events
    ;(async () => {
      try {
        console.log('[API] Starting pipeline execution')
        const { pipelineEngine, semanticCache } = services!
        const startTime = Date.now()

        const result = await pipelineEngine.execute(stored.plan, params || {})

        const durationMs = Date.now() - startTime
        console.log('[API] Pipeline execution complete, duration:', durationMs)

        // Store execution result in semantic cache if available and execution was successful
        if (semanticCache && result.execution.status === 'success') {
          try {
            // Get the original message from the stored plan
            const originalMessage = stored.plan.intent?.description || ''
            if (originalMessage) {
              const sourcesTouched = stored.plan.intent?.steps
                ?.map((step: any) => (step.config as any)?.datasource || 'default')
                .filter((v: string, i: number, a: string[]) => a.indexOf(v) === i) || []
              await semanticCache.store(
                originalMessage,
                stored.plan.intent,
                stored.plan,
                sourcesTouched,
                result.plan.graph.id
              )
              console.log('[API] Execution result stored in semantic cache (SSE)')
            }
          } catch (e) {
            console.warn('[API] Failed to store execution result in semantic cache (SSE):', e)
          }
        }

        sseWrite(res, 'pipeline_complete', {
          pipelineId: result.plan.graph.id,
          durationMs,
          status: result.execution.status,
          output: {
            rows: [],
            schema: [],
          },
        })

        sseDone(res)
      } catch (error) {
        console.error('[API] Execute Error:', error)
        sseWrite(res, 'error', {
          code: 'EXECUTE_ERROR',
          message: error instanceof Error ? error.message : 'Unknown error',
        })
        sseDone(res)
      }
    })()
  })

  // POST /api/execute/:conversationId/clear-cache - Clear semantic cache
  router.post('/:conversationId/clear-cache', async (req: Request, res: Response) => {
    console.log('[API] POST /api/execute/:conversationId/clear-cache called')

    if (!services) {
      res.status(500).json({
        error: {
          code: 'SERVICES_NOT_INITIALIZED',
          message: 'Services not initialized',
        },
      })
      return
    }

    if (!services.semanticCache) {
      res.status(503).json({
        error: {
          code: 'CACHE_NOT_AVAILABLE',
          message: 'Semantic cache not available',
        },
      })
      return
    }

    try {
      const count = await services.semanticCache.invalidateAll('Manual cache clear requested by user')
      console.log(`[API] Cleared ${count} cache entries`)
      res.json({
        success: true,
        clearedEntries: count,
      })
    } catch (error) {
      console.error('[API] Clear cache error:', error)
      res.status(500).json({
        error: {
          code: 'CLEAR_CACHE_ERROR',
          message: error instanceof Error ? error.message : 'Unknown error',
        },
      })
    }
  })

  return router
}
