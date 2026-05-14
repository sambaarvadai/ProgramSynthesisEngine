import { Router, type Request, type Response } from 'express'
import { createConversation, getConversation, listConversations, updatePlanOptionalFields } from '../types.js'
import type { Conversation, ConversationHistory } from '../types.js'

export function conversationRoutes(): Router {
  const router = Router()

  // POST /api/conversations - Create a new conversation
  router.post('/', (req: Request, res: Response) => {
    const conversation = createConversation(req.userId, req.workspaceId)
    res.status(201).json(conversation)
  })

  // GET /api/conversations - List recent conversations
  router.get('/', (req: Request, res: Response) => {
    const conversations = listConversations()
    res.json(conversations)
  })

  // GET /api/conversations/:id/history - Get conversation history
  router.get('/:id/history', (req: Request, res: Response) => {
    const conversation = getConversation(req.params.id)
    
    if (!conversation) {
      res.status(404).json({
        error: {
          code: 'NOT_FOUND',
          message: 'Conversation not found',
        },
      })
      return
    }
    
    // For now, return empty history since we're not persisting messages
    // This would be implemented by querying pee_pipelines table
    const history: ConversationHistory = {
      messages: [],
    }
    
    res.json(history)
  })

  // PATCH /api/conversations/:id/plans/:planId/optional-fields - Update optional field values
  router.patch('/:id/plans/:planId/optional-fields', (req: Request, res: Response) => {
    const { stepId, values } = req.body
    
    if (!stepId || !values) {
      res.status(400).json({
        error: {
          code: 'INVALID_REQUEST',
          message: 'stepId and values are required',
        },
      })
      return
    }
    
    const success = updatePlanOptionalFields(req.params.planId, stepId, values)
    
    if (!success) {
      res.status(404).json({
        error: {
          code: 'PLAN_NOT_FOUND',
          message: 'Plan not found or expired',
        },
      })
      return
    }
    
    res.json({ ok: true })
  })

  return router
}
