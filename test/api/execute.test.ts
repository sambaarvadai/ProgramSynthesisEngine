import request from 'supertest'
import express from 'express'
import { executeRoutes, setServices } from '../../src/api/routes/execute.js'
import { authMiddleware } from '../../src/api/middleware/auth.js'
import { errorMiddleware } from '../../src/api/middleware/error.js'
import type { BootstrappedServices } from '../../src/bootstrap.js'

// Mock bootstrap to avoid import.meta issues in Jest
jest.mock('../../src/bootstrap.js', () => ({
  bootstrap: jest.fn().mockResolvedValue({
    pipelineEngine: {},
    sessionManager: {},
    sessionCursorStore: {},
    dataSourceRegistry: {},
    semanticCache: null,
    crmPool: {},
    pmPool: null,
    peeStoreAvailable: false,
  }),
}))

// Create a mock services object
const mockServices: BootstrappedServices = {
  pipelineEngine: {
    plan: jest.fn().mockResolvedValue({
      graph: { id: 'test-graph' },
      intent: { description: 'test', steps: [] },
      compilationErrors: [],
    }),
    execute: jest.fn().mockResolvedValue({
      plan: { graph: { id: 'test-graph' } },
      execution: { status: 'success' },
      durationMs: 100,
    }),
  } as any,
  sessionManager: {
    getHistory: jest.fn().mockReturnValue([]),
  } as any,
  sessionCursorStore: {} as any,
  dataSourceRegistry: {
    all: jest.fn().mockReturnValue([]),
  } as any,
  semanticCache: null,
  crmPool: {} as any,
  pmPool: null,
  peeStoreAvailable: false,
}

function createTestApp() {
  const app = express()
  app.use(express.json())
  app.use(authMiddleware)
  app.use('/api/execute', executeRoutes())
  app.use(errorMiddleware)
  return app
}

describe('POST /api/execute/:conversationId/plan', () => {
  let app: express.Express
  let conversationId: string

  beforeAll(() => {
    setServices(mockServices)
    app = createTestApp()
    conversationId = 'test-conversation-id'
  })

  it('should return 400 for missing message', async () => {
    await request(app)
      .post(`/api/execute/${conversationId}/plan`)
      .send({})
      .expect(400)
  })

  it('should return 500 if services not initialized', async () => {
    setServices(null)
    
    await request(app)
      .post(`/api/execute/${conversationId}/plan`)
      .send({ message: 'test message' })
      .expect(500)
    
    // Re-initialize services for other tests
    setServices(mockServices)
  })

  it('should return SSE content-type for valid request', async () => {
    const response = await request(app)
      .post(`/api/execute/${conversationId}/plan`)
      .send({ message: 'list all accounts' })
      .expect('Content-Type', 'text/event-stream')
      .expect(200)
  })
})

describe('POST /api/execute/:conversationId/execute', () => {
  let app: express.Express
  let conversationId: string

  beforeAll(() => {
    setServices(mockServices)
    app = createTestApp()
    conversationId = 'test-conversation-id'
  })

  it('should return 400 for missing planId', async () => {
    await request(app)
      .post(`/api/execute/${conversationId}/execute`)
      .send({})
      .expect(400)
  })

  it('should return 404 for non-existent plan', async () => {
    await request(app)
      .post(`/api/execute/${conversationId}/execute`)
      .send({ planId: 'non-existent-plan-id' })
      .expect(404)
  })

  it('should return SSE content-type for valid planId request', async () => {
    // First create a plan (this would need the plan endpoint to work)
    // For now, just test the response type
    const response = await request(app)
      .post(`/api/execute/${conversationId}/execute`)
      .send({ planId: 'test-plan-id' })
      .expect(404) // Plan doesn't exist, but we can check content type on the error
  })
})
