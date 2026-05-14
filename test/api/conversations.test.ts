import request from 'supertest'
import express from 'express'
import { conversationRoutes } from '../../src/api/routes/conversations.js'
import { authMiddleware } from '../../src/api/middleware/auth.js'
import { errorMiddleware } from '../../src/api/middleware/error.js'

function createTestApp() {
  const app = express()
  app.use(express.json())
  app.use(authMiddleware)
  app.use('/api/conversations', conversationRoutes())
  app.use(errorMiddleware)
  return app
}

describe('POST /api/conversations', () => {
  let app: express.Express

  beforeAll(() => {
    app = createTestApp()
  })

  it('should create a new conversation', async () => {
    const response = await request(app)
      .post('/api/conversations')
      .expect('Content-Type', /json/)
      .expect(201)

    expect(response.body).toHaveProperty('conversationId')
    expect(response.body).toHaveProperty('sessionId')
    expect(response.body).toHaveProperty('workspaceId', 1)
    expect(response.body).toHaveProperty('userId', 1)

    expect(typeof response.body.conversationId).toBe('string')
    expect(typeof response.body.sessionId).toBe('string')
  })

  it('should use userId from header when provided', async () => {
    const response = await request(app)
      .post('/api/conversations')
      .set('X-PEE-User-ID', '42')
      .expect(201)

    expect(response.body.userId).toBe(42)
  })
})

describe('GET /api/conversations', () => {
  let app: express.Express

  beforeAll(() => {
    app = createTestApp()
  })

  it('should return list of conversations', async () => {
    const response = await request(app)
      .get('/api/conversations')
      .expect('Content-Type', /json/)
      .expect(200)

    expect(Array.isArray(response.body)).toBe(true)
  })
})

describe('GET /api/conversations/:id/history', () => {
  let app: express.Express
  let conversationId: string

  beforeAll(async () => {
    app = createTestApp()
    const createResponse = await request(app)
      .post('/api/conversations')
    conversationId = createResponse.body.conversationId
  })

  it('should return 404 for non-existent conversation', async () => {
    await request(app)
      .get('/api/conversations/non-existent/history')
      .expect(404)
  })

  it('should return history for existing conversation', async () => {
    const response = await request(app)
      .get(`/api/conversations/${conversationId}/history`)
      .expect('Content-Type', /json/)
      .expect(200)

    expect(response.body).toHaveProperty('messages')
    expect(Array.isArray(response.body.messages)).toBe(true)
  })
})

describe('PATCH /api/conversations/:id/plans/:planId/optional-fields', () => {
  let app: express.Express
  let conversationId: string
  let planId: string

  beforeAll(async () => {
    app = createTestApp()
    const createResponse = await request(app)
      .post('/api/conversations')
    conversationId = createResponse.body.conversationId
    planId = 'test-plan-id'
  })

  it('should return 400 for missing stepId', async () => {
    await request(app)
      .patch(`/api/conversations/${conversationId}/plans/${planId}/optional-fields`)
      .send({ values: { description: 'test' } })
      .expect(400)
  })

  it('should return 400 for missing values', async () => {
    await request(app)
      .patch(`/api/conversations/${conversationId}/plans/${planId}/optional-fields`)
      .send({ stepId: 'test-step' })
      .expect(400)
  })

  it('should return 404 for non-existent plan', async () => {
    await request(app)
      .patch(`/api/conversations/${conversationId}/plans/${planId}/optional-fields`)
      .send({ stepId: 'test-step', values: { description: 'test' } })
      .expect(404)
  })
})
