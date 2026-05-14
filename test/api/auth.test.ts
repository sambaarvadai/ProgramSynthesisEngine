import request from 'supertest'
import express from 'express'
import { authMiddleware } from '../../src/api/middleware/auth.js'

describe('Auth Middleware', () => {
  let app: express.Express

  beforeAll(() => {
    app = express()
    app.use(authMiddleware)
    app.get('/test', (req, res) => {
      res.json({ userId: req.userId, workspaceId: req.workspaceId })
    })
  })

  it('should default userId to 1 when header is missing', async () => {
    const response = await request(app)
      .get('/test')
      .expect(200)

    expect(response.body.userId).toBe(1)
    expect(response.body.workspaceId).toBe(1)
  })

  it('should use userId from X-PEE-User-ID header when provided', async () => {
    const response = await request(app)
      .get('/test')
      .set('X-PEE-User-ID', '42')
      .expect(200)

    expect(response.body.userId).toBe(42)
    expect(response.body.workspaceId).toBe(1)
  })

  it('should parse userId as integer', async () => {
    const response = await request(app)
      .get('/test')
      .set('X-PEE-User-ID', '123')
      .expect(200)

    expect(response.body.userId).toBe(123)
    expect(typeof response.body.userId).toBe('number')
  })
})
