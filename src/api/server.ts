import express from 'express'
import cors from 'cors'
import 'express-async-errors'
import dotenv from 'dotenv'
import { conversationRoutes } from './routes/conversations.js'
import { executeRoutes, setServices } from './routes/execute.js'
import { workspaceRoutes, setWorkspaceServices } from './routes/workspaces.js'
import { authRoutes } from './routes/auth.js'
import { errorMiddleware } from './middleware/error.js'
import { authMiddleware } from './middleware/auth.js'
import { bootstrap } from '../bootstrap.js'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

// Load .env from project root
const __dirname = dirname(fileURLToPath(import.meta.url))
dotenv.config({ path: join(__dirname, '../../.env') })

const app = express()

app.use(cors({ origin: 'http://localhost:3000', credentials: true }))
app.use(express.json())

// Auth routes don't require authentication
app.use('/api/auth', authRoutes())

// Apply auth middleware to protected routes
app.use(authMiddleware)

app.use('/api/conversations', conversationRoutes())
app.use('/api/execute', executeRoutes())
app.use('/api/workspaces', workspaceRoutes())
app.use(errorMiddleware)

async function start() {
  // Bootstrap services
  const services = await bootstrap()
  setServices(services)
  setWorkspaceServices(services)

  const PORT = process.env.API_PORT ?? 3001
  app.listen(PORT, () => {
    console.log(`[PEE API] listening on http://localhost:${PORT}`)
  })
}

start().catch(console.error)
