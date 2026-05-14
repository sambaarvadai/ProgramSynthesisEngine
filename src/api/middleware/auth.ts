import type { Request, Response, NextFunction } from 'express'
import jwt from 'jsonwebtoken'

declare global {
  namespace Express {
    interface Request {
      userId: string
      username: string
      role: string
      workspaceId: number
    }
  }
}

const JWT_SECRET = process.env.JWT_SECRET || 'pee-dev-secret-key'

export function authMiddleware(req: Request, res: Response, next: NextFunction): void {
  const authHeader = req.header('Authorization')
  
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    res.status(401).json({
      error: {
        code: 'UNAUTHORIZED',
        message: 'Missing or invalid authorization header',
      },
    })
    return
  }

  const token = authHeader.substring(7)
  
  try {
    const decoded = jwt.verify(token, JWT_SECRET) as {
      userId: string
      username: string
      role: string
    }
    
    req.userId = decoded.userId
    req.username = decoded.username
    req.role = decoded.role
    req.workspaceId = 1 // Default workspace for now
    
    next()
  } catch (error) {
    res.status(401).json({
      error: {
        code: 'INVALID_TOKEN',
        message: 'Invalid or expired token',
      },
    })
  }
}
