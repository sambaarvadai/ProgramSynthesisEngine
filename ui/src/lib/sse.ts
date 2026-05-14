import type { Plan, ExecutionEvent } from './types'
import { useAuthStore } from '../store/auth'

function getAuthHeaders(): HeadersInit {
  const token = useAuthStore.getState().token
  const headers: HeadersInit = {}
  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }
  return headers
}

export function streamPlan(
  conversationId: string,
  message: string,
  onThinking: (stage: string) => void,
  onPlan: (plan: Plan) => void,
  onError: (err: string) => void,
  onDone: () => void
): () => void {
  const url = `${process.env.NEXT_PUBLIC_API_URL}/api/execute/${conversationId}/plan`
  
  const controller = new AbortController()
  
  fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...getAuthHeaders(),
      'Accept': 'text/event-stream'
    },
    body: JSON.stringify({ message }),
    signal: controller.signal
  }).then(async res => {
    console.log('SSE Response status:', res.status)
    console.log('SSE Response headers:', Object.fromEntries(res.headers.entries()))
    
    if (!res.body) {
      onError('No response body')
      return
    }
    
    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    
    while (true) {
      const { done, value } = await reader.read()
      if (done) { 
        console.log('SSE Stream done')
        onDone(); 
        break 
      }
      
      const chunk = decoder.decode(value, { stream: true })
      console.log('SSE Chunk:', chunk)
      buffer += chunk
      const events = buffer.split('\n\n')
      buffer = events.pop() ?? ''
      
      for (const block of events) {
        console.log('SSE Block:', block)
        const eventLine = block.match(/^event: (.+)$/m)?.[1]
        const dataLine = block.match(/^data: (.+)$/m)?.[1]
        
        console.log('Parsed eventLine:', eventLine, 'dataLine:', dataLine)
        
        // Defensive check: skip malformed blocks
        if (!eventLine && dataLine) {
          console.log('Skipping block: missing event line')
          continue
        }
        if (!eventLine || !dataLine) {
          console.log('Skipping block: missing event or data line')
          continue
        }
        
        const data = JSON.parse(dataLine)
        console.log('SSE Event:', eventLine, data)
        if (eventLine === 'thinking') onThinking(data.stage)
        if (eventLine === 'plan') onPlan(data)
        if (eventLine === 'error') onError(data.message)
        if (eventLine === 'done') onDone()
      }
    }
  }).catch(err => {
    console.error('SSE Error:', err)
    if (err.name !== 'AbortError') onError(err.message)
  })
  
  return () => controller.abort()
}

export function streamExecution(
  conversationId: string,
  planId: string,
  optionalValues: Record<string, Record<string, unknown>>,
  onNodeStart: (nodeId: string, kind: string) => void,
  onNodeComplete: (event: Extract<ExecutionEvent, { event: 'node_complete' }>) => void,
  onPipelineComplete: (event: Extract<ExecutionEvent, { event: 'pipeline_complete' }>) => void,
  onError: (err: string) => void,
  onDone: () => void
): () => void {
  const url = `${process.env.NEXT_PUBLIC_API_URL}/api/execute/${conversationId}/execute`
  
  const controller = new AbortController()
  
  fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...getAuthHeaders(),
      'Accept': 'text/event-stream'
    },
    body: JSON.stringify({ planId, optionalValues }),
    signal: controller.signal
  }).then(async res => {
    const reader = res.body!.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    
    while (true) {
      const { done, value } = await reader.read()
      if (done) { onDone(); break }
      
      buffer += decoder.decode(value, { stream: true })
      const events = buffer.split('\n\n')
      buffer = events.pop() ?? ''
      
      for (const block of events) {
        const eventLine = block.match(/^event: (.+)$/m)?.[1]
        const dataLine = block.match(/^data: (.+)$/m)?.[1]
        
        // Defensive check: skip malformed blocks
        if (!eventLine && dataLine) continue
        if (!eventLine || !dataLine) continue
        
        const data = JSON.parse(dataLine)
        if (eventLine === 'node_start') onNodeStart(data.nodeId, data.kind)
        if (eventLine === 'node_complete') onNodeComplete(data)
        if (eventLine === 'pipeline_complete') onPipelineComplete(data)
        if (eventLine === 'error') onError(data.message)
        if (eventLine === 'done') onDone()
      }
    }
  }).catch(err => {
    if (err.name !== 'AbortError') onError(err.message)
  })
  
  return () => controller.abort()
}
