'use client'

import { useState, useEffect } from 'react'
import { useConversationStore } from '@/store/conversation'
import { useExecutionStore } from '@/store/execution'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { api } from '@/lib/api'

export function OptionalForm() {
  const currentPlan = useConversationStore((state) => state.currentPlan)
  const pendingConfirmation = useConversationStore((state) => state.pendingConfirmation)
  const activeConversationId = useConversationStore((state) => state.activeConversationId)
  const startExecution = useExecutionStore((state) => state.startExecution)
  const stopExecution = useExecutionStore((state) => state.stopExecution)
  const setPipelineResult = useExecutionStore((state) => state.setPipelineResult)
  const setPendingConfirmation = useConversationStore((state) => state.setPendingConfirmation)
  const setCurrentPlan = useConversationStore((state) => state.setCurrentPlan)

  const [values, setValues] = useState<Record<string, Record<string, unknown>>>({})
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [shouldAutoExecute, setShouldAutoExecute] = useState(false)

  useEffect(() => {
    if (pendingConfirmation && currentPlan) {
      // Initialize form when pending confirmation
      const initialValues: Record<string, Record<string, unknown>> = {}
      currentPlan.steps.forEach((step) => {
        if (step.optionalFields && step.optionalFields.length > 0) {
          initialValues[step.id] = {}
        }
      })
      setValues(initialValues)
      
      // Check if we should auto-execute (no optional fields)
      const writeSteps = currentPlan.steps.filter(
        (step) => step.optionalFields && step.optionalFields.length > 0
      )
      setShouldAutoExecute(writeSteps.length === 0)
    }
  }, [pendingConfirmation, currentPlan])

  useEffect(() => {
    if (shouldAutoExecute && activeConversationId && currentPlan) {
      const execute = async () => {
        try {
          startExecution()
          const result = await api.executePlan(activeConversationId, currentPlan.planId, {})
          setPipelineResult(result.output)
          
          // Add execution description as a chat message
          if (result.description) {
            useConversationStore.getState().addMessage({
              id: crypto.randomUUID(),
              role: 'assistant',
              content: result.description,
              timestamp: new Date().toISOString()
            })
          }
          
          setShouldAutoExecute(false)
        } catch (err) {
          console.error('Execution error:', err)
        } finally {
          stopExecution()
          setPendingConfirmation(false)
          setCurrentPlan(null)
        }
      }
      execute()
    }
  }, [shouldAutoExecute, activeConversationId, currentPlan])

  if (!pendingConfirmation || !currentPlan) {
    return (
      <div className="p-4 text-sm text-gray-500">
        No optional fields to configure
      </div>
    )
  }

  if (shouldAutoExecute) {
    return <div className="p-4 text-sm">Executing plan...</div>
  }

  const writeSteps = currentPlan.steps.filter(
    (step) => step.optionalFields && step.optionalFields.length > 0
  )

  const handleSubmit = async () => {
    if (!activeConversationId || !currentPlan) return

    setIsSubmitting(true)
    try {
      // Update optional fields for each step
      for (const step of writeSteps) {
        if (values[step.id]) {
          await api.updateOptionalFields(
            currentPlan.planId,
            step.id,
            values[step.id] as Record<string, unknown>
          )
        }
      }

      // Execute plan
      startExecution()
      const result = await api.executePlan(activeConversationId, currentPlan.planId, values)
      setPipelineResult(result.output)
      
      // Add execution description as a chat message
      if (result.description) {
        useConversationStore.getState().addMessage({
          id: crypto.randomUUID(),
          role: 'assistant',
          content: result.description,
          timestamp: new Date().toISOString()
        })
      }
    } catch (error) {
      console.error('Error submitting optional fields:', error)
    } finally {
      stopExecution()
      setIsSubmitting(false)
      setPendingConfirmation(false)
      setCurrentPlan(null)
    }
  }

  const handleSkip = async () => {
    if (!activeConversationId || !currentPlan) return

    startExecution()
    try {
      const result = await api.executePlan(activeConversationId, currentPlan.planId, {})
      setPipelineResult(result.output)
      
      // Add execution description as a chat message
      if (result.description) {
        useConversationStore.getState().addMessage({
          id: crypto.randomUUID(),
          role: 'assistant',
          content: result.description,
          timestamp: new Date().toISOString()
        })
      }
    } catch (err) {
      console.error('Execution error:', err)
    } finally {
      stopExecution()
      setPendingConfirmation(false)
      setCurrentPlan(null)
    }
  }

  return (
    <div className="p-4 space-y-4">
      <h3 className="font-medium">Optional Fields</h3>
      {writeSteps.map((step) => (
        <div key={step.id} className="space-y-2">
          <h4 className="text-sm font-medium">{step.description}</h4>
          {step.optionalFields?.map((field) => (
            <div key={field.column}>
              <label className="text-sm text-gray-600">{field.column}</label>
              <Input
                type={
                  field.type === 'number' || field.type === 'integer'
                    ? 'number'
                    : field.type === 'date'
                    ? 'date'
                    : 'text'
                }
                value={(values[step.id]?.[field.column] as string) || ''}
                onChange={(e) =>
                  setValues((prev) => ({
                    ...prev,
                    [step.id]: { ...prev[step.id], [field.column]: e.target.value }
                  }))
                }
              />
            </div>
          ))}
        </div>
      ))}
      <div className="flex gap-2">
        <Button onClick={handleSubmit} disabled={isSubmitting}>
          Submit
        </Button>
        <Button onClick={handleSkip} variant="outline" disabled={isSubmitting}>
          Skip
        </Button>
      </div>
    </div>
  )
}
