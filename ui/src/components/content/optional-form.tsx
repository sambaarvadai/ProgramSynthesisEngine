'use client'

import { useState, useEffect } from 'react'
import { useConversationStore } from '@/store/conversation'
import { useExecutionStore } from '@/store/execution'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { api } from '@/lib/api'
import type { OptionalField } from '@/lib/types'

const FK_HINTS: Record<string, string> = {
  pipeline_id: 'Find pipeline IDs by asking "show me all pipelines"',
  stage_id: 'Find stage IDs by asking "show me stages for pipeline [id]"',
  account_id: 'Find account IDs by asking "show me all accounts"',
  contact_id: 'Find contact IDs by asking "show me contacts"',
  user_id: 'Find user IDs by asking "show me all users"',
  owner_user_id: 'Find user IDs by asking "show me all users"',
}

function getFKHint(paramKey: string): string | undefined {
  return FK_HINTS[paramKey.toLowerCase()]
}

function FieldInput({
  field,
  value,
  onChange
}: {
  field: OptionalField
  value: string
  onChange: (val: string) => void
}) {
  if (field.type === 'enum' && field.enumValues?.length) {
    return (
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
      >
        <option value="">Select {field.column}…</option>
        {field.enumValues.map((v) => (
          <option key={v} value={v}>
            {v}
          </option>
        ))}
      </select>
    )
  }

  if (field.type === 'boolean') {
    return (
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm"
      >
        <option value="">Select…</option>
        <option value="true">Yes</option>
        <option value="false">No</option>
      </select>
    )
  }

  return (
    <Input
      type={
        field.type === 'number' || field.type === 'integer'
          ? 'number'
          : field.type === 'date'
          ? 'date'
          : 'text'
      }
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={field.nullable ? 'Optional' : 'Required'}
    />
  )
}

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
  const [params, setParams] = useState<Record<string, string>>({})
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [confirmed, setConfirmed] = useState(false)
  const [validationError, setValidationError] = useState<string | null>(null)

  // Initialize form values
  useEffect(() => {
    if (!pendingConfirmation || !currentPlan) return

    const initialValues: Record<string, Record<string, unknown>> = {}
    currentPlan.steps.forEach((step) => {
      if (step.optionalFields && step.optionalFields.length > 0) {
        initialValues[step.id] = {}
      }
      // Also init for steps with missing columns
      if (step.kind === 'write') {
        initialValues[step.id] = initialValues[step.id] ?? {}
      }
    })
    setValues(initialValues)
  }, [pendingConfirmation, currentPlan])

  // Derived values - compute directly, don't store in state
  const writeStepsWithOptionalFields = currentPlan?.steps.filter(
    (step) => step.optionalFields && step.optionalFields.length > 0
  ) ?? []

  const missingColumnsByStep: Record<string, Array<{ column: string; description: string }>> = {}
  if (currentPlan?.compilationErrors) {
    for (const error of currentPlan.compilationErrors) {
      if (error.stepId && error.missingColumns?.length) {
        missingColumnsByStep[error.stepId] = error.missingColumns.map(col => ({
          column: col.column,
          description: col.description || 'Required field'
        }))
      }
    }
  }

  const hasParams = currentPlan?.params && Object.keys(currentPlan.params).length > 0
  const hasMissingColumns = Object.keys(missingColumnsByStep).length > 0
  const hasOptionalFields = writeStepsWithOptionalFields.length > 0

  // Only auto-execute if there is genuinely nothing for the user to fill in
  const canAutoExecute = !hasOptionalFields && !hasParams && !hasMissingColumns

  // Named async function for execution
  const executeNow = async (optionalValues: Record<string, Record<string, unknown>>) => {
    if (!activeConversationId || !currentPlan) return
    try {
      startExecution()
      const result = await api.executePlan(
        activeConversationId,
        currentPlan.planId,
        optionalValues,
        params
      )
      setPipelineResult(result.output)
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
      setConfirmed(false)
    }
  }

  // Handle auto-execute in a separate effect that only runs when the condition is definitively met
  useEffect(() => {
    if (!pendingConfirmation || !currentPlan || confirmed) return
    if (canAutoExecute) {
      setConfirmed(true)
      executeNow({})
    }
  }, [pendingConfirmation, currentPlan, canAutoExecute])

  // Reset confirmed state when pendingConfirmation goes false
  useEffect(() => {
    if (!pendingConfirmation) {
      setConfirmed(false)
    }
  }, [pendingConfirmation])

  // Form render conditions
  if (!pendingConfirmation || !currentPlan) {
    return (
      <div className="p-6 text-sm text-muted-foreground">
        Confirm a plan to configure options
      </div>
    )
  }

  if (canAutoExecute) {
    return (
      <div className="p-6 text-sm text-muted-foreground flex items-center gap-2">
        <span className="animate-spin">⟳</span>
        Executing…
      </div>
    )
  }

  const handleSubmit = async () => {
    if (!activeConversationId || !currentPlan) return

    // Validate required params
    const missingRequiredParams = Object.keys(currentPlan.params ?? {}).filter(
      key => !params[key] || params[key].trim() === ''
    )
    if (missingRequiredParams.length > 0) {
      setValidationError(`Required fields missing: ${missingRequiredParams.join(', ')}`)
      return
    }

    setIsSubmitting(true)
    setValidationError(null)
    try {
      // Convert numeric string values to numbers for optional fields
      const convertedValues: Record<string, Record<string, unknown>> = {}
      for (const [stepId, stepValues] of Object.entries(values)) {
        convertedValues[stepId] = {}
        for (const [key, value] of Object.entries(stepValues)) {
          // Convert amount, probability_percent, and other numeric fields
          if (key === 'amount' || key === 'probability_percent' || key.endsWith('_percent') || key.endsWith('_id')) {
            convertedValues[stepId][key] = value === '' ? null : Number(value)
          } else {
            convertedValues[stepId][key] = value
          }
        }
      }

      // Update optional fields for each step
      for (const step of writeStepsWithOptionalFields) {
        if (convertedValues[step.id]) {
          await api.updateOptionalFields(
            activeConversationId,
            currentPlan.planId,
            step.id,
            convertedValues[step.id] as Record<string, unknown>
          )
        }
      }

      // Also update missing required fields
      for (const [stepId, columns] of Object.entries(missingColumnsByStep)) {
        if (convertedValues[stepId]) {
          await api.updateOptionalFields(
            activeConversationId,
            currentPlan.planId,
            stepId,
            convertedValues[stepId] as Record<string, unknown>
          )
        }
      }

      // Execute plan with params
      await executeNow(convertedValues)
    } catch (error) {
      console.error('Error submitting optional fields:', error)
    } finally {
      setIsSubmitting(false)
    }
  }

  const handleSkip = async () => {
    if (!activeConversationId || !currentPlan) return

    startExecution()
    try {
      const result = await api.executePlan(activeConversationId, currentPlan.planId, {}, params)
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
      {currentPlan.params && Object.keys(currentPlan.params).length > 0 && (
        <div className="space-y-2">
          <h3 className="font-medium">Parameters</h3>
          {Object.entries(currentPlan.params).map(([paramKey, paramDesc]) => {
            const fkHint = getFKHint(paramKey)
            return (
              <div key={paramKey}>
                <label className="text-sm text-gray-600">{paramKey}</label>
                <p className="text-xs text-gray-500 mb-1">{paramDesc}</p>
                <Input
                  type={paramKey.toLowerCase().includes('date') || paramDesc.toLowerCase().includes('date') ? 'date' : 'text'}
                  value={params[paramKey] || ''}
                  onChange={(e) =>
                    setParams((prev) => ({
                      ...prev,
                      [paramKey]: e.target.value
                    }))
                  }
                  placeholder={`Enter ${paramKey}`}
                />
                {fkHint && (
                  <p className="text-xs text-gray-500 mt-1">
                    💡 {fkHint}
                  </p>
                )}
                <p className="text-xs text-gray-500">{paramDesc}</p>
              </div>
            )
          })}
        </div>
      )}
      {writeStepsWithOptionalFields.length > 0 && (
        <>
          <h3 className="font-medium">Optional Fields</h3>
          {writeStepsWithOptionalFields.map((step) => (
            <div key={step.id} className="space-y-2">
              <h4 className="text-sm font-medium">{step.description}</h4>
              {step.optionalFields?.map((field) => (
                <div key={field.column}>
                  <label className="text-sm text-gray-600">{field.column}</label>
                  <FieldInput
                    field={field}
                    value={(values[step.id]?.[field.column] as string) || ''}
                    onChange={(val) =>
                      setValues((prev) => ({
                        ...prev,
                        [step.id]: { ...prev[step.id], [field.column]: val }
                      }))
                    }
                  />
                </div>
              ))}
            </div>
          ))}
        </>
      )}
      {Object.keys(missingColumnsByStep).length > 0 && (
        <>
          <h3 className="font-medium text-red-600">Required Fields (Missing)</h3>
          {Object.entries(missingColumnsByStep).map(([stepId, columns]) => {
            const step = currentPlan.steps.find((s) => s.id === stepId)
            return (
              <div key={stepId} className="space-y-2 border border-red-200 p-2 rounded">
                <h4 className="text-sm font-medium">{step?.description || stepId}</h4>
                {columns.map((field) => (
                  <div key={field.column}>
                    <label className="text-sm text-gray-600">{field.column} *</label>
                    <p className="text-xs text-gray-500 mb-1">{field.description}</p>
                    <Input
                      type={field.column.toLowerCase().includes('date') ? 'date' : 'text'}
                      value={(values[stepId]?.[field.column] as string) || ''}
                      onChange={(e) =>
                        setValues((prev) => ({
                          ...prev,
                          [stepId]: { ...prev[stepId], [field.column]: e.target.value }
                        }))
                      }
                      placeholder={`Enter ${field.column}`}
                      className="border-red-300"
                    />
                  </div>
                ))}
              </div>
            )
          })}
        </>
      )}
      {validationError && (
        <div className="text-sm text-red-600 bg-red-50 p-3 rounded border border-red-200">
          {validationError}
        </div>
      )}
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
