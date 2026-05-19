'use client'

import { useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { OptionalForm } from './optional-form'
import { ResolvedFields } from './resolved-fields'
import { PreviewTab } from './preview-tab'
import { PlanTab } from './plan-tab'
import { useExecutionStore } from '@/store/execution'
import { useConversationStore } from '@/store/conversation'

export function ContentPane() {
  const activeTab = useExecutionStore((state) => state.activeTab)
  const setActiveTab = useExecutionStore((state) => state.setActiveTab)
  const pipelineResult = useExecutionStore((state) => state.pipelineResult)
  const lastCreatedRow = useExecutionStore((state) => state.lastCreatedRow)
  const currentPlan = useConversationStore((state) => state.currentPlan)
  const pendingConfirmation = useConversationStore((state) => state.pendingConfirmation)

  useEffect(() => {
    // Auto-switch to preview tab when results are available
    if (pipelineResult || lastCreatedRow) {
      setActiveTab('preview')
    }
  }, [pipelineResult, lastCreatedRow, setActiveTab])

  useEffect(() => {
    // Auto-switch to plan tab when a plan is generated
    if (currentPlan) {
      setActiveTab('plan')
    }
  }, [currentPlan, setActiveTab])

  useEffect(() => {
    // Auto-switch to form tab when pendingConfirmation becomes true
    if (pendingConfirmation) {
      setActiveTab('form')
    }
  }, [pendingConfirmation, setActiveTab])

  return (
    <div className="border-l bg-white flex flex-col h-full overflow-hidden">
      <div className="border-b flex-shrink-0 h-12 flex items-center px-2 gap-2">
        <Button
          variant={activeTab === 'form' ? 'default' : 'ghost'}
          size="sm"
          onClick={() => setActiveTab('form')}
        >
          Form
        </Button>
        <Button
          variant={activeTab === 'preview' ? 'default' : 'ghost'}
          size="sm"
          onClick={() => setActiveTab('preview')}
        >
          Preview
        </Button>
        <Button
          variant={activeTab === 'plan' ? 'default' : 'ghost'}
          size="sm"
          onClick={() => setActiveTab('plan')}
        >
          Plan
        </Button>
      </div>
      <div className="flex-1 overflow-y-auto min-h-0">
        {activeTab === 'form' && <OptionalForm />}
        {activeTab === 'preview' && <PreviewTab />}
        {activeTab === 'plan' && <PlanTab />}
      </div>
    </div>
  )
}
