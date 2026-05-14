'use client'

import { useEffect } from 'react'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
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

  return (
    <div className="border-l bg-white flex flex-col flex-1 min-w-0">
      <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col">
        <div className="border-b">
          <TabsList className="w-full justify-start rounded-none h-12 px-2">
            <TabsTrigger value="form">Form</TabsTrigger>
            <TabsTrigger value="preview">Preview</TabsTrigger>
            <TabsTrigger value="plan">Plan</TabsTrigger>
          </TabsList>
        </div>
        <TabsContent value="form" className="flex-1 overflow-y-auto m-0">
          <OptionalForm />
        </TabsContent>
        <TabsContent value="preview" className="flex-1 overflow-y-auto m-0">
          <PreviewTab />
        </TabsContent>
        <TabsContent value="plan" className="flex-1 overflow-y-auto m-0">
          <PlanTab />
        </TabsContent>
      </Tabs>
    </div>
  )
}
