import { create } from 'zustand'
import { immer } from 'zustand/middleware/immer'
import type { NodeExecutionState, QueryOutput } from '../lib/types'

interface ExecutionStore {
  isExecuting: boolean
  nodeStates: Record<string, NodeExecutionState>
  pipelineResult: QueryOutput | null
  lastCreatedRow: Record<string, unknown> | null
  activeTab: string

  startExecution: () => void
  stopExecution: () => void
  setNodeState: (nodeId: string, state: Partial<NodeExecutionState>) => void
  setPipelineResult: (result: QueryOutput) => void
  setLastCreatedRow: (row: Record<string, unknown>) => void
  setActiveTab: (tab: string) => void
  resetTab: () => void
  reset: () => void
}

export const useExecutionStore = create<ExecutionStore>()(
  immer((set) => ({
    isExecuting: false,
    nodeStates: {},
    pipelineResult: null,
    lastCreatedRow: null,
    activeTab: 'form',

    startExecution: () => set({ isExecuting: true, nodeStates: {} }),

    stopExecution: () => set({ isExecuting: false }),

    setNodeState: (nodeId, state) =>
      set((draft) => {
        draft.nodeStates[nodeId] = { ...draft.nodeStates[nodeId], ...state }
      }),

    setPipelineResult: (result) => set({ pipelineResult: result }),

    setLastCreatedRow: (row) => set({ lastCreatedRow: row }),

    setActiveTab: (tab) => set({ activeTab: tab }),

    resetTab: () => set({ activeTab: 'form' }),

    reset: () =>
      set({
        isExecuting: false,
        nodeStates: {},
        pipelineResult: null,
        lastCreatedRow: null,
        activeTab: 'form',
      }),
  }))
)
