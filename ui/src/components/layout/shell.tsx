'use client'

import { Topbar } from './topbar'

export function Shell({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-screen flex flex-col overflow-hidden">
      <Topbar />
      <div className="flex-1 flex overflow-hidden">
        {children}
      </div>
    </div>
  )
}
