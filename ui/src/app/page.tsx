'use client'

import { redirect } from 'next/navigation'
import { useAuthStore } from '@/store/auth'

export default function Home() {
  if (typeof window !== 'undefined') {
    const isAuthenticated = useAuthStore.getState().isAuthenticated
    if (isAuthenticated) {
      redirect('/conversations/new')
    } else {
      redirect('/login')
    }
  }
  return null
}
