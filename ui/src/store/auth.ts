import { create } from 'zustand'

export interface User {
  id: string
  username: string
  role: string
}

interface AuthStore {
  user: User | null
  token: string | null
  isAuthenticated: boolean
  setAuth: (user: User, token: string) => void
  logout: () => void
}

export const useAuthStore = create<AuthStore>((set) => ({
  user: null,
  token: null,
  isAuthenticated: false,
  setAuth: (user, token) => {
    // Store in localStorage for persistence
    localStorage.setItem('pee_token', token)
    localStorage.setItem('pee_user', JSON.stringify(user))
    set({ user, token, isAuthenticated: true })
  },
  logout: () => {
    localStorage.removeItem('pee_token')
    localStorage.removeItem('pee_user')
    set({ user: null, token: null, isAuthenticated: false })
  }
}))

// Initialize auth from localStorage on app load
if (typeof window !== 'undefined') {
  const token = localStorage.getItem('pee_token')
  const userStr = localStorage.getItem('pee_user')
  if (token && userStr) {
    try {
      const user = JSON.parse(userStr) as User
      useAuthStore.getState().setAuth(user, token)
    } catch {
      localStorage.removeItem('pee_token')
      localStorage.removeItem('pee_user')
    }
  }
}
