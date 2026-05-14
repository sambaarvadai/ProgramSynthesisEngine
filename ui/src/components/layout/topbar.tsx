'use client'

import { useAuthStore } from '@/store/auth'
import { Button } from '@/components/ui/button'
import { useRouter } from 'next/navigation'

export function Topbar() {
  const user = useAuthStore((state) => state.user)
  const logout = useAuthStore((state) => state.logout)
  const router = useRouter()

  const handleLogout = () => {
    logout()
    router.push('/login')
  }

  return (
    <div className="h-12 border-b bg-white flex items-center px-4 justify-between">
      <div className="flex items-center gap-2">
        <span className="font-semibold text-sm">PEE</span>
      </div>
      {user && (
        <div className="flex items-center gap-4">
          <div className="text-sm">
            <span className="font-medium">{user.username}</span>
            <span className="text-gray-500 ml-2">({user.role})</span>
          </div>
          <Button variant="outline" size="sm" onClick={handleLogout}>
            Logout
          </Button>
        </div>
      )}
    </div>
  )
}
