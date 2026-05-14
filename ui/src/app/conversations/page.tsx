import { redirect } from 'next/navigation'

export default function ConversationsPage() {
  // For now, redirect to new conversation
  // In the future, this would show a list of conversations
  redirect('/conversations/new')
}
