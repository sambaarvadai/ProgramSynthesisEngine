/**
 * Referential Resolver for detecting natural language references to prior results
 */

import type { SessionCursor } from './SessionCursor.js';

/**
 * Detects whether a natural language input is referencing a prior result
 * Uses word-boundary matching (case insensitive)
 */
export function isReferentialQuery(input: string): boolean {
  if (!input || typeof input !== 'string') {
    return false;
  }

  const lowerInput = input.toLowerCase();

  // Demonstratives
  const demonstratives = ['this', 'these', 'that', 'those', 'it'];
  for (const word of demonstratives) {
    const regex = new RegExp(`\\b${word}\\b`, 'i');
    if (regex.test(lowerInput)) {
      return true;
    }
  }

  // Implicit references
  const implicitReferences = ['the ticket', 'the record', 'the result', 'the same', 'them', 'they'];
  for (const phrase of implicitReferences) {
    const regex = new RegExp(`\\b${phrase}\\b`, 'i');
    if (regex.test(lowerInput)) {
      return true;
    }
  }

  // Positional references
  const positional = ['above', 'previous', 'last one'];
  for (const phrase of positional) {
    const regex = new RegExp(`\\b${phrase}\\b`, 'i');
    if (regex.test(lowerInput)) {
      return true;
    }
  }

  return false;
}

/**
 * Builds a system prompt fragment for the intent generator
 * that includes cursor context for resolving referential queries
 */
export function buildCursorSystemPromptFragment(cursor: SessionCursor): string {
  const contextDetails = cursor.ids
    ? `Resolved IDs: ${JSON.stringify(cursor.ids)}`
    : `Source filter: ${JSON.stringify(cursor.sourceFilter)}`;

  return `PRIOR QUERY CONTEXT:
The user's last query was: "${cursor.description}"
Table: ${cursor.table}
Rows returned: ${cursor.rowCount}
${contextDetails}

If the user's current input references this result using terms like "this", "these", "it", "them" or "the [entity]", resolve the target using the above context. Do NOT emit $ticket_id or similar unresolved parameters — use the actual IDs or filter directly in the plan.`;
}
