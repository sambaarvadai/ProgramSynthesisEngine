import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import { SessionManager } from '../session-manager.js';
import type { PipelineGraph, PipelineMetadata } from '../../core/graph/pipeline-graph.js';

describe('SessionManager', () => {
  test('creates session with unique ID', () => {
    const sessionManager = new SessionManager('test-key', 'test-user');
    const sessionId = sessionManager.getSessionId();
    
    assert.ok(sessionId);
    assert.strictEqual(typeof sessionId, 'string');
    assert.strictEqual(sessionId.length, 36); // UUID length
  });

  test('starts with empty history', () => {
    const sessionManager = new SessionManager('test-key', 'test-user');
    const history = sessionManager.getHistory();
    
    assert.strictEqual(history, '');
  });

  test('adds conversational turn to history', () => {
    const sessionManager = new SessionManager('test-key', 'test-user');
    
    const mockIntent = {
      description: 'Test response',
      steps: [],
      params: {},
    };
    
    const mockPlan = {
      intent: mockIntent,
      graph: {
        id: 'test-graph',
        version: 1,
        nodes: new Map(),
        edges: new Map(),
        entryNode: '_input',
        exitNodes: ['_output'],
        metadata: {
          createdAt: Date.now(),
          description: 'Test pipeline',
          tags: [],
          budget: {}
        }
      } as PipelineGraph,
      compilationErrors: [],
      intentRaw: 'Test response',
    };
    
    sessionManager.addTurn('Hello', mockIntent, mockPlan, true);
    const history = sessionManager.getHistory();
    
    assert.ok(history.includes('User: Hello'));
    assert.ok(history.includes('Assistant: Test response'));
  });

  test('adds workflow turn to history with steps', () => {
    const sessionManager = new SessionManager('test-key', 'test-user');
    
    const mockIntent = {
      description: 'Fetch all customers',
      steps: [
        {
          id: 'get_customers',
          kind: 'query' as const,
          description: 'SELECT * FROM customers',
          dependsOn: [],
        },
      ],
      params: {},
    };
    
    const mockPlan = {
      intent: mockIntent,
      graph: {
        id: 'test-graph',
        version: 1,
        nodes: new Map(),
        edges: new Map(),
        entryNode: '_input',
        exitNodes: ['_output'],
        metadata: {
          createdAt: Date.now(),
          description: 'Test pipeline',
          tags: [],
          budget: {}
        }
      } as PipelineGraph,
      compilationErrors: [],
      intentRaw: 'Fetch all customers',
    };
    
    sessionManager.addTurn('get all customers', mockIntent, mockPlan, false);
    const history = sessionManager.getHistory();
    
    assert.ok(history.includes('User: get all customers'));
    assert.ok(history.includes('Assistant: Fetch all customers'));
    assert.ok(history.includes('Steps:'));
    assert.ok(history.includes('1. [query] get_customers: SELECT * FROM customers'));
  });

  test('provides session info', () => {
    const sessionManager = new SessionManager('test-key', 'test-user');
    const info = sessionManager.getSessionInfo();
    
    assert.ok(info.id);
    assert.ok(info.userId);
    assert.strictEqual(info.userId, 'test-user');
    assert.ok(info.startTime);
    assert.strictEqual(info.turnCount, 0);
    
    // Add a turn and check count
    const mockIntent = { description: 'Test', steps: [], params: {} };
    const mockPlan = {
      intent: mockIntent,
      graph: {
        id: 'test-graph',
        version: 1,
        nodes: new Map(),
        edges: new Map(),
        entryNode: '_input',
        exitNodes: ['_output'],
        metadata: {
          createdAt: Date.now(),
          description: 'Test pipeline',
          tags: [],
          budget: {}
        }
      } as PipelineGraph,
      compilationErrors: [],
      intentRaw: 'Test',
    };
    
    sessionManager.addTurn('test', mockIntent, mockPlan, true);
    const updatedInfo = sessionManager.getSessionInfo();
    assert.strictEqual(updatedInfo.turnCount, 1);
  });
});
