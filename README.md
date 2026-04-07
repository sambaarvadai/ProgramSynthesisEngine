# ProgramSynthesisEngine

An intent-native pipeline execution engine. Describe a workflow in plain English — the engine plans it, compiles it to an executable graph, and runs it against real data.

```
"fetch the top 5 enterprise customers by ARR, then for each one write a one-sentence sales pitch"
        ↓
Sonnet plans the pipeline
        ↓
Haiku pre-selects relevant tables
        ↓
Sonnet generates structured query intent
        ↓
Deterministic compiler builds physical operator tree
        ↓
Operators execute against Postgres with server-side cursor
        ↓
Haiku enriches each row with AI-generated content
        ↓
Formatted table output in ~4 seconds
```

---

## Architecture

The engine is built in layers. Each layer has a single responsibility and a clean typed interface to the layers around it.

```
┌─────────────────────────────────────────────────────┐
│                   CLI / API Layer                   │
│         Natural language in, results out            │
├─────────────────────────────────────────────────────┤
│              PipelineEngine                         │
│   Orchestrates planning, compilation, execution     │
├──────────────────────┬──────────────────────────────┤
│  PipelineIntent      │  QueryIntent                 │
│  Generator (Sonnet)  │  Generator (Sonnet + Haiku)  │
├──────────────────────┴──────────────────────────────┤
│              PipelineCompiler                       │
│   Deterministic: PipelineIntent → PipelineGraph     │
├─────────────────────────────────────────────────────┤
│              QueryPlanner                           │
│   QueryAST → QueryDAG with optimizations            │
├─────────────────────────────────────────────────────┤
│              Scheduler                              │
│   Drives PipelineGraph: linear, parallel, CFG       │
├─────────────────────────────────────────────────────┤
│           Physical Operators                        │
│   Scan, Filter, Join, Agg, Sort, Limit, Project     │
├─────────────────────────────────────────────────────┤
│           Storage Backends                          │
│        PostgresBackend / SQLiteTempStore            │
└─────────────────────────────────────────────────────┘
```

### Design Principles

**WHAT/HOW separation.** The LLM always operates in the WHAT layer — it declares intent as structured JSON. Deterministic compilers handle the HOW — turning intent into executable plans with no LLM involvement. This means results are reproducible, auditable, and safe from prompt injection in the execution path.

**Open registries.** Node types and functions are registered at startup, not hardcoded. Adding a new node type requires zero changes to core engine code.

**Typed data flow.** Every edge in a pipeline graph carries a typed schema. The compiler validates type compatibility before execution starts.

**CFG-backed execution.** The scheduler is not a simple task queue — it executes a Control Flow Graph with first-class support for conditionals, loops with scope isolation, parallel branches, and merge points.

---

## Core Concepts

### PipelineGraph

The executable unit. A directed graph where nodes are processing steps and edges carry data or control signals.

```
Nodes:   InputNode, QueryNode, TransformNode, LLMNode, HttpNode,
         ConditionalNode, LoopNode, MergeNode, ParallelNode, OutputNode

Edges:   data edges  — carry RowSets between nodes
         control edges — activate/deactivate branches (conditional routing)
```

Graphs are fully serializable to JSON. Every graph can be inspected, versioned, and replayed.

### ExprAST

Every computation in the engine — filter predicates, computed fields, conditional routing predicates, loop bounds, mathematical expressions — is an `ExprAST` node evaluated by a recursive tree-walking interpreter.

```typescript
type ExprAST =
  | { kind: 'Literal';      value: Value }
  | { kind: 'FieldRef';     field: string; table?: string }
  | { kind: 'VarRef';       name: string }
  | { kind: 'BinaryOp';     op: BinaryOperator; left: ExprAST; right: ExprAST }
  | { kind: 'FunctionCall'; name: string; args: ExprAST[] }
  | { kind: 'Conditional';  condition: ExprAST; then: ExprAST; else: ExprAST }
  | ...
```

New functions register into `FunctionRegistry` — no changes to the evaluator.

### Scope Chain

Variables flow through pipelines via a lexically-scoped chain — the same closure semantics as a programming language. Loop iterations get isolated child scopes. Conditional branches share parent scope but have independent activation state. Nested loops nest scopes.

### Physical Operators

Query execution uses a batch iterator protocol (pull-based, batch-granular):

```typescript
interface PhysicalOperator {
  open(ctx: ExecutionContext): Promise<void>
  nextBatch(size: number): Promise<RowBatch>   // empty = exhausted
  close(): Promise<void>
}
```

Each operator wraps its child, transforming the batch stream:

```
ScanOperator     → opens server-side cursor on Postgres, yields batches
FilterOperator   → evaluates ExprAST predicate per row, passes matching rows
JoinOperator     → hash join: build phase on left, probe phase on right
AggOperator      → accumulates groups, finalizes on exhaustion
ProjectOperator  → evaluates projection expressions, builds output rows
SortOperator     → external merge sort with TempStore spill
LimitOperator    → short-circuits after N rows
```

---

## Project Structure

```
src/
├── core/                         # Engine primitives — no business logic
│   ├── types/                    # EngineType, Value, RowSet, Row, Schema
│   ├── ast/                      # ExprAST type definitions
│   ├── scope/                    # Scope chain — create, push, pop, resolve
│   ├── graph/                    # PipelineGraph, Node, Edge types
│   ├── context/                  # ExecutionContext, Budget, Trace
│   ├── registry/                 # NodeRegistry, FunctionRegistry
│   └── storage/                  # StorageBackend and TempStore interfaces
│
├── compiler/
│   ├── schema/                   # SchemaConfig — table/column/FK metadata
│   ├── query/                    # QueryIntent → QueryAST → QueryDAG
│   │   ├── query-intent.ts       # LLM-facing intent schema
│   │   ├── query-ast.ts          # Relational algebra AST
│   │   ├── query-ast-builder.ts  # Intent → AST (deterministic)
│   │   ├── query-planner.ts      # AST → optimized DAG
│   │   ├── operator-tree-builder.ts  # DAG → physical operators
│   │   ├── query-intent-generator.ts # NL → QueryIntent (Sonnet)
│   │   └── table-pre-selector.ts # Schema narrowing (Haiku)
│   └── pipeline/
│       ├── pipeline-intent.ts    # LLM-facing pipeline intent schema
│       ├── pipeline-compiler.ts  # Intent → PipelineGraph (deterministic)
│       └── pipeline-intent-generator.ts  # NL → PipelineIntent (Sonnet)
│
├── executors/
│   ├── expr-evaluator.ts         # ExprAST tree-walking interpreter
│   ├── query-executor.ts         # Wires query compilation → execution
│   └── operators/                # Physical operator implementations
│       ├── scan-operator.ts
│       ├── filter-operator.ts
│       ├── join-operator.ts
│       ├── agg-operator.ts
│       ├── project-operator.ts
│       ├── sort-operator.ts
│       └── limit-operator.ts
│
├── scheduler/
│   ├── scheduler.ts              # CFG execution engine
│   ├── graph-utils.ts            # Topological sort, edge traversal, validation
│   ├── loop-helpers.ts           # Iteration, scope forking, accumulation
│   └── types.ts                  # ExecutionState, SchedulerConfig, Events
│
├── nodes/
│   ├── payloads.ts               # All node payload type definitions
│   └── definitions/              # NodeDefinition implementations
│       ├── input-node.ts
│       ├── output-node.ts
│       ├── query-node.ts
│       ├── transform-node.ts
│       ├── llm-node.ts
│       ├── conditional-node.ts
│       ├── loop-node.ts
│       ├── merge-node.ts
│       └── parallel-node.ts
│
├── storage/
│   ├── postgres-backend.ts       # StorageBackend for Postgres
│   └── sqlite-temp-store.ts      # TempStore for intermediates
│
├── functions/
│   └── builtin.ts                # Math, string, date built-in functions
│
├── config/
│   ├── models.ts                 # Model name constants
│   └── crm-schema.ts             # CRM SchemaConfig (demo schema)
│
├── scripts/
│   └── verify-db.ts              # DB setup verification
│
├── pipeline-engine.ts            # High-level engine entry point
├── cli.ts                        # Interactive CLI
└── index.ts                      # Public exports
```

---

## Getting Started

### Prerequisites

- Node.js 20+
- Docker (for Postgres)
- Anthropic API key

### Install

```bash
git clone <repo>
cd ProgramExecutionEngine
npm install
```

### Configure

```bash
cp .env.example .env
# Edit .env:
#   ANTHROPIC_API_KEY=sk-ant-...
#   DATABASE_URL=postgresql://pee_user:pee_password@localhost:5432/pee_dev
```

### Start the database

```bash
npm run db:up       # starts Postgres in Docker
npm run db:verify   # confirms tables and row counts
```

Expected output:
```
✔ customers: 50 rows
✔ products: 10 rows
✔ orders: 180 rows
✔ order_items: 360 rows
✔ support_tickets: 110 rows
```

### Run the CLI

```bash
npm run cli
```

```
🔧 ProgramExecutionEngine CLI
Describe a workflow and the engine will plan and execute it.
Type "exit" to quit.

🗄️  Connected to Postgres: postgresql://***@localhost:5432/pee_dev
📊 Schema: customers, products, orders, order_items, support_tickets

PEE> fetch all enterprise customers ordered by ARR descending

📋 Planning...

Pipeline: Fetch all enterprise customers ordered by ARR descending
Steps:
  1. [query] fetch_enterprise_customers: Query customers where segment='enterprise', ORDER BY arr DESC
     Depends on: none

Execute this plan? (y/n/refine) y

⚡ Executing...
✅ Completed in 18ms

Outputs:
  _output (10 rows):
  name              | segment    | region   | arr
  Stark Industries  | enterprise | us-west  | 120000.00
  Massive Dynamic   | enterprise | us-east  | 108000.00
  Umbrella Ltd      | enterprise | us-east  | 96000.00
  ...
```

### Database management

```bash
npm run db:up       # start Postgres
npm run db:down     # stop Postgres
npm run db:reset    # wipe and recreate (re-runs seed data)
npm run db:psql     # open psql shell
npm run db:verify   # verify tables and row counts
```

---

## Example Queries

### Simple queries

```
fetch all products sorted by price descending

show all cancelled orders

list customers in the eu-west region

show all open support tickets with high or critical priority
```

### Aggregations

```
count orders by status

show total revenue by region from completed orders

what is the average order value per customer segment

show the top 5 products by number of order items
```

### Joins

```
show all orders with customer names and regions

list open support tickets with the customer name and segment

show all enterprise customers with their total order count
```

### Transforms

```
fetch all customers and rename arr to annual_revenue

get all completed orders and add a field is_large set to true if total is over 1000

fetch products and add a field price_tier: premium if price over 200 otherwise standard
```

### AI enrichment

```
fetch the top 5 customers by ARR and write a one-sentence sales pitch for each

get all open critical support tickets and for each one suggest a priority action

fetch the 3 most expensive products and write a marketing tagline for each
```

### Complex pipelines

```
get all enterprise customers with their total completed order value, sorted by revenue descending

fetch open high-priority support tickets joined with customer names, show the 10 oldest first

fetch the top 3 enterprise customers by ARR, then for each write a one-sentence account summary
```

---

## LLM Model Usage

The engine uses different models at different stages based on task complexity and cost:

| Stage | Model | Purpose |
|---|---|---|
| PipelineIntent generation | claude-sonnet-4-20250514 | Understands complex multi-step workflow descriptions |
| QueryIntent generation | claude-sonnet-4-20250514 | Translates natural language to precise SQL-level intent |
| Table pre-selection | claude-haiku-4-5-20251001 | Fast, cheap schema narrowing before main prompt |
| LLM node execution | claude-haiku-4-5-20251001 | Per-row enrichment (fast, cost-controlled) |
| TransformNode enrichment | claude-haiku-4-5-20251001 | Converts step description to transform operations |
| ConditionalNode enrichment | claude-haiku-4-5-20251001 | Converts condition string to ExprAST predicate |

Model names are centralized in `src/config/models.ts`.

---

## Adding a New Node Type

The registry pattern means adding a node type requires no changes to the core engine.

### 1. Define the payload type

```typescript
// src/nodes/payloads.ts
export type MyNodePayload = {
  someConfig: string
  expression?: ExprAST
}
```

### 2. Create the node definition

```typescript
// src/nodes/definitions/my-node.ts
import type { NodeDefinition } from '../../core/registry/node-registry.js'
import type { MyNodePayload } from '../payloads.js'

export const myNodeDefinition: NodeDefinition<MyNodePayload, RowSet, RowSet> = {
  kind: 'my_node',
  displayName: 'My Node',
  inputPorts: [{ key: 'input', label: 'Input', type: { kind: 'any' }, required: true }],
  outputPorts: [{ key: 'output', label: 'Output', type: { kind: 'any' }, required: true }],

  validate(payload) {
    return validationOk()
  },

  inferOutputSchema(payload, inputSchema) {
    return inputSchema
  },

  async execute(payload, input: RowSet, ctx): Promise<RowSet> {
    // transform input rows
    return input
  }
}
```

### 3. Register it

```typescript
// src/nodes/index.ts
import { myNodeDefinition } from './definitions/my-node.js'

export function registerAllNodes(registry: NodeRegistry): void {
  // existing registrations...
  registry.register(myNodeDefinition)
}
```

### 4. Add enrichment (optional)

If the node needs LLM-generated config from a natural language description, add a case in `PipelineEngine.enrichNodes()`.

### 5. Update the PipelineIntentGenerator prompt

Add the new node kind to the system prompt's available node list so Sonnet knows when to use it.

---

## Adding a New Built-in Function

```typescript
// src/functions/builtin.ts
registry.register({
  name: 'MY_FUNCTION',
  inferType: (args) => ({ kind: 'number' }),
  validate: (args) => args.length === 1
    ? { ok: true }
    : { ok: false, errors: [{ code: 'WRONG_ARGS', message: 'MY_FUNCTION takes 1 argument' }] },
  execute: (args) => /* your logic */ args[0]
})
```

The function is immediately available in all ExprAST `FunctionCall` nodes.

---

## Budget and Safety

Every pipeline execution runs under a budget enforced by the scheduler:

```typescript
{
  maxLLMCalls: 20,       // hard cap on Anthropic API calls
  maxIterations: 1000,   // hard cap on loop iterations
  timeoutMs: 60000,      // pipeline-level timeout
  maxRowsPerNode: 10000, // memory pressure guard
  maxMemoryMB: 512       // memory limit
}
```

Budget is configurable per pipeline and per execution. If any limit is exceeded, the pipeline fails with a `BudgetExceededError` — it never silently truncates or continues in a degraded state.

---

## Tests

```bash
# Physical operators
npx tsx src/executors/operators/__tests__/operators.test.ts

# Query pipeline (AST, planner, executor)
npx tsx src/compiler/query/__tests__/query-pipeline.test.ts

# Scheduler (linear, parallel, CFG, loops, conditionals)
npx tsx src/scheduler/__tests__/scheduler.test.ts
```

Current status: **34/34 tests passing**.

| Suite | Tests | Coverage |
|---|---|---|
| operators | 9/9 | Scan, Filter, Join, Agg, Sort, Limit, Project, chains |
| query-pipeline | 8/8 | AST builder, planner, optimizer, executor, explain |
| scheduler | 17/17 | Linear, parallel, error policies, budget, CFG, loops, nested conditionals |

---

## What's Not Built Yet

The following are designed in the architecture but not yet implemented:

- **HttpNode** — external API calls per row or batch
- **FileNode** — read CSV/JSON/Parquet from disk
- **WriteFileNode** — write results to files
- **SubPipelineNode** — compose pipelines from other pipelines
- **Pipeline persistence** — save/load named pipelines to SQLite
- **EmbedNode** — vector embeddings via Voyage
- **VectorSearchNode** — similarity search via ChromaDB
- **SwitchNode** — multi-way conditional routing
- **ValidateNode** — schema and constraint assertions
- **ApprovalNode** — human-in-the-loop confirmation mid-pipeline
- **Streaming output** — yield rows as they arrive

---

## Technical Decisions

**Why physical operators instead of pushing SQL to Postgres?**
Postgres becomes a storage backend, not a query engine. This enables cross-source joins (Postgres + CSV + API), streaming results, and pluggable backends without rewriting query logic.

**Why ExprAST instead of eval() or template strings?**
Safety and portability. ExprAST is validated at compile time, serializable, inspectable, and executable on any backend. No injection surface, no runtime surprises.

**Why a CFG-backed scheduler instead of a simple task queue?**
Real workflows have branches and loops. A task queue can't express "for each customer, if their ARR is over 100k, run the enterprise analysis, otherwise run the standard one." A CFG can.

**Why open registries for node types and functions?**
The engine should grow without touching core code. Every new capability is an additive registration, not a modification to existing logic.

**Why Haiku for table pre-selection?**
The full schema prompt for 20+ tables with descriptions is expensive for Sonnet. Haiku narrows it to the 3-5 relevant tables in ~100ms at near-zero cost. Sonnet then operates on a focused schema.

**Why SQLite for temp storage?**
Embedded, zero-config, fast for small-to-medium intermediate results. SortOperator and JoinOperator spill to SQLite when data exceeds memory limits. The `TempStore` interface means swapping to Redis or S3 requires only a new implementation.