# Pipeline Architecture Overview

## What's Well Covered

- **Data retrieval**        &rarr; QueryNode (full SQL expressibility)
- **Row transformation**    &rarr; TransformNode (map, filter, rename, cast, dedup)
- **AI enrichment**         &rarr; LLMNode (per-row or per-batch)
- **External calls**        &rarr; HttpNode (defined, not fully implemented)
- **Control flow**          &rarr; ConditionalNode, LoopNode, MergeNode, ParallelNode
- **Pipeline composition**  &rarr; SubPipelineNode (defined, not implemented)

## What's Missing or Incomplete

### 1. Data Sources Beyond Postgres
Right now the only way to get data into a pipeline is via QueryNode hitting Postgres. Real workflows pull from many sources:
- **FileNode**             &rarr; read CSV, JSON, Parquet, Excel from disk or S3
- **StreamNode**           &rarr; consume from Kafka, Redis pub/sub, WebSocket
- **EmailNode**            &rarr; read from Gmail/Outlook inbox (you're connected to Gmail)
- **CalendarNode**         &rarr; read events from Google Calendar (you're connected)
- **WebScrapeNode**        &rarr; fetch and parse a webpage
- **DatabaseNode**         &rarr; generic SQL for MySQL, SQLite, other Postgres instances

### 2. Data Sinks - Writing Output Somewhere
Everything outputs to the CLI right now. Real pipelines write results:
- **WriteFileNode**        &rarr; write CSV, JSON, Parquet to disk or S3
- **WebhookNode**          &rarr; POST results to a URL
- **EmailSendNode**        &rarr; send results as email (you have Gmail MCP)
- **DatabaseWriteNode**    &rarr; INSERT/UPDATE/UPSERT back to Postgres
- **SlackNode**            &rarr; post to a Slack channel
- **NotificationNode**     &rarr; generic push notification

### 3. Data Quality and Validation
No node type handles data quality today:
- **ValidateNode**         &rarr; assert schema constraints, throw on violations
- **DeduplicateNode**      &rarr; cross-batch dedup (TransformNode.dedup is in-memory only)
- **FillNullNode**         &rarr; fill missing values with defaults or computed values
- **SampleNode**           &rarr; random sample N rows or X% of dataset
- **AssertNode**           &rarr; pipeline-level assertions, fail if condition not met
- **ProfilingNode**        &rarr; compute statistics (min, max, mean, nulls, cardinality)

### 4. Data Reshaping
TransformNode handles row-level operations but not structural reshaping:
- **PivotNode**            &rarr; rows to columns (status &rarr; pending_count, completed_count)
- **UnpivotNode**          &rarr; columns to rows (wide &rarr; long format)
- **FlattenNode**          &rarr; unnest arrays or JSON fields into rows
- **AggregateNode**        &rarr; group-by outside of SQL (post-fetch aggregation)
- **WindowNode**           &rarr; rolling averages, rank, lag/lead over sorted rows

### 5. Control Flow Gaps
- **SwitchNode**           &rarr; multi-way conditional (like switch/case, not just if/else)
  - "route by segment: enterprise&rarr;A, smb&rarr;B, startup&rarr;C"
- **TryCatchNode**         &rarr; catch errors from a sub-pipeline, run recovery branch
- **RetryNode**            &rarr; retry a failing sub-pipeline N times with backoff
- **WaitNode**             &rarr; sleep for duration or until a condition is true
- **ThrottleNode**         &rarr; rate limit execution (max N rows/sec to downstream)
- **BatchNode**            &rarr; collect N rows then emit as single batch

### 6. ML and Vector Operations
Given your PTM research background these are natural extensions:
- **EmbedNode**            &rarr; embed text fields into vectors (via Voyage/OpenAI)
- **VectorSearchNode**     &rarr; similarity search against ChromaDB/pgvector
- **ClassifyNode**         &rarr; run a classification model on rows
- **ScoreNode**            &rarr; apply a scoring function (sigmoid, softmax, custom)
- **ClusterNode**          &rarr; k-means or similar clustering on row vectors

### 7. Time and Scheduling
No temporal awareness in the engine today:
- **ScheduleNode**         &rarr; run pipeline on cron schedule
- **WindowTimeNode**       &rarr; filter/group by time windows (last 7 days, this month)
- **WatermarkNode**        &rarr; track pipeline last-run watermark for incremental loads

### 8. Human-in-the-Loop
The confirmation step exists in CLI but not as a first-class node:
- **ApprovalNode**         &rarr; pause execution, wait for human confirmation
  - "before sending 500 emails, show preview and wait"
- **ReviewNode**           &rarr; surface N samples for human inspection mid-pipeline
- **FeedbackNode**         &rarr; collect human rating/correction, feed back to LLM

### 9. Observability and Side Effects
- **LogNode**              &rarr; emit structured log events mid-pipeline
- **MetricsNode**          &rarr; record a metric value (Prometheus, StatsD)
- **AuditNode**            &rarr; write immutable audit trail per row
- **CacheNode**            &rarr; cache output by input hash, skip if cache hit
- **CheckpointNode**       &rarr; save intermediate results to resume from on failure

## Near-term Implementation Priorities

### 1. HttpNode - call external APIs inside pipelines
&rarr; enrich customer data from Clearbit, send Slack alerts, post to webhooks
&rarr; straightforward to build given the existing node pattern

### 2. Pipeline persistence (SQLite)
&rarr; save named pipelines, rerun without replanning
&rarr; "save this as 'weekly revenue report'" &rarr; reruns with npm run cli weekly

### 3. Session context / memory
&rarr; "show me the same report but for last month"
&rarr; pronoun resolution across turns in the cli

### 4. SubPipelineNode
&rarr; compose pipelines from other pipelines
&rarr; build a library of reusable pipeline components

### 5. Streaming output
&rarr; yield rows as they arrive rather than collecting everything
&rarr; essential for large datasets and long-running LLM enrichments

### 6. Physical operator improvements
&rarr; push ORDER BY into Postgres scan (huge latency win for large tables)
&rarr; hash join spill to disk for large joins

### 7. REST API layer
&rarr; POST /pipeline/plan, POST /pipeline/execute
&rarr; exposes the engine over HTTP
&rarr; lets other tools (dashboards, Slack bots) drive it

### 8. Visualization layer (what you mentioned early on)
&rarr; render PipelineGraph as interactive canvas
&rarr; edit nodes, reconnect edges, re-execute
&rarr; the graph JSON is already fully serializable
