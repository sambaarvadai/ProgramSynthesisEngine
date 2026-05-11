-- ============================================================
-- PM Tool Seed Data
-- References CRM data in pee_dev:
--   workspace_id 1 = Acme Corp
--   users: 1=Priya, 2=Karthik, 3=Ananya, 4=Meena, 5=Rajan, 6=Sneha, 7=Vikram
--   won opportunities from CRM:
--     id 1 = TechNova Solutions (account_id 1)
--     id 2 = Meridian Retail    (account_id 2)
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- WORKSPACES (mirrors CRM)
-- ────────────────────────────────────────────────────────────
INSERT INTO workspaces (id, name, slug) VALUES
  (1, 'Acme Corp', 'acme-corp'),
  (2, 'Beta Workspace', 'beta-workspace');

-- ────────────────────────────────────────────────────────────
-- PROJECTS
-- Based on won opportunities from CRM
-- ────────────────────────────────────────────────────────────
INSERT INTO projects (
  id, workspace_id,
  crm_opportunity_id, crm_account_id, crm_contact_id,
  name, description, code,
  owner_user_id, created_by_user_id,
  status, priority,
  contract_value, budget, budget_spent,
  start_date, due_date,
  tags, notes
) VALUES
  (
    1, 1,
    1, 1, 1,  -- TechNova opportunity, account, contact
    'TechNova AI Pipeline Implementation',
    'End-to-end implementation of AI-powered data pipeline for TechNova Solutions. '
    'Includes data ingestion, transformation, and analytics layers.',
    'PROJ-001',
    4, 1,  -- owner: Meena, created by: Priya
    'active', 'high',
    120000.00, 110000.00, 34500.00,
    '2026-04-01', '2026-07-31',
    ARRAY['ai', 'data-pipeline', 'enterprise'],
    'Key account — weekly steering committee with CTO Ananya Singh'
  ),
  (
    2, 1,
    2, 2, 2,  -- Meridian opportunity, account, contact
    'Meridian Retail Analytics Platform',
    'Build and deploy retail analytics platform for Meridian. '
    'Phase 1 covers inventory intelligence and demand forecasting.',
    'PROJ-002',
    2, 1,  -- owner: Karthik, created by: Priya
    'active', 'critical',
    850000.00, 800000.00, 210000.00,
    '2026-03-15', '2026-09-30',
    ARRAY['retail', 'analytics', 'forecasting', 'enterprise'],
    'Largest deal this year. Executive sponsor: Rohan Mehta (CPO)'
  ),
  (
    3, 1,
    NULL, 4, NULL,  -- Internal project, Apex Logistics account
    'Apex Logistics Integration',
    'Technical integration project connecting Apex ERP to our platform. '
    'Pre-sales technical proof of concept.',
    'PROJ-003',
    5, 2,  -- owner: Rajan, created by: Karthik
    'planning', 'medium',
    32000.00, 25000.00, 2000.00,
    '2026-05-15', '2026-08-15',
    ARRAY['integration', 'erp', 'logistics'],
    'PoC phase — convert to full project once deal closes'
  ),
  (
    4, 1,
    NULL, 6, NULL,  -- PulseHealth account
    'PulseHealth Data Compliance Audit',
    'HIPAA compliance audit and data governance framework '
    'implementation for PulseHealth Technologies.',
    'PROJ-004',
    3, 1,  -- owner: Ananya, created by: Priya
    'on_hold', 'high',
    90000.00, 85000.00, 12000.00,
    '2026-04-20', '2026-08-20',
    ARRAY['healthcare', 'compliance', 'hipaa'],
    'On hold pending legal review of data processing agreement'
  );

-- ────────────────────────────────────────────────────────────
-- PROJECT MEMBERS
-- ────────────────────────────────────────────────────────────
INSERT INTO project_members (project_id, user_id, role, invited_by) VALUES
  -- PROJ-001: TechNova
  (1, 4, 'owner',   1),  -- Meena (owner)
  (1, 1, 'manager', 1),  -- Priya (manager)
  (1, 3, 'member',  4),  -- Ananya (member)
  (1, 7, 'member',  4),  -- Vikram (member)
  (1, 6, 'viewer',  1),  -- Sneha (viewer)

  -- PROJ-002: Meridian
  (2, 2, 'owner',   1),  -- Karthik (owner)
  (2, 1, 'manager', 1),  -- Priya (manager)
  (2, 5, 'member',  2),  -- Rajan (member)
  (2, 6, 'member',  2),  -- Sneha (member)
  (2, 7, 'member',  2),  -- Vikram (member)

  -- PROJ-003: Apex
  (3, 5, 'owner',   2),  -- Rajan (owner)
  (3, 2, 'manager', 2),  -- Karthik (manager)
  (3, 7, 'member',  5),  -- Vikram (member)

  -- PROJ-004: PulseHealth
  (4, 3, 'owner',   1),  -- Ananya (owner)
  (4, 1, 'manager', 1),  -- Priya (manager)
  (4, 4, 'member',  3);  -- Meena (member)

-- ────────────────────────────────────────────────────────────
-- MILESTONES
-- ────────────────────────────────────────────────────────────
INSERT INTO milestones (
  id, project_id, workspace_id,
  name, description, status,
  due_date, completed_at, owner_user_id
) VALUES
  -- PROJ-001: TechNova milestones
  (1, 1, 1, 'Discovery & Architecture',
   'Requirements gathering, stakeholder interviews, architecture design',
   'completed', '2026-04-30', '2026-04-28', 4),

  (2, 1, 1, 'Data Ingestion Layer',
   'Build connectors for all data sources, validation framework',
   'completed', '2026-05-31', '2026-05-29', 4),

  (3, 1, 1, 'Transformation Pipeline',
   'ETL jobs, data quality rules, lineage tracking',
   'in_progress', '2026-06-30', NULL, 3),

  (4, 1, 1, 'Analytics & Dashboards',
   'BI layer, dashboards, alerting system',
   'upcoming', '2026-07-31', NULL, 7),

  -- PROJ-002: Meridian milestones
  (5, 2, 1, 'Platform Setup & Infrastructure',
   'Cloud infrastructure, security baseline, CI/CD pipeline',
   'completed', '2026-04-15', '2026-04-12', 2),

  (6, 2, 1, 'Data Warehouse Design',
   'Schema design, data modeling, historical data migration',
   'completed', '2026-05-15', '2026-05-14', 2),

  (7, 2, 1, 'Inventory Intelligence Module',
   'Real-time inventory tracking, anomaly detection, alerts',
   'in_progress', '2026-06-30', NULL, 5),

  (8, 2, 1, 'Demand Forecasting Engine',
   'ML models for demand prediction, seasonal adjustments',
   'upcoming', '2026-08-15', NULL, 6),

  (9, 2, 1, 'UAT & Go-Live',
   'User acceptance testing, training, production deployment',
   'upcoming', '2026-09-30', NULL, 2),

  -- PROJ-003: Apex milestones
  (10, 3, 1, 'ERP API Assessment',
   'Document existing ERP APIs, identify integration points',
   'in_progress', '2026-06-15', NULL, 5),

  -- PROJ-004: PulseHealth milestones
  (11, 4, 1, 'Data Inventory & Classification',
   'Map all data flows, classify PHI/PII data elements',
   'in_progress', '2026-06-30', NULL, 3);

-- ────────────────────────────────────────────────────────────
-- TASKS
-- ────────────────────────────────────────────────────────────
INSERT INTO tasks (
  id, project_id, milestone_id, workspace_id,
  title, description,
  assigned_to, created_by,
  status, priority,
  due_date, estimated_hours, completed_at,
  tags
) VALUES
  -- PROJ-001 / M3: Transformation Pipeline
  (1, 1, 3, 1,
   'Design transformation DAG',
   'Define the directed acyclic graph for all transformation jobs',
   3, 4, 'done', 'high',
   '2026-06-05', 16, NOW() - INTERVAL '5 days',
   ARRAY['design', 'architecture']),

  (2, 1, 3, 1,
   'Implement customer dimension transform',
   'Build ETL job for customer dimension with SCD Type 2',
   7, 4, 'in_progress', 'high',
   '2026-06-12', 24, NULL,
   ARRAY['etl', 'customer']),

  (3, 1, 3, 1,
   'Implement revenue fact table transform',
   'Aggregate transaction data into daily/monthly revenue facts',
   3, 4, 'in_progress', 'high',
   '2026-06-15', 20, NULL,
   ARRAY['etl', 'revenue']),

  (4, 1, 3, 1,
   'Data quality validation framework',
   'Build automated data quality checks with alerting',
   7, 3, 'todo', 'medium',
   '2026-06-20', 16, NULL,
   ARRAY['quality', 'testing']),

  (5, 1, 3, 1,
   'Data lineage documentation',
   'Document complete data lineage from source to consumption',
   3, 4, 'todo', 'low',
   '2026-06-28', 8, NULL,
   ARRAY['documentation', 'lineage']),

  -- PROJ-001 / M4: Analytics (upcoming)
  (6, 1, 4, 1,
   'Design dashboard wireframes',
   'Create wireframes for executive and operational dashboards',
   7, 4, 'todo', 'medium',
   '2026-07-10', 12, NULL,
   ARRAY['design', 'ux']),

  -- PROJ-002 / M7: Inventory Intelligence
  (7, 2, 7, 1,
   'Real-time inventory tracking API',
   'Build API layer for real-time inventory position tracking',
   5, 2, 'in_progress', 'urgent',
   '2026-06-10', 32, NULL,
   ARRAY['api', 'real-time']),

  (8, 2, 7, 1,
   'Anomaly detection model',
   'Train and deploy inventory anomaly detection model',
   6, 2, 'in_progress', 'high',
   '2026-06-15', 28, NULL,
   ARRAY['ml', 'anomaly-detection']),

  (9, 2, 7, 1,
   'Alert configuration UI',
   'Build UI for configuring inventory alerts and thresholds',
   5, 2, 'todo', 'medium',
   '2026-06-25', 16, NULL,
   ARRAY['ui', 'alerts']),

  (10, 2, 7, 1,
   'Integration with ERP system',
   'Connect inventory module to Meridian ERP for live data sync',
   7, 5, 'todo', 'high',
   '2026-06-28', 24, NULL,
   ARRAY['integration', 'erp']),

  -- PROJ-002 / M8: Forecasting (upcoming)
  (11, 2, 8, 1,
   'Historical data analysis',
   'Analyze 3 years of sales history to identify patterns',
   6, 2, 'todo', 'high',
   '2026-07-15', 20, NULL,
   ARRAY['analysis', 'ml']),

  (12, 2, 8, 1,
   'Seasonal adjustment model',
   'Build model incorporating festival and seasonal demand patterns',
   6, 2, 'todo', 'high',
   '2026-07-31', 32, NULL,
   ARRAY['ml', 'forecasting']),

  -- PROJ-003 / M10: ERP Assessment
  (13, 3, 10, 1,
   'Map Apex ERP API endpoints',
   'Document all available REST endpoints in Apex ERP v4.2',
   7, 5, 'in_progress', 'high',
   '2026-06-01', 12, NULL,
   ARRAY['api', 'documentation']),

  (14, 3, 10, 1,
   'Authentication flow design',
   'Design OAuth2 integration with Apex ERP',
   5, 5, 'todo', 'high',
   '2026-06-08', 8, NULL,
   ARRAY['auth', 'security']),

  -- PROJ-004 / M11: PulseHealth
  (15, 4, 11, 1,
   'PHI data flow mapping',
   'Map all protected health information data flows',
   3, 3, 'in_progress', 'urgent',
   '2026-06-15', 20, NULL,
   ARRAY['phi', 'compliance', 'hipaa']),

  (16, 4, 11, 1,
   'Data retention policy review',
   'Review and document data retention requirements per HIPAA',
   4, 3, 'todo', 'high',
   '2026-06-22', 12, NULL,
   ARRAY['policy', 'compliance']);

-- ────────────────────────────────────────────────────────────
-- TIME LOGS
-- ────────────────────────────────────────────────────────────
INSERT INTO time_logs (
  task_id, project_id, workspace_id,
  user_id, hours, logged_date, description
) VALUES
  -- PROJ-001 tasks
  (1, 1, 1, 3, 4.0, '2026-06-01', 'Initial DAG design session'),
  (1, 1, 1, 3, 6.0, '2026-06-02', 'Revised DAG after stakeholder review'),
  (1, 1, 1, 3, 6.0, '2026-06-03', 'Final design and documentation'),
  (2, 1, 1, 7, 8.0, '2026-06-04', 'SCD Type 2 logic implementation'),
  (2, 1, 1, 7, 6.0, '2026-06-05', 'Unit tests and bug fixes'),
  (3, 1, 1, 3, 8.0, '2026-06-06', 'Revenue aggregation logic'),
  (7, 2, 1, 5, 8.0, '2026-06-01', 'API design and scaffolding'),
  (7, 2, 1, 5, 8.0, '2026-06-02', 'Core tracking endpoints'),
  (8, 2, 1, 6, 6.0, '2026-06-01', 'Data exploration and feature engineering'),
  (8, 2, 1, 6, 8.0, '2026-06-03', 'Model training — baseline'),
  (13, 3, 1, 7, 4.0, '2026-06-02', 'ERP API documentation review'),
  (15, 4, 1, 3, 5.0, '2026-05-28', 'PHI data discovery workshop');

-- ────────────────────────────────────────────────────────────
-- COMMENTS
-- ────────────────────────────────────────────────────────────
INSERT INTO comments (
  project_id, task_id, milestone_id,
  workspace_id, author_user_id, body
) VALUES
  (1, 2, NULL, 1, 4,
   'Vikram — make sure SCD Type 2 handles NULL source keys gracefully. '
   'We had issues with this in the DataSphere project.'),

  (1, 2, NULL, 1, 7,
   'Good catch Meena. I''ve added a null guard in the lookup logic. '
   'Will push the fix today.'),

  (2, 7, NULL, 1, 5,
   'Real-time sync confirmed working with test environment. '
   'Getting ~200ms latency on inventory updates. Acceptable for now.'),

  (2, 8, NULL, 1, 2,
   'Sneha — can we get the training data pull scheduled by EOD Friday? '
   'Need it to hit the June 15 target.'),

  (2, 8, NULL, 1, 6,
   'On it. Will coordinate with Meridian''s data team to extract '
   '3 years of POS data.'),

  (4, 15, NULL, 1, 1,
   'Legal has flagged the data processing agreement as incomplete. '
   'Project is on hold until resolved. ETA from legal: 2 weeks.');

-- ────────────────────────────────────────────────────────────
-- PROJECT ACTIVITY
-- ────────────────────────────────────────────────────────────
INSERT INTO project_activity (
  project_id, workspace_id,
  user_id, action, entity_type, entity_id,
  old_value, new_value
) VALUES
  (1, 1, 1, 'created',        'project',   1, NULL,
   '{"status":"planning"}'::jsonb),

  (1, 1, 4, 'status_changed', 'project',   1,
   '{"status":"planning"}'::jsonb, '{"status":"active"}'::jsonb),

  (1, 1, 4, 'milestone_completed', 'milestone', 1,
   '{"status":"in_progress"}'::jsonb, '{"status":"completed"}'::jsonb),

  (1, 1, 4, 'milestone_completed', 'milestone', 2,
   '{"status":"in_progress"}'::jsonb, '{"status":"completed"}'::jsonb),

  (2, 1, 1, 'created',        'project',   2, NULL,
   '{"status":"planning"}'::jsonb),

  (2, 1, 2, 'status_changed', 'project',   2,
   '{"status":"planning"}'::jsonb, '{"status":"active"}'::jsonb),

  (4, 1, 1, 'status_changed', 'project',   4,
   '{"status":"active"}'::jsonb, '{"status":"on_hold"}'::jsonb);

-- ────────────────────────────────────────────────────────────
-- RESET SEQUENCES
-- ────────────────────────────────────────────────────────────
SELECT setval('projects_id_seq',    (SELECT MAX(id) FROM projects));
SELECT setval('milestones_id_seq',  (SELECT MAX(id) FROM milestones));
SELECT setval('tasks_id_seq',       (SELECT MAX(id) FROM tasks));
SELECT setval('time_logs_id_seq',   (SELECT MAX(id) FROM time_logs));
SELECT setval('comments_id_seq',    (SELECT MAX(id) FROM comments));
SELECT setval('project_members_id_seq', (SELECT MAX(id) FROM project_members));
SELECT setval('project_activity_id_seq', (SELECT MAX(id) FROM project_activity));
