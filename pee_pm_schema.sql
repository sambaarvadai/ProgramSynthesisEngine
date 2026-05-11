-- ============================================================
-- PEE PM Tool Schema
-- Shared user IDs with CRM (SSO model)
-- projects.crm_opportunity_id → pee_dev.opportunities.id
-- projects.crm_account_id     → pee_dev.accounts.id
-- All user_id fields          → pee_dev.users.id
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- SEQUENCES
-- ────────────────────────────────────────────────────────────
CREATE SEQUENCE projects_code_seq START 1;

-- ────────────────────────────────────────────────────────────
-- WORKSPACES
-- Mirrors CRM workspaces — same workspace_id
-- ────────────────────────────────────────────────────────────
CREATE TABLE workspaces (
  id            SERIAL        PRIMARY KEY,
  name          TEXT          NOT NULL,
  slug          TEXT          NOT NULL UNIQUE,
  created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- PROJECT STATUSES (lookup)
-- Using TEXT + CHECK constraints instead of ENUM for DDLParser compatibility
-- ────────────────────────────────────────────────────────────
-- CREATE TYPE project_status AS ENUM (
--   'planning',
--   'active',
--   'on_hold',
--   'completed',
--   'cancelled'
-- );

-- CREATE TYPE project_priority AS ENUM (
--   'low',
--   'medium',
--   'high',
--   'critical'
-- );

-- CREATE TYPE task_status AS ENUM (
--   'todo',
--   'in_progress',
--   'in_review',
--   'done',
--   'cancelled'
-- );

-- CREATE TYPE task_priority AS ENUM (
--   'low',
--   'medium',
--   'high',
--   'urgent'
-- );

-- CREATE TYPE milestone_status AS ENUM (
--   'upcoming',
--   'in_progress',
--   'completed',
--   'missed'
-- );

-- CREATE TYPE member_role AS ENUM (
--   'owner',
--   'manager',
--   'member',
--   'viewer'
-- );

-- ────────────────────────────────────────────────────────────
-- PROJECTS
-- One project per won CRM opportunity
-- ────────────────────────────────────────────────────────────
CREATE TABLE projects (
  id                    SERIAL          PRIMARY KEY,
  workspace_id          INT             NOT NULL REFERENCES workspaces(id),

  -- CRM references (cross-DB FKs — enforced by PEE, not Postgres)
  crm_opportunity_id    INT,            -- pee_dev.opportunities.id
  crm_account_id        INT,            -- pee_dev.accounts.id
  crm_contact_id        INT,            -- pee_dev.contacts.id (primary contact)

  -- Project identity
  name                  TEXT            NOT NULL,
  description           TEXT,
  code                  TEXT            UNIQUE DEFAULT 'PROJ-' || LPAD(nextval('projects_code_seq')::text, 3, '0'),  -- e.g. "PROJ-001"

  -- Ownership (shared user IDs from CRM)
  owner_user_id         INT             NOT NULL, -- pee_dev.users.id
  created_by_user_id    INT             NOT NULL, -- pee_dev.users.id

  -- Status & priority
  status                TEXT            NOT NULL DEFAULT 'planning'
    CHECK (status IN ('planning', 'active', 'on_hold', 'completed', 'cancelled')),
  priority              TEXT            NOT NULL DEFAULT 'medium'
    CHECK (priority IN ('low', 'medium', 'high', 'critical')),

  -- Financials (mirrors won opportunity value)
  contract_value        NUMERIC(15,2),
  budget                NUMERIC(15,2),
  budget_spent          NUMERIC(15,2)   NOT NULL DEFAULT 0,

  -- Timeline
  start_date            DATE,
  due_date              DATE,
  completed_at          TIMESTAMPTZ,

  -- Metadata
  tags                  TEXT[],
  notes                 TEXT,

  created_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  deleted_at            TIMESTAMPTZ     -- soft delete
);

-- ────────────────────────────────────────────────────────────
-- PROJECT MEMBERS
-- Team assigned to a project (shared user IDs)
-- ────────────────────────────────────────────────────────────
CREATE TABLE project_members (
  id              SERIAL        PRIMARY KEY,
  project_id      INT           NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  user_id         INT           NOT NULL, -- pee_dev.users.id
  role            TEXT          NOT NULL DEFAULT 'member'
    CHECK (role IN ('owner', 'manager', 'member', 'viewer')),
  joined_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  invited_by      INT,                   -- pee_dev.users.id

  UNIQUE(project_id, user_id)
);

-- ────────────────────────────────────────────────────────────
-- MILESTONES
-- Key deliverables within a project
-- ────────────────────────────────────────────────────────────
CREATE TABLE milestones (
  id              SERIAL            PRIMARY KEY,
  project_id      INT               NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  workspace_id    INT               NOT NULL REFERENCES workspaces(id),

  name            TEXT              NOT NULL,
  description     TEXT,
  status          TEXT              NOT NULL DEFAULT 'upcoming'
    CHECK (status IN ('upcoming', 'in_progress', 'completed', 'missed')),

  -- Timeline
  due_date        DATE,
  completed_at    TIMESTAMPTZ,

  -- Ownership
  owner_user_id   INT,              -- pee_dev.users.id

  created_at      TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ       NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- TASKS
-- Granular work items within a milestone (or standalone)
-- ────────────────────────────────────────────────────────────
CREATE TABLE tasks (
  id                SERIAL          PRIMARY KEY,
  project_id        INT             NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  milestone_id      INT             REFERENCES milestones(id) ON DELETE SET NULL,
  workspace_id      INT             NOT NULL REFERENCES workspaces(id),
  parent_task_id    INT             REFERENCES tasks(id) ON DELETE CASCADE, -- subtasks

  title             TEXT            NOT NULL,
  description       TEXT,

  -- Assignment (shared user IDs)
  assigned_to       INT,            -- pee_dev.users.id
  created_by        INT             NOT NULL, -- pee_dev.users.id

  -- Status & priority
  status            TEXT            NOT NULL DEFAULT 'todo'
    CHECK (status IN ('todo', 'in_progress', 'in_review', 'done', 'cancelled')),
  priority          TEXT            NOT NULL DEFAULT 'medium'
    CHECK (priority IN ('low', 'medium', 'high', 'urgent')),

  -- Timeline
  due_date          DATE,
  estimated_hours   NUMERIC(6,2),
  completed_at      TIMESTAMPTZ,

  -- Metadata
  tags              TEXT[],

  created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- TIME LOGS
-- Hours tracked against tasks
-- ────────────────────────────────────────────────────────────
CREATE TABLE time_logs (
  id              SERIAL        PRIMARY KEY,
  task_id         INT           NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  project_id      INT           NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  workspace_id    INT           NOT NULL REFERENCES workspaces(id),

  user_id         INT           NOT NULL, -- pee_dev.users.id
  hours           NUMERIC(6,2)  NOT NULL,
  logged_date     DATE          NOT NULL DEFAULT CURRENT_DATE,
  description     TEXT,

  created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- COMMENTS
-- Discussion on tasks and milestones
-- ────────────────────────────────────────────────────────────
CREATE TABLE comments (
  id              SERIAL        PRIMARY KEY,
  project_id      INT           NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  task_id         INT           REFERENCES tasks(id) ON DELETE CASCADE,
  milestone_id    INT           REFERENCES milestones(id) ON DELETE CASCADE,
  workspace_id    INT           NOT NULL REFERENCES workspaces(id),

  author_user_id  INT           NOT NULL, -- pee_dev.users.id
  body            TEXT          NOT NULL,
  edited_at       TIMESTAMPTZ,

  created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

  CONSTRAINT comment_has_target CHECK (
    task_id IS NOT NULL OR milestone_id IS NOT NULL
  )
);

-- ────────────────────────────────────────────────────────────
-- PROJECT ACTIVITY LOG
-- Audit trail for project changes
-- ────────────────────────────────────────────────────────────
CREATE TABLE project_activity (
  id              SERIAL        PRIMARY KEY,
  project_id      INT           NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  workspace_id    INT           NOT NULL REFERENCES workspaces(id),

  user_id         INT           NOT NULL, -- pee_dev.users.id
  action          TEXT          NOT NULL, -- 'created', 'status_changed', 'member_added' etc.
  entity_type     TEXT          NOT NULL, -- 'project', 'task', 'milestone'
  entity_id       INT           NOT NULL,
  old_value       JSONB,
  new_value       JSONB,

  created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- INDEXES
-- ────────────────────────────────────────────────────────────
CREATE INDEX idx_projects_workspace     ON projects(workspace_id);
CREATE INDEX idx_projects_crm_opp      ON projects(crm_opportunity_id);
CREATE INDEX idx_projects_crm_account  ON projects(crm_account_id);
CREATE INDEX idx_projects_owner        ON projects(owner_user_id);
CREATE INDEX idx_projects_status       ON projects(status);
CREATE INDEX idx_projects_deleted      ON projects(deleted_at) WHERE deleted_at IS NULL;

CREATE INDEX idx_milestones_project    ON milestones(project_id);
CREATE INDEX idx_milestones_status     ON milestones(status);
CREATE INDEX idx_milestones_due        ON milestones(due_date);

CREATE INDEX idx_tasks_project         ON tasks(project_id);
CREATE INDEX idx_tasks_milestone       ON tasks(milestone_id);
CREATE INDEX idx_tasks_assigned        ON tasks(assigned_to);
CREATE INDEX idx_tasks_status          ON tasks(status);
CREATE INDEX idx_tasks_due             ON tasks(due_date);

CREATE INDEX idx_time_logs_task        ON time_logs(task_id);
CREATE INDEX idx_time_logs_project     ON time_logs(project_id);
CREATE INDEX idx_time_logs_user        ON time_logs(user_id);
CREATE INDEX idx_time_logs_date        ON time_logs(logged_date);

CREATE INDEX idx_comments_task         ON comments(task_id);
CREATE INDEX idx_comments_project      ON comments(project_id);

CREATE INDEX idx_project_members_user  ON project_members(user_id);
CREATE INDEX idx_project_activity_proj ON project_activity(project_id);
