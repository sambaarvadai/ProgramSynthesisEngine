-- =================================================================
-- CRM Schema — PostgreSQL
-- Converted from SQLite source
-- Key changes:
--   INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY
--   TEXT (dates)                      → TIMESTAMPTZ / DATE
--   INTEGER (booleans 0/1)            → BOOLEAN
--   REAL                              → NUMERIC(15,4)
--   CHECK constraints                 → kept as-is (PG supports them)
--   PRAGMA foreign_keys               → removed (always on in PG DDL)
--   options_json / config_json etc.   → JSONB
-- =================================================================

-- -----------------------------------------------------------------
-- WORKSPACES / USERS / TEAMS / ROLES
-- -----------------------------------------------------------------

CREATE TABLE workspaces (
    id                  SERIAL PRIMARY KEY,
    name                TEXT        NOT NULL,
    slug                TEXT        NOT NULL UNIQUE,
    plan                TEXT,
    status              TEXT        NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'suspended', 'cancelled')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE users (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    first_name          TEXT        NOT NULL,
    last_name           TEXT,
    email               TEXT        NOT NULL UNIQUE,
    phone               TEXT,
    password_hash       TEXT        NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'invited', 'disabled')),
    last_login_at       TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE TABLE teams (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    name                TEXT        NOT NULL,
    manager_user_id     INT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)    REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (manager_user_id) REFERENCES users(id)      ON DELETE SET NULL,
    UNIQUE (workspace_id, name)
);

CREATE TABLE roles (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    name                TEXT        NOT NULL,
    description         TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    UNIQUE (workspace_id, name)
);

CREATE TABLE user_workspace_memberships (
    id                  SERIAL PRIMARY KEY,
    user_id             INT         NOT NULL,
    workspace_id        INT         NOT NULL,
    role_id             INT,
    team_id             INT,
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (user_id)      REFERENCES users(id)      ON DELETE CASCADE,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (role_id)      REFERENCES roles(id)      ON DELETE SET NULL,
    FOREIGN KEY (team_id)      REFERENCES teams(id)      ON DELETE SET NULL,
    UNIQUE (user_id, workspace_id)
);

-- -----------------------------------------------------------------
-- ACCOUNTS / CONTACTS
-- -----------------------------------------------------------------

CREATE TABLE accounts (
    id                      SERIAL PRIMARY KEY,
    workspace_id            INT             NOT NULL,
    owner_user_id           INT,
    name                    TEXT            NOT NULL,
    legal_name              TEXT,
    website                 TEXT,
    industry                TEXT,
    employee_count          INT,
    annual_revenue          NUMERIC(15,4),
    phone                   TEXT,
    email                   TEXT,
    billing_address_line1   TEXT,
    billing_address_line2   TEXT,
    billing_city            TEXT,
    billing_state           TEXT,
    billing_country         TEXT,
    billing_postal_code     TEXT,
    shipping_address_line1  TEXT,
    shipping_address_line2  TEXT,
    shipping_city           TEXT,
    shipping_state          TEXT,
    shipping_country        TEXT,
    shipping_postal_code    TEXT,
    description             TEXT,
    status                  TEXT            NOT NULL DEFAULT 'prospect'
                            CHECK (status IN ('prospect', 'customer', 'partner', 'inactive')),
    source                  TEXT,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    deleted_at              TIMESTAMPTZ,
    FOREIGN KEY (workspace_id)      REFERENCES workspaces(id)      ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id)     REFERENCES users(id)           ON DELETE SET NULL
);

CREATE TABLE contacts (
    id                      SERIAL PRIMARY KEY,
    workspace_id            INT             NOT NULL,
    owner_user_id           INT,
    primary_account_id      INT,
    first_name              TEXT            NOT NULL,
    last_name               TEXT,
    full_name               TEXT,
    job_title               TEXT,
    email                   TEXT,
    phone                   TEXT,
    mobile                  TEXT,
    department              TEXT,
    lifecycle_stage         TEXT            NOT NULL DEFAULT 'mql'
                            CHECK (lifecycle_stage IN ('mql', 'sql', 'opportunity', 'customer')),
    source                  TEXT,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)        REFERENCES workspaces(id)      ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id)       REFERENCES users(id)           ON DELETE SET NULL,
    FOREIGN KEY (primary_account_id)  REFERENCES accounts(id)        ON DELETE SET NULL
);

CREATE TABLE contact_account_links (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    contact_id          INT         NOT NULL,
    account_id          INT         NOT NULL,
    relationship_type   TEXT,
    is_primary          BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (contact_id)   REFERENCES contacts(id)   ON DELETE CASCADE,
    FOREIGN KEY (account_id)   REFERENCES accounts(id)   ON DELETE CASCADE,
    UNIQUE (contact_id, account_id)
);

-- -----------------------------------------------------------------
-- PIPELINES / STAGES / OPPORTUNITIES
-- -----------------------------------------------------------------

CREATE TABLE pipelines (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    name            TEXT        NOT NULL,
    type            TEXT        NOT NULL DEFAULT 'sales'
                        CHECK (type IN ('sales', 'onboarding', 'renewals', 'support')),
    is_default      BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    UNIQUE (workspace_id, name)
);

CREATE TABLE pipeline_stages (
    id                  SERIAL PRIMARY KEY,
    pipeline_id         INT         NOT NULL,
    name                TEXT        NOT NULL,
    stage_order         INT         NOT NULL,
    probability_percent INT         NOT NULL DEFAULT 0
                            CHECK (probability_percent BETWEEN 0 AND 100),
    is_closed_won       BOOLEAN     NOT NULL DEFAULT FALSE,
    is_closed_lost      BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
    UNIQUE (pipeline_id, stage_order),
    UNIQUE (pipeline_id, name)
);

CREATE TABLE opportunities (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT             NOT NULL,
    pipeline_id         INT             NOT NULL,
    stage_id            INT             NOT NULL,
    owner_user_id       INT,
    account_id          INT,
    primary_contact_id  INT,
    name                TEXT            NOT NULL,
    description         TEXT,
    amount              NUMERIC(15,4),
    currency_code       TEXT            NOT NULL DEFAULT 'USD',
    probability_percent INT             NOT NULL DEFAULT 0
                            CHECK (probability_percent BETWEEN 0 AND 100),
    expected_close_date DATE,
    actual_close_date   DATE,
    status              TEXT            NOT NULL DEFAULT 'open'
                            CHECK (status IN ('open', 'won', 'lost', 'abandoned')),
    loss_reason         TEXT,
    source              TEXT,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    deleted_at          TIMESTAMPTZ,
    FOREIGN KEY (workspace_id)      REFERENCES workspaces(id)      ON DELETE CASCADE,
    FOREIGN KEY (pipeline_id)       REFERENCES pipelines(id)       ON DELETE RESTRICT,
    FOREIGN KEY (stage_id)          REFERENCES pipeline_stages(id) ON DELETE RESTRICT,
    FOREIGN KEY (owner_user_id)     REFERENCES users(id)           ON DELETE SET NULL,
    FOREIGN KEY (account_id)        REFERENCES accounts(id)        ON DELETE SET NULL,
    FOREIGN KEY (primary_contact_id) REFERENCES contacts(id)       ON DELETE SET NULL
);

CREATE TABLE opportunity_stage_history (
    id                  SERIAL PRIMARY KEY,
    opportunity_id      INT         NOT NULL,
    from_stage_id       INT,
    to_stage_id         INT         NOT NULL,
    changed_by_user_id  INT,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (opportunity_id)     REFERENCES opportunities(id)   ON DELETE CASCADE,
    FOREIGN KEY (from_stage_id)      REFERENCES pipeline_stages(id) ON DELETE SET NULL,
    FOREIGN KEY (to_stage_id)        REFERENCES pipeline_stages(id) ON DELETE SET NULL,
    FOREIGN KEY (changed_by_user_id) REFERENCES users(id)           ON DELETE SET NULL
);

CREATE TABLE assignments_history (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    entity_type         TEXT        NOT NULL
                            CHECK (entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'task')),
    entity_id           INT         NOT NULL,
    from_user_id        INT,
    to_user_id          INT,
    changed_by_user_id  INT,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)       REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (from_user_id)       REFERENCES users(id)      ON DELETE SET NULL,
    FOREIGN KEY (to_user_id)         REFERENCES users(id)      ON DELETE SET NULL,
    FOREIGN KEY (changed_by_user_id) REFERENCES users(id)      ON DELETE SET NULL
);

-- -----------------------------------------------------------------
-- LEADS
-- -----------------------------------------------------------------

CREATE TABLE leads (
    id                          SERIAL PRIMARY KEY,
    workspace_id                INT             NOT NULL,
    owner_user_id               INT,
    first_name                  TEXT,
    last_name                   TEXT,
    company_name                TEXT,
    title                       TEXT,
    email                       TEXT,
    phone                       TEXT,
    website                     TEXT,
    source                      TEXT,
    status                      TEXT            NOT NULL DEFAULT 'new'
                                    CHECK (status IN ('new', 'working', 'qualified', 'disqualified')),
    score                       INT             NOT NULL DEFAULT 0,
    estimated_value             NUMERIC(15,4),
    notes                       TEXT,
    converted_contact_id        INT,
    converted_account_id        INT,
    converted_opportunity_id    INT,
    converted_at                TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)             REFERENCES workspaces(id)   ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id)            REFERENCES users(id)         ON DELETE SET NULL,
    FOREIGN KEY (converted_contact_id)     REFERENCES contacts(id)      ON DELETE SET NULL,
    FOREIGN KEY (converted_account_id)     REFERENCES accounts(id)      ON DELETE SET NULL,
    FOREIGN KEY (converted_opportunity_id) REFERENCES opportunities(id) ON DELETE SET NULL
);

-- -----------------------------------------------------------------
-- PRODUCTS / QUOTES
-- -----------------------------------------------------------------

CREATE TABLE products (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT             NOT NULL,
    sku             TEXT,
    name            TEXT            NOT NULL,
    description     TEXT,
    unit_price      NUMERIC(15,4)   NOT NULL DEFAULT 0,
    currency_code   TEXT            NOT NULL DEFAULT 'USD',
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    UNIQUE (workspace_id, sku)
);

CREATE TABLE opportunity_products (
    id                  SERIAL PRIMARY KEY,
    opportunity_id      INT             NOT NULL,
    product_id          INT             NOT NULL,
    quantity            NUMERIC(15,4)   NOT NULL DEFAULT 1,
    unit_price          NUMERIC(15,4)   NOT NULL DEFAULT 0,
    discount_percent    NUMERIC(5,2)    NOT NULL DEFAULT 0,
    tax_percent         NUMERIC(5,2)    NOT NULL DEFAULT 0,
    line_total          NUMERIC(15,4)   NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    FOREIGN KEY (opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id)     REFERENCES products(id)      ON DELETE RESTRICT
);

CREATE TABLE quotes (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT             NOT NULL,
    opportunity_id      INT,
    account_id          INT             NOT NULL,
    contact_id          INT,
    quote_number        TEXT            NOT NULL,
    status              TEXT            NOT NULL DEFAULT 'draft'
                            CHECK (status IN ('draft', 'sent', 'accepted', 'rejected', 'expired')),
    issue_date          DATE,
    expiry_date         DATE,
    subtotal            NUMERIC(15,4)   NOT NULL DEFAULT 0,
    discount_total      NUMERIC(15,4)   NOT NULL DEFAULT 0,
    tax_total           NUMERIC(15,4)   NOT NULL DEFAULT 0,
    grand_total         NUMERIC(15,4)   NOT NULL DEFAULT 0,
    currency_code       TEXT            NOT NULL DEFAULT 'USD',
    terms               TEXT,
    created_by_user_id  INT,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)      REFERENCES workspaces(id)   ON DELETE CASCADE,
    FOREIGN KEY (opportunity_id)    REFERENCES opportunities(id) ON DELETE SET NULL,
    FOREIGN KEY (account_id)        REFERENCES accounts(id)     ON DELETE RESTRICT,
    FOREIGN KEY (contact_id)        REFERENCES contacts(id)     ON DELETE SET NULL,
    FOREIGN KEY (created_by_user_id) REFERENCES users(id)       ON DELETE SET NULL,
    UNIQUE (workspace_id, quote_number)
);

CREATE TABLE quote_items (
    id                  SERIAL PRIMARY KEY,
    quote_id            INT             NOT NULL,
    product_id          INT,
    description         TEXT            NOT NULL,
    quantity            NUMERIC(15,4)   NOT NULL DEFAULT 1,
    unit_price          NUMERIC(15,4)   NOT NULL DEFAULT 0,
    discount_percent    NUMERIC(5,2)    NOT NULL DEFAULT 0,
    tax_percent         NUMERIC(5,2)    NOT NULL DEFAULT 0,
    line_total          NUMERIC(15,4)   NOT NULL DEFAULT 0,
    FOREIGN KEY (quote_id)   REFERENCES quotes(id)   ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET NULL
);

-- -----------------------------------------------------------------
-- TASKS / ACTIVITIES / NOTES / ATTACHMENTS
-- -----------------------------------------------------------------

CREATE TABLE tasks (
    id                      SERIAL PRIMARY KEY,
    workspace_id            INT         NOT NULL,
    owner_user_id           INT,
    assigned_to_user_id     INT,
    related_entity_type     TEXT        NOT NULL
                                CHECK (related_entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'quote')),
    related_entity_id       INT         NOT NULL,
    title                   TEXT        NOT NULL,
    description             TEXT,
    due_at                  TIMESTAMPTZ,
    priority                TEXT        NOT NULL DEFAULT 'medium'
                                CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
    status                  TEXT        NOT NULL DEFAULT 'open'
                                CHECK (status IN ('open', 'in_progress', 'completed', 'cancelled')),
    completed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)        REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id)       REFERENCES users(id)      ON DELETE SET NULL,
    FOREIGN KEY (assigned_to_user_id) REFERENCES users(id)      ON DELETE SET NULL
);

CREATE TABLE activities (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    actor_user_id       INT,
    related_entity_type TEXT        NOT NULL
                            CHECK (related_entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'quote')),
    related_entity_id   INT         NOT NULL,
    activity_type       TEXT        NOT NULL
                            CHECK (activity_type IN ('call', 'meeting', 'email', 'demo', 'task', 'note', 'status_change', 'sms', 'whatsapp', 'system')),
    subject             TEXT,
    description         TEXT,
    activity_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_minutes    INT,
    outcome             TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (actor_user_id) REFERENCES users(id)     ON DELETE SET NULL
);

CREATE TABLE notes (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    created_by_user_id  INT,
    related_entity_type TEXT        NOT NULL
                            CHECK (related_entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'quote')),
    related_entity_id   INT         NOT NULL,
    content             TEXT        NOT NULL,
    is_private          BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)       REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by_user_id) REFERENCES users(id)      ON DELETE SET NULL
);

CREATE TABLE attachments (
    id                      SERIAL PRIMARY KEY,
    workspace_id            INT         NOT NULL,
    uploaded_by_user_id     INT,
    related_entity_type     TEXT        NOT NULL
                                CHECK (related_entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'quote', 'note', 'email')),
    related_entity_id       INT         NOT NULL,
    file_name               TEXT        NOT NULL,
    file_url                TEXT        NOT NULL,
    mime_type               TEXT,
    file_size_bytes         BIGINT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)        REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (uploaded_by_user_id) REFERENCES users(id)      ON DELETE SET NULL
);

-- -----------------------------------------------------------------
-- EMAILS / CALLS / COMMUNICATION
-- -----------------------------------------------------------------

CREATE TABLE emails (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    owner_user_id       INT,
    related_entity_type TEXT
                            CHECK (related_entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'quote')),
    related_entity_id   INT,
    provider_message_id TEXT,
    thread_id           TEXT,
    direction           TEXT        NOT NULL
                            CHECK (direction IN ('inbound', 'outbound')),
    subject             TEXT,
    body_text           TEXT,
    body_html           TEXT,
    sent_at             TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'draft'
                            CHECK (status IN ('draft', 'queued', 'sent', 'delivered', 'bounced', 'failed')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id) REFERENCES users(id)     ON DELETE SET NULL
);

CREATE TABLE email_participants (
    id                  SERIAL PRIMARY KEY,
    email_id            INT         NOT NULL,
    participant_type    TEXT        NOT NULL
                            CHECK (participant_type IN ('from', 'to', 'cc', 'bcc')),
    contact_id          INT,
    email_address       TEXT        NOT NULL,
    display_name        TEXT,
    FOREIGN KEY (email_id)   REFERENCES emails(id)   ON DELETE CASCADE,
    FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE SET NULL
);

CREATE TABLE calls (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    owner_user_id       INT,
    related_entity_type TEXT        NOT NULL
                            CHECK (related_entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket')),
    related_entity_id   INT         NOT NULL,
    contact_id          INT,
    started_at          TIMESTAMPTZ,
    ended_at            TIMESTAMPTZ,
    duration_seconds    INT,
    direction           TEXT        NOT NULL
                            CHECK (direction IN ('inbound', 'outbound')),
    outcome             TEXT,
    recording_url       TEXT,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id) REFERENCES users(id)     ON DELETE SET NULL,
    FOREIGN KEY (contact_id)   REFERENCES contacts(id)   ON DELETE SET NULL
);

CREATE TABLE messages (
    id                      SERIAL PRIMARY KEY,
    workspace_id            INT         NOT NULL,
    related_entity_type     TEXT        NOT NULL
                                CHECK (related_entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket')),
    related_entity_id       INT         NOT NULL,
    channel                 TEXT        NOT NULL
                                CHECK (channel IN ('sms', 'whatsapp', 'chat', 'linkedin', 'other')),
    direction               TEXT        NOT NULL
                                CHECK (direction IN ('inbound', 'outbound')),
    sender_contact_id       INT,
    sender_user_id          INT,
    recipient_contact_id    INT,
    body                    TEXT        NOT NULL,
    sent_at                 TIMESTAMPTZ,
    status                  TEXT
                                CHECK (status IN ('queued', 'sent', 'delivered', 'read', 'failed')),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)       REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (sender_contact_id)  REFERENCES contacts(id)   ON DELETE SET NULL,
    FOREIGN KEY (sender_user_id)     REFERENCES users(id)      ON DELETE SET NULL,
    FOREIGN KEY (recipient_contact_id) REFERENCES contacts(id) ON DELETE SET NULL
);

-- -----------------------------------------------------------------
-- SUPPORT / TICKETS
-- -----------------------------------------------------------------

CREATE TABLE tickets (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    account_id          INT,
    contact_id          INT,
    owner_user_id       INT,
    subject             TEXT        NOT NULL,
    description         TEXT,
    priority            TEXT        NOT NULL DEFAULT 'medium'
                            CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
    status              TEXT        NOT NULL DEFAULT 'open'
                            CHECK (status IN ('open', 'pending', 'resolved', 'closed')),
    channel             TEXT
                            CHECK (channel IN ('email', 'web', 'chat', 'phone', 'whatsapp')),
    category            TEXT,
    resolution_summary  TEXT,
    first_response_at   TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ,
    closed_at           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (account_id)   REFERENCES accounts(id)   ON DELETE SET NULL,
    FOREIGN KEY (contact_id)   REFERENCES contacts(id)   ON DELETE SET NULL,
    FOREIGN KEY (owner_user_id) REFERENCES users(id)     ON DELETE SET NULL
);

CREATE TABLE ticket_comments (
    id          SERIAL PRIMARY KEY,
    ticket_id   INT         NOT NULL,
    user_id     INT,
    contact_id  INT,
    body        TEXT        NOT NULL,
    is_private  BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (ticket_id)  REFERENCES tickets(id)  ON DELETE CASCADE,
    FOREIGN KEY (user_id)    REFERENCES users(id)    ON DELETE SET NULL,
    FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE SET NULL
);

-- -----------------------------------------------------------------
-- TAGS / CUSTOM FIELDS
-- -----------------------------------------------------------------

CREATE TABLE tags (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    name            TEXT        NOT NULL,
    color           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    UNIQUE (workspace_id, name)
);

CREATE TABLE entity_tags (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    tag_id          INT         NOT NULL,
    entity_type     TEXT        NOT NULL
                        CHECK (entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'quote', 'product')),
    entity_id       INT         NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id)       REFERENCES tags(id)       ON DELETE CASCADE,
    UNIQUE (tag_id, entity_type, entity_id)
);

CREATE TABLE custom_fields (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    entity_type     TEXT        NOT NULL
                        CHECK (entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'quote', 'product')),
    field_key       TEXT        NOT NULL,
    label           TEXT        NOT NULL,
    data_type       TEXT        NOT NULL
                        CHECK (data_type IN ('text', 'number', 'date', 'boolean', 'select', 'multi_select', 'json')),
    is_required     BOOLEAN     NOT NULL DEFAULT FALSE,
    options_json    JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    UNIQUE (workspace_id, entity_type, field_key)
);

CREATE TABLE custom_field_values (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    custom_field_id     INT         NOT NULL,
    entity_type         TEXT        NOT NULL
                            CHECK (entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'quote', 'product')),
    entity_id           INT         NOT NULL,
    value_text          TEXT,
    value_number        NUMERIC(15,4),
    value_date          DATE,
    value_boolean       BOOLEAN,
    value_json          JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id)  REFERENCES workspaces(id)  ON DELETE CASCADE,
    FOREIGN KEY (custom_field_id) REFERENCES custom_fields(id) ON DELETE CASCADE,
    UNIQUE (custom_field_id, entity_type, entity_id)
);

-- -----------------------------------------------------------------
-- LEAD SCORING / AUTOMATION / INTEGRATIONS
-- -----------------------------------------------------------------

CREATE TABLE lead_score_events (
    id          SERIAL PRIMARY KEY,
    lead_id     INT         NOT NULL,
    event_type  TEXT        NOT NULL,
    score_delta INT         NOT NULL,
    reason      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
);

CREATE TABLE integrations (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    provider        TEXT        NOT NULL,
    config_json     JSONB,
    status          TEXT        NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'disabled', 'error')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    UNIQUE (workspace_id, provider)
);

CREATE TABLE webhooks (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    event_name      TEXT        NOT NULL,
    endpoint_url    TEXT        NOT NULL,
    secret          TEXT,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE TABLE workflow_rules (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    name            TEXT        NOT NULL,
    entity_type     TEXT        NOT NULL
                        CHECK (entity_type IN ('lead', 'contact', 'account', 'opportunity', 'ticket', 'task')),
    trigger_event   TEXT        NOT NULL,
    condition_json  JSONB,
    action_json     JSONB       NOT NULL,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

-- -----------------------------------------------------------------
-- IMPORTS / DEDUP / MERGE / AUDIT
-- -----------------------------------------------------------------

CREATE TABLE imports (
    id                      SERIAL PRIMARY KEY,
    workspace_id            INT         NOT NULL,
    imported_by_user_id     INT,
    entity_type             TEXT        NOT NULL
                                CHECK (entity_type IN ('lead', 'contact', 'account', 'product')),
    file_name               TEXT,
    status                  TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    total_rows              INT         DEFAULT 0,
    success_rows            INT         DEFAULT 0,
    failed_rows             INT         DEFAULT 0,
    error_log               TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at            TIMESTAMPTZ,
    FOREIGN KEY (workspace_id)        REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (imported_by_user_id) REFERENCES users(id)      ON DELETE SET NULL
);

CREATE TABLE dedup_rules (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    entity_type     TEXT        NOT NULL
                        CHECK (entity_type IN ('lead', 'contact', 'account')),
    name            TEXT        NOT NULL,
    rule_json       JSONB       NOT NULL,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE TABLE merge_history (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INT         NOT NULL,
    entity_type         TEXT        NOT NULL
                            CHECK (entity_type IN ('lead', 'contact', 'account')),
    source_entity_id    INT         NOT NULL,
    target_entity_id    INT         NOT NULL,
    merged_by_user_id   INT,
    merged_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    merge_summary_json  JSONB,
    FOREIGN KEY (workspace_id)      REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (merged_by_user_id) REFERENCES users(id)      ON DELETE SET NULL
);

CREATE TABLE audit_logs (
    id              SERIAL PRIMARY KEY,
    workspace_id    INT         NOT NULL,
    user_id         INT,
    entity_type     TEXT        NOT NULL,
    entity_id       INT         NOT NULL,
    action          TEXT        NOT NULL
                        CHECK (action IN ('create', 'update', 'delete', 'merge', 'convert', 'assign', 'system')),
    old_values_json JSONB,
    new_values_json JSONB,
    ip_address      INET,
    user_agent      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)      REFERENCES users(id)      ON DELETE SET NULL
);

-- -----------------------------------------------------------------
-- INDEXES  (add as needed for your query patterns)
-- -----------------------------------------------------------------

CREATE INDEX idx_users_workspace         ON users(workspace_id);
CREATE INDEX idx_accounts_workspace      ON accounts(workspace_id);
CREATE INDEX idx_accounts_owner          ON accounts(owner_user_id);
CREATE INDEX idx_contacts_workspace      ON contacts(workspace_id);
CREATE INDEX idx_contacts_account        ON contacts(primary_account_id);
CREATE INDEX idx_leads_workspace         ON leads(workspace_id);
CREATE INDEX idx_leads_owner             ON leads(owner_user_id);
CREATE INDEX idx_leads_status            ON leads(status);
CREATE INDEX idx_opportunities_workspace ON opportunities(workspace_id);
CREATE INDEX idx_opportunities_stage     ON opportunities(stage_id);
CREATE INDEX idx_opportunities_account   ON opportunities(account_id);
CREATE INDEX idx_tickets_workspace       ON tickets(workspace_id);
CREATE INDEX idx_tickets_status          ON tickets(status);
CREATE INDEX idx_activities_entity       ON activities(related_entity_type, related_entity_id);
CREATE INDEX idx_tasks_entity            ON tasks(related_entity_type, related_entity_id);
CREATE INDEX idx_audit_logs_entity       ON audit_logs(entity_type, entity_id);
CREATE INDEX idx_audit_logs_workspace    ON audit_logs(workspace_id);
