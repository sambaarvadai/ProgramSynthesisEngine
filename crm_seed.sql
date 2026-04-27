-- =================================================================
-- CRM Seed Data — PostgreSQL
-- Realistic B2B SaaS scenario: two workspaces, sales team,
-- accounts, contacts, leads, opportunities, quotes, tickets,
-- activities, tasks, notes, emails, calls, tags, custom fields.
-- Run AFTER crm_postgres.sql
-- =================================================================

-- -----------------------------------------------------------------
-- WORKSPACES
-- -----------------------------------------------------------------
INSERT INTO workspaces (name, slug, plan, status) VALUES
    ('Acme Corp CRM',    'acme-corp',    'enterprise', 'active'),
    ('Startup Hub CRM',  'startup-hub',  'growth',     'active');

-- -----------------------------------------------------------------
-- ROLES
-- -----------------------------------------------------------------
INSERT INTO roles (workspace_id, name, description) VALUES
    (1, 'Admin',       'Full access to all workspace features'),
    (1, 'Sales Rep',   'Can manage leads, contacts, opportunities'),
    (1, 'Sales Manager','Can view all reps'' data and run reports'),
    (1, 'Support Agent','Can manage tickets and customer comms'),
    (2, 'Admin',       'Full access'),
    (2, 'Sales Rep',   'Standard sales access');

-- -----------------------------------------------------------------
-- USERS  (password_hash is bcrypt placeholder)
-- -----------------------------------------------------------------
INSERT INTO users (workspace_id, first_name, last_name, email, phone, password_hash, status, last_login_at) VALUES
    -- Acme Corp
    (1, 'Priya',    'Sharma',    'priya.sharma@acmecorp.io',   '+91-9876543210', '$2b$12$placeholder_hash_1', 'active', NOW() - INTERVAL '1 hour'),
    (1, 'Rajan',    'Mehta',     'rajan.mehta@acmecorp.io',    '+91-9123456780', '$2b$12$placeholder_hash_2', 'active', NOW() - INTERVAL '3 hours'),
    (1, 'Divya',    'Nair',      'divya.nair@acmecorp.io',     '+91-9988776655', '$2b$12$placeholder_hash_3', 'active', NOW() - INTERVAL '2 days'),
    (1, 'Karthik',  'Iyer',      'karthik.iyer@acmecorp.io',   '+91-9001122334', '$2b$12$placeholder_hash_4', 'active', NOW() - INTERVAL '5 hours'),
    (1, 'Sneha',    'Pillai',    'sneha.pillai@acmecorp.io',   '+91-8877665544', '$2b$12$placeholder_hash_5', 'active', NOW() - INTERVAL '1 day'),
    -- Startup Hub
    (2, 'Arjun',    'Kapoor',    'arjun.kapoor@startuphub.io', '+91-9765432109', '$2b$12$placeholder_hash_6', 'active', NOW() - INTERVAL '30 minutes'),
    (2, 'Meena',    'Reddy',     'meena.reddy@startuphub.io',  '+91-9654321098', '$2b$12$placeholder_hash_7', 'active', NOW() - INTERVAL '6 hours');

-- -----------------------------------------------------------------
-- TEAMS
-- -----------------------------------------------------------------
INSERT INTO teams (workspace_id, name, manager_user_id) VALUES
    (1, 'Enterprise Sales', 3),
    (1, 'SMB Sales',        2),
    (1, 'Customer Support', 5),
    (2, 'Growth',           6);

-- -----------------------------------------------------------------
-- USER WORKSPACE MEMBERSHIPS
-- -----------------------------------------------------------------
INSERT INTO user_workspace_memberships (user_id, workspace_id, role_id, team_id, is_active) VALUES
    (1, 1, 1, NULL, TRUE),  -- Priya: Admin
    (2, 1, 3, 2,    TRUE),  -- Rajan: Sales Manager, SMB
    (3, 1, 3, 1,    TRUE),  -- Divya: Sales Manager, Enterprise
    (4, 1, 2, 1,    TRUE),  -- Karthik: Sales Rep, Enterprise
    (5, 1, 4, 3,    TRUE),  -- Sneha: Support Agent
    (6, 2, 5, 4,    TRUE),  -- Arjun: Admin, Growth
    (7, 2, 6, 4,    TRUE);  -- Meena: Sales Rep, Growth

-- -----------------------------------------------------------------
-- ACCOUNTS
-- -----------------------------------------------------------------
INSERT INTO accounts (workspace_id, owner_user_id, name, legal_name, website, industry, employee_count, annual_revenue, phone, email, billing_city, billing_state, billing_country, status, source) VALUES
    (1, 4, 'TechNova Solutions',   'TechNova Solutions Pvt Ltd',  'https://technova.io',     'SaaS',           500,   12000000, '+91-80-12345678', 'hello@technova.io',     'Bangalore',  'Karnataka',     'India',         'customer',  'inbound'),
    (1, 4, 'Meridian Retail',      'Meridian Retail Ltd',         'https://meridianretail.in','Retail',         2000,  85000000, '+91-22-98765432', 'procurement@meridian.in','Mumbai',    'Maharashtra',   'India',         'customer',  'outbound'),
    (1, 2, 'CloudBridge Inc',      'CloudBridge Inc',             'https://cloudbridge.com',  'Cloud Infra',    150,   4500000,  '+1-415-555-0101', 'info@cloudbridge.com',  'San Francisco','California', 'USA',           'prospect',  'referral'),
    (1, 2, 'Apex Logistics',       'Apex Logistics Pvt Ltd',      'https://apexlogistics.co', 'Logistics',      800,   32000000, '+91-44-44556677', 'ops@apexlogistics.co',  'Chennai',    'Tamil Nadu',    'India',         'customer',  'partner'),
    (1, 4, 'Vertex Analytics',     'Vertex Analytics GmbH',       'https://vertexanalytics.de','Analytics',    75,    2100000,  '+49-30-55667788', 'contact@vertexanalytics.de','Berlin', 'Berlin',        'Germany',       'prospect',  'webinar'),
    (1, 2, 'PulseHealth',          'PulseHealth Technologies',    'https://pulsehealth.io',   'HealthTech',     300,   9000000,  '+91-40-33221100', 'info@pulsehealth.io',   'Hyderabad',  'Telangana',     'India',         'partner',   'conference'),
    (1, 4, 'Orion Fintech',        'Orion Fintech Pvt Ltd',       'https://orionfintech.in',  'Fintech',        120,   5500000,  '+91-80-99887766', 'bd@orionfintech.in',    'Bangalore',  'Karnataka',     'India',         'prospect',  'cold_outreach'),
    (2, 6, 'GreenLeaf Farms',      'GreenLeaf Farms Ltd',         'https://greenleaffarms.com','AgriTech',      50,    800000,   '+91-20-11223344', 'ceo@greenleaffarms.com','Pune',       'Maharashtra',   'India',         'prospect',  'inbound');

-- -----------------------------------------------------------------
-- CONTACTS
-- -----------------------------------------------------------------
INSERT INTO contacts (workspace_id, owner_user_id, primary_account_id, first_name, last_name, full_name, job_title, email, phone, mobile, department, lifecycle_stage, source) VALUES
    (1, 4, 1, 'Ananya',   'Singh',    'Ananya Singh',    'CTO',                      'ananya.singh@technova.io',    '+91-80-12345679', '+91-9900112233', 'Engineering',   'customer',   'inbound'),
    (1, 4, 1, 'Rohan',    'Verma',    'Rohan Verma',     'VP Engineering',            'rohan.verma@technova.io',     '+91-80-12345680', '+91-9900112244', 'Engineering',   'customer',   'inbound'),
    (1, 4, 2, 'Suresh',   'Patel',    'Suresh Patel',    'Head of Procurement',       'suresh.patel@meridian.in',    '+91-22-98765433', '+91-9811223344', 'Procurement',   'customer',   'outbound'),
    (1, 2, 3, 'Emily',    'Chen',     'Emily Chen',      'CEO',                       'emily.chen@cloudbridge.com',  '+1-415-555-0102', '+1-415-555-0103','Executive',     'sql',        'referral'),
    (1, 2, 4, 'Deepak',   'Kumar',    'Deepak Kumar',    'Director of Operations',    'deepak.kumar@apexlogistics.co','+91-44-44556678','+91-9888001122', 'Operations',    'customer',   'partner'),
    (1, 4, 5, 'Klaus',    'Müller',   'Klaus Müller',    'Head of Data Science',      'k.muller@vertexanalytics.de', '+49-30-55667789', '+49-151-22334455','Data',         'mql',        'webinar'),
    (1, 2, 6, 'Lakshmi',  'Rao',      'Lakshmi Rao',     'CTO',                       'lakshmi.rao@pulsehealth.io',  '+91-40-33221101', '+91-9700556677', 'Technology',    'customer',   'conference'),
    (1, 4, 7, 'Vijay',    'Krishnan', 'Vijay Krishnan',  'Founder & CEO',             'vijay@orionfintech.in',       '+91-80-99887767', '+91-9500334455', 'Executive',     'sql',        'cold_outreach'),
    (1, 2, 3, 'Marcus',   'Wright',   'Marcus Wright',   'VP Sales',                  'marcus.wright@cloudbridge.com','+1-415-555-0104','+1-415-555-0105','Sales',         'opportunity','referral'),
    (2, 6, 8, 'Pramod',   'Desai',    'Pramod Desai',    'Founder',                   'pramod@greenleaffarms.com',   '+91-20-11223345', '+91-9600778899', 'Executive',     'mql',        'inbound');

-- -----------------------------------------------------------------
-- CONTACT ACCOUNT LINKS
-- -----------------------------------------------------------------
INSERT INTO contact_account_links (workspace_id, contact_id, account_id, relationship_type, is_primary) VALUES
    (1, 1, 1, 'employee', TRUE),
    (1, 2, 1, 'employee', FALSE),
    (1, 3, 2, 'employee', TRUE),
    (1, 4, 3, 'employee', TRUE),
    (1, 5, 4, 'employee', TRUE),
    (1, 6, 5, 'employee', TRUE),
    (1, 7, 6, 'employee', TRUE),
    (1, 8, 7, 'employee', TRUE),
    (1, 9, 3, 'employee', FALSE),
    (2, 10, 8, 'employee', TRUE);

-- -----------------------------------------------------------------
-- LEADS
-- -----------------------------------------------------------------
INSERT INTO leads (workspace_id, owner_user_id, first_name, last_name, company_name, title, email, phone, source, status, score, estimated_value, notes) VALUES
    (1, 4, 'Aditya',  'Joshi',    'DataSphere AI',     'Head of Product',     'aditya.joshi@datasphere.ai',  '+91-9123450000', 'webinar',       'working',      75,  250000, 'Attended our AI pipeline webinar. Very engaged.'),
    (1, 2, 'Preethi', 'Anand',    'LogiTrack',         'CTO',                 'preethi.anand@logitrack.in',  '+91-9234561111', 'inbound',       'qualified',    90,  480000, 'Inbound demo request. Clear pain point around data ops.'),
    (1, 4, 'Samuel',  'Okonkwo',  'AfriPay',           'VP Engineering',      's.okonkwo@afripay.com',       '+234-8012345678','conference',    'new',          30,  150000, 'Met at FinTech Africa summit.'),
    (1, 2, 'Lin',     'Wei',      'Shenzen Cloud Co',  'Director',            'lin.wei@shenzencloud.cn',      '+86-13800138000','cold_outreach', 'disqualified', 10,  0,      'No budget this FY. Follow up Q1 next year.'),
    (1, 4, 'Meera',   'Subramanian','EduNext',         'Co-Founder',          'meera@edunext.io',             '+91-9876501234', 'referral',      'working',      60,  90000,  'Referral from Lakshmi Rao at PulseHealth.'),
    (2, 7, 'Rahul',   'Bose',     'FoodFast Delivery', 'CTO',                 'rahul@foodfast.in',            '+91-9000123456', 'inbound',       'new',          20,  70000,  'Signed up for free trial.');

-- -----------------------------------------------------------------
-- PIPELINES & STAGES
-- -----------------------------------------------------------------
INSERT INTO pipelines (workspace_id, name, type, is_default) VALUES
    (1, 'Enterprise Sales Pipeline', 'sales',     TRUE),
    (1, 'SMB Sales Pipeline',        'sales',     FALSE),
    (1, 'Customer Onboarding',       'onboarding',FALSE),
    (1, 'Renewals 2025',             'renewals',  FALSE),
    (2, 'Startup Sales',             'sales',     TRUE);

INSERT INTO pipeline_stages (pipeline_id, name, stage_order, probability_percent, is_closed_won, is_closed_lost) VALUES
    -- Enterprise Sales Pipeline (1)
    (1, 'Prospecting',         1,  10,  FALSE, FALSE),
    (1, 'Discovery',           2,  25,  FALSE, FALSE),
    (1, 'Demo / Evaluation',   3,  50,  FALSE, FALSE),
    (1, 'Proposal Sent',       4,  70,  FALSE, FALSE),
    (1, 'Negotiation',         5,  85,  FALSE, FALSE),
    (1, 'Closed Won',          6, 100,  TRUE,  FALSE),
    (1, 'Closed Lost',         7,   0,  FALSE, TRUE),
    -- SMB Sales Pipeline (2)
    (2, 'Lead In',             1,  15,  FALSE, FALSE),
    (2, 'Qualified',           2,  40,  FALSE, FALSE),
    (2, 'Demo Done',           3,  65,  FALSE, FALSE),
    (2, 'Closed Won',          4, 100,  TRUE,  FALSE),
    (2, 'Closed Lost',         5,   0,  FALSE, TRUE),
    -- Customer Onboarding (3)
    (3, 'Kickoff Scheduled',   1,  100, FALSE, FALSE),
    (3, 'In Progress',         2,  100, FALSE, FALSE),
    (3, 'Go Live',             3,  100, TRUE,  FALSE),
    -- Renewals 2025 (4)
    (4, 'Renewal Due',         1,  70,  FALSE, FALSE),
    (4, 'In Negotiation',      2,  85,  FALSE, FALSE),
    (4, 'Renewed',             3,  100, TRUE,  FALSE),
    (4, 'Churned',             4,  0,   FALSE, TRUE),
    -- Startup Sales (5)
    (5, 'Interested',          1,  20,  FALSE, FALSE),
    (5, 'Demo',                2,  50,  FALSE, FALSE),
    (5, 'Proposal',            3,  75,  FALSE, FALSE),
    (5, 'Closed Won',          4,  100, TRUE,  FALSE),
    (5, 'Closed Lost',         5,  0,   FALSE, TRUE);

-- -----------------------------------------------------------------
-- OPPORTUNITIES
-- -----------------------------------------------------------------
INSERT INTO opportunities (workspace_id, pipeline_id, stage_id, owner_user_id, account_id, primary_contact_id, name, amount, currency_code, probability_percent, expected_close_date, status, source) VALUES
    (1, 1, 4, 4, 1, 1,  'TechNova — Platform Expansion Q2',   480000, 'INR', 70, '2025-06-30', 'open', 'inbound'),
    (1, 1, 3, 4, 5, 6,  'Vertex Analytics — New Contract',    210000, 'EUR', 50, '2025-07-15', 'open', 'webinar'),
    (1, 1, 5, 4, 7, 8,  'Orion Fintech — Enterprise Deal',    550000, 'INR', 85, '2025-05-31', 'open', 'cold_outreach'),
    (1, 2, 9, 2, 3, 4,  'CloudBridge — SMB Starter',          95000,  'USD', 40, '2025-06-15', 'open', 'referral'),
    (1, 1, 6, 4, 2, 3,  'Meridian Retail — Renewal Upsell',   920000, 'INR', 100,'2025-04-01', 'won',  'outbound'),
    (1, 1, 7, 2, 3, 9,  'CloudBridge — Enterprise (Lost)',    300000, 'USD', 0,  '2025-03-15', 'lost', 'referral'),
    (1, 4, 16,2, 4, 5,  'Apex Logistics — 2025 Renewal',      640000, 'INR', 70, '2025-08-31', 'open', 'partner'),
    (2, 5, 21,6, 8, 10, 'GreenLeaf Farms — Starter Plan',     48000,  'INR', 20, '2025-07-01', 'open', 'inbound');

-- -----------------------------------------------------------------
-- OPPORTUNITY STAGE HISTORY
-- -----------------------------------------------------------------
INSERT INTO opportunity_stage_history (opportunity_id, from_stage_id, to_stage_id, changed_by_user_id, changed_at) VALUES
    (1, NULL, 1, 4, NOW() - INTERVAL '60 days'),
    (1, 1,    2, 4, NOW() - INTERVAL '45 days'),
    (1, 2,    3, 4, NOW() - INTERVAL '30 days'),
    (1, 3,    4, 4, NOW() - INTERVAL '10 days'),
    (2, NULL, 1, 4, NOW() - INTERVAL '40 days'),
    (2, 1,    2, 4, NOW() - INTERVAL '25 days'),
    (2, 2,    3, 4, NOW() - INTERVAL '10 days'),
    (3, NULL, 1, 4, NOW() - INTERVAL '50 days'),
    (3, 1,    2, 4, NOW() - INTERVAL '35 days'),
    (3, 2,    3, 4, NOW() - INTERVAL '20 days'),
    (3, 3,    4, 4, NOW() - INTERVAL '10 days'),
    (3, 4,    5, 4, NOW() - INTERVAL '3 days'),
    (5, NULL, 1, 4, NOW() - INTERVAL '90 days'),
    (5, 1,    6, 4, NOW() - INTERVAL '15 days');

-- -----------------------------------------------------------------
-- PRODUCTS
-- -----------------------------------------------------------------
INSERT INTO products (workspace_id, sku, name, description, unit_price, currency_code, is_active) VALUES
    (1, 'PLT-ENT-ANN',  'Platform — Enterprise Annual',   'Full platform access, unlimited users, SLA 99.9%', 480000, 'INR', TRUE),
    (1, 'PLT-PRO-ANN',  'Platform — Pro Annual',          'Up to 50 users, SLA 99.5%',                        180000, 'INR', TRUE),
    (1, 'PLT-SMB-ANN',  'Platform — SMB Annual',          'Up to 10 users',                                    72000, 'INR', TRUE),
    (1, 'ADD-API',       'API Add-on',                    'Additional 1M API calls/month',                      24000, 'INR', TRUE),
    (1, 'ADD-STORAGE',   'Storage Add-on (100GB)',        'Additional 100GB object storage',                    12000, 'INR', TRUE),
    (1, 'SVC-IMPL',      'Implementation Services',       'Dedicated implementation engineer, 40 hours',       120000, 'INR', TRUE),
    (1, 'PLT-ENT-USD',  'Platform — Enterprise (USD)',    'USD pricing for international accounts',              5800, 'USD', TRUE),
    (2, 'PLT-STR-ANN',  'Starter Plan Annual',            'Up to 5 users',                                      48000, 'INR', TRUE);

-- -----------------------------------------------------------------
-- OPPORTUNITY PRODUCTS
-- -----------------------------------------------------------------
INSERT INTO opportunity_products (opportunity_id, product_id, quantity, unit_price, discount_percent, tax_percent, line_total) VALUES
    (1, 1, 1, 480000, 0,  18, 480000),
    (1, 4, 2, 24000,  10, 18,  43200),
    (2, 7, 1, 5800,   5,  19,   5510),
    (3, 1, 1, 480000, 10, 18, 432000),
    (3, 6, 1, 120000, 0,  18, 120000),
    (4, 7, 1, 5800,   0,  0,    5800),
    (5, 1, 1, 480000, 0,  18, 480000),
    (5, 5, 4, 12000,  0,  18,  48000);

-- -----------------------------------------------------------------
-- QUOTES
-- -----------------------------------------------------------------
INSERT INTO quotes (workspace_id, opportunity_id, account_id, contact_id, quote_number, status, issue_date, expiry_date, subtotal, discount_total, tax_total, grand_total, currency_code, terms, created_by_user_id) VALUES
    (1, 1, 1, 1,  'QT-2025-001', 'sent',     '2025-04-01', '2025-05-01',  523200,  43200, 96048,  576048, 'INR', 'Net 30. Prices valid for 30 days.',        4),
    (1, 3, 7, 8,  'QT-2025-002', 'draft',    '2025-04-10', '2025-05-10',  552000,  48000, 90720,  594720, 'INR', 'Net 15. Subject to final legal review.',   4),
    (1, 5, 2, 3,  'QT-2025-003', 'accepted', '2025-03-15', '2025-04-15',  528000,  0,     95040,  623040, 'INR', 'Net 30.',                                  4),
    (1, 4, 3, 4,  'QT-2025-004', 'sent',     '2025-04-05', '2025-05-05',  5800,    0,     0,       5800,  'USD', 'Net 30.',                                  2);

-- -----------------------------------------------------------------
-- QUOTE ITEMS
-- -----------------------------------------------------------------
INSERT INTO quote_items (quote_id, product_id, description, quantity, unit_price, discount_percent, tax_percent, line_total) VALUES
    (1, 1, 'Platform — Enterprise Annual',    1, 480000, 0,  18, 480000),
    (1, 4, 'API Add-on (x2)',                 2,  24000, 10, 18,  43200),
    (2, 1, 'Platform — Enterprise Annual',    1, 480000, 10, 18, 432000),
    (2, 6, 'Implementation Services',         1, 120000, 0,  18, 120000),
    (3, 1, 'Platform — Enterprise Annual',    1, 480000, 0,  18, 480000),
    (3, 5, 'Storage Add-on (100GB) x4',       4,  12000, 0,  18,  48000),
    (4, 7, 'Platform — Enterprise (USD)',     1,   5800, 0,  0,    5800);

-- -----------------------------------------------------------------
-- TASKS
-- -----------------------------------------------------------------
INSERT INTO tasks (workspace_id, owner_user_id, assigned_to_user_id, related_entity_type, related_entity_id, title, description, due_at, priority, status) VALUES
    (1, 4, 4, 'opportunity', 1, 'Send revised pricing proposal',        'Update discount structure per last call with Ananya.',          NOW() + INTERVAL '2 days',  'high',   'open'),
    (1, 4, 4, 'opportunity', 2, 'Schedule technical deep dive',         'Set up call with Klaus and our solutions engineer.',           NOW() + INTERVAL '5 days',  'medium', 'open'),
    (1, 4, 4, 'opportunity', 3, 'Prepare contract for legal review',    'Draft final agreement for Orion Fintech.',                     NOW() + INTERVAL '1 day',   'urgent', 'in_progress'),
    (1, 2, 2, 'lead',        2, 'Follow up with Preethi Anand',         'She requested a product demo. Send Calendly link.',            NOW() + INTERVAL '1 day',   'high',   'open'),
    (1, 5, 5, 'ticket',      1, 'Investigate API timeout issue',        'Customer reporting 504 errors on bulk export endpoint.',       NOW(),                      'urgent', 'in_progress'),
    (1, 4, 4, 'contact',     8, 'Send case study deck to Vijay',        'Share FinTech use case PDF.',                                  NOW() + INTERVAL '3 days',  'medium', 'open'),
    (1, 2, 2, 'account',     3, 'Quarterly business review prep',       'Prepare QBR slides for CloudBridge.',                         NOW() + INTERVAL '14 days', 'medium', 'open'),
    (2, 6, 7, 'lead',        6, 'Demo with Rahul Bose',                 'Show FoodFast how our pipeline handles high-volume orders.',  NOW() + INTERVAL '3 days',  'medium', 'open');

-- -----------------------------------------------------------------
-- ACTIVITIES
-- -----------------------------------------------------------------
INSERT INTO activities (workspace_id, actor_user_id, related_entity_type, related_entity_id, activity_type, subject, description, activity_at, duration_minutes, outcome) VALUES
    (1, 4, 'opportunity', 1, 'call',    'Discovery call with Ananya Singh',          'Discussed current pain points in data orchestration. Strong fit.',     NOW() - INTERVAL '30 days', 45,  'positive'),
    (1, 4, 'opportunity', 1, 'demo',    'Platform demo — TechNova',                  'Live demo of pipeline builder and analytics module. Very engaged.',    NOW() - INTERVAL '20 days', 90,  'positive'),
    (1, 4, 'opportunity', 1, 'email',   'Proposal sent to Ananya',                   'Sent QT-2025-001 via email. Awaiting feedback.',                       NOW() - INTERVAL '5 days',  NULL, 'pending'),
    (1, 4, 'opportunity', 3, 'meeting', 'In-person meeting with Vijay Krishnan',     'Met at Orion Fintech HQ. Discussed enterprise rollout plan.',          NOW() - INTERVAL '8 days',  120, 'positive'),
    (1, 2, 'lead',        2, 'call',    'Qualification call with Preethi Anand',     'Confirmed budget and decision timeline. Moving to demo stage.',        NOW() - INTERVAL '5 days',  30,  'qualified'),
    (1, 5, 'ticket',      1, 'note',    'Escalated to engineering',                  'Reproduced the 504 error. Passing to backend team.',                   NOW() - INTERVAL '1 day',   NULL, NULL),
    (1, 4, 'contact',     6, 'email',   'Follow-up after webinar',                   'Shared product one-pager and case study with Klaus.',                  NOW() - INTERVAL '12 days', NULL, 'pending'),
    (1, 2, 'opportunity', 4, 'demo',    'CloudBridge SMB demo',                      'Showed core features. Emily Chen was impressed by the API layer.',     NOW() - INTERVAL '15 days', 60,  'positive'),
    (2, 6, 'lead',        6, 'call',    'Initial call with Rahul Bose',              'Understood their delivery tracking problem. Sending trial access.',    NOW() - INTERVAL '2 days',  25,  'positive');

-- -----------------------------------------------------------------
-- NOTES
-- -----------------------------------------------------------------
INSERT INTO notes (workspace_id, created_by_user_id, related_entity_type, related_entity_id, content, is_private) VALUES
    (1, 4, 'opportunity', 1, 'Ananya mentioned they are evaluating two other vendors. Key differentiator for us is the intent-native pipeline builder. Emphasize this in the proposal.',                   FALSE),
    (1, 4, 'opportunity', 3, 'Vijay is the sole decision maker. Legal team will need 2 weeks to review contract once submitted. Plan accordingly.',                                                        FALSE),
    (1, 4, 'contact',     8, 'Vijay is very technically sharp. Avoid generic pitches. He responds well to architecture diagrams and benchmark data.',                                                      TRUE),
    (1, 2, 'lead',        2, 'Preethi''s current stack: Airflow + dbt + Redshift. Our platform can replace the Airflow + dbt layer. Strong upsell angle.',                                               FALSE),
    (1, 5, 'ticket',      1, 'Engineering confirmed a rate-limiting bug in the bulk export API. Fix scheduled for v2.4.1 release next week. Will notify customer.',                                        FALSE),
    (1, 2, 'account',     3, 'CloudBridge had a bad experience with a competitor''s support team. Proactively schedule a support intro call to build trust.',                                             TRUE),
    (2, 6, 'lead',        6, 'Rahul runs a lean team of 3 engineers. They need a solution that requires minimal setup. Starter plan is right. Offer extended trial.',                                     FALSE);

-- -----------------------------------------------------------------
-- TICKETS
-- -----------------------------------------------------------------
INSERT INTO tickets (workspace_id, account_id, contact_id, owner_user_id, subject, description, priority, status, channel, category, first_response_at) VALUES
    (1, 1, 1, 5, 'Bulk export API returning 504 timeout',              'When exporting more than 50k rows via the bulk export API, we consistently get 504 Gateway Timeout after ~30 seconds.',   'urgent', 'open',     'email', 'Bug',         NOW() - INTERVAL '1 day'),
    (1, 2, 3, 5, 'Invoice for April not received',                     'We have not received the April 2025 invoice yet. Can you resend to accounts@meridian.in?',                                'medium', 'resolved', 'email', 'Billing',     NOW() - INTERVAL '3 days'),
    (1, 4, 5, 5, 'SSO configuration issue with Okta',                  'Our Okta SSO integration keeps failing on the callback URL. Error: invalid_client.',                                       'high',   'pending',  'web',   'Integration', NOW() - INTERVAL '5 hours'),
    (1, 1, 2, 5, 'Request for custom role permissions documentation',  'Is there documentation on how to configure custom roles beyond the four default ones?',                                    'low',    'closed',   'web',   'Docs',        NOW() - INTERVAL '10 days');

-- -----------------------------------------------------------------
-- TICKET COMMENTS
-- -----------------------------------------------------------------
INSERT INTO ticket_comments (ticket_id, user_id, contact_id, body, is_private) VALUES
    (1, 5,    NULL, 'Thank you for reporting this. We have reproduced the issue and escalated to our engineering team. Expected fix in v2.4.1 next week.',          FALSE),
    (1, NULL, 1,    'Thanks for the quick response. Is there a workaround we can use in the meantime? We need to run this export for a board report.',              FALSE),
    (1, 5,    NULL, 'Workaround: batch your requests to 10k rows per call using the offset/limit params. I will share a sample script shortly.',                    FALSE),
    (1, 4,    NULL, 'Internal: engineering confirmed root cause is a missing index on the export queue table. PR is in review.',                                     TRUE),
    (2, 5,    NULL, 'Invoice resent to accounts@meridian.in. Please confirm receipt.',                                                                              FALSE),
    (3, 5,    NULL, 'Could you share your Okta application client ID and the exact callback URL you configured? We will verify against our allowed redirect list.', FALSE);

-- -----------------------------------------------------------------
-- EMAILS
-- -----------------------------------------------------------------
INSERT INTO emails (workspace_id, owner_user_id, related_entity_type, related_entity_id, direction, subject, body_text, sent_at, status) VALUES
    (1, 4, 'opportunity', 1, 'outbound', 'Your Q2 Proposal — TechNova Solutions',       'Hi Ananya, please find the proposal attached. Happy to walk through it on a call.',           NOW() - INTERVAL '5 days',  'delivered'),
    (1, 4, 'contact',     6, 'outbound', 'Following up from our webinar',                'Hi Klaus, great connecting at the webinar. Attaching our analytics platform one-pager.',      NOW() - INTERVAL '12 days', 'delivered'),
    (1, 2, 'lead',        2, 'outbound', 'Platform Demo — Scheduling',                   'Hi Preethi, I would love to show you how we can simplify your Airflow + dbt workflow.',       NOW() - INTERVAL '4 days',  'delivered'),
    (1, 5, 'ticket',      1, 'outbound', 'Re: Bulk export API returning 504 timeout',    'Hi Ananya, see workaround details below. Engineering fix is scheduled for next week.',        NOW() - INTERVAL '20 hours','delivered'),
    (1, 4, 'opportunity', 1, 'inbound',  'Re: Your Q2 Proposal — TechNova Solutions',   'Hi Karthik, thanks for sending this over. A few questions on the API add-on pricing...',     NOW() - INTERVAL '3 days',  'delivered');

-- -----------------------------------------------------------------
-- EMAIL PARTICIPANTS
-- -----------------------------------------------------------------
INSERT INTO email_participants (email_id, participant_type, contact_id, email_address, display_name) VALUES
    (1, 'from', NULL, 'karthik.iyer@acmecorp.io',      'Karthik Iyer'),
    (1, 'to',   1,    'ananya.singh@technova.io',       'Ananya Singh'),
    (2, 'from', NULL, 'karthik.iyer@acmecorp.io',       'Karthik Iyer'),
    (2, 'to',   6,    'k.muller@vertexanalytics.de',    'Klaus Müller'),
    (3, 'from', NULL, 'rajan.mehta@acmecorp.io',        'Rajan Mehta'),
    (3, 'to',   4,    'preethi.anand@logitrack.in',     'Preethi Anand'),
    (4, 'from', NULL, 'sneha.pillai@acmecorp.io',       'Sneha Pillai'),
    (4, 'to',   1,    'ananya.singh@technova.io',       'Ananya Singh'),
    (5, 'from', 1,    'ananya.singh@technova.io',       'Ananya Singh'),
    (5, 'to',   NULL, 'karthik.iyer@acmecorp.io',       'Karthik Iyer');

-- -----------------------------------------------------------------
-- CALLS
-- -----------------------------------------------------------------
INSERT INTO calls (workspace_id, owner_user_id, related_entity_type, related_entity_id, contact_id, started_at, ended_at, duration_seconds, direction, outcome, notes) VALUES
    (1, 4, 'opportunity', 1, 1, NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days' + INTERVAL '45 minutes', 2700, 'outbound', 'positive',    'Strong discovery. Agreed to demo next week.'),
    (1, 4, 'opportunity', 3, 8, NOW() - INTERVAL '10 days', NOW() - INTERVAL '10 days' + INTERVAL '35 minutes', 2100, 'outbound', 'positive',    'Vijay confirmed decision timeline: end of May.'),
    (1, 2, 'lead',        2, NULL, NOW() - INTERVAL '5 days',NOW() - INTERVAL '5 days' + INTERVAL '30 minutes', 1800, 'inbound',  'qualified',   'Preethi reached out. Ready for demo.'),
    (1, 5, 'ticket',      1, 1, NOW() - INTERVAL '1 day',   NOW() - INTERVAL '1 day'  + INTERVAL '20 minutes',  1200, 'inbound',  'workaround',  'Walked Ananya through the batch workaround.');

-- -----------------------------------------------------------------
-- MESSAGES
-- -----------------------------------------------------------------
INSERT INTO messages (workspace_id, related_entity_type, related_entity_id, channel, direction, sender_user_id, recipient_contact_id, body, sent_at, status) VALUES
    (1, 'lead',        2, 'whatsapp', 'outbound', 2, 4,  'Hi Preethi, I just sent you a calendar invite for the demo. Let me know if the time works!', NOW() - INTERVAL '3 days', 'read'),
    (1, 'opportunity', 3, 'whatsapp', 'outbound', 4, 8,  'Hi Vijay, following up on the proposal. Happy to jump on a quick call if you have questions.', NOW() - INTERVAL '2 days', 'delivered'),
    (2, 'lead',        6, 'whatsapp', 'inbound',  NULL, NULL, 'Hey, I signed up for the trial but cannot figure out how to connect our DB. Can you help?', NOW() - INTERVAL '1 day',  'read');

-- -----------------------------------------------------------------
-- TAGS
-- -----------------------------------------------------------------
INSERT INTO tags (workspace_id, name, color) VALUES
    (1, 'High Value',       '#F59E0B'),
    (1, 'Enterprise',       '#6366F1'),
    (1, 'At Risk',          '#EF4444'),
    (1, 'Quick Win',        '#10B981'),
    (1, 'Partner Referral', '#3B82F6'),
    (1, 'FinTech',          '#8B5CF6'),
    (1, 'SaaS',             '#06B6D4'),
    (2, 'Startup',          '#F97316');

-- -----------------------------------------------------------------
-- ENTITY TAGS
-- -----------------------------------------------------------------
INSERT INTO entity_tags (workspace_id, tag_id, entity_type, entity_id) VALUES
    (1, 1, 'opportunity', 1),   -- TechNova: High Value
    (1, 2, 'opportunity', 1),   -- TechNova: Enterprise
    (1, 7, 'account',     1),   -- TechNova: SaaS
    (1, 1, 'opportunity', 3),   -- Orion: High Value
    (1, 2, 'opportunity', 3),   -- Orion: Enterprise
    (1, 6, 'account',     7),   -- Orion Fintech: FinTech
    (1, 3, 'opportunity', 2),   -- Vertex: At Risk
    (1, 5, 'lead',        5),   -- EduNext lead: Partner Referral
    (1, 4, 'lead',        2),   -- LogiTrack lead: Quick Win
    (2, 8, 'account',     8);   -- GreenLeaf: Startup

-- -----------------------------------------------------------------
-- CUSTOM FIELDS
-- -----------------------------------------------------------------
INSERT INTO custom_fields (workspace_id, entity_type, field_key, label, data_type, is_required, options_json) VALUES
    (1, 'opportunity', 'competitor',       'Primary Competitor',      'select', FALSE, '{"options": ["Salesforce", "HubSpot", "Pipedrive", "Zoho", "None"]}'),
    (1, 'opportunity', 'deal_source_camp', 'Campaign Source',         'text',   FALSE, NULL),
    (1, 'contact',     'linkedin_score',   'LinkedIn Engagement Score','number', FALSE, NULL),
    (1, 'account',     'nps_score',        'NPS Score',               'number', FALSE, NULL),
    (1, 'lead',        'intent_signal',    'Intent Signal',           'select', FALSE, '{"options": ["webinar", "pricing_page", "docs", "trial", "none"]}');

-- -----------------------------------------------------------------
-- CUSTOM FIELD VALUES
-- -----------------------------------------------------------------
INSERT INTO custom_field_values (workspace_id, custom_field_id, entity_type, entity_id, value_text, value_number) VALUES
    (1, 1, 'opportunity', 1, 'HubSpot',    NULL),
    (1, 1, 'opportunity', 3, 'Salesforce', NULL),
    (1, 1, 'opportunity', 4, 'Pipedrive',  NULL),
    (1, 3, 'contact',     1, NULL,         82),
    (1, 3, 'contact',     6, NULL,         67),
    (1, 4, 'account',     1, NULL,         72),
    (1, 4, 'account',     4, NULL,         85),
    (1, 5, 'lead',        2, 'pricing_page', NULL),
    (1, 5, 'lead',        1, 'webinar',      NULL);

-- -----------------------------------------------------------------
-- LEAD SCORE EVENTS
-- -----------------------------------------------------------------
INSERT INTO lead_score_events (lead_id, event_type, score_delta, reason) VALUES
    (1, 'webinar_attended',     25, 'Attended AI pipeline webinar'),
    (1, 'email_opened',          5, 'Opened follow-up email'),
    (1, 'pricing_page_visited', 20, 'Visited pricing page twice'),
    (1, 'demo_requested',       25, 'Submitted demo request form'),
    (2, 'demo_requested',       40, 'Submitted inbound demo request'),
    (2, 'email_replied',        20, 'Replied to outbound sequence'),
    (2, 'call_completed',       30, 'Qualification call completed'),
    (5, 'referral',             30, 'Referred by existing customer'),
    (5, 'email_opened',          5, 'Opened intro email'),
    (6, 'trial_signup',         20, 'Self-service trial signup');

-- -----------------------------------------------------------------
-- INTEGRATIONS
-- -----------------------------------------------------------------
INSERT INTO integrations (workspace_id, provider, config_json, status) VALUES
    (1, 'google_workspace', '{"domain": "acmecorp.io", "sync_calendar": true}',         'active'),
    (1, 'slack',            '{"workspace": "acmecorp", "channel": "#crm-alerts"}',       'active'),
    (1, 'stripe',           '{"account_id": "acct_placeholder", "webhook_enabled": true}','active'),
    (1, 'sendgrid',         '{"sender_domain": "acmecorp.io"}',                          'active'),
    (2, 'google_workspace', '{"domain": "startuphub.io", "sync_calendar": false}',       'active');

-- -----------------------------------------------------------------
-- WEBHOOKS
-- -----------------------------------------------------------------
INSERT INTO webhooks (workspace_id, event_name, endpoint_url, secret, is_active) VALUES
    (1, 'opportunity.won',      'https://hooks.acmecorp.io/crm/opp-won',    'whsec_placeholder_1', TRUE),
    (1, 'lead.converted',       'https://hooks.acmecorp.io/crm/lead-conv',  'whsec_placeholder_2', TRUE),
    (1, 'ticket.created',       'https://hooks.acmecorp.io/crm/ticket-new', 'whsec_placeholder_3', TRUE),
    (2, 'opportunity.won',      'https://hooks.startuphub.io/won',          'whsec_placeholder_4', TRUE);

-- -----------------------------------------------------------------
-- WORKFLOW RULES
-- -----------------------------------------------------------------
INSERT INTO workflow_rules (workspace_id, name, entity_type, trigger_event, condition_json, action_json, is_active) VALUES
    (1, 'Notify Slack on High-Value Opportunity Won',
        'opportunity', 'status_changed',
        '{"status": "won", "amount_gte": 400000}',
        '{"type": "slack_message", "channel": "#crm-alerts", "message": "🎉 Big deal won: {{opportunity.name}} — ₹{{opportunity.amount}}"}',
        TRUE),
    (1, 'Auto-assign Enterprise Leads to Karthik',
        'lead', 'created',
        '{"estimated_value_gte": 200000}',
        '{"type": "assign", "user_id": 4}',
        TRUE),
    (1, 'Create Follow-up Task After Demo Activity',
        'opportunity', 'activity_logged',
        '{"activity_type": "demo"}',
        '{"type": "create_task", "title": "Follow up post-demo", "due_days": 2, "priority": "high"}',
        TRUE),
    (1, 'Escalate Urgent Tickets to Manager',
        'ticket', 'created',
        '{"priority": "urgent"}',
        '{"type": "notify", "user_id": 3, "message": "Urgent ticket created: {{ticket.subject}}"}',
        TRUE);

-- -----------------------------------------------------------------
-- ASSIGNMENTS HISTORY
-- -----------------------------------------------------------------
INSERT INTO assignments_history (workspace_id, entity_type, entity_id, from_user_id, to_user_id, changed_by_user_id) VALUES
    (1, 'opportunity', 1, NULL, 4, 1),
    (1, 'opportunity', 2, NULL, 4, 1),
    (1, 'opportunity', 3, NULL, 4, 1),
    (1, 'lead',        2, NULL, 2, 1),
    (1, 'ticket',      1, NULL, 5, 1),
    (1, 'ticket',      3, NULL, 5, 1);

-- -----------------------------------------------------------------
-- AUDIT LOGS (sample)
-- -----------------------------------------------------------------
INSERT INTO audit_logs (workspace_id, user_id, entity_type, entity_id, action, new_values_json, ip_address) VALUES
    (1, 4, 'opportunity', 1, 'create', '{"name": "TechNova — Platform Expansion Q2", "amount": 480000}', '103.21.45.67'),
    (1, 4, 'opportunity', 1, 'update', '{"stage_id": 4, "probability_percent": 70}',                    '103.21.45.67'),
    (1, 4, 'opportunity', 5, 'update', '{"status": "won", "actual_close_date": "2025-04-01"}',           '103.21.45.67'),
    (1, 2, 'lead',        4, 'update', '{"status": "disqualified"}',                                     '182.74.12.99'),
    (1, 5, 'ticket',      2, 'update', '{"status": "resolved", "resolved_at": "NOW()"}',                 '103.21.45.68'),
    (1, 1, 'user',        4, 'assign', '{"team_id": 1, "role_id": 2}',                                   '103.21.45.60');

-- -----------------------------------------------------------------
-- DEDUP RULES
-- -----------------------------------------------------------------
INSERT INTO dedup_rules (workspace_id, entity_type, name, rule_json, is_active) VALUES
    (1, 'lead',    'Email exact match',    '{"fields": ["email"], "match": "exact"}',                       TRUE),
    (1, 'contact', 'Email exact match',    '{"fields": ["email"], "match": "exact"}',                       TRUE),
    (1, 'account', 'Domain fuzzy match',   '{"fields": ["website"], "match": "domain", "threshold": 0.9}', TRUE);

-- -----------------------------------------------------------------
-- IMPORTS
-- -----------------------------------------------------------------
INSERT INTO imports (workspace_id, imported_by_user_id, entity_type, file_name, status, total_rows, success_rows, failed_rows) VALUES
    (1, 1, 'lead',    'leads_q1_2025.csv',    'completed', 320, 312, 8),
    (1, 1, 'contact', 'contacts_export.csv',  'completed', 145, 145, 0),
    (1, 1, 'account', 'accounts_crm_old.csv', 'completed', 58,  55,  3);
