import type { SchemaConfig } from '../compiler/schema/schema-config.js';

export const crmSchema: SchemaConfig = {
  version: '1.0.0',
  description: 'Comprehensive CRM database schema with workspaces, users, accounts, contacts, leads, opportunities, products, quotes, tasks, activities, notes, attachments, emails, calls, messages, tickets, tags, custom fields, automation, and audit logging',
  tables: new Map([

    [
      'workspaces',
      {
        name: 'workspaces',
        description: 'workspaces table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'slug',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'plan',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'active'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'users',
      {
        name: 'users',
        description: 'users table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'first_name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'last_name',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'email',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'phone',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'password_hash',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'active'
          },
          {
            name: 'last_login_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'teams',
      {
        name: 'teams',
        description: 'teams table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'manager_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'roles',
      {
        name: 'roles',
        description: 'roles table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'user_workspace_memberships',
      {
        name: 'user_workspace_memberships',
        description: 'user_workspace_memberships table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'user_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'role_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'team_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'is_active',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'TRUE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'accounts',
      {
        name: 'accounts',
        description: 'accounts table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'owner_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'legal_name',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'website',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'industry',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'employee_count',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'annual_revenue',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'phone',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'email',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'billing_address_line1',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'billing_address_line2',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'billing_city',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'billing_state',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'billing_postal_code',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'billing_country',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'shipping_address_line1',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'shipping_address_line2',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'shipping_city',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'shipping_state',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'shipping_postal_code',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'shipping_country',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'prospect'
          },
          {
            name: 'source',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'deleted_at',
            type: { kind: 'datetime' },
            nullable: true
          }
        ]
      }
    ],
    [
      'contacts',
      {
        name: 'contacts',
        description: 'contacts table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'owner_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'primary_account_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'first_name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'last_name',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'full_name',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'job_title',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'email',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'alternate_email',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'phone',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'mobile',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'linkedin_url',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'department',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'birthday',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'address_line1',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'address_line2',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'city',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'state',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'postal_code',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'country',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'lifecycle_stage',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'lead'
          },
          {
            name: 'source',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'deleted_at',
            type: { kind: 'datetime' },
            nullable: true
          }
        ]
      }
    ],
    [
      'contact_account_links',
      {
        name: 'contact_account_links',
        description: 'contact_account_links table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'contact_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'account_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'relationship_type',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'is_primary',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'FALSE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'leads',
      {
        name: 'leads',
        description: 'leads table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'owner_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'first_name',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'last_name',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'company_name',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'title',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'email',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'phone',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'website',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'source',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'new'
          },
          {
            name: 'score',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            defaultValue: 0
          },
          {
            name: 'estimated_value',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'notes',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'converted_contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'converted_account_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'converted_opportunity_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'converted_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'pipelines',
      {
        name: 'pipelines',
        description: 'pipelines table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'type',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'sales'
          },
          {
            name: 'is_default',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'FALSE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'pipeline_stages',
      {
        name: 'pipeline_stages',
        description: 'pipeline_stages table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'pipeline_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'stage_order',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'probability_percent',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            defaultValue: 0
          },
          {
            name: 'is_closed_won',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'FALSE'
          },
          {
            name: 'is_closed_lost',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'FALSE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'opportunities',
      {
        name: 'opportunities',
        description: 'opportunities table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'pipeline_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'stage_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'owner_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'account_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'primary_contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'amount',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'currency_code',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'USD'
          },
          {
            name: 'probability_percent',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            defaultValue: 0
          },
          {
            name: 'expected_close_date',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'actual_close_date',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'open'
          },
          {
            name: 'loss_reason',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'source',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'deleted_at',
            type: { kind: 'datetime' },
            nullable: true
          }
        ]
      }
    ],
    [
      'opportunity_stage_history',
      {
        name: 'opportunity_stage_history',
        description: 'opportunity_stage_history table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'opportunity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'from_stage_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'to_stage_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'changed_by_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'changed_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'assignments_history',
      {
        name: 'assignments_history',
        description: 'assignments_history table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'from_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'to_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'changed_by_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'changed_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'products',
      {
        name: 'products',
        description: 'products table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'sku',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'unit_price',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'currency_code',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'USD'
          },
          {
            name: 'is_active',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'TRUE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'opportunity_products',
      {
        name: 'opportunity_products',
        description: 'opportunity_products table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'opportunity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'product_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'quantity',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'unit_price',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'discount_percent',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'tax_percent',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'line_total',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'quotes',
      {
        name: 'quotes',
        description: 'quotes table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'opportunity_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'account_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'quote_number',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'draft'
          },
          {
            name: 'issue_date',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'expiry_date',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'subtotal',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'discount_total',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'tax_total',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'grand_total',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'currency_code',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'USD'
          },
          {
            name: 'terms',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_by_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'quote_items',
      {
        name: 'quote_items',
        description: 'quote_items table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'quote_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'product_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'quantity',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'unit_price',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'discount_percent',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'tax_percent',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'line_total',
            type: { kind: 'number' },
            nullable: true
          }
        ]
      }
    ],
    [
      'tasks',
      {
        name: 'tasks',
        description: 'tasks table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'owner_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'assigned_to_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'related_entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'related_entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'title',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'due_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'priority',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'medium'
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'open'
          },
          {
            name: 'completed_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'activities',
      {
        name: 'activities',
        description: 'activities table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'actor_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'related_entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'related_entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'activity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'subject',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'activity_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'duration_minutes',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'outcome',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'notes',
      {
        name: 'notes',
        description: 'notes table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'created_by_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'related_entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'related_entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'content',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'is_private',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'FALSE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'attachments',
      {
        name: 'attachments',
        description: 'attachments table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'uploaded_by_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'related_entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'related_entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'file_name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'file_url',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'mime_type',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'file_size_bytes',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'emails',
      {
        name: 'emails',
        description: 'emails table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'owner_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'related_entity_type',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'related_entity_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'provider_message_id',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'thread_id',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'direction',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'subject',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'body_text',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'body_html',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'sent_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'draft'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'email_participants',
      {
        name: 'email_participants',
        description: 'email_participants table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'email_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'participant_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'email_address',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'display_name',
            type: { kind: 'string' },
            nullable: true
          }
        ]
      }
    ],
    [
      'calls',
      {
        name: 'calls',
        description: 'calls table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'owner_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'related_entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'related_entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'started_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'ended_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'duration_seconds',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'direction',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'outcome',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'recording_url',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'notes',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'messages',
      {
        name: 'messages',
        description: 'messages table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'related_entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'related_entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'channel',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'direction',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'sender_contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'sender_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'recipient_contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'body',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'sent_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'tickets',
      {
        name: 'tickets',
        description: 'tickets table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'account_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'owner_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'subject',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'description',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'priority',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'medium'
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'open'
          },
          {
            name: 'channel',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'category',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'resolution_summary',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'first_response_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'resolved_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'closed_at',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'ticket_comments',
      {
        name: 'ticket_comments',
        description: 'ticket_comments table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'ticket_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'contact_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'body',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'is_private',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'FALSE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'tags',
      {
        name: 'tags',
        description: 'tags table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'color',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'entity_tags',
      {
        name: 'entity_tags',
        description: 'entity_tags table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'tag_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'custom_fields',
      {
        name: 'custom_fields',
        description: 'custom_fields table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'field_key',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'label',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'data_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'is_required',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'FALSE'
          },
          {
            name: 'options_json',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'custom_field_values',
      {
        name: 'custom_field_values',
        description: 'custom_field_values table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'custom_field_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'value_text',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'value_number',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'value_date',
            type: { kind: 'datetime' },
            nullable: true
          },
          {
            name: 'value_boolean',
            type: { kind: 'boolean' },
            nullable: true
          },
          {
            name: 'value_json',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'lead_score_events',
      {
        name: 'lead_score_events',
        description: 'lead_score_events table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'lead_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'event_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'score_delta',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'reason',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'integrations',
      {
        name: 'integrations',
        description: 'integrations table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'provider',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'config_json',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'active'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'webhooks',
      {
        name: 'webhooks',
        description: 'webhooks table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'event_name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'endpoint_url',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'secret',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'is_active',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'TRUE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'workflow_rules',
      {
        name: 'workflow_rules',
        description: 'workflow_rules table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'trigger_event',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'condition_json',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'action_json',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'is_active',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'TRUE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'updated_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'imports',
      {
        name: 'imports',
        description: 'imports table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'imported_by_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'file_name',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'status',
            type: { kind: 'string' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'pending'
          },
          {
            name: 'total_rows',
            type: { kind: 'number' },
            nullable: true,
            hasDefault: true,
            defaultValue: 0
          },
          {
            name: 'success_rows',
            type: { kind: 'number' },
            nullable: true,
            hasDefault: true,
            defaultValue: 0
          },
          {
            name: 'failed_rows',
            type: { kind: 'number' },
            nullable: true,
            hasDefault: true,
            defaultValue: 0
          },
          {
            name: 'error_log',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'completed_at',
            type: { kind: 'datetime' },
            nullable: true
          }
        ]
      }
    ],
    [
      'dedup_rules',
      {
        name: 'dedup_rules',
        description: 'dedup_rules table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'name',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'rule_json',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'is_active',
            type: { kind: 'boolean' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'TRUE'
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
    [
      'merge_history',
      {
        name: 'merge_history',
        description: 'merge_history table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'source_entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'target_entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'merged_by_user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'merged_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          },
          {
            name: 'merge_summary_json',
            type: { kind: 'string' },
            nullable: true
          }
        ]
      }
    ],
    [
      'audit_logs',
      {
        name: 'audit_logs',
        description: 'audit_logs table',
        primaryKey: ['id'],
        columns: [
          {
            name: 'id',
            type: { kind: 'number' },
            nullable: false,
            hasDefault: true,
            isGenerated: true,
            primaryKey: true,
            description: 'Auto-incrementing primary key (SERIAL)'
          },
          {
            name: 'workspace_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'user_id',
            type: { kind: 'number' },
            nullable: true
          },
          {
            name: 'entity_type',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'entity_id',
            type: { kind: 'number' },
            nullable: false
          },
          {
            name: 'action',
            type: { kind: 'string' },
            nullable: false
          },
          {
            name: 'old_values_json',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'new_values_json',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'ip_address',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'user_agent',
            type: { kind: 'string' },
            nullable: true
          },
          {
            name: 'created_at',
            type: { kind: 'datetime' },
            nullable: false,
            hasDefault: true,
            defaultValue: 'NOW()'
          }
        ]
      }
    ],
  ]),
  foreignKeys: [
    {
      fromTable: 'users',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'teams',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'teams',
      fromColumn: 'manager_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'roles',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'user_workspace_memberships',
      fromColumn: 'user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'user_workspace_memberships',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'user_workspace_memberships',
      fromColumn: 'role_id',
      toTable: 'roles',
      toColumn: 'id',
      description: 'References roles.id'
    },
    {
      fromTable: 'user_workspace_memberships',
      fromColumn: 'team_id',
      toTable: 'teams',
      toColumn: 'id',
      description: 'References teams.id'
    },
    {
      fromTable: 'accounts',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'accounts',
      fromColumn: 'owner_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'contacts',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'contacts',
      fromColumn: 'owner_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'contacts',
      fromColumn: 'primary_account_id',
      toTable: 'accounts',
      toColumn: 'id',
      description: 'References accounts.id'
    },
    {
      fromTable: 'contact_account_links',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'contact_account_links',
      fromColumn: 'contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'contact_account_links',
      fromColumn: 'account_id',
      toTable: 'accounts',
      toColumn: 'id',
      description: 'References accounts.id'
    },
    {
      fromTable: 'leads',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'leads',
      fromColumn: 'owner_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'leads',
      fromColumn: 'converted_contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'leads',
      fromColumn: 'converted_account_id',
      toTable: 'accounts',
      toColumn: 'id',
      description: 'References accounts.id'
    },
    {
      fromTable: 'leads',
      fromColumn: 'converted_opportunity_id',
      toTable: 'opportunities',
      toColumn: 'id',
      description: 'References opportunities.id'
    },
    {
      fromTable: 'pipelines',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'pipeline_stages',
      fromColumn: 'pipeline_id',
      toTable: 'pipelines',
      toColumn: 'id',
      description: 'References pipelines.id'
    },
    {
      fromTable: 'opportunities',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'opportunities',
      fromColumn: 'pipeline_id',
      toTable: 'pipelines',
      toColumn: 'id',
      description: 'References pipelines.id'
    },
    {
      fromTable: 'opportunities',
      fromColumn: 'stage_id',
      toTable: 'pipeline_stages',
      toColumn: 'id',
      description: 'References pipeline_stages.id'
    },
    {
      fromTable: 'opportunities',
      fromColumn: 'owner_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'opportunities',
      fromColumn: 'account_id',
      toTable: 'accounts',
      toColumn: 'id',
      description: 'References accounts.id'
    },
    {
      fromTable: 'opportunities',
      fromColumn: 'primary_contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'opportunity_stage_history',
      fromColumn: 'opportunity_id',
      toTable: 'opportunities',
      toColumn: 'id',
      description: 'References opportunities.id'
    },
    {
      fromTable: 'opportunity_stage_history',
      fromColumn: 'from_stage_id',
      toTable: 'pipeline_stages',
      toColumn: 'id',
      description: 'References pipeline_stages.id'
    },
    {
      fromTable: 'opportunity_stage_history',
      fromColumn: 'to_stage_id',
      toTable: 'pipeline_stages',
      toColumn: 'id',
      description: 'References pipeline_stages.id'
    },
    {
      fromTable: 'opportunity_stage_history',
      fromColumn: 'changed_by_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'assignments_history',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'assignments_history',
      fromColumn: 'from_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'assignments_history',
      fromColumn: 'to_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'assignments_history',
      fromColumn: 'changed_by_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'products',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'opportunity_products',
      fromColumn: 'opportunity_id',
      toTable: 'opportunities',
      toColumn: 'id',
      description: 'References opportunities.id'
    },
    {
      fromTable: 'opportunity_products',
      fromColumn: 'product_id',
      toTable: 'products',
      toColumn: 'id',
      description: 'References products.id'
    },
    {
      fromTable: 'quotes',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'quotes',
      fromColumn: 'opportunity_id',
      toTable: 'opportunities',
      toColumn: 'id',
      description: 'References opportunities.id'
    },
    {
      fromTable: 'quotes',
      fromColumn: 'account_id',
      toTable: 'accounts',
      toColumn: 'id',
      description: 'References accounts.id'
    },
    {
      fromTable: 'quotes',
      fromColumn: 'contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'quotes',
      fromColumn: 'created_by_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'quote_items',
      fromColumn: 'quote_id',
      toTable: 'quotes',
      toColumn: 'id',
      description: 'References quotes.id'
    },
    {
      fromTable: 'quote_items',
      fromColumn: 'product_id',
      toTable: 'products',
      toColumn: 'id',
      description: 'References products.id'
    },
    {
      fromTable: 'tasks',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'tasks',
      fromColumn: 'owner_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'tasks',
      fromColumn: 'assigned_to_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'activities',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'activities',
      fromColumn: 'actor_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'notes',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'notes',
      fromColumn: 'created_by_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'attachments',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'attachments',
      fromColumn: 'uploaded_by_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'emails',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'emails',
      fromColumn: 'owner_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'email_participants',
      fromColumn: 'email_id',
      toTable: 'emails',
      toColumn: 'id',
      description: 'References emails.id'
    },
    {
      fromTable: 'email_participants',
      fromColumn: 'contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'calls',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'calls',
      fromColumn: 'owner_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'calls',
      fromColumn: 'contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'messages',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'messages',
      fromColumn: 'sender_contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'messages',
      fromColumn: 'sender_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'messages',
      fromColumn: 'recipient_contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'tickets',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'tickets',
      fromColumn: 'account_id',
      toTable: 'accounts',
      toColumn: 'id',
      description: 'References accounts.id'
    },
    {
      fromTable: 'tickets',
      fromColumn: 'contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'tickets',
      fromColumn: 'owner_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'ticket_comments',
      fromColumn: 'ticket_id',
      toTable: 'tickets',
      toColumn: 'id',
      description: 'References tickets.id'
    },
    {
      fromTable: 'ticket_comments',
      fromColumn: 'user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'ticket_comments',
      fromColumn: 'contact_id',
      toTable: 'contacts',
      toColumn: 'id',
      description: 'References contacts.id'
    },
    {
      fromTable: 'tags',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'entity_tags',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'entity_tags',
      fromColumn: 'tag_id',
      toTable: 'tags',
      toColumn: 'id',
      description: 'References tags.id'
    },
    {
      fromTable: 'custom_fields',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'custom_field_values',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'custom_field_values',
      fromColumn: 'custom_field_id',
      toTable: 'custom_fields',
      toColumn: 'id',
      description: 'References custom_fields.id'
    },
    {
      fromTable: 'lead_score_events',
      fromColumn: 'lead_id',
      toTable: 'leads',
      toColumn: 'id',
      description: 'References leads.id'
    },
    {
      fromTable: 'integrations',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'webhooks',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'workflow_rules',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'imports',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'imports',
      fromColumn: 'imported_by_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'dedup_rules',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'merge_history',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'merge_history',
      fromColumn: 'merged_by_user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
    {
      fromTable: 'audit_logs',
      fromColumn: 'workspace_id',
      toTable: 'workspaces',
      toColumn: 'id',
      description: 'References workspaces.id'
    },
    {
      fromTable: 'audit_logs',
      fromColumn: 'user_id',
      toTable: 'users',
      toColumn: 'id',
      description: 'References users.id'
    },
  ]
};