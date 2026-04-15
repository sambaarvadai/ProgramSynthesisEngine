import { apiRegistryStore } from './api-registry-store';

// Seed data for API registry
const endpoints = [
  {
    id: 'mock.enrich_customer',
    displayName: 'Mock Customer Enrichment API',
    baseUrl: 'http://localhost:3457/enrich/customer',
    method: 'POST' as const,
    batchMode: false,
    responseMode: 'object' as const,
    auth: { kind: 'none' as const },
    urlPattern: /http:\/\/localhost:3457\/enrich\/customer/,
    description: 'Enrich a single customer record with additional metadata like description, tier, and health score',
    defaultConcurrency: 5,
    requestFields: [
      { name: 'id', type: 'number' as const, required: true, description: 'Customer ID', sortOrder: 0 },
      { name: 'name', type: 'string' as const, required: false, description: 'Customer name', sortOrder: 1 },
      { name: 'email', type: 'string' as const, required: false, description: 'Customer email', sortOrder: 2 },
      { name: 'segment', type: 'string' as const, required: false, description: 'Customer segment (enterprise/startup/smb)', sortOrder: 3 },
      { name: 'region', type: 'string' as const, required: false, description: 'Customer geographic region', sortOrder: 4 },
      { name: 'arr', type: 'number' as const, required: false, description: 'Annual recurring revenue', sortOrder: 5 }
    ],
    responseFields: [
      { name: 'description', type: 'string' as const, description: 'Enriched customer description', sortOrder: 0 },
      { name: 'tier', type: 'string' as const, description: 'Customer tier (premium/standard/basic)', sortOrder: 1 },
      { name: 'health_score', type: 'number' as const, description: 'Customer health score (0-100)', sortOrder: 2 }
    ]
  },
  {
    id: 'mock.enrich_customers_batch',
    displayName: 'Mock Batch Customer Enrichment API',
    baseUrl: 'http://localhost:3457/enrich/customers',
    method: 'POST' as const,
    batchMode: true,
    responseMode: 'object' as const,
    auth: { kind: 'none' as const },
    urlPattern: /http:\/\/localhost:3457\/enrich\/customers/,
    description: 'Enrich multiple customer records in a single batch request',
    defaultConcurrency: 1,
    requestFields: [
      { name: 'id', type: 'number' as const, required: true, description: 'Customer ID', sortOrder: 0 },
      { name: 'name', type: 'string' as const, required: false, description: 'Customer name', sortOrder: 1 },
      { name: 'email', type: 'string' as const, required: false, description: 'Customer email', sortOrder: 2 },
      { name: 'segment', type: 'string' as const, required: false, description: 'Customer segment (enterprise/startup/smb)', sortOrder: 3 },
      { name: 'region', type: 'string' as const, required: false, description: 'Customer geographic region', sortOrder: 4 },
      { name: 'arr', type: 'number' as const, required: false, description: 'Annual recurring revenue', sortOrder: 5 }
    ],
    responseFields: [
      { name: 'description', type: 'string' as const, description: 'Enriched customer description', sortOrder: 0 },
      { name: 'tier', type: 'string' as const, description: 'Customer tier (premium/standard/basic)', sortOrder: 1 },
      { name: 'health_score', type: 'number' as const, description: 'Customer health score (0-100)', sortOrder: 2 }
    ]
  },
  {
    id: 'mock.search_customers',
    displayName: 'Customer Search API (array response)',
    baseUrl: 'http://localhost:3456/search/customers',
    method: 'POST' as const,
    batchMode: false,
    responseMode: 'array' as const,
    responseRoot: 'results',
    auth: { kind: 'none' as const },
    urlPattern: /http:\/\/localhost:3456\/search\/customers/,
    description: 'Search customers by query string, returns array of matches',
    defaultConcurrency: 1,
    requestFields: [
      { name: 'query', type: 'string' as const, required: true, description: 'Search query string', sortOrder: 0 }
    ],
    responseFields: [
      { name: 'id', type: 'string' as const, apiFieldName: 'customer_id', description: 'Customer ID', sortOrder: 0 },
      { name: 'name', type: 'string' as const, description: 'Customer name', sortOrder: 1 },
      { name: 'score', type: 'number' as const, apiFieldName: 'match_score', description: 'Match score', sortOrder: 2 }
    ]
  },
  {
    id: 'resend.send_email',
    displayName: 'Resend Email API',
    baseUrl: 'https://api.resend.com/emails',
    method: 'POST' as const,
    batchMode: false,
    responseMode: 'object' as const,
    auth: { kind: 'bearer' as const, envVar: 'RESEND_API_KEY' },
    urlPattern: /https:\/\/api\.resend\.com\/emails/,
    description: 'Send transactional emails using the Resend email service',
    defaultConcurrency: 3,
    defaultRateLimit: 10,
    requestFields: [
      { name: 'from', type: 'string' as const, required: true, description: 'Sender email address', sortOrder: 0 },
      { name: 'to', type: 'string' as const, required: true, description: 'Recipient email address', sortOrder: 1 },
      { name: 'subject', type: 'string' as const, required: true, description: 'Email subject line', sortOrder: 2 },
      { name: 'html', type: 'string' as const, required: false, description: 'HTML email content', sortOrder: 3 },
      { name: 'text', type: 'string' as const, required: false, description: 'Plain text email content', sortOrder: 4 }
    ],
    responseFields: [
      { name: 'id', type: 'string' as const, description: 'Email message ID', sortOrder: 0 },
      { name: 'from', type: 'string' as const, description: 'Sender email address', sortOrder: 1 },
      { name: 'to', type: 'string' as const, description: 'Recipient email address', sortOrder: 2 },
      { name: 'subject', type: 'string' as const, description: 'Email subject line', sortOrder: 3 }
    ]
  }
];

async function seedApiRegistry() {
  console.log('Seeding API registry...');
  
  for (const endpoint of endpoints) {
    apiRegistryStore.register(endpoint);
    console.log(`  Registered: ${endpoint.id}`);
  }
  
  console.log(`\nRegistered ${endpoints.length} API endpoints.`);
  
  // Close the database connection
  apiRegistryStore.close();
}

// Run the seed function
seedApiRegistry().catch(console.error);
