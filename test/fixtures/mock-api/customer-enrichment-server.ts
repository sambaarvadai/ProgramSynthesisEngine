#!/usr/bin/env node

import { createServer } from 'http';

// Configuration
const PORT = process.env.MOCK_API_PORT ? parseInt(process.env.MOCK_API_PORT, 10) : 3456;

// Sample customer data for search functionality
const customers = [
  { id: '1', name: 'Acme Corp', segment: 'enterprise', region: 'us-east', arr: 48000 },
  { id: '2', name: 'Globex Inc', segment: 'enterprise', region: 'us-west', arr: 72000 },
  { id: '3', name: 'Initech', segment: 'enterprise', region: 'eu-west', arr: 36000 },
  { id: '4', name: 'Umbrella Ltd', segment: 'enterprise', region: 'us-east', arr: 96000 },
  { id: '5', name: 'Stark Industries', segment: 'enterprise', region: 'us-west', arr: 120000 },
  { id: '6', name: 'Wayne Enterprises', segment: 'enterprise', region: 'us-east', arr: 84000 },
  { id: '7', name: 'Pied Piper', segment: 'smb', region: 'us-west', arr: 18000 },
  { id: '8', name: 'Hooli', segment: 'smb', region: 'us-west', arr: 24000 },
  { id: '9', name: 'Aviato', segment: 'smb', region: 'us-east', arr: 9600 },
  { id: '10', name: 'Raviga Capital', segment: 'smb', region: 'us-west', arr: 14400 },
  { id: '11', name: 'YC Batch W24 #1', segment: 'startup', region: 'us-west', arr: 2400 },
  { id: '12', name: 'YC Batch S24 #1', segment: 'startup', region: 'us-west', arr: 2400 },
  { id: '13', name: 'Stealth Mode Co', segment: 'startup', region: 'us-east', arr: 4800 },
  { id: '14', name: 'NanoSoft', segment: 'startup', region: 'eu-west', arr: 3600 },
  { id: '15', name: 'DeepThought AI', segment: 'startup', region: 'us-west', arr: 6000 }
];

// Helper functions for deterministic enrichment logic
function getSegmentLabel(segment: string): string {
  switch (segment?.toLowerCase()) {
    case 'enterprise': return 'Enterprise client';
    case 'smb': return 'Mid-market business';
    case 'startup': return 'Early-stage startup';
    default: return 'Business client';
  }
}

function getRegionLabel(region: string): string {
  switch (region?.toLowerCase()) {
    case 'us-east': return 'on the US East Coast';
    case 'us-west': return 'on the US West Coast';
    case 'eu-west': return 'in Western Europe';
    case 'ap-south': return 'in Asia Pacific';
    default: return 'in global markets';
  }
}

function getArrLabel(arr: number): string {
  if (arr > 100000) return 'with significant annual revenue';
  if (arr > 10000) return 'with moderate annual revenue';
  return 'in early revenue stage';
}

function getTier(arr: number): 'platinum' | 'gold' | 'silver' | 'bronze' {
  if (arr >= 100000) return 'platinum';
  if (arr >= 50000) return 'gold';
  if (arr >= 10000) return 'silver';
  return 'bronze';
}

function getHealthScore(id: string, arr: number): number {
  const score = ((parseInt(id, 10) * 17 + Math.floor(arr / 1000)) % 41) + 60;
  return Math.max(0, Math.min(100, score));
}

function generateDescription(customer: any): string {
  const segmentLabel = getSegmentLabel(customer.segment);
  const regionLabel = getRegionLabel(customer.region);
  const arrLabel = getArrLabel(customer.arr || 0);
  const name = customer.name || 'Unknown';
  
  return `${segmentLabel} ${regionLabel} ${arrLabel}. Customer: ${name}.`;
}

function enrichCustomer(customer: any): any {
  const arr = Number(customer.arr) || 0;
  const id = String(customer.id || '0');
  
  return {
    customer_id: id,
    description: generateDescription(customer),
    tier: getTier(arr),
    health_score: getHealthScore(id, arr)
  };
}

// HTTP request handler
function handleRequest(req: any, res: any) {
  const startTime = Date.now();
  const method = req.method;
  const url = req.url;

  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  // Parse request body for POST requests
  let body = '';
  req.on('data', (chunk: Buffer) => {
    body += chunk.toString();
  });

  req.on('end', () => {
    try {
      if (method === 'POST' && url === '/enrich/customer') {
        // Single customer enrichment
        const customer = JSON.parse(body);
        const result = enrichCustomer(customer);
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
        
      } else if (method === 'POST' && url === '/enrich/customers') {
        // Batch customer enrichment
        const customers = JSON.parse(body);
        if (!Array.isArray(customers)) {
          throw new Error('Request body must be an array');
        }
        
        const results = customers.map(customer => enrichCustomer(customer));
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(results));
        
      } else if (method === 'POST' && url === '/search/customers') {
        // Customer search
        const request = JSON.parse(body);
        const query = request.query?.toLowerCase() || '';
        
        const filteredCustomers = customers.filter(c => 
          c.name.toLowerCase().includes(query)
        );
        
        const searchResults = filteredCustomers.map(c => ({
          customer_id: c.id,
          name: c.name,
          match_score: 0.9
        }));
        
        const response = {
          results: searchResults,
          total: searchResults.length
        };
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(response));
        
      } else if (method === 'GET' && url === '/health') {
        // Health check
        const healthResponse = {
          status: 'ok',
          timestamp: new Date().toISOString()
        };
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(healthResponse));
        
      } else {
        // Unknown route
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
      }
      
    } catch (error) {
      // Error handling
      if (error instanceof SyntaxError) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid JSON body' }));
      } else {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Internal server error' }));
      }
    }
    
    // Log request
    const duration = Date.now() - startTime;
    console.log(`[mock-api] ${method} ${url} -> ${res.statusCode} (${duration}ms)`);
  });
}

// Export startServer function for testing
export function startServer(port: number = PORT): any {
  const server = createServer(handleRequest);
  
  return new Promise((resolve) => {
    server.listen(port, () => {
      console.log(`Mock Customer Enrichment API running on port ${port}`);
      console.log(`Endpoints:`);
      console.log(`  POST /enrich/customer - Enrich single customer`);
      console.log(`  POST /enrich/customers - Enrich multiple customers`);
      console.log(`  POST /search/customers - Search customers by query`);
      console.log(`  GET /health - Health check`);
      resolve(server);
    });
  });
}

// Start server if this file is run directly
if (import.meta.url === `file://${process.argv[1]}`) {
  startServer().then(() => {
    // Graceful shutdown
    process.on('SIGINT', () => {
      console.log('\nShutting down mock API server...');
      process.exit(0);
    });

    process.on('SIGTERM', () => {
      console.log('\nShutting down mock API server...');
      process.exit(0);
    });
  });
}
