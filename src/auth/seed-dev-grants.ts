import Database from 'better-sqlite3';
import { v4 as uuidv4 } from 'uuid';

const db = new Database('./pipelines.db');

// Define the users to seed
const users = [
  { id: 'user_admin', username: 'admin', role: 'admin', passwordHash: 'dev' },
  { id: 'user_alice', username: 'alice', role: 'operator', passwordHash: 'dev' },
  { id: 'user_bob', username: 'bob', role: 'operator', passwordHash: 'dev' },
  { id: 'user_viewer', username: 'viewer', role: 'viewer', passwordHash: 'dev' },
];

// Define the CRM tables
const crmTables = ['customers', 'products', 'orders', 'order_items', 'support_tickets', 'email_log'];

function seedUsers() {
  console.log('Seeding users...');
  
  const insertUser = db.prepare(`
    INSERT OR IGNORE INTO users (id, username, password_hash, role, created_at)
    VALUES (?, ?, ?, ?, ?)
  `);

  const now = Date.now();
  for (const user of users) {
    insertUser.run(user.id, user.username, user.passwordHash, user.role, now);
  }
  
  console.log(`  Seeded ${users.length} users`);
}

function seedGrants() {
  console.log('Seeding grants...');
  
  const insertTableGrant = db.prepare(`
    INSERT OR IGNORE INTO table_grants (id, user_id, table_name, can_read, can_write)
    VALUES (?, ?, ?, ?, ?)
  `);

  const insertColumnGrant = db.prepare(`
    INSERT OR IGNORE INTO column_grants (id, user_id, table_name, column_name, can_read, can_write)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  // Admin: full read+write on all tables
  console.log('  Admin grants...');
  for (const table of crmTables) {
    insertTableGrant.run(uuidv4(), 'user_admin', table, 1, 1);
  }

  // Alice (sales operator):
  // READ+WRITE: orders, order_items, email_log
  // READ only: customers (but NOT customers.arr)
  console.log('  Alice grants...');
  const aliceReadWriteTables = ['orders', 'order_items', 'email_log'];
  for (const table of aliceReadWriteTables) {
    insertTableGrant.run(uuidv4(), 'user_alice', table, 1, 1);
  }
  
  // Alice read-only customers
  insertTableGrant.run(uuidv4(), 'user_alice', 'customers', 1, 0);
  
  // Alice restriction: cannot read customers.arr column
  insertColumnGrant.run(uuidv4(), 'user_alice', 'customers', 'arr', 0, 0);

  // Bob (support operator):
  // READ+WRITE: support_tickets
  // READ only: customers, orders
  console.log('  Bob grants...');
  insertTableGrant.run(uuidv4(), 'user_bob', 'support_tickets', 1, 1);
  
  const bobReadOnlyTables = ['customers', 'orders'];
  for (const table of bobReadOnlyTables) {
    insertTableGrant.run(uuidv4(), 'user_bob', table, 1, 0);
  }

  // Viewer:
  // READ only: orders, support_tickets
  console.log('  Viewer grants...');
  const viewerReadOnlyTables = ['orders', 'support_tickets'];
  for (const table of viewerReadOnlyTables) {
    insertTableGrant.run(uuidv4(), 'user_viewer', table, 1, 0);
  }
  
  console.log('  Grants seeded successfully');
}

function printSummary() {
  console.log('\n' + '='.repeat(80));
  console.log('DEVELOPMENT GRANTS SUMMARY');
  console.log('='.repeat(80));
  
  const summary = db.prepare(`
    SELECT 
      u.username,
      u.role,
      GROUP_CONCAT(
        CASE 
          WHEN tg.can_read = 1 AND tg.can_write = 1 THEN tg.table_name || '(RW)'
          WHEN tg.can_read = 1 AND tg.can_write = 0 THEN tg.table_name || '(R)'
          ELSE NULL
        END,
        ', '
      ) as tables
    FROM users u
    LEFT JOIN table_grants tg ON u.id = tg.user_id
    GROUP BY u.id, u.username, u.role
    ORDER BY u.username
  `).all() as Array<{
    username: string;
    role: string;
    tables: string | null;
  }>;

  console.log('User       | Role     | Tables (read/write)');
  console.log('---------- | -------- | --------------------');
  
  for (const user of summary) {
    const username = user.username.padEnd(10);
    const role = user.role.padEnd(8);
    const tables = user.tables || 'none';
    console.log(`${username} | ${role} | ${tables}`);
  }
  
  // Show Alice's column restriction specifically
  console.log('\nColumn Restrictions:');
  const aliceRestriction = db.prepare(`
    SELECT table_name, column_name, can_read, can_write
    FROM column_grants cg
    JOIN users u ON cg.user_id = u.id
    WHERE u.username = 'alice' AND (can_read = 0 OR can_write = 0)
  `).get() as { table_name: string; column_name: string; can_read: number; can_write: number } | undefined;
  
  if (aliceRestriction) {
    console.log(`  alice: Cannot read ${aliceRestriction.table_name}.${aliceRestriction.column_name}`);
  }
  
  console.log('\nLogin Credentials:');
  console.log('  Username: admin, Password: dev (Role: admin)');
  console.log('  Username: alice, Password: dev (Role: operator - sales)');
  console.log('  Username: bob,   Password: dev (Role: operator - support)');
  console.log('  Username: viewer, Password: dev (Role: viewer - read-only)');
  console.log('='.repeat(80));
}

async function main() {
  try {
    console.log('Seeding development grants...\n');
    
    // Initialize schema (this will create tables if they don't exist)
    // The GrantStore constructor automatically initializes the schema
    // We'll initialize it directly to avoid import issues
    const { grantStore } = await import('./grant-store.js');
    
    seedUsers();
    seedGrants();
    printSummary();
    
    console.log('\n\u2709 Development grants seeded successfully!');
    console.log('Run "npm run auth:seed" to reseed at any time.');
    
  } catch (error) {
    console.error('\u274c Error seeding grants:', error);
    process.exit(1);
  } finally {
    db.close();
  }
}

// Check if this file is being run directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
