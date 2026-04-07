-- Products (10)
INSERT INTO products (name, category, price) VALUES
  ('Starter Plan',     'subscription', 49.00),
  ('Pro Plan',         'subscription', 149.00),
  ('Enterprise Plan',  'subscription', 499.00),
  ('API Add-on',       'add-on',       99.00),
  ('Analytics Add-on', 'add-on',       79.00),
  ('Storage 100GB',    'storage',      29.00),
  ('Storage 1TB',      'storage',      99.00),
  ('Support Basic',    'support',      49.00),
  ('Support Premium',  'support',      199.00),
  ('Onboarding',       'service',      999.00);

-- Customers (50)
INSERT INTO customers (name, email, segment, region, arr) VALUES
  ('Acme Corp',           'billing@acme.com',         'enterprise', 'us-east',   48000.00),
  ('Globex Inc',          'accounts@globex.com',      'enterprise', 'us-west',   72000.00),
  ('Initech',             'finance@initech.com',      'enterprise', 'eu-west',   36000.00),
  ('Umbrella Ltd',        'billing@umbrella.com',     'enterprise', 'us-east',   96000.00),
  ('Stark Industries',    'ap@stark.com',             'enterprise', 'us-west',  120000.00),
  ('Wayne Enterprises',   'finance@wayne.com',        'enterprise', 'us-east',   84000.00),
  ('Cyberdyne Systems',   'billing@cyberdyne.com',    'enterprise', 'us-west',   60000.00),
  ('Soylent Corp',        'accounts@soylent.com',     'enterprise', 'eu-west',   48000.00),
  ('Vandelay Industries', 'billing@vandelay.com',     'enterprise', 'ap-south',  36000.00),
  ('Massive Dynamic',     'finance@massive.com',      'enterprise', 'us-east',  108000.00),
  ('Pied Piper',          'billing@piedpiper.com',    'smb',        'us-west',   18000.00),
  ('Hooli',               'accounts@hooli.com',       'smb',        'us-west',   24000.00),
  ('Aviato',              'finance@aviato.com',       'smb',        'us-east',    9600.00),
  ('Raviga Capital',      'billing@raviga.com',       'smb',        'us-west',   14400.00),
  ('Bachmanity',          'accounts@bachmanity.com',  'smb',        'eu-west',   12000.00),
  ('See Food',            'billing@seefood.com',      'smb',        'us-west',    7200.00),
  ('EndFrame',            'finance@endframe.com',     'smb',        'us-east',   16800.00),
  ('Intersite',           'billing@intersite.com',    'smb',        'ap-south',  10800.00),
  ('Flutterbeam',         'accounts@flutter.com',     'smb',        'eu-west',    9600.00),
  ('Optimoji',            'billing@optimoji.com',     'smb',        'us-west',   13200.00),
  ('YC Batch W24 #1',     'founder@yc1.com',          'startup',    'us-west',    2400.00),
  ('YC Batch W24 #2',     'founder@yc2.com',          'startup',    'us-east',    1800.00),
  ('YC Batch W24 #3',     'founder@yc3.com',          'startup',    'eu-west',    3600.00),
  ('YC Batch S24 #1',     'founder@ycs1.com',         'startup',    'us-west',    2400.00),
  ('YC Batch S24 #2',     'founder@ycs2.com',         'startup',    'ap-south',   1200.00),
  ('Stealth Mode Co',     'team@stealth.com',         'startup',    'us-east',    4800.00),
  ('NanoSoft',            'billing@nanosoft.com',     'startup',    'eu-west',    3600.00),
  ('DeepThought AI',      'accounts@deepthought.com', 'startup',    'us-west',    6000.00),
  ('Quantum Bits',        'finance@quantumbits.com',  'startup',    'us-east',    4800.00),
  ('Neural Forge',        'billing@neuralforge.com',  'startup',    'ap-south',   2400.00),
  ('TechStart Alpha',     'team@techstart.com',       'startup',    'us-west',    1800.00),
  ('BuildFast Inc',       'billing@buildfast.com',    'startup',    'us-east',    3000.00),
  ('CodeCraft',           'accounts@codecraft.com',   'startup',    'eu-west',    2400.00),
  ('DataDriven Co',       'finance@datadriven.com',   'startup',    'us-west',    4200.00),
  ('CloudNative Ltd',     'billing@cloudnative.com',  'startup',    'ap-south',   3600.00),
  ('APIFirst',            'team@apifirst.com',        'startup',    'us-east',    2400.00),
  ('DevTools Pro',        'billing@devtools.com',     'startup',    'us-west',    6000.00),
  ('ShipFast',            'accounts@shipfast.com',    'startup',    'eu-west',    1800.00),
  ('MicroSaaS One',       'finance@microsaas.com',    'startup',    'us-east',    3000.00),
  ('LaunchPad',           'billing@launchpad.com',    'startup',    'us-west',    2400.00),
  ('GrowthHack Ltd',      'team@growthhack.com',      'startup',    'ap-south',   1800.00),
  ('ProductFirst',        'billing@productfirst.com', 'startup',    'us-east',    4200.00),
  ('IterateFast',         'accounts@iterate.com',     'startup',    'eu-west',    3600.00),
  ('MetricsMatter',       'finance@metrics.com',      'startup',    'us-west',    2400.00),
  ('ScaleUp Inc',         'billing@scaleup.com',      'startup',    'ap-south',   6000.00),
  ('BootstrappedCo',      'team@bootstrapped.com',    'startup',    'us-east',    1200.00),
  ('SoloFounder',         'me@solofound.com',         'startup',    'us-west',    2400.00),
  ('RemoteFirst',         'billing@remotefirst.com',  'startup',    'eu-west',    3000.00),
  ('AsyncTeam',           'accounts@asyncteam.com',   'startup',    'us-east',    1800.00),
  ('GlobalSaaS',          'finance@globalsaas.com',   'startup',    'ap-south',   4800.00);

-- Orders: ~200 rows spread across customers
-- Enterprise customers: more orders, higher totals
-- Using generate_series for bulk inserts

INSERT INTO orders (customer_id, status, total, created_at, completed_at)
SELECT
  c.id,
  (ARRAY['pending','processing','completed','completed','completed','cancelled','refunded'])[floor(random()*7+1)::int],
  ROUND((random() * 2000 + 100)::numeric, 2),
  NOW() - (random() * 365 || ' days')::interval,
  CASE WHEN random() > 0.3 THEN NOW() - (random() * 30 || ' days')::interval ELSE NULL END
FROM customers c
CROSS JOIN generate_series(1,
  CASE c.segment
    WHEN 'enterprise' THEN 8
    WHEN 'smb' THEN 4
    ELSE 2
  END
) s;

-- Order items: 1-4 items per order
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT
  o.id,
  floor(random() * 10 + 1)::int,
  floor(random() * 3 + 1)::int,
  p.price * (0.9 + random() * 0.2)
FROM orders o
CROSS JOIN LATERAL generate_series(1, floor(random() * 3 + 1)::int) s
JOIN products p ON p.id = floor(random() * 10 + 1)::int;

-- Support tickets: enterprise gets more
INSERT INTO support_tickets (customer_id, subject, status, priority, created_at, resolved_at)
SELECT
  c.id,
  (ARRAY[
    'Login issue', 'Billing question', 'Feature request',
    'Performance problem', 'API error', 'Data export help',
    'Integration setup', 'Account access', 'Upgrade inquiry',
    'Bug report'
  ])[floor(random()*10+1)::int],
  (ARRAY['open','in_progress','resolved','closed'])[floor(random()*4+1)::int],
  (ARRAY['low','medium','high','critical'])[floor(random()*4+1)::int],
  NOW() - (random() * 180 || ' days')::interval,
  CASE WHEN random() > 0.4 THEN NOW() - (random() * 30 || ' days')::interval ELSE NULL END
FROM customers c
CROSS JOIN generate_series(1,
  CASE c.segment
    WHEN 'enterprise' THEN 5
    WHEN 'smb' THEN 3
    ELSE 1
  END
) s;
