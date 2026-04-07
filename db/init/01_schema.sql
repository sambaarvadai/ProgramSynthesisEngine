-- Customers
CREATE TABLE customers (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  email       TEXT UNIQUE NOT NULL,
  segment     TEXT NOT NULL CHECK (segment IN ('enterprise', 'smb', 'startup')),
  region      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  arr         NUMERIC(12,2) DEFAULT 0        -- annual recurring revenue
);

-- Products
CREATE TABLE products (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  category    TEXT NOT NULL,
  price       NUMERIC(10,2) NOT NULL,
  is_active   BOOLEAN NOT NULL DEFAULT true
);

-- Orders
CREATE TABLE orders (
  id           SERIAL PRIMARY KEY,
  customer_id  INTEGER NOT NULL REFERENCES customers(id),
  status       TEXT NOT NULL CHECK (status IN ('pending','processing','completed','cancelled','refunded')),
  total        NUMERIC(12,2) NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

-- Order items
CREATE TABLE order_items (
  id          SERIAL PRIMARY KEY,
  order_id    INTEGER NOT NULL REFERENCES orders(id),
  product_id  INTEGER NOT NULL REFERENCES products(id),
  quantity    INTEGER NOT NULL DEFAULT 1,
  unit_price  NUMERIC(10,2) NOT NULL
);

-- Support tickets
CREATE TABLE support_tickets (
  id           SERIAL PRIMARY KEY,
  customer_id  INTEGER NOT NULL REFERENCES customers(id),
  subject      TEXT NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('open','in_progress','resolved','closed')),
  priority     TEXT NOT NULL CHECK (priority IN ('low','medium','high','critical')),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at  TIMESTAMPTZ
);

-- Create indexes for common query patterns
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_support_tickets_customer_id ON support_tickets(customer_id);
CREATE INDEX idx_support_tickets_status ON support_tickets(status);
CREATE INDEX idx_customers_segment ON customers(segment);
CREATE INDEX idx_customers_region ON customers(region);
