CREATE TABLE IF NOT EXISTS email_log (
  id          SERIAL PRIMARY KEY,
  order_id    INTEGER NOT NULL REFERENCES orders(id),
  customer_id INTEGER NOT NULL REFERENCES customers(id),
  email       TEXT NOT NULL,
  subject     TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'sent',
  sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  provider    TEXT,
  provider_id TEXT
);

CREATE INDEX idx_email_log_customer_id ON email_log(customer_id);
CREATE INDEX idx_email_log_sent_at ON email_log(sent_at);
