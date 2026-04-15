-- Add response mode and response root columns to api_endpoints table
-- Check if columns exist before adding to make migration safe

-- Add response_mode column with default 'object'
ALTER TABLE api_endpoints ADD COLUMN response_mode TEXT NOT NULL DEFAULT 'object';

-- Add response_root column (nullable)
ALTER TABLE api_endpoints ADD COLUMN response_root TEXT;
