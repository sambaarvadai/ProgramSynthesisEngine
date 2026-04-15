-- Add concurrency, rate limit, and chunk size defaults to api_endpoints table
ALTER TABLE api_endpoints ADD COLUMN default_concurrency INTEGER NOT NULL DEFAULT 1;
ALTER TABLE api_endpoints ADD COLUMN default_rate_limit INTEGER;
ALTER TABLE api_endpoints ADD COLUMN default_chunk_size INTEGER;
