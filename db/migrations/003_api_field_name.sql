-- Add api_field_name column to api_response_fields table
-- This allows mapping API field names to canonical field names

ALTER TABLE api_response_fields ADD COLUMN api_field_name TEXT;
