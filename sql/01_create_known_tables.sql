CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.known_tables (
    table_schema STRING,
    table_name STRING,
    created_at TIMESTAMP
);

-- Insert existing tables only if known_tables is empty
INSERT INTO monitoring.known_tables (table_schema, table_name, created_at)
SELECT t.table_schema, t.table_name, t.created
FROM information_schema.tables t
WHERE t.table_catalog = CURRENT_DATABASE()
  AND t.table_schema NOT IN ('INFORMATION_SCHEMA', 'MONITORING')
  AND NOT EXISTS (
      SELECT 1
      FROM monitoring.known_tables k
      WHERE k.table_schema = t.table_schema AND k.table_name = t.table_name
  );
