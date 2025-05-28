-- This should only be run ONCE manually or at first deploy
INSERT INTO monitoring.known_tables (table_schema, table_name, created_at)
SELECT t.table_schema, t.table_name, t.created
FROM information_schema.tables t
WHERE t.table_catalog = CURRENT_DATABASE()
  AND t.table_schema NOT IN ('INFORMATION_SCHEMA', 'MONITORING');
