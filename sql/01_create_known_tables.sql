CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.known_tables (
    table_schema STRING,
    table_name STRING,
    created_at TIMESTAMP
);
