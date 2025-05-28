CREATE SCHEMA IF NOT EXISTS CONFIG;

CREATE TABLE IF NOT EXISTS config.known_tables (
    table_schema STRING,
    table_name STRING,
    created_at TIMESTAMP
);


