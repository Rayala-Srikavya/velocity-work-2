CREATE TABLE IF NOT EXISTS config.alert_log (
    event_time TIMESTAMP,
    schema_name STRING,
    table_name STRING,
    created_at TIMESTAMP,
    message STRING
);

