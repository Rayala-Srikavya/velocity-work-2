CREATE OR REPLACE PROCEDURE config.velocity_detect_new_tables_all_schemas()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    new_table_count INTEGER DEFAULT 0;
BEGIN
    -- Step 1: Snapshot current tables
    CREATE OR REPLACE TEMP TABLE temp_current_tables AS
    SELECT table_schema, table_name, created
    FROM information_schema.tables
    WHERE table_catalog = CURRENT_DATABASE()
      AND table_schema NOT IN ('INFORMATION_SCHEMA', 'CONFIG');

    -- Step 2: Identify new tables
    CREATE OR REPLACE TEMP TABLE temp_new_tables AS
    SELECT c.table_schema, c.table_name, c.created
    FROM temp_current_tables c
    LEFT JOIN config.known_tables k
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name
    WHERE k.table_name IS NULL;

    -- Step 3: Count new tables
    SELECT COUNT(*) INTO :new_table_count FROM temp_new_tables;

    -- Step 4: Log new tables if any
    IF (new_table_count > 0) THEN
        INSERT INTO config.alert_log (event_time, schema_name, table_name, created_at, message)
        SELECT CURRENT_TIMESTAMP, table_schema, table_name, created,
               'New table detected in schema "' || table_schema || '"'
        FROM temp_new_tables;
    END IF;

    -- Step 5: Refresh known_tables
    DELETE FROM config.known_tables;
    INSERT INTO config.known_tables (table_schema, table_name, created_at)
    SELECT table_schema, table_name, created FROM temp_current_tables;

    -- Cleanup
    DROP TABLE IF EXISTS temp_current_tables;
    DROP TABLE IF EXISTS temp_new_tables;

    RETURN 'Detection completed. New table count: ' || new_table_count;
END;
$$;
