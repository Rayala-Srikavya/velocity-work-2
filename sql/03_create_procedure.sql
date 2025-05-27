CREATE OR REPLACE PROCEDURE monitoring.detect_new_tables_all_schemas()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    new_table_count INTEGER;
    return_message STRING;
BEGIN
    -- Step 1: Capture current state of all user-created tables
    CREATE OR REPLACE TEMP TABLE temp_current_tables AS
    SELECT table_schema, table_name, created
    FROM information_schema.tables
    WHERE table_catalog = CURRENT_DATABASE()
      AND table_schema NOT IN ('INFORMATION_SCHEMA', 'MONITORING');

    -- Step 2: Find tables not in known_tables (i.e., new ones)
    CREATE OR REPLACE TEMP TABLE temp_new_tables AS
    SELECT c.table_schema, c.table_name, c.created
    FROM temp_current_tables c
    LEFT JOIN monitoring.known_tables k
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name
    WHERE k.table_name IS NULL;

    -- Step 3: Count and log new tables
    SELECT COUNT(*) INTO new_table_count FROM temp_new_tables;

    IF new_table_count > 0 THEN
        -- Insert each new table into alert_log with structured fields
        INSERT INTO monitoring.alert_log (event_time, schema_name, table_name, created_at, message)
        SELECT
            CURRENT_TIMESTAMP,
            table_schema,
            table_name,
            created,
            'New table detected: ' || table_schema || '.' || table_name || 
            ' (Created: ' || TO_CHAR(created, 'YYYY-MM-DD HH24:MI:SS') || ')'
        FROM temp_new_tables;
    END IF;

    -- Step 4: Refresh known_tables with current snapshot
    TRUNCATE TABLE monitoring.known_tables;

    INSERT INTO monitoring.known_tables (table_schema, table_name, created_at)
    SELECT table_schema, table_name, created
    FROM temp_current_tables;

    -- Cleanup
    DROP TABLE IF EXISTS temp_current_tables;
    DROP TABLE IF EXISTS temp_new_tables;

    return_message := 'Procedure completed successfully.';
    RETURN return_message;
END;
$$;
