CREATE OR REPLACE PROCEDURE monitoring.detect_new_tables_all_schemas()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    new_table_details STRING;
    return_message STRING;
BEGIN
    -- Step 1: Create a snapshot of current tables
    CREATE OR REPLACE TEMP TABLE temp_current_tables AS
    SELECT table_schema, table_name, created
    FROM information_schema.tables
    WHERE table_catalog = CURRENT_DATABASE()
      AND table_schema NOT IN ('INFORMATION_SCHEMA', 'MONITORING');

    -- Step 2: Identify new tables (not in known_tables)
    CREATE OR REPLACE TEMP TABLE temp_new_tables AS
    SELECT c.table_schema, c.table_name, c.created
    FROM temp_current_tables c
    LEFT JOIN monitoring.known_tables k
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name
    WHERE k.table_name IS NULL;

    -- Step 3: Log and alert for new tables
    IF EXISTS (SELECT 1 FROM temp_new_tables) THEN
        SELECT COALESCE(
            TRY(
                LISTAGG(
                    '- ' || table_schema || '.' || table_name || ' (Created: ' || TO_CHAR(created, 'YYYY-MM-DD HH24:MI:SS') || ')',
                    '\n'
                ) WITHIN GROUP (ORDER BY created)
            ),
            'New tables detected, but could not format details.'
        )
        INTO new_table_details
        FROM temp_new_tables;

        INSERT INTO monitoring.alert_log (event_time, message)
        VALUES (
            CURRENT_TIMESTAMP,
            'New tables detected:\n' || new_table_details
        );

        -- Optional: Trigger alert email (if using Snowflake alert integration)
        -- This is handled by your alert definition in sql/05_create_alert.sql
    END IF;

    -- Step 4: Replace known_tables with current snapshot
    DELETE FROM monitoring.known_tables;

    INSERT INTO monitoring.known_tables (table_schema, table_name, created_at)
    SELECT table_schema, table_name, created FROM temp_current_tables;

    -- Cleanup
    DROP TABLE IF EXISTS temp_current_tables;
    DROP TABLE IF EXISTS temp_new_tables;

    return_message := 'Schema scan completed and known_tables refreshed.';
    RETURN return_message;
END;
$$;
