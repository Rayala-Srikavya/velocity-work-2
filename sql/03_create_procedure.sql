CREATE OR REPLACE PROCEDURE monitoring.detect_new_tables_all_schemas()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    new_table_count INTEGER DEFAULT 0;
    return_message STRING DEFAULT '';
BEGIN
    -- Step 1: Create snapshot of current tables
    CREATE OR REPLACE TEMP TABLE temp_current_tables AS
    SELECT table_schema, table_name, created
    FROM information_schema.tables
    WHERE table_catalog = CURRENT_DATABASE()
      AND table_schema NOT IN ('INFORMATION_SCHEMA', 'MONITORING');

    -- Step 2: Identify new tables
    CREATE OR REPLACE TEMP TABLE temp_new_tables AS
    SELECT c.table_schema, c.table_name, c.created
    FROM temp_current_tables c
    LEFT JOIN monitoring.known_tables k
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name
    WHERE k.table_name IS NULL;

    -- Step 3: Count new tables
    SELECT COUNT(*) INTO :new_table_count FROM temp_new_tables;

    -- Step 4: Log new tables
    IF (:new_table_count > 0) THEN
        FOR rec IN (
            SELECT table_schema, table_name, created
            FROM temp_new_tables
        ) DO
            INSERT INTO monitoring.alert_log (event_time, message)
            VALUES (
                CURRENT_TIMESTAMP,
                'New table detected: ' || rec.table_schema || '.' || rec.table_name || 
                ' (Created: ' || TO_CHAR(rec.created, 'YYYY-MM-DD HH24:MI:SS') || ')'
            );
        END FOR;
    END IF;

    -- Step 5: Refresh known_tables
    TRUNCATE TABLE monitoring.known_tables;

    INSERT INTO monitoring.known_tables (table_schema, table_name, created_at)
    SELECT table_schema, table_name, created FROM temp_current_tables;

    -- Step 6: Log refresh
    INSERT INTO monitoring.alert_log (event_time, message)
    VALUES (
        CURRENT_TIMESTAMP,
        'known_tables refreshed with latest snapshot.'
    );

    -- Cleanup
    DROP TABLE IF EXISTS temp_current_tables;
    DROP TABLE IF EXISTS temp_new_tables;

    return_message := 'Schema scan completed and known_tables refreshed.';
    RETURN return_message;
END;
$$;
