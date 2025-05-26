CREATE OR REPLACE PROCEDURE monitoring.detect_new_tables_all_schemas()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_count INTEGER;
    known_count INTEGER;
    new_table_details STRING;
BEGIN
    FOR schema_row IN (
        SELECT schema_name
        FROM information_schema.schemata
        WHERE catalog_name = CURRENT_DATABASE()
          AND schema_name NOT IN ('INFORMATION_SCHEMA', 'MONITORING')
    )
    DO
        -- Count current tables in the schema
        SELECT COUNT(*) INTO current_count
        FROM information_schema.tables
        WHERE table_catalog = CURRENT_DATABASE()
          AND table_schema = schema_row.schema_name;

        -- Count known tables in monitoring.known_tables
        SELECT COUNT(*) INTO known_count
        FROM monitoring.known_tables
        WHERE table_schema = schema_row.schema_name;

        -- If new tables exist, insert them and log alert
        IF (current_count > known_count) THEN
            -- Capture new tables before inserting
            CREATE OR REPLACE TEMP TABLE monitoring._new_tables AS
            SELECT t.table_schema, t.table_name, CURRENT_TIMESTAMP AS created_at
            FROM information_schema.tables t
            LEFT JOIN monitoring.known_tables k
              ON t.table_schema = k.table_schema AND t.table_name = k.table_name
            WHERE t.table_catalog = CURRENT_DATABASE()
              AND t.table_schema = schema_row.schema_name
              AND k.table_name IS NULL;

            -- Insert new tables into known_tables
            INSERT INTO monitoring.known_tables (table_schema, table_name, created_at)
            SELECT * FROM monitoring._new_tables;

            -- Aggregate new table details for alert message
            SELECT COALESCE(
                LISTAGG('- ' || table_name || ' (Detected: ' || TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') || ')', '\n'),
                'No new tables detected.'
            )
            INTO new_table_details
            FROM monitoring._new_tables;

            -- Insert alert log entry
            INSERT INTO monitoring.alert_log (event_time, message)
            VALUES (
                CURRENT_TIMESTAMP,
                'New tables detected in schema: ' || schema_row.schema_name || ':\n' || new_table_details
            );
        END IF;
    END FOR;

    RETURN 'Schema scan completed for database: ' || CURRENT_DATABASE();
END;
$$;

