CREATE OR REPLACE PROCEDURE monitoring.detect_new_tables_all_schemas()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_count INTEGER;
    known_count INTEGER;
    new_table_details STRING;
    return_message STRING;
BEGIN
    FOR schema_row IN (
        SELECT schema_name
        FROM information_schema.schemata
        WHERE catalog_name = CURRENT_DATABASE()
          AND schema_name NOT IN ('INFORMATION_SCHEMA', 'MONITORING')
    )
    DO
        -- Count current tables
        SELECT COUNT(*) INTO current_count
        FROM information_schema.tables
        WHERE table_catalog = CURRENT_DATABASE()
          AND table_schema = schema_row.schema_name;

        -- Count known tables
        SELECT COUNT(*) INTO known_count
        FROM monitoring.known_tables
        WHERE table_schema = schema_row.schema_name;

        -- If new tables exist
        IF (current_count > known_count) THEN
            -- Capture new tables before inserting
            CREATE TEMP TABLE temp_new_tables AS
            SELECT t.table_schema, t.table_name, t.created
            FROM information_schema.tables t
            LEFT JOIN monitoring.known_tables k
              ON t.table_schema = k.table_schema AND t.table_name = k.table_name
            WHERE t.table_catalog = CURRENT_DATABASE()
              AND t.table_schema = schema_row.schema_name
              AND k.table_name IS NULL;

            -- Insert new tables
            INSERT INTO monitoring.known_tables (table_schema, table_name, created_at)
            SELECT table_schema, table_name, created FROM temp_new_tables;

            -- Log alert
            SELECT COALESCE(
                TRY(
                    LISTAGG(
                        '- ' || table_name || ' (Created: ' || TO_CHAR(created, 'YYYY-MM-DD HH24:MI:SS') || ')',
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
                'New tables detected in schema: ' || schema_row.schema_name || ':\n' || new_table_details
            );

            DROP TABLE IF EXISTS temp_new_tables;
        END IF;
    END FOR;

    return_message := 'Schema scan completed for database: ' || CURRENT_DATABASE();
    RETURN return_message;
END;
$$;
