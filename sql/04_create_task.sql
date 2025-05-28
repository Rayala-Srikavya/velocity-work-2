CREATE OR REPLACE TASK config.detect_new_tables_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 30 7 * * * UTC'  -- Runs daily at 1:30 PM IST
AS
  CALL config.velocity_detect_new_tables_all_schemas();
