CREATE OR REPLACE TASK monitoring.detect_new_tables_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 30 7 * * * UTC'  -- Runs daily at 1:00 PM IST
AS
  CALL monitoring.detect_new_tables_all_schemas();
