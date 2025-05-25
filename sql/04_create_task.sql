CREATE OR REPLACE TASK monitoring.detect_new_tables_task
  WAREHOUSE = your_warehouse
  SCHEDULE = 'USING CRON 30 7 * * * UTC'  -- Runs daily at 1:00 PM IST
AS
  CALL monitoring.detect_new_tables_all_schemas();
