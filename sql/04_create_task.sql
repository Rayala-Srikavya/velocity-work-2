CREATE OR REPLACE TASK CONFIG.detect_new_tables_velocity_task
  WAREHOUSE = BUSINESS_ANALYTICS_WH
  SCHEDULE = 'USING CRON 30 7 * * * UTC'
AS
  CALL CONFIG.velocity_detect_new_tables_all_schemas();
