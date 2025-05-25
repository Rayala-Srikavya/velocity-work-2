CREATE OR REPLACE ALERT monitoring.new_table_alert
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  IF (
    EXISTS (
      SELECT 1
      FROM monitoring.alert_log
      WHERE event_time > DATEADD(MINUTE, -2, CURRENT_TIMESTAMP)
    )
  )
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'new_table_email_integration',
      'rayalasrikavya9@gmail.com',
      'New Tables Detected',
      'New tables were added. Check the alert_log table for details.'
    );
