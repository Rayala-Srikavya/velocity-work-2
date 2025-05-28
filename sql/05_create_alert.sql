CREATE OR REPLACE ALERT CONFIG.new_table_velocity_alert
  WAREHOUSE = BUSINESS_ANALYTICS_WH
  SCHEDULE = '1 MINUTE'
  IF (
    EXISTS (
      SELECT 1
      FROM CONFIG.alert_log
      WHERE event_time > DATEADD(MINUTE, -2, CURRENT_TIMESTAMP)
    )
  )
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'new_table_email_integration',
      'rayalasrikavya9@gmail.com',
      'New Tables Detected',
      'New tables detected in CONFIG_CLOUD_VELOCITY. See CONFIG.alert_log.'
    );
