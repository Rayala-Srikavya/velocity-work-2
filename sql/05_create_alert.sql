CREATE OR REPLACE ALERT config.new_table_alert
  WAREHOUSE = COMPTE_WH
  SCHEDULE = '1 MINUTE'
  IF (
    EXISTS (
      SELECT 1
      FROM config.alert_log
      WHERE event_time > DATEADD(MINUTE, -2, CURRENT_TIMESTAMP)
    )
  )
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'new_table_email_integration',
      'rayalasrikavya9@gmail.com',
      'New Tables Detected',
      'New tables detected in your database. See recent entries in config.alert_log.'
    );
