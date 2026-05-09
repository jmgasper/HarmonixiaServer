-- Runtime scan worker count for import pipeline path processing.

ALTER TABLE system_config
  ADD COLUMN scan_thread_count integer NOT NULL DEFAULT 8;

ALTER TABLE system_config
  ADD CONSTRAINT system_config_scan_thread_count_check
  CHECK (scan_thread_count > 0);
