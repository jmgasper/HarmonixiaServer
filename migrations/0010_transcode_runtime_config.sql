-- Runtime transcode capacity configuration for direct AAC output and future HLS.

ALTER TABLE system_config
  ADD COLUMN transcode_concurrency_limit integer NOT NULL DEFAULT 2;

ALTER TABLE system_config
  ADD CONSTRAINT system_config_transcode_concurrency_limit_check
  CHECK (transcode_concurrency_limit >= 0);
