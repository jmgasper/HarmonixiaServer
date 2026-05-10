-- Public base URL for LAN-reachable remote playback callbacks.

ALTER TABLE system_config
  ADD COLUMN public_base_url text;

ALTER TABLE system_config
  ADD CONSTRAINT system_config_public_base_url_nonempty_check
  CHECK (public_base_url IS NULL OR btrim(public_base_url) <> '');
