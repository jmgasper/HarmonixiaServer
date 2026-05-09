-- Durable runtime configuration for the foundation slice.
-- Environment variables bootstrap these rows, but Postgres is the source of
-- truth after first initialization.

CREATE TABLE system_config (
  id integer PRIMARY KEY DEFAULT 1,
  library_root text NOT NULL,
  dropbox_root text NOT NULL,
  podcast_subtree text NOT NULL DEFAULT 'Podcasts',
  updated_at timestamptz NOT NULL,
  CONSTRAINT system_config_singleton_check CHECK (id = 1),
  CONSTRAINT system_config_library_root_nonempty_check CHECK (btrim(library_root) <> ''),
  CONSTRAINT system_config_dropbox_root_nonempty_check CHECK (btrim(dropbox_root) <> ''),
  CONSTRAINT system_config_podcast_subtree_nonempty_check CHECK (btrim(podcast_subtree) <> '')
);

CREATE TABLE provider_settings (
  provider provider_kind PRIMARY KEY,
  enabled boolean NOT NULL,
  requires_api_key boolean NOT NULL DEFAULT false,
  api_key_configured boolean NOT NULL DEFAULT false,
  api_key_secret text,
  updated_at timestamptz NOT NULL
);

CREATE INDEX provider_settings_enabled_idx
  ON provider_settings (enabled, provider);
