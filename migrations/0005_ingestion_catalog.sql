-- Canonical ingest/catalog persistence for Harmonixia v1.
-- The maintenance foundation queues import work; this migration adds the
-- durable catalog records that the shared import pipeline publishes into.

ALTER TYPE import_job_kind ADD VALUE IF NOT EXISTS 'initial_scan';
ALTER TYPE import_job_kind ADD VALUE IF NOT EXISTS 'dropbox_ingest';

CREATE TYPE media_kind AS ENUM (
  'music',
  'podcast'
);

CREATE TYPE album_kind AS ENUM (
  'album',
  'compilation',
  'single',
  'unknown'
);

CREATE TYPE media_file_status AS ENUM (
  'staged',
  'published',
  'duplicate',
  'quarantined',
  'failed'
);

CREATE TYPE catalog_entity_type AS ENUM (
  'artist',
  'album',
  'track',
  'podcast',
  'episode',
  'media_file'
);

CREATE TYPE artwork_kind AS ENUM (
  'cover',
  'artist',
  'fanart',
  'thumbnail',
  'other'
);

CREATE TYPE metadata_match_kind AS ENUM (
  'exact_identifier',
  'high_confidence',
  'moderate_confidence',
  'local_only'
);

CREATE TABLE artists (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  normalized_name text NOT NULL,
  sort_name text,
  stable_grouping boolean NOT NULL DEFAULT false,
  published_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT artists_name_nonempty_check CHECK (btrim(name) <> ''),
  CONSTRAINT artists_normalized_name_nonempty_check CHECK (btrim(normalized_name) <> '')
);

CREATE UNIQUE INDEX artists_normalized_name_idx
  ON artists (normalized_name);

CREATE TABLE albums (
  id uuid PRIMARY KEY,
  artist_id uuid NOT NULL REFERENCES artists(id),
  title text NOT NULL,
  normalized_title text NOT NULL,
  album_kind album_kind NOT NULL DEFAULT 'unknown',
  release_year integer,
  stable_grouping boolean NOT NULL DEFAULT false,
  published_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT albums_title_nonempty_check CHECK (btrim(title) <> ''),
  CONSTRAINT albums_normalized_title_nonempty_check CHECK (btrim(normalized_title) <> '')
);

CREATE UNIQUE INDEX albums_artist_normalized_title_idx
  ON albums (artist_id, normalized_title);

CREATE TABLE tracks (
  id uuid PRIMARY KEY,
  album_id uuid NOT NULL REFERENCES albums(id),
  artist_id uuid NOT NULL REFERENCES artists(id),
  title text NOT NULL,
  normalized_title text NOT NULL,
  disc_number integer,
  track_number integer,
  duration_seconds integer,
  stable_grouping boolean NOT NULL DEFAULT false,
  published_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT tracks_title_nonempty_check CHECK (btrim(title) <> ''),
  CONSTRAINT tracks_normalized_title_nonempty_check CHECK (btrim(normalized_title) <> '')
);

CREATE UNIQUE INDEX tracks_album_position_title_idx
  ON tracks (
    album_id,
    COALESCE(disc_number, 0),
    COALESCE(track_number, 0),
    normalized_title
  );

CREATE TABLE podcasts (
  id uuid PRIMARY KEY,
  title text NOT NULL,
  normalized_title text NOT NULL,
  stable_grouping boolean NOT NULL DEFAULT false,
  published_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT podcasts_title_nonempty_check CHECK (btrim(title) <> ''),
  CONSTRAINT podcasts_normalized_title_nonempty_check CHECK (btrim(normalized_title) <> '')
);

CREATE UNIQUE INDEX podcasts_normalized_title_idx
  ON podcasts (normalized_title);

CREATE TABLE episodes (
  id uuid PRIMARY KEY,
  podcast_id uuid NOT NULL REFERENCES podcasts(id),
  title text NOT NULL,
  normalized_title text NOT NULL,
  season_number integer,
  episode_number integer,
  duration_seconds integer,
  stable_grouping boolean NOT NULL DEFAULT false,
  published_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT episodes_title_nonempty_check CHECK (btrim(title) <> ''),
  CONSTRAINT episodes_normalized_title_nonempty_check CHECK (btrim(normalized_title) <> '')
);

CREATE UNIQUE INDEX episodes_podcast_position_title_idx
  ON episodes (
    podcast_id,
    COALESCE(season_number, 0),
    COALESCE(episode_number, 0),
    normalized_title
  );

CREATE TABLE media_files (
  id uuid PRIMARY KEY,
  media_kind media_kind NOT NULL,
  status media_file_status NOT NULL,
  source_path text NOT NULL,
  managed_path text,
  file_hash text NOT NULL,
  file_size bigint NOT NULL,
  mime_type text,
  container text,
  audio_codec text,
  duration_seconds integer,
  bitrate integer,
  sample_rate integer,
  channels integer,
  track_id uuid REFERENCES tracks(id),
  episode_id uuid REFERENCES episodes(id),
  duplicate_of_media_file_id uuid REFERENCES media_files(id),
  import_job_id uuid REFERENCES import_jobs(id),
  discovered_at timestamptz NOT NULL,
  published_at timestamptz,
  updated_at timestamptz NOT NULL,
  CONSTRAINT media_files_source_path_nonempty_check CHECK (btrim(source_path) <> ''),
  CONSTRAINT media_files_file_hash_nonempty_check CHECK (btrim(file_hash) <> ''),
  CONSTRAINT media_files_file_size_nonnegative_check CHECK (file_size >= 0),
  CONSTRAINT media_files_one_catalog_item_check CHECK (
    (track_id IS NOT NULL AND episode_id IS NULL)
    OR (track_id IS NULL AND episode_id IS NOT NULL)
    OR (track_id IS NULL AND episode_id IS NULL)
  )
);

CREATE UNIQUE INDEX media_files_source_path_idx
  ON media_files (source_path);

CREATE UNIQUE INDEX media_files_managed_path_idx
  ON media_files (managed_path)
  WHERE managed_path IS NOT NULL;

CREATE INDEX media_files_hash_status_idx
  ON media_files (file_hash, status);

CREATE INDEX media_files_track_idx
  ON media_files (track_id)
  WHERE track_id IS NOT NULL;

CREATE INDEX media_files_episode_idx
  ON media_files (episode_id)
  WHERE episode_id IS NOT NULL;

ALTER TABLE quarantine_items
  ADD CONSTRAINT quarantine_items_media_file_id_fkey
  FOREIGN KEY (media_file_id) REFERENCES media_files(id);

CREATE TABLE artwork_assets (
  id uuid PRIMARY KEY,
  entity_type catalog_entity_type NOT NULL,
  entity_id uuid NOT NULL,
  provider provider_kind NOT NULL,
  artwork_kind artwork_kind NOT NULL,
  source_uri text,
  file_path text,
  mime_type text,
  width integer,
  height integer,
  confidence real NOT NULL,
  created_at timestamptz NOT NULL,
  CONSTRAINT artwork_assets_confidence_check CHECK (confidence >= 0 AND confidence <= 1)
);

CREATE INDEX artwork_assets_entity_idx
  ON artwork_assets (entity_type, entity_id, artwork_kind);

CREATE TABLE metadata_provider_links (
  id uuid PRIMARY KEY,
  entity_type catalog_entity_type NOT NULL,
  entity_id uuid NOT NULL,
  provider provider_kind NOT NULL,
  provider_item_id text NOT NULL,
  external_url text,
  match_kind metadata_match_kind NOT NULL,
  confidence real NOT NULL,
  auto_accepted boolean NOT NULL DEFAULT false,
  raw_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT metadata_provider_links_provider_item_nonempty_check
    CHECK (btrim(provider_item_id) <> ''),
  CONSTRAINT metadata_provider_links_confidence_check CHECK (confidence >= 0 AND confidence <= 1)
);

CREATE UNIQUE INDEX metadata_provider_links_entity_provider_item_idx
  ON metadata_provider_links (entity_type, entity_id, provider, provider_item_id);

CREATE TABLE metadata_provenance (
  id uuid PRIMARY KEY,
  entity_type catalog_entity_type NOT NULL,
  entity_id uuid NOT NULL,
  field_name text NOT NULL,
  provider provider_kind NOT NULL,
  value jsonb NOT NULL,
  confidence real NOT NULL,
  auto_accepted boolean NOT NULL DEFAULT false,
  import_job_id uuid REFERENCES import_jobs(id),
  source_path text,
  created_at timestamptz NOT NULL,
  CONSTRAINT metadata_provenance_field_nonempty_check CHECK (btrim(field_name) <> ''),
  CONSTRAINT metadata_provenance_confidence_check CHECK (confidence >= 0 AND confidence <= 1)
);

CREATE INDEX metadata_provenance_entity_field_idx
  ON metadata_provenance (entity_type, entity_id, field_name, created_at DESC);

CREATE TABLE catalog_search_projection (
  entity_type catalog_entity_type NOT NULL,
  entity_id uuid NOT NULL,
  display_title text NOT NULL,
  search_text text NOT NULL,
  normalized_text text NOT NULL,
  published boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (entity_type, entity_id),
  CONSTRAINT catalog_search_projection_display_nonempty_check CHECK (btrim(display_title) <> ''),
  CONSTRAINT catalog_search_projection_normalized_nonempty_check CHECK (btrim(normalized_text) <> '')
);

CREATE INDEX catalog_search_projection_published_idx
  ON catalog_search_projection (published, entity_type, normalized_text);

CREATE TABLE catalog_import_work_items (
  id uuid PRIMARY KEY,
  import_job_id uuid NOT NULL REFERENCES import_jobs(id),
  source_path text NOT NULL,
  media_file_id uuid REFERENCES media_files(id),
  status media_file_status NOT NULL DEFAULT 'staged',
  attempts integer NOT NULL DEFAULT 0,
  last_error text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT catalog_import_work_items_source_nonempty_check CHECK (btrim(source_path) <> '')
);

CREATE UNIQUE INDEX catalog_import_work_items_job_source_idx
  ON catalog_import_work_items (import_job_id, source_path);
