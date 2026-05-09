-- Metadata maintenance and provider repair persistence model.
-- These tables are the Postgres-backed system of record for the initial
-- maintenance API slice.

CREATE TYPE import_job_kind AS ENUM (
  'full_rescan',
  'subtree_rescan',
  'provider_repair',
  'quarantine_retry'
);

CREATE TYPE import_job_status AS ENUM (
  'queued',
  'running',
  'completed',
  'failed',
  'quarantined',
  'retrying'
);

CREATE TYPE provider_kind AS ENUM (
  'music_brainz',
  'cover_art_archive',
  'discogs',
  'fanart_tv',
  'the_audio_db',
  'local_sidecars'
);

CREATE TYPE provider_status AS ENUM (
  'healthy',
  'degraded',
  'backing_off',
  'disabled',
  'unconfigured'
);

CREATE TYPE quarantine_reason AS ENUM (
  'duplicate',
  'metadata_failure',
  'file_error',
  'unsupported_format',
  'conflicting_metadata'
);

CREATE TYPE quarantine_status AS ENUM (
  'open',
  'retrying',
  'resolved',
  'deleted'
);

CREATE TABLE import_jobs (
  id uuid PRIMARY KEY,
  kind import_job_kind NOT NULL,
  status import_job_status NOT NULL,
  scope jsonb NOT NULL,
  repair_plan jsonb NOT NULL,
  catalog_mutation_policy text NOT NULL DEFAULT 'preserve_visible_until_stable_grouping',
  provider_filter provider_kind[] NOT NULL DEFAULT '{}',
  pipeline text NOT NULL DEFAULT 'import_pipeline',
  source text NOT NULL,
  reason text,
  related_quarantine_item_id uuid,
  idempotency_key text NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT import_jobs_pipeline_check CHECK (pipeline = 'import_pipeline'),
  CONSTRAINT import_jobs_catalog_policy_check
    CHECK (catalog_mutation_policy = 'preserve_visible_until_stable_grouping')
);

CREATE UNIQUE INDEX import_jobs_active_idempotency_idx
  ON import_jobs (idempotency_key)
  WHERE status IN ('queued', 'running', 'retrying');

CREATE INDEX import_jobs_status_created_idx
  ON import_jobs (status, created_at DESC);

CREATE TABLE provider_health (
  provider provider_kind PRIMARY KEY,
  enabled boolean NOT NULL,
  status provider_status NOT NULL,
  api_key_configured boolean NOT NULL DEFAULT false,
  maintenance_ready boolean NOT NULL DEFAULT true,
  failure_count integer NOT NULL DEFAULT 0,
  retry_after timestamptz,
  last_success_at timestamptz,
  last_failure_at timestamptz,
  message text,
  updated_at timestamptz NOT NULL
);

CREATE INDEX provider_health_status_idx
  ON provider_health (enabled, status);

CREATE TABLE quarantine_items (
  id uuid PRIMARY KEY,
  media_file_id uuid,
  source_path text NOT NULL,
  reason quarantine_reason NOT NULL,
  status quarantine_status NOT NULL DEFAULT 'open',
  retry_count integer NOT NULL DEFAULT 0,
  retry_eligible boolean NOT NULL DEFAULT true,
  last_import_job_id uuid REFERENCES import_jobs(id),
  admin_note text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE INDEX quarantine_items_status_reason_idx
  ON quarantine_items (status, reason, created_at DESC);
