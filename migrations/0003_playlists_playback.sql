-- User-scoped playlist and playback foundations.

CREATE TYPE playlist_scope AS ENUM (
  'personal',
  'shared'
);

CREATE TYPE playback_item_type AS ENUM (
  'track',
  'episode'
);

CREATE TABLE playlists (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  description text,
  scope playlist_scope NOT NULL,
  owner_account_id uuid REFERENCES local_accounts(id) ON DELETE CASCADE,
  created_by_account_id uuid REFERENCES local_accounts(id) ON DELETE SET NULL,
  updated_by_account_id uuid REFERENCES local_accounts(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT playlists_name_nonempty_check CHECK (btrim(name) <> ''),
  CONSTRAINT playlists_scope_owner_check CHECK (
    (scope = 'personal' AND owner_account_id IS NOT NULL)
    OR
    (scope = 'shared' AND owner_account_id IS NULL)
  )
);

CREATE INDEX playlists_owner_created_idx
  ON playlists (owner_account_id, created_at DESC)
  WHERE scope = 'personal';

CREATE INDEX playlists_shared_created_idx
  ON playlists (created_at DESC)
  WHERE scope = 'shared';

CREATE TABLE playlist_items (
  id uuid PRIMARY KEY,
  playlist_id uuid NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
  item_type playback_item_type NOT NULL,
  item_id uuid NOT NULL,
  position integer NOT NULL,
  added_by_account_id uuid REFERENCES local_accounts(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL,
  CONSTRAINT playlist_items_position_nonnegative_check CHECK (position >= 0)
);

CREATE UNIQUE INDEX playlist_items_playlist_position_idx
  ON playlist_items (playlist_id, position);

CREATE INDEX playlist_items_playlist_idx
  ON playlist_items (playlist_id);

CREATE TABLE playback_progress (
  account_id uuid NOT NULL REFERENCES local_accounts(id) ON DELETE CASCADE,
  item_type playback_item_type NOT NULL,
  item_id uuid NOT NULL,
  position_seconds integer NOT NULL,
  duration_seconds integer,
  completed boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (account_id, item_type, item_id),
  CONSTRAINT playback_progress_position_nonnegative_check CHECK (position_seconds >= 0),
  CONSTRAINT playback_progress_duration_nonnegative_check CHECK (
    duration_seconds IS NULL OR duration_seconds >= 0
  )
);

CREATE INDEX playback_progress_account_updated_idx
  ON playback_progress (account_id, updated_at DESC);

CREATE TABLE playback_history_events (
  id uuid PRIMARY KEY,
  account_id uuid NOT NULL REFERENCES local_accounts(id) ON DELETE CASCADE,
  item_type playback_item_type NOT NULL,
  item_id uuid NOT NULL,
  position_seconds integer NOT NULL,
  duration_seconds integer,
  completed boolean NOT NULL DEFAULT false,
  played_at timestamptz NOT NULL,
  CONSTRAINT playback_history_position_nonnegative_check CHECK (position_seconds >= 0),
  CONSTRAINT playback_history_duration_nonnegative_check CHECK (
    duration_seconds IS NULL OR duration_seconds >= 0
  )
);

CREATE INDEX playback_history_account_played_idx
  ON playback_history_events (account_id, played_at DESC);

CREATE INDEX playback_history_account_item_idx
  ON playback_history_events (account_id, item_type, item_id, played_at DESC);
