-- Track the album/playlist/podcast context that produced playback progress and history.

CREATE TYPE playback_context_type AS ENUM (
  'album',
  'playlist',
  'podcast'
);

ALTER TABLE playback_progress
  ADD COLUMN context_type playback_context_type,
  ADD COLUMN context_id uuid,
  ADD CONSTRAINT playback_progress_context_pair_check CHECK (
    (context_type IS NULL AND context_id IS NULL)
    OR
    (context_type IS NOT NULL AND context_id IS NOT NULL)
  );

CREATE INDEX playback_progress_account_context_idx
  ON playback_progress (account_id, context_type, context_id, updated_at DESC)
  WHERE context_type IS NOT NULL;

ALTER TABLE playback_history_events
  ADD COLUMN context_type playback_context_type,
  ADD COLUMN context_id uuid,
  ADD CONSTRAINT playback_history_context_pair_check CHECK (
    (context_type IS NULL AND context_id IS NULL)
    OR
    (context_type IS NOT NULL AND context_id IS NOT NULL)
  );

CREATE INDEX playback_history_account_context_idx
  ON playback_history_events (account_id, context_type, context_id, played_at DESC)
  WHERE context_type IS NOT NULL;
