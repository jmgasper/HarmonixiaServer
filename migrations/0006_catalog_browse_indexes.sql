-- Browse read-model indexes for published, stable catalog pages.

CREATE INDEX IF NOT EXISTS artists_published_browse_name_idx
  ON artists (
    (lower(COALESCE(sort_name, name))),
    (lower(name)),
    id
  )
  WHERE published_at IS NOT NULL
    AND stable_grouping;

CREATE INDEX IF NOT EXISTS albums_published_browse_artist_title_idx
  ON albums (
    artist_id,
    (lower(title)),
    id
  )
  WHERE published_at IS NOT NULL
    AND stable_grouping;

CREATE INDEX IF NOT EXISTS tracks_published_browse_album_position_idx
  ON tracks (
    album_id,
    (COALESCE(disc_number, 0)),
    (COALESCE(track_number, 0)),
    (lower(title)),
    id
  )
  WHERE published_at IS NOT NULL
    AND stable_grouping;

CREATE INDEX IF NOT EXISTS tracks_published_browse_artist_idx
  ON tracks (
    artist_id,
    album_id,
    id
  )
  WHERE published_at IS NOT NULL
    AND stable_grouping;

CREATE INDEX IF NOT EXISTS podcasts_published_browse_title_idx
  ON podcasts (
    (lower(title)),
    id
  )
  WHERE published_at IS NOT NULL
    AND stable_grouping;

CREATE INDEX IF NOT EXISTS episodes_published_browse_podcast_position_idx
  ON episodes (
    podcast_id,
    (COALESCE(season_number, 0)),
    (COALESCE(episode_number, 0)),
    (lower(title)),
    id
  )
  WHERE published_at IS NOT NULL
    AND stable_grouping;

CREATE INDEX IF NOT EXISTS media_files_published_canonical_track_idx
  ON media_files (track_id)
  WHERE track_id IS NOT NULL
    AND status = 'published'::media_file_status
    AND published_at IS NOT NULL
    AND duplicate_of_media_file_id IS NULL;

CREATE INDEX IF NOT EXISTS media_files_published_canonical_episode_idx
  ON media_files (episode_id)
  WHERE episode_id IS NOT NULL
    AND status = 'published'::media_file_status
    AND published_at IS NOT NULL
    AND duplicate_of_media_file_id IS NULL;

CREATE INDEX IF NOT EXISTS quarantine_items_active_media_file_idx
  ON quarantine_items (media_file_id)
  WHERE media_file_id IS NOT NULL
    AND status IN ('open'::quarantine_status, 'retrying'::quarantine_status);
