-- Explicit canonical original-media associations for published catalog items.

ALTER TABLE tracks
  ADD COLUMN canonical_media_file_id uuid;

ALTER TABLE episodes
  ADD COLUMN canonical_media_file_id uuid;

ALTER TABLE media_files
  ADD CONSTRAINT media_files_id_track_id_unique UNIQUE (id, track_id),
  ADD CONSTRAINT media_files_id_episode_id_unique UNIQUE (id, episode_id);

UPDATE tracks t
SET canonical_media_file_id = (
  SELECT mf.id
  FROM media_files mf
  WHERE mf.track_id = t.id
    AND mf.status = 'published'::media_file_status
    AND mf.published_at IS NOT NULL
    AND mf.duplicate_of_media_file_id IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM quarantine_items qi
      WHERE qi.media_file_id = mf.id
        AND qi.status IN ('open'::quarantine_status, 'retrying'::quarantine_status)
    )
  ORDER BY mf.published_at ASC NULLS LAST, mf.discovered_at ASC, mf.id ASC
  LIMIT 1
)
WHERE t.canonical_media_file_id IS NULL
  AND EXISTS (
    SELECT 1
    FROM media_files mf
    WHERE mf.track_id = t.id
      AND mf.status = 'published'::media_file_status
      AND mf.published_at IS NOT NULL
      AND mf.duplicate_of_media_file_id IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM quarantine_items qi
        WHERE qi.media_file_id = mf.id
          AND qi.status IN ('open'::quarantine_status, 'retrying'::quarantine_status)
      )
  );

UPDATE episodes e
SET canonical_media_file_id = (
  SELECT mf.id
  FROM media_files mf
  WHERE mf.episode_id = e.id
    AND mf.status = 'published'::media_file_status
    AND mf.published_at IS NOT NULL
    AND mf.duplicate_of_media_file_id IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM quarantine_items qi
      WHERE qi.media_file_id = mf.id
        AND qi.status IN ('open'::quarantine_status, 'retrying'::quarantine_status)
    )
  ORDER BY mf.published_at ASC NULLS LAST, mf.discovered_at ASC, mf.id ASC
  LIMIT 1
)
WHERE e.canonical_media_file_id IS NULL
  AND EXISTS (
    SELECT 1
    FROM media_files mf
    WHERE mf.episode_id = e.id
      AND mf.status = 'published'::media_file_status
      AND mf.published_at IS NOT NULL
      AND mf.duplicate_of_media_file_id IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM quarantine_items qi
        WHERE qi.media_file_id = mf.id
          AND qi.status IN ('open'::quarantine_status, 'retrying'::quarantine_status)
      )
  );

ALTER TABLE tracks
  ADD CONSTRAINT tracks_canonical_media_file_matches_track_fkey
  FOREIGN KEY (canonical_media_file_id, id)
  REFERENCES media_files (id, track_id);

ALTER TABLE episodes
  ADD CONSTRAINT episodes_canonical_media_file_matches_episode_fkey
  FOREIGN KEY (canonical_media_file_id, id)
  REFERENCES media_files (id, episode_id);

CREATE INDEX tracks_canonical_media_file_idx
  ON tracks (canonical_media_file_id)
  WHERE canonical_media_file_id IS NOT NULL;

CREATE INDEX episodes_canonical_media_file_idx
  ON episodes (canonical_media_file_id)
  WHERE canonical_media_file_id IS NOT NULL;
