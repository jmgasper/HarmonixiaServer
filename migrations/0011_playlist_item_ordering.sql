-- Make playlist item position uniqueness deferrable so transactional inserts,
-- removals, and full reorders can resequence a playlist without transient
-- uniqueness violations.

WITH ordered AS (
  SELECT
    id,
    (row_number() OVER (PARTITION BY playlist_id ORDER BY position ASC, id ASC) - 1)::integer
      AS new_position
  FROM playlist_items
)
UPDATE playlist_items pi
SET position = ordered.new_position
FROM ordered
WHERE pi.id = ordered.id
  AND pi.position <> ordered.new_position;

DROP INDEX IF EXISTS playlist_items_playlist_position_idx;

ALTER TABLE playlist_items
  ADD CONSTRAINT playlist_items_playlist_position_key
  UNIQUE (playlist_id, position)
  DEFERRABLE INITIALLY IMMEDIATE;
