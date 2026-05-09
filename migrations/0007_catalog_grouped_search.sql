-- Grouped search read-model support for published catalog entities.

ALTER TABLE catalog_search_projection
  ADD COLUMN normalized_display_title text;

UPDATE catalog_search_projection csp
SET normalized_display_title = ar.normalized_name
FROM artists ar
WHERE csp.entity_type = 'artist'::catalog_entity_type
  AND csp.entity_id = ar.id
  AND csp.normalized_display_title IS NULL;

UPDATE catalog_search_projection csp
SET normalized_display_title = al.normalized_title
FROM albums al
WHERE csp.entity_type = 'album'::catalog_entity_type
  AND csp.entity_id = al.id
  AND csp.normalized_display_title IS NULL;

UPDATE catalog_search_projection csp
SET normalized_display_title = t.normalized_title
FROM tracks t
WHERE csp.entity_type = 'track'::catalog_entity_type
  AND csp.entity_id = t.id
  AND csp.normalized_display_title IS NULL;

UPDATE catalog_search_projection csp
SET normalized_display_title = p.normalized_title
FROM podcasts p
WHERE csp.entity_type = 'podcast'::catalog_entity_type
  AND csp.entity_id = p.id
  AND csp.normalized_display_title IS NULL;

UPDATE catalog_search_projection csp
SET normalized_display_title = e.normalized_title
FROM episodes e
WHERE csp.entity_type = 'episode'::catalog_entity_type
  AND csp.entity_id = e.id
  AND csp.normalized_display_title IS NULL;

UPDATE catalog_search_projection
SET normalized_display_title = normalized_text
WHERE normalized_display_title IS NULL;

ALTER TABLE catalog_search_projection
  ALTER COLUMN normalized_display_title SET NOT NULL;

ALTER TABLE catalog_search_projection
  ADD CONSTRAINT catalog_search_projection_normalized_display_nonempty_check
  CHECK (btrim(normalized_display_title) <> '');

CREATE INDEX catalog_search_projection_published_display_prefix_idx
  ON catalog_search_projection (
    entity_type,
    normalized_display_title text_pattern_ops,
    entity_id
  )
  WHERE published;

CREATE INDEX catalog_search_projection_published_text_prefix_idx
  ON catalog_search_projection (
    entity_type,
    normalized_text text_pattern_ops,
    entity_id
  )
  WHERE published;
