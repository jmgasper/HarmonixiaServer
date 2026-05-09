-- Final grouped-search facets and playlist participation.

ALTER TYPE catalog_entity_type ADD VALUE IF NOT EXISTS 'playlist';

CREATE OR REPLACE FUNCTION pg_temp.catalog_search_normalize_text(raw_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN normalized.key LIKE 'the %' THEN substr(normalized.key, 5)
    WHEN normalized.key LIKE 'a %' THEN substr(normalized.key, 3)
    WHEN normalized.key LIKE 'an %' THEN substr(normalized.key, 4)
    ELSE normalized.key
  END
  FROM (
    SELECT btrim(
      regexp_replace(
        lower(btrim(COALESCE(raw_value, ''))),
        '[^[:alnum:]]+',
        ' ',
        'g'
      )
    ) AS key
  ) AS normalized
$$;

CREATE OR REPLACE FUNCTION pg_temp.catalog_search_filter_keys(raw_value text)
RETURNS text[]
LANGUAGE sql
IMMUTABLE
AS $$
  WITH raw_values(value) AS (
    SELECT raw_value
    UNION ALL
    SELECT part
    FROM regexp_split_to_table(COALESCE(raw_value, ''), '[,;/|]') AS part
  ),
  normalized_values AS (
    SELECT pg_temp.catalog_search_normalize_text(value) AS key
    FROM raw_values
    WHERE NULLIF(btrim(value), '') IS NOT NULL
  )
  SELECT COALESCE(array_agg(DISTINCT key ORDER BY key), '{}'::text[])
  FROM normalized_values
  WHERE key <> ''
$$;

CREATE OR REPLACE FUNCTION pg_temp.catalog_search_json_filter_keys(raw_value jsonb)
RETURNS text[]
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  keys text[] := '{}'::text[];
  nested_keys text[] := '{}'::text[];
  item jsonb;
  field_name text;
BEGIN
  CASE jsonb_typeof(raw_value)
    WHEN 'string' THEN
      RETURN pg_temp.catalog_search_filter_keys(raw_value #>> '{}');
    WHEN 'array' THEN
      FOR item IN SELECT value FROM jsonb_array_elements(raw_value) AS values(value)
      LOOP
        nested_keys := pg_temp.catalog_search_json_filter_keys(item);
        SELECT COALESCE(array_agg(DISTINCT key ORDER BY key), '{}'::text[])
        INTO keys
        FROM unnest(keys || nested_keys) AS merged(key)
        WHERE key <> '';
      END LOOP;
      RETURN keys;
    WHEN 'object' THEN
      FOREACH field_name IN ARRAY ARRAY['name', 'title', 'value', 'genre']
      LOOP
        IF raw_value ? field_name THEN
          nested_keys := pg_temp.catalog_search_json_filter_keys(raw_value -> field_name);
          SELECT COALESCE(array_agg(DISTINCT key ORDER BY key), '{}'::text[])
          INTO keys
          FROM unnest(keys || nested_keys) AS merged(key)
          WHERE key <> '';
        END IF;
      END LOOP;
      RETURN keys;
    ELSE
      RETURN '{}'::text[];
  END CASE;
END;
$$;

ALTER TABLE media_files
  ADD COLUMN genres text[] NOT NULL DEFAULT '{}'::text[],
  ADD COLUMN format_keys text[] NOT NULL DEFAULT '{}'::text[];

UPDATE media_files
SET genres = COALESCE((
  SELECT array_agg(DISTINCT genre_key.key ORDER BY genre_key.key)
  FROM metadata_provenance mp
  CROSS JOIN LATERAL unnest(pg_temp.catalog_search_json_filter_keys(mp.value)) AS genre_key(key)
  WHERE mp.source_path = media_files.source_path
    AND pg_temp.catalog_search_filter_keys(mp.field_name) && ARRAY['genre', 'genres']::text[]
), '{}'::text[])
WHERE genres = '{}'::text[];

UPDATE media_files
SET format_keys = COALESCE((
  SELECT array_agg(DISTINCT format_key.key ORDER BY format_key.key)
  FROM unnest(ARRAY[mime_type, container, audio_codec]) AS raw_key(value)
  CROSS JOIN LATERAL unnest(pg_temp.catalog_search_filter_keys(raw_key.value)) AS format_key(key)
), '{}'::text[])
WHERE format_keys = '{}'::text[];

CREATE INDEX media_files_published_genres_idx
  ON media_files USING gin (genres)
  WHERE status = 'published';

CREATE INDEX media_files_published_format_keys_idx
  ON media_files USING gin (format_keys)
  WHERE status = 'published';

CREATE INDEX media_files_published_media_kind_idx
  ON media_files (media_kind)
  WHERE status = 'published';

CREATE INDEX albums_published_release_year_idx
  ON albums (release_year)
  WHERE published_at IS NOT NULL
    AND stable_grouping;
