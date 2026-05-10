# Harmonixia Client Integration Guide

This guide is for desktop and mobile clients that want to provide a
Tidal/Spotify-style experience backed by Harmonixia Server.

The HTTP API is rooted at the server base URL, usually:

```text
http://127.0.0.1:3000
```

All client-facing endpoints use `/api/v1`. The live OpenAPI document is
available at:

```text
GET /api/v1/api-docs/openapi.json
GET /openapi.json
```

Swagger UI is served at:

```text
GET /api/v1/api-docs
```

## Authentication

Harmonixia currently uses HTTP Basic authentication for all authenticated
client APIs. There is no bearer token or refresh-token flow yet.

Send the header on every authenticated request:

```http
Authorization: Basic base64(username:password)
```

Example:

```sh
curl -u "alice:correct horse battery staple" \
  http://127.0.0.1:3000/api/v1/auth/me
```

The authenticated account response has this shape:

```json
{
  "id": "7f2460b3-79f2-4f12-9f3d-b204f8da9a20",
  "username": "alice",
  "role": "user"
}
```

Roles are:

```text
admin
user
```

Clients should store credentials in the platform credential store and should
prefer HTTPS when the server is exposed off-device or outside a trusted local
network. A `401` response includes `WWW-Authenticate: Basic realm="Harmonixia
API", charset="UTF-8"`.

## First-Run Bootstrap

An app that helps users connect to a new server can check bootstrap status
without authentication:

```text
GET /api/v1/bootstrap/status
```

Response:

```json
{
  "users_exist": false,
  "first_admin_required": true,
  "initial_scan_started": false
}
```

If `first_admin_required` is true, create the first admin account:

```text
POST /api/v1/bootstrap/first-admin
Content-Type: application/json

{
  "username": "admin",
  "password": "change-me"
}
```

This returns `201 Created` with a `UserAccount`. Once any local user exists,
this endpoint returns `409 Conflict`.

## Error Model

JSON API errors use this shape:

```json
{
  "code": "bad_request",
  "message": "limit must be between 1 and 200"
}
```

Some errors may include optional structured details:

```json
{
  "code": "conflict",
  "message": "target is reconnecting",
  "details": {
    "reason": "target_reconnecting"
  }
}
```

`details.reason` is currently reserved for Sonos remote playback contracts and
is omitted on existing non-Sonos API errors unless a handler explicitly provides
it.

Common codes are:

```text
unauthorized
forbidden
bad_request
not_found
conflict
service_unavailable
internal
```

Media range failures are an exception: invalid or unsatisfiable byte ranges
return `416 Range Not Satisfiable` with range headers and an empty body.

## Catalog Model

The catalog is split into music and podcasts.

Music entities:

```text
Artist -> Album -> Track
```

Podcast entities:

```text
Podcast -> Episode
```

Playable item types are:

```text
track
episode
```

Catalog browse and search endpoints only return published, stable, playable
content. Tracks and episodes must have a published canonical media file that is
not quarantined or marked duplicate.

### Core Response Fields

Artist:

```json
{
  "id": "artist-uuid",
  "name": "Artist Name",
  "normalized_name": "artist name",
  "sort_name": "Artist Name",
  "stable_grouping": true,
  "published_at": "2026-05-09T10:00:00Z",
  "created_at": "2026-05-09T10:00:00Z",
  "updated_at": "2026-05-09T10:00:00Z"
}
```

Album:

```json
{
  "id": "album-uuid",
  "artist_id": "artist-uuid",
  "title": "Album Title",
  "normalized_title": "album title",
  "album_kind": "album",
  "release_year": 2026,
  "stable_grouping": true,
  "published_at": "2026-05-09T10:00:00Z",
  "created_at": "2026-05-09T10:00:00Z",
  "updated_at": "2026-05-09T10:00:00Z"
}
```

Track:

```json
{
  "id": "track-uuid",
  "album_id": "album-uuid",
  "artist_id": "artist-uuid",
  "title": "Track Title",
  "normalized_title": "track title",
  "disc_number": 1,
  "track_number": 4,
  "duration_seconds": 212,
  "stable_grouping": true,
  "published_at": "2026-05-09T10:00:00Z",
  "created_at": "2026-05-09T10:00:00Z",
  "updated_at": "2026-05-09T10:00:00Z"
}
```

Podcast:

```json
{
  "id": "podcast-uuid",
  "title": "Podcast Title",
  "normalized_title": "podcast title",
  "stable_grouping": true,
  "published_at": "2026-05-09T10:00:00Z",
  "created_at": "2026-05-09T10:00:00Z",
  "updated_at": "2026-05-09T10:00:00Z"
}
```

Episode:

```json
{
  "id": "episode-uuid",
  "podcast_id": "podcast-uuid",
  "title": "Episode Title",
  "normalized_title": "episode title",
  "season_number": 1,
  "episode_number": 12,
  "duration_seconds": 3600,
  "stable_grouping": true,
  "published_at": "2026-05-09T10:00:00Z",
  "created_at": "2026-05-09T10:00:00Z",
  "updated_at": "2026-05-09T10:00:00Z"
}
```

## Browsing Music

Browse endpoints are paginated with an opaque cursor.

Query parameters:

```text
limit   optional, default 50, maximum 200
cursor  optional, use page.next_cursor from the previous response
sort    optional, resource-specific
```

Artists:

```text
GET /api/v1/catalog/artists?limit=50&sort=name
```

Response:

```json
{
  "artists": [],
  "page": {
    "limit": 50,
    "next_cursor": null,
    "sort": "name"
  }
}
```

Albums:

```text
GET /api/v1/catalog/albums?limit=50&sort=artist_title
```

Tracks:

```text
GET /api/v1/catalog/tracks?limit=50&sort=album_position
```

Supported sorts:

```text
artists: name
albums: artist_title
tracks: album_position
```

For a full offline catalog cache, page through artists, albums, and tracks until
`page.next_cursor` is `null`, then join objects locally by `artist_id` and
`album_id`.

There are no individual `GET /artists/{id}`, `GET /albums/{id}`, or
`GET /tracks/{id}` routes yet. Use browse/search results as the source of truth
for those entities.

## Searching

Grouped search:

```text
GET /api/v1/catalog/search?q=radiohead&limit=10
```

Query parameters:

```text
q           required search text
limit       optional per result group, default 10, maximum 50
year        optional release year filter for catalog media
genre       optional normalized genre filter
format      optional container, MIME, or codec filter
media_type  optional: music or podcast
```

Response:

```json
{
  "query": "radiohead",
  "normalized_query": "radiohead",
  "limit": 10,
  "artists": [],
  "albums": [],
  "tracks": [],
  "podcasts": [],
  "episodes": [],
  "playlists": []
}
```

Search matching is normalized for case, diacritics, punctuation, separators,
and leading articles. The response is grouped by entity type, which is useful
for apps that show sections such as Artists, Albums, Songs, Podcasts, Episodes,
and Playlists.

## Playlists

Playlists can contain tracks and podcast episodes.

List visible playlists:

```text
GET /api/v1/playlists
```

Create a playlist:

```text
POST /api/v1/playlists
Content-Type: application/json

{
  "name": "Road Trip",
  "description": "Long drives",
  "scope": "personal"
}
```

Playlist scopes:

```text
personal  visible to the owner
shared    household-visible
```

Get, update, or delete one playlist:

```text
GET    /api/v1/playlists/{playlist_id}
PUT    /api/v1/playlists/{playlist_id}
DELETE /api/v1/playlists/{playlist_id}
```

Update body:

```json
{
  "name": "Road Trip",
  "description": "Updated description"
}
```

List items:

```text
GET /api/v1/playlists/{playlist_id}/items
```

Add an item:

```text
POST /api/v1/playlists/{playlist_id}/items
Content-Type: application/json

{
  "item_type": "track",
  "item_id": "track-uuid",
  "position": null
}
```

Omit `position` or set it to `null` to append. Use a zero-based `position` from
`0` through the current item count to insert.

Reorder items:

```text
PUT /api/v1/playlists/{playlist_id}/items
Content-Type: application/json

{
  "item_ids": [
    "playlist-item-uuid-1",
    "playlist-item-uuid-2"
  ]
}
```

The reorder array must contain every current playlist item ID exactly once.

Remove an item:

```text
DELETE /api/v1/playlists/{playlist_id}/items/{playlist_item_id}
```

Playlist items have this shape:

```json
{
  "id": "playlist-item-uuid",
  "playlist_id": "playlist-uuid",
  "item_type": "track",
  "item_id": "track-uuid",
  "position": 0,
  "added_by_account_id": "account-uuid",
  "created_at": "2026-05-09T10:00:00Z"
}
```

Clients should resolve each item by `item_type` plus `item_id` from their local
catalog cache. Use `track` for music playback and `episode` for podcast
playback.

## Streaming Originals

Original media streams are authenticated and support byte-range requests.

Inline playback:

```text
GET /api/v1/media/{item_type}/{item_id}/original
```

Download:

```text
GET /api/v1/media/{item_type}/{item_id}/original/download
```

`item_type` can be:

```text
track
episode
```

The media router also accepts plural aliases `tracks` and `episodes`, but new
clients should use the singular values.

Original media responses include:

```text
Accept-Ranges: bytes
Content-Length: ...
Content-Type: source file MIME type or application/octet-stream
Content-Disposition: inline; filename="..."
```

For seeking or buffering, send a standard range header:

```http
Range: bytes=0-1048575
```

Partial responses return:

```text
206 Partial Content
Content-Range: bytes start-end/total
```

Use original streaming when the client can play the source format directly and
when preserving source quality is preferred.

## Direct AAC Transcoding

Direct transcodes are useful for clients that want a simple streaming URL with
a predictable codec.

```text
GET /api/v1/media/{item_type}/{item_id}/transcode/{profile}
```

Profiles:

```text
mobile    AAC 64k
standard  AAC 128k
high      AAC 256k
```

The response is an ADTS AAC stream:

```text
Content-Type: audio/aac
Content-Disposition: inline; filename="source-standard.aac"
```

Direct transcode streams do not support byte ranges. If the user seeks, restart
the request or prefer HLS for better seek behavior.

Transcoding uses a hard server-side slot limit. If all slots are in use, the
server returns:

```text
503 Service Unavailable
```

with an error body such as:

```json
{
  "code": "service_unavailable",
  "message": "transcode capacity is exhausted; retry later or request original media"
}
```

Clients should treat this as a fast failure, not as a queued operation. Retry
with backoff, offer original playback, or pick a lower-bandwidth path if
appropriate.

Admins can inspect slots:

```text
GET /api/v1/admin/media/transcode-slots
```

Response:

```json
{
  "limit": 2,
  "in_use": 1,
  "available": 1
}
```

## HLS AAC Transcoding

HLS is the best fit for mobile and desktop apps that need robust buffering and
seeking while still using server-side AAC transcoding.

Manifest:

```text
GET /api/v1/media/{item_type}/{item_id}/hls/{profile}/manifest.m3u8
```

Compatibility alias:

```text
GET /api/v1/media/{item_type}/{item_id}/hls/{profile}/playlist.m3u8
```

Segments:

```text
GET /api/v1/media/{item_type}/{item_id}/hls/{profile}/segments/{segment}
```

The manifest response uses:

```text
Content-Type: application/vnd.apple.mpegurl
```

Segment responses use:

```text
Content-Type: video/mp2t
```

The manifest contains relative segment URLs, for example:

```text
segments/segment-00000.ts
```

Clients must send the same Basic auth credentials for segment requests. Some
platform HLS players do not automatically attach custom headers to segment
requests; use the platform networking hooks or a local authenticated proxy if
needed.

HLS generation uses the same transcode slot pool as direct AAC. Cold manifest
requests for the same item and profile share in-flight generation, but if no
slot is available the request returns `503` immediately. Once generated, the
server reuses the cached rendition for that media file and profile.

## Playback Progress and History

Use playback progress for resume state and history for recently played views.
Both are scoped to the authenticated account.

List all saved progress:

```text
GET /api/v1/me/playback/progress
```

Get one progress record:

```text
GET /api/v1/me/playback/progress/{item_type}/{item_id}
```

Write progress:

```text
PUT /api/v1/me/playback/progress/{item_type}/{item_id}
Content-Type: application/json

{
  "position_seconds": 73,
  "duration_seconds": 212,
  "completed": false
}
```

The write response includes both the upserted progress record and a playback
history event:

```json
{
  "progress": {
    "item_type": "track",
    "item_id": "track-uuid",
    "position_seconds": 73,
    "duration_seconds": 212,
    "completed": false,
    "updated_at": "2026-05-09T10:00:00Z"
  },
  "history_event": {
    "id": "history-event-uuid",
    "item_type": "track",
    "item_id": "track-uuid",
    "position_seconds": 73,
    "duration_seconds": 212,
    "completed": false,
    "played_at": "2026-05-09T10:00:00Z"
  }
}
```

`position_seconds` must not exceed `duration_seconds` when duration is supplied.
If `completed` is omitted, it defaults to `false`.

Record a history event without updating progress:

```text
POST /api/v1/me/playback/history
Content-Type: application/json

{
  "item_type": "track",
  "item_id": "track-uuid",
  "position_seconds": 212,
  "duration_seconds": 212,
  "completed": true
}
```

List recent history:

```text
GET /api/v1/me/playback/history?limit=50
```

The history limit is clamped to `1..=200`; the default is `50`.

Suggested client behavior:

```text
1. Write progress periodically, for example every 10 to 30 seconds.
2. Write progress when playback pauses, seeks, completes, or the app backgrounds.
3. Mark completed near the end of a track or episode according to client UX.
4. Refresh progress after login to support multi-device resume.
```

## Podcasts

Podcast browsing is separate from music browsing.

List podcast series:

```text
GET /api/v1/catalog/podcasts?limit=50&sort=title
```

Get one podcast series:

```text
GET /api/v1/catalog/podcasts/{podcast_id}
```

List all episodes:

```text
GET /api/v1/catalog/episodes?limit=50&sort=podcast_position
```

List episodes for one podcast:

```text
GET /api/v1/catalog/podcasts/{podcast_id}/episodes?limit=50&sort=podcast_position
```

Get one episode with series and resume state:

```text
GET /api/v1/catalog/episodes/{episode_id}
```

Response:

```json
{
  "podcast": {
    "id": "podcast-uuid",
    "title": "Podcast Title",
    "normalized_title": "podcast title",
    "stable_grouping": true,
    "published_at": "2026-05-09T10:00:00Z",
    "created_at": "2026-05-09T10:00:00Z",
    "updated_at": "2026-05-09T10:00:00Z"
  },
  "episode": {
    "id": "episode-uuid",
    "podcast_id": "podcast-uuid",
    "title": "Episode Title",
    "normalized_title": "episode title",
    "season_number": 1,
    "episode_number": 12,
    "duration_seconds": 3600,
    "stable_grouping": true,
    "published_at": "2026-05-09T10:00:00Z",
    "created_at": "2026-05-09T10:00:00Z",
    "updated_at": "2026-05-09T10:00:00Z"
  },
  "resume": null
}
```

Episode-specific resume helpers:

```text
GET /api/v1/catalog/episodes/{episode_id}/resume
PUT /api/v1/catalog/episodes/{episode_id}/resume
```

The `PUT` body is the same as a playback progress write:

```json
{
  "position_seconds": 1200,
  "duration_seconds": 3600,
  "completed": false
}
```

Podcast episodes use the same media endpoints as tracks with
`item_type=episode`:

```text
GET /api/v1/media/episode/{episode_id}/original
GET /api/v1/media/episode/{episode_id}/transcode/standard
GET /api/v1/media/episode/{episode_id}/hls/standard/manifest.m3u8
```

## Metadata and Images

The current client-facing catalog responses expose normalized core metadata:

```text
artist names and sort names
album titles, album kind, and release year
track titles, disc numbers, track numbers, and duration
podcast titles
episode titles, season numbers, episode numbers, and duration
timestamps and stable/published state
```

The import pipeline also stores provider links, metadata provenance, genres,
format keys, and artwork assets internally. Client APIs expose normalized
catalog metadata and artwork metadata/images; raw provider links and provenance
remain internal server state.

Practical guidance for app builders:

```text
1. Do not query the database directly from clients.
2. Do not assume artwork file paths or source URIs are public URLs.
3. Use the artwork metadata `url` field for image loads.
4. Use the OpenAPI document as the source of truth for fields currently exposed.
```

Artwork metadata lookup:

```text
GET /api/v1/catalog/{entity_type}/{entity_id}/artwork
GET /api/v1/catalog/{entity_type}/{entity_id}/artwork?kind=cover
```

Supported `entity_type` values are `artist`, `band`, `album`, `track`,
`podcast`, and `episode`. `band` is an alias for `artist`. The entity must be
published and visible through the public catalog. Supported `kind` values are
`cover`, `artist`, `fanart`, `thumbnail`, and `other`.

Response:

```json
{
  "artwork": [
    {
      "id": "artwork-uuid",
      "entity_type": "album",
      "entity_id": "album-uuid",
      "artwork_kind": "cover",
      "mime_type": "image/jpeg",
      "width": 1200,
      "height": 1200,
      "confidence": 0.98,
      "url": "/api/v1/artwork/artwork-uuid"
    }
  ]
}
```

The response never exposes server file paths or provider source URIs. If a
visible entity has no local artwork, the server returns an empty `artwork`
array.

Artwork image delivery:

```text
GET /api/v1/artwork/{artwork_asset_id}
GET /api/v1/artwork/{artwork_asset_id}?width=300
GET /api/v1/artwork/{artwork_asset_id}?height=300
GET /api/v1/artwork/{artwork_asset_id}?width=300&height=300
```

Without `width` or `height`, the server streams the original full-size image.
With one dimension, the missing dimension is derived from the source aspect
ratio. With both dimensions, the image is resized to fit within that box while
preserving aspect ratio. Each requested dimension must be between `1` and
`4096` pixels.

The image endpoint requires the same Basic authentication as the catalog
endpoints. The asset must still belong to a published visible catalog entity.

## Suggested App Data Flow

For a streaming-app style client:

```text
1. GET /api/v1/auth/me to validate credentials and identify the account.
2. Page through /catalog/artists, /catalog/albums, and /catalog/tracks.
3. Page through /catalog/podcasts and /catalog/episodes if podcast UI is enabled.
4. GET /api/v1/playlists and then /playlists/{id}/items for each visible playlist.
5. GET /api/v1/me/playback/progress to hydrate resume state.
6. Use /catalog/search for interactive search rather than local-only matching.
7. For playback, prefer original media when supported, HLS for mobile/seeking,
   and direct AAC for simple transcoded playback.
8. Write playback progress during and after playback.
```

For cache invalidation, use the `updated_at` fields on returned objects and
periodically repage the catalog. There is not yet a delta-sync or event stream
endpoint.

## Admin-Only Endpoints Useful During Setup

Normal end-user media apps do not need admin endpoints, but setup tools may use
them after authenticating as an admin.

System config:

```text
GET /api/v1/admin/system/config
PUT /api/v1/admin/system/config
```

The system config object includes `public_base_url`, which is returned as
`null` when unset. On `PUT`, `library_root` and `dropbox_root` remain required;
optional fields such as `podcast_subtree`, `public_base_url`,
`transcode_concurrency_limit`, and `scan_thread_count` preserve their existing
stored value when omitted.

Provider settings:

```text
GET   /api/v1/admin/providers/settings
PATCH /api/v1/admin/providers/{provider}/settings
```

Transcode slot usage:

```text
GET /api/v1/admin/media/transcode-slots
```

Admin endpoints return `403 Forbidden` for non-admin accounts.

## Current Client-Facing Gaps

These are important for Spotify/Tidal-like clients:

```text
Artist, album, and track detail-by-ID endpoints are not available yet.
Track responses do not include album or artist display objects inline.
Media-file technical metadata is not exposed through a public client endpoint.
Raw provider metadata and matching provenance are not exposed through public
client endpoints.
There is no token-based auth, device authorization, or OAuth-like flow.
There is no delta sync, websocket event stream, favorites, ratings, follows,
or library collection endpoint yet.
```

Design clients so these can be added as capabilities without breaking the
existing browse, search, playlist, playback, and media URL flows.
