# Harmonixia Server

Greenfield Rust implementation of Harmonixia, a self-hosted music and podcast server.

This foundation slice implements the Postgres-backed core API surface:

- First-admin bootstrap when no local users exist.
- Local Basic-auth accounts with admin-only user creation, password reset, and deletion.
- Full library rescans and selected path/subtree rescans.
- Provider health, retry/backoff visibility, and provider repair jobs.
- Postgres-backed system configuration and provider settings seeded from
  startup defaults.
- Quarantine retry handoff back into the shared import pipeline.
- Personal and household-shared playlist CRUD foundations.
- User-scoped playback progress and history persistence.
- Podcast-specific series, episode, and episode resume read APIs.
- Authenticated original media delivery, direct AAC transcode endpoints, and
  HLS manifests/segments with hard transcode slot admission control.
- Swagger UI and OpenAPI docs at `/api/v1/api-docs`.
- OpenAPI JSON at `GET /api/v1/api-docs/openapi.json` and legacy
  `GET /openapi.json`.

Canonical admin endpoints live under `/api/v1/admin`. The router also serves
`/api/admin` as an early compatibility alias.

Run locally:

```sh
HARMONIXIA_DATABASE_URL=postgres://user:password@localhost/harmonixia cargo run
```

Startup requires Postgres. The server connects before binding the HTTP listener,
applies embedded migrations from `migrations/`, and verifies the tables used by
system configuration, provider settings, accounts, import jobs, provider health,
quarantine retry state, playlists, and playback progress/history.

Environment variables:

- `HARMONIXIA_DATABASE_URL` or `DATABASE_URL`, required Postgres connection URL
- `HARMONIXIA_DATABASE_MAX_CONNECTIONS`, default `5`
- `HARMONIXIA_DATABASE_CONNECT_TIMEOUT_SECONDS`, default `5`
- `HARMONIXIA_DATABASE_SCHEMA`, optional schema for migrations and runtime state
- `HARMONIXIA_BIND_ADDR`, default `127.0.0.1:3000`
- `HARMONIXIA_LIBRARY_ROOT`, bootstrap default `/srv/harmonixia/library`
- `HARMONIXIA_DROPBOX_ROOT`, bootstrap default `/srv/harmonixia/dropbox`
- `HARMONIXIA_PUBLIC_BASE_URL`, optional bootstrap-only absolute `http` or
  `https` URL for LAN-reachable remote playback clients; localhost and loopback
  hosts are rejected because remote players cannot reach them
- `HARMONIXIA_FFMPEG_PATH`, default `ffmpeg`
- `HARMONIXIA_TRANSCODE_CONCURRENCY_LIMIT`, bootstrap default `2`; `0`
  disables new direct and HLS transcodes by saturating admission
- `HARMONIXIA_SCAN_THREAD_COUNT`, bootstrap default `8`; controls concurrent
  import scan workers
- `HARMONIXIA_PROVIDER_<PROVIDER>_ENABLED`, bootstrap default, for example `HARMONIXIA_PROVIDER_DISCOGS_ENABLED=false`
- `HARMONIXIA_PROVIDER_<PROVIDER>_API_KEY` or `_TOKEN`, bootstrap default for providers that require credentials

The library root, dropbox root, public base URL, transcode concurrency limit,
scan thread count, and provider settings are stored durably in Postgres on first
startup. Later environment changes do not override existing rows; use the admin
settings endpoints to update runtime configuration. The public base URL is
intended for LAN-reachable remote playback URLs and is not inferred from
incoming requests.

- `GET /api/v1/admin/system/config`
- `PUT /api/v1/admin/system/config`
- `GET /api/v1/admin/media/transcode-slots`
- `GET /api/v1/admin/providers/settings`
- `PATCH /api/v1/admin/providers/{provider}/settings`

Podcast series and episodes are browsable separately from music browse APIs:

- `GET /api/v1/catalog/podcasts`
- `GET /api/v1/catalog/podcasts/{podcast_id}`
- `GET /api/v1/catalog/podcasts/{podcast_id}/episodes`
- `GET /api/v1/catalog/episodes`
- `GET /api/v1/catalog/episodes/{episode_id}`
- `GET /api/v1/catalog/episodes/{episode_id}/resume`
- `PUT /api/v1/catalog/episodes/{episode_id}/resume`

Direct AAC transcodes are available at
`GET /api/v1/media/{track|episode}/{id}/transcode/{mobile|standard|high}`.
Requests require Basic auth. When all configured transcode slots are in use,
new transcode requests return `503` immediately; they are not queued and do not
fall back to original media.

Authenticated HLS output is available at
`GET /api/v1/media/{track|episode}/{id}/hls/{mobile|standard|high}/manifest.m3u8`.
The manifest references relative segment URLs under
`/api/v1/media/{track|episode}/{id}/hls/{profile}/segments/{segment}`. Manifest
generation uses the same hard transcode admission model, publishes once initial
playlist output is usable, and shares same-item/profile cold requests with the
in-flight rendition; segment fetches also require Basic auth.

Postgres-backed integration tests run when `HARMONIXIA_TEST_DATABASE_URL` or
`DATABASE_URL` is set. Each test uses a unique schema and runs the same migration
bootstrap path as server startup.
