# Emby Playback Bridge

Emby Media Server bridge for MuxCore playback monitoring (Tracearr parity Phase 2).

## Capabilities

- `playback.emby` — Emby session source
- `playback` — publishes `playback.started` / `playback.progress` / `playback.stopped` mesh events
- `settings`

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `EMBY_URL` | — | Emby server base URL (e.g. `http://emby:8096`) |
| `EMBY_TOKEN` | — | API token (`X-Emby-Token`) |
| `EMBY_DATA_DIR` | `/var/lib/muxcore-emby` | Durable settings store (`settings.json`) |
| `EMBY_SESSIONS_POLL_SEC` | `30` | Poll interval for `/Sessions` (fallback when WebSocket disconnected) |
| `EMBY_WEBSOCKET` | on | Connect outbound WebSocket to `/embywebsocket` for session push (`SessionsStart`) |
| `EMBY_CATALOG_SYNC_SEC` | `21600` | Full library catalog sync interval (6 hours) |
| `EMBY_SSE_SECRET` | — | **Required** for `POST /emby/sse/events` when ingest is enabled |
| `EMBY_GRPC_ADDR` | `:9477` | gRPC listen address |
| `EMBY_HTTP_ADDR` | `:8477` | HTTP (health + SSE ingest) |
| `MUXCORE_GRPC_ADDR` | — | Core mesh for event publish |

See [`.env.example`](.env.example) for a copy-paste template.

## HTTP

- `GET /healthz` — readiness (503 when unconfigured or Emby unreachable)
- `POST /emby/sse/events` — Tracearr-compatible session push ingest

### SSE auth

When `EMBY_SSE_SECRET` (or admin setting `emby_sse_secret`) is set, every ingest request must include either:

- header `X-Emby-SSE-Secret: <secret>`, or
- header `Authorization: Bearer <secret>`

When the secret is **empty**, ingest is **denied** (401). HTTP listens on `:8477` on all interfaces — configure a secret before exposing the port.

## gRPC

- `Status` — configured flag + active session count
- `TerminateSession` — `POST /Sessions/{id}/Playing/Stop` on Emby

## Status

v0.1.0 — Session poll + WebSocket, SSE ingest with auth, per-library catalog sync with provider IDs, durable settings, mesh reconnect.
