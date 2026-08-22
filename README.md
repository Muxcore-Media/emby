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
| `EMBY_SESSIONS_POLL_SEC` | `30` | Poll interval for `/Sessions` (fallback when WebSocket disconnected) |
| `EMBY_WEBSOCKET` | on | Connect outbound WebSocket to `/embywebsocket` for session push (`SessionsStart`) |
| `EMBY_SSE_SECRET` | — | Optional bearer/secret for `POST /emby/sse/events` |
| `EMBY_GRPC_ADDR` | `:9477` | gRPC listen address |
| `EMBY_HTTP_ADDR` | `:8477` | HTTP (health + SSE ingest) |
| `MUXCORE_GRPC_ADDR` | — | Core mesh for event publish |

## HTTP

- `GET /healthz` — liveness
- `POST /emby/sse/events` — Tracearr-compatible session push ingest (optional; poll is primary)

## gRPC

- `Status` — configured flag + active session count
- `TerminateSession` — `POST /Sessions/{id}/Playing/Stop` on Emby

## Status

v0.1.0 — Session poll (fallback), outbound WebSocket to Emby `/embywebsocket`, inbound SSE ingest (`POST /emby/sse/events`), library catalog sync, terminate RPC.
