package internal

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
)

// handleSSEEvent accepts Tracearr Media-Server-SSE compatible session push JSON.
func (m *Module) handleSSEEvent(w http.ResponseWriter, r *http.Request) {
	if !m.checkSSEAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	eventType, ev := parseSSESessionEvent(raw)
	if eventType == "" {
		if eventName := stringField(raw, "event", "Event", "eventName", "event_name"); eventName != "" {
			dataStr := stringField(raw, "data", "Data")
			if dataStr == "" {
				if dataRaw, ok := raw["data"]; ok {
					if b, err := json.Marshal(dataRaw); err == nil {
						dataStr = string(b)
					}
				}
			}
			if dataStr != "" {
				m.handlePluginLibrarySSE(r.Context(), eventName, dataStr)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"ok":true}`))
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"ignored":true}`))
		return
	}
	m.publishPlaybackEvent(r.Context(), eventType, ev)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (m *Module) checkSSEAuth(r *http.Request) bool {
	m.mu.RLock()
	secret := m.sseSecret
	m.mu.RUnlock()
	if secret == "" {
		return false
	}
	if r.Header.Get("X-Emby-SSE-Secret") == secret {
		return true
	}
	auth := r.Header.Get("Authorization")
	return strings.HasPrefix(auth, "Bearer ") && strings.TrimPrefix(auth, "Bearer ") == secret
}

func parseSSESessionEvent(raw map[string]any) (string, playbackEventPayload) {
	state := strings.ToLower(stringField(raw, "state", "State"))
	sessionID := stringField(raw, "sessionId", "session_id", "SessionId")
	itemID := stringField(raw, "itemId", "item_id", "ItemId")
	userID := stringField(raw, "userId", "user_id", "UserId")
	eventType := ""
	switch state {
	case "playing", "started", "start":
		eventType = playbackevents.EventPlaybackStarted
	case "paused", "progress":
		eventType = playbackevents.EventPlaybackProgress
	case "stopped", "stop", "idle":
		eventType = playbackevents.EventPlaybackStopped
	default:
		if v, ok := raw["event"].(string); ok {
			switch strings.ToLower(v) {
			case playbackevents.EventPlaybackStarted, "session.started":
				eventType = playbackevents.EventPlaybackStarted
			case playbackevents.EventPlaybackProgress, "session.progress":
				eventType = playbackevents.EventPlaybackProgress
			case playbackevents.EventPlaybackStopped, "session.stopped":
				eventType = playbackevents.EventPlaybackStopped
			}
		}
	}
	if eventType == "" {
		return "", playbackEventPayload{}
	}
	posTicks := int64Field(raw, "positionTicks", "position_ticks", "PositionTicks")
	durTicks := int64Field(raw, "runTimeTicks", "run_time_ticks", "RunTimeTicks", "durationTicks", "DurationTicks")
	ev := playbackEventPayload{
		ItemID:          itemID,
		EmbyItemID:      itemID,
		UserID:          userID,
		UserName:        stringField(raw, "userName", "user_name", "UserName"),
		SessionID:       sessionID,
		Title:           stringField(raw, "title", "Title", "name", "Name"),
		MediaType:       stringField(raw, "mediaType", "media_type", "type", "Type", "itemType"),
		MediaPath:       stringField(raw, "mediaPath", "media_path", "path", "Path"),
		PositionSeconds: ticksToSeconds(posTicks),
		DurationSeconds: ticksToSeconds(durTicks),
		ServerType:      "emby",
	}
	return eventType, ev
}

func stringField(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func int64Field(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case float64:
				return int64(t)
			case int64:
				return t
			case json.Number:
				n, _ := t.Int64()
				return n
			}
		}
	}
	return 0
}
